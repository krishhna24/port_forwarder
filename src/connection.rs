//! Per-connection lifecycle: upstream connect, bidirectional forwarding,
//! half-close propagation and teardown.
//!
//! # Teardown policy
//!
//! Each connection is served by two threads, one per direction. They share
//! the two sockets (via `try_clone`, which duplicates the file descriptor but
//! not the underlying socket), so a `shutdown` issued by either thread is
//! observed by both.
//!
//! * **EOF** on a direction's source is a *half-close*: only the write side
//!   of the destination is shut down (`shutdown(Write)`), forwarding the FIN.
//!   The opposite direction keeps running, so a client that has finished
//!   sending still receives the full response.
//! * **A hard error** (reset, broken pipe, ...), an **idle timeout** or a
//!   **forced shutdown** means the connection must end now. The thread that
//!   notices shuts down *both* sockets in *both* directions, which wakes the
//!   other thread out of a blocked `read` (it sees EOF) or `write` (it sees an
//!   error) so it exits promptly instead of waiting for a peer that will never
//!   speak again.
//!
//! # Deadlines without async I/O
//!
//! Blocking calls cannot be cancelled directly, so every socket gets a short
//! read/write timeout (the *tick*). A timed-out call is not an error: the loop
//! checks the connection-wide deadlines (idle timeout, forced shutdown) and
//! retries. Idle time is measured across *both* directions, so a connection
//! that is quiet one way but streaming the other is never considered idle.

use std::fmt;
use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use tracing::{debug, info, warn};

use crate::config::{self, Config};
use crate::error::{Error, Result};
use crate::forwarder::{Permit, ShutdownSignal};

/// Upper bound on how long a blocked call waits before re-checking deadlines.
/// This bounds how quickly a forced shutdown takes effect.
const MAX_TICK: Duration = Duration::from_millis(250);
/// Lower bound on the tick, so tiny idle timeouts do not cause a busy loop.
const MIN_TICK: Duration = Duration::from_millis(10);

/// One accepted client and everything needed to serve it.
#[derive(Debug)]
pub(crate) struct Connection {
    pub(crate) client: TcpStream,
    pub(crate) config: Arc<Config>,
    pub(crate) signal: Arc<ShutdownSignal>,
    /// Connection slot; released when the connection finishes.
    pub(crate) permit: Permit,
}

impl Connection {
    /// Serves the connection to completion. I/O failures never panic; every
    /// failure is logged and both sockets are closed on return.
    pub(crate) fn run(self) {
        let Self {
            client,
            config,
            signal,
            permit,
        } = self;

        let upstream = match connect_upstream(&config) {
            Ok(stream) => stream,
            Err(err) => {
                // Dropping would close it too, but shutting down first makes
                // the intent explicit and sends FIN even if a clone leaked.
                let _ = client.shutdown(Shutdown::Both);
                let active = permit.remaining();
                warn!(error = %err, active, "upstream connection failed; closed client");
                return;
            }
        };
        let upstream_addr = upstream.peer_addr().ok();
        info!(upstream = ?upstream_addr, "upstream connected");

        // The connect may have taken up to connect-timeout; do not start
        // forwarding if a forced shutdown arrived meanwhile.
        if signal.is_forced() {
            let active = permit.remaining();
            info!(
                active,
                "shutdown in progress; closed connection before forwarding"
            );
            return;
        }

        let result = forward(&client, &upstream, &config, &signal);
        drop((client, upstream));
        // Log before `permit` drops at the end of this scope: once the slot
        // is free, shutdown may complete and the process exit.
        let active = permit.remaining();
        match result {
            Ok((up, down)) => info!(
                bytes_client_to_upstream = up.bytes,
                bytes_upstream_to_client = down.bytes,
                client_to_upstream = %up.end,
                upstream_to_client = %down.end,
                active,
                "connection closed"
            ),
            Err(err) => warn!(error = %err, active, "failed to start forwarding"),
        }
    }
}

/// Which way bytes flow through a copy loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Direction {
    ClientToUpstream,
    UpstreamToClient,
}

impl fmt::Display for Direction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::ClientToUpstream => "client->upstream",
            Self::UpstreamToClient => "upstream->client",
        })
    }
}

/// Why a copy loop stopped.
#[derive(Debug)]
pub(crate) enum End {
    /// The source sent FIN; it was propagated as a half-close.
    Eof,
    /// The other direction tore the connection down.
    TornDown,
    /// No bytes moved in either direction for the idle timeout.
    IdleTimeout,
    /// The forwarder is shutting down and the grace period expired.
    Shutdown,
    /// Reading from the source failed.
    ReadError(io::Error),
    /// Writing to the destination failed.
    WriteError(io::Error),
    /// The thread running this direction panicked.
    Panicked,
}

impl End {
    /// Whether this end requires tearing down the whole connection, as
    /// opposed to a clean half-close or a reaction to the other side's
    /// teardown.
    fn requires_teardown(&self) -> bool {
        !matches!(self, Self::Eof | Self::TornDown)
    }
}

impl fmt::Display for End {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Eof => f.write_str("eof"),
            Self::TornDown => f.write_str("torn down"),
            Self::IdleTimeout => f.write_str("idle timeout"),
            Self::Shutdown => f.write_str("shutdown"),
            Self::ReadError(e) => write!(f, "read error: {e}"),
            Self::WriteError(e) => write!(f, "write error: {e}"),
            Self::Panicked => f.write_str("panicked"),
        }
    }
}

/// Result of one direction.
#[derive(Debug)]
pub(crate) struct DirectionStats {
    pub(crate) bytes: u64,
    pub(crate) end: End,
}

/// State shared by the two directions of one connection.
#[derive(Debug)]
struct Pair<'a> {
    client: &'a TcpStream,
    upstream: &'a TcpStream,
    torn_down: AtomicBool,
    started: Instant,
    /// Milliseconds since `started` at which bytes last moved.
    last_activity_ms: AtomicU64,
    idle_timeout: Option<Duration>,
    signal: &'a ShutdownSignal,
}

impl Pair<'_> {
    /// Shuts down both sockets in both directions, unblocking the peer thread.
    fn tear_down(&self) {
        self.torn_down.store(true, Ordering::Release);
        let _ = self.client.shutdown(Shutdown::Both);
        let _ = self.upstream.shutdown(Shutdown::Both);
    }

    fn is_torn_down(&self) -> bool {
        self.torn_down.load(Ordering::Acquire)
    }

    fn elapsed_ms(&self) -> u64 {
        u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    /// Records that bytes moved in some direction.
    fn touch(&self) {
        self.last_activity_ms
            .store(self.elapsed_ms(), Ordering::Relaxed);
    }

    /// Called whenever a blocking call times out: returns why the connection
    /// must end now, or `None` to keep waiting.
    fn deadline_reached(&self) -> Option<End> {
        if self.is_torn_down() {
            return Some(End::TornDown);
        }
        if self.signal.is_forced() {
            return Some(End::Shutdown);
        }
        let idle = self.idle_timeout?;
        let idle_for = self
            .elapsed_ms()
            .saturating_sub(self.last_activity_ms.load(Ordering::Relaxed));
        (u128::from(idle_for) >= idle.as_millis()).then_some(End::IdleTimeout)
    }
}

/// Tears the connection down if the owning thread unwinds, so a panic in one
/// direction cannot leave the other blocked forever (`thread::scope` waits for
/// all of its threads before propagating a panic).
struct TearDownOnPanic<'p, 'a>(&'p Pair<'a>);

impl Drop for TearDownOnPanic<'_, '_> {
    fn drop(&mut self) {
        if thread::panicking() {
            self.0.tear_down();
        }
    }
}

/// How long blocking calls wait before re-checking deadlines.
fn tick_for(idle_timeout: Option<Duration>) -> Duration {
    idle_timeout.map_or(MAX_TICK, |idle| (idle / 4).clamp(MIN_TICK, MAX_TICK))
}

/// Relays bytes in both directions until both have finished.
///
/// The upstream->client direction runs on a scoped helper thread while the
/// calling thread handles client->upstream; the two run fully concurrently so
/// a response can flow while the request is still being sent.
fn forward(
    client: &TcpStream,
    upstream: &TcpStream,
    config: &Config,
    signal: &ShutdownSignal,
) -> io::Result<(DirectionStats, DirectionStats)> {
    // Socket options live on the socket, not the descriptor, so these also
    // apply to the clones created below.
    let tick = Some(tick_for(config.idle_timeout));
    for stream in [client, upstream] {
        stream.set_read_timeout(tick)?;
        stream.set_write_timeout(tick)?;
    }
    // Independent handles for reading so each thread owns its own `&mut` I/O.
    let client_read = client.try_clone()?;
    let upstream_read = upstream.try_clone()?;
    let pair = Pair {
        client,
        upstream,
        torn_down: AtomicBool::new(false),
        started: Instant::now(),
        last_activity_ms: AtomicU64::new(0),
        idle_timeout: config.idle_timeout,
        signal,
    };
    let buffer_size = config.buffer_size;

    // Scoped threads do not inherit the tracing span; carry it over so both
    // directions' log lines carry the connection id.
    let span = tracing::Span::current();
    let name = format!("{}-down", thread::current().name().unwrap_or("conn"));
    thread::scope(|scope| {
        let down = thread::Builder::new().name(name).spawn_scoped(scope, || {
            span.in_scope(|| {
                let _guard = TearDownOnPanic(&pair);
                copy(
                    Direction::UpstreamToClient,
                    &upstream_read,
                    client,
                    &pair,
                    buffer_size,
                )
            })
        })?;
        let up = {
            let _guard = TearDownOnPanic(&pair);
            copy(
                Direction::ClientToUpstream,
                &client_read,
                upstream,
                &pair,
                buffer_size,
            )
        };
        let down = down.join().unwrap_or_else(|_| {
            // The guard already tore the connection down while unwinding.
            warn!(dir = %Direction::UpstreamToClient, "forwarding thread panicked");
            DirectionStats {
                bytes: 0,
                end: End::Panicked,
            }
        });
        Ok((up, down))
    })
}

/// Copies `src` into `dst` until EOF, error or deadline, applying the
/// teardown policy described in the module documentation.
fn copy(
    dir: Direction,
    mut src: &TcpStream,
    mut dst: &TcpStream,
    pair: &Pair<'_>,
    buffer_size: usize,
) -> DirectionStats {
    let mut buf = vec![0u8; buffer_size];
    let mut bytes: u64 = 0;
    let end = 'copy: loop {
        let n = match src.read(&mut buf) {
            Ok(0) => {
                if pair.is_torn_down() {
                    break End::TornDown;
                }
                // Half-close: forward the FIN but keep the reverse direction.
                if let Err(err) = dst.shutdown(Shutdown::Write) {
                    if err.kind() != io::ErrorKind::NotConnected {
                        debug!(%dir, error = %err, "failed to propagate half-close");
                    }
                }
                break End::Eof;
            }
            Ok(n) => n,
            Err(err) if is_timeout(&err) => match pair.deadline_reached() {
                Some(end) => break end,
                None => continue,
            },
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            Err(err) => break End::ReadError(err),
        };
        pair.touch();

        // Hand-rolled `write_all`: a timed-out write is retried after the
        // deadline check, and `bytes` counts exactly what was delivered. A
        // destination that stops reading (a stalled client) makes no
        // progress, so it eventually trips the idle timeout.
        let mut written = 0;
        while written < n {
            match dst.write(&buf[written..n]) {
                Ok(0) => break 'copy End::WriteError(io::ErrorKind::WriteZero.into()),
                Ok(m) => {
                    written += m;
                    bytes += m as u64;
                    pair.touch();
                }
                Err(err) if is_timeout(&err) => {
                    if let Some(end) = pair.deadline_reached() {
                        break 'copy end;
                    }
                }
                Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
                Err(err) => break 'copy End::WriteError(err),
            }
        }
    };

    if !end.requires_teardown() {
        debug!(%dir, bytes, reason = %end, "direction finished");
        return DirectionStats { bytes, end };
    }
    if pair.is_torn_down() {
        // The other direction already tore down; our error is a symptom.
        debug!(%dir, bytes, cause = %end, "direction finished after teardown");
        return DirectionStats {
            bytes,
            end: End::TornDown,
        };
    }
    if is_peer_reset(&end) {
        debug!(%dir, reason = %end, "peer reset; tearing down connection");
    } else {
        info!(%dir, reason = %end, "tearing down connection");
    }
    pair.tear_down();
    DirectionStats { bytes, end }
}

/// Timed-out blocking calls report `WouldBlock` on Unix and `TimedOut` on
/// Windows.
fn is_timeout(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
    )
}

/// Resets and broken pipes are routine on the internet; log them quietly.
fn is_peer_reset(end: &End) -> bool {
    match end {
        End::ReadError(e) | End::WriteError(e) => matches!(
            e.kind(),
            io::ErrorKind::ConnectionReset
                | io::ErrorKind::ConnectionAborted
                | io::ErrorKind::BrokenPipe
        ),
        _ => false,
    }
}

/// Resolves the upstream (on every call, so DNS changes are honoured) and
/// tries each address in turn with the configured connect timeout.
///
/// Both sockets of a connection get `TCP_NODELAY`: the forwarder writes
/// whatever it has just read, so Nagle's algorithm would only add latency on
/// top of whatever batching the original sender already chose.
pub(crate) fn connect_upstream(config: &Config) -> Result<TcpStream> {
    let addrs = config::resolve("upstream", &config.upstream)?;
    let mut last_err = None;
    for addr in addrs {
        match TcpStream::connect_timeout(&addr, config.connect_timeout) {
            Ok(stream) => {
                if let Err(err) = stream.set_nodelay(true) {
                    debug!(error = %err, "failed to set TCP_NODELAY on upstream");
                }
                return Ok(stream);
            }
            Err(err) => {
                debug!(%addr, error = %err, "upstream address failed");
                last_err = Some(err);
            }
        }
    }
    Err(Error::Connect {
        addr: config.upstream.clone(),
        source: last_err.unwrap_or_else(|| io::Error::other("no upstream address")),
    })
}

/// Address to connect to in order to reach a listener bound to `addr`:
/// unspecified addresses (`0.0.0.0`, `::`) are replaced by loopback.
pub(crate) fn connectable(addr: SocketAddr) -> SocketAddr {
    let mut addr = addr;
    if addr.ip().is_unspecified() {
        addr.set_ip(match addr {
            SocketAddr::V4(_) => std::net::Ipv4Addr::LOCALHOST.into(),
            SocketAddr::V6(_) => std::net::Ipv6Addr::LOCALHOST.into(),
        });
    }
    addr
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tick_is_clamped() {
        assert_eq!(tick_for(None), MAX_TICK);
        assert_eq!(tick_for(Some(Duration::from_secs(300))), MAX_TICK);
        assert_eq!(
            tick_for(Some(Duration::from_millis(400))),
            Duration::from_millis(100)
        );
        assert_eq!(tick_for(Some(Duration::from_millis(1))), MIN_TICK);
    }

    #[test]
    fn unspecified_addresses_map_to_loopback() {
        let v4: SocketAddr = "0.0.0.0:8080".parse().unwrap();
        assert_eq!(connectable(v4), "127.0.0.1:8080".parse().unwrap());
        let v6: SocketAddr = "[::]:8080".parse().unwrap();
        assert_eq!(connectable(v6), "[::1]:8080".parse().unwrap());
        let specific: SocketAddr = "192.0.2.1:80".parse().unwrap();
        assert_eq!(connectable(specific), specific);
    }

    #[test]
    fn only_abnormal_ends_tear_down() {
        assert!(!End::Eof.requires_teardown());
        assert!(!End::TornDown.requires_teardown());
        assert!(End::IdleTimeout.requires_teardown());
        assert!(End::Shutdown.requires_teardown());
        assert!(End::ReadError(io::ErrorKind::ConnectionReset.into()).requires_teardown());
    }
}

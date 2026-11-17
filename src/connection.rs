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
//! * **A hard error** (reset, broken pipe, ...) on either direction means the
//!   connection is unusable. The failing thread shuts down *both* sockets in
//!   *both* directions, which wakes the other thread out of a blocked `read`
//!   (it sees EOF) or `write` (it sees an error) so it can exit promptly
//!   instead of waiting for a peer that will never speak again.

use std::fmt;
use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use tracing::{debug, info, warn};

use crate::config::{self, Config};
use crate::error::{Error, Result};

/// One accepted client and everything needed to serve it.
#[derive(Debug)]
pub(crate) struct Connection {
    pub(crate) client: TcpStream,
    pub(crate) config: Arc<Config>,
}

impl Connection {
    /// Serves the connection to completion. Never panics on I/O errors; every
    /// failure is logged and both sockets are closed on return.
    pub(crate) fn run(self) {
        let upstream = match connect_upstream(&self.config) {
            Ok(stream) => stream,
            Err(err) => {
                warn!(error = %err, "upstream connection failed; closing client");
                // Dropping would close it too, but shutting down first makes
                // the intent explicit and sends FIN even if a clone leaked.
                let _ = self.client.shutdown(Shutdown::Both);
                return;
            }
        };
        let upstream_addr = upstream.peer_addr().ok();
        info!(upstream = ?upstream_addr, "upstream connected");

        match forward(&self.client, &upstream, self.config.buffer_size) {
            Ok((up, down)) => info!(
                bytes_client_to_upstream = up.bytes,
                bytes_upstream_to_client = down.bytes,
                client_to_upstream = %up.end,
                upstream_to_client = %down.end,
                "connection closed"
            ),
            Err(err) => warn!(error = %err, "failed to start forwarding"),
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
    /// Reading from the source failed.
    ReadError(io::Error),
    /// Writing to the destination failed.
    WriteError(io::Error),
}

impl End {
    pub(crate) fn is_error(&self) -> bool {
        matches!(self, Self::ReadError(_) | Self::WriteError(_))
    }
}

impl fmt::Display for End {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Eof => f.write_str("eof"),
            Self::TornDown => f.write_str("torn down"),
            Self::ReadError(e) => write!(f, "read error: {e}"),
            Self::WriteError(e) => write!(f, "write error: {e}"),
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
}

/// Relays bytes in both directions until both have finished.
///
/// The upstream->client direction runs on a scoped helper thread while the
/// calling thread handles client->upstream; the two run fully concurrently so
/// a response can flow while the request is still being sent.
fn forward(
    client: &TcpStream,
    upstream: &TcpStream,
    buffer_size: usize,
) -> io::Result<(DirectionStats, DirectionStats)> {
    // Independent handles for reading so each thread owns its `&mut` I/O.
    let client_read = client.try_clone()?;
    let upstream_read = upstream.try_clone()?;
    let pair = Pair {
        client,
        upstream,
        torn_down: AtomicBool::new(false),
    };

    // Scoped threads do not inherit the tracing span; carry it over so both
    // directions' log lines carry the connection id.
    let span = tracing::Span::current();
    Ok(thread::scope(|scope| {
        let down = scope.spawn(|| {
            span.in_scope(|| {
                copy(
                    Direction::UpstreamToClient,
                    &upstream_read,
                    client,
                    &pair,
                    buffer_size,
                )
            })
        });
        let up = copy(
            Direction::ClientToUpstream,
            &client_read,
            upstream,
            &pair,
            buffer_size,
        );
        let down = down.join().unwrap_or_else(|_| {
            pair.tear_down();
            DirectionStats {
                bytes: 0,
                end: End::ReadError(io::Error::other("forwarding thread panicked")),
            }
        });
        (up, down)
    }))
}

/// Copies `src` into `dst` until EOF or error, applying the teardown policy
/// described in the module documentation.
fn copy(
    dir: Direction,
    mut src: &TcpStream,
    mut dst: &TcpStream,
    pair: &Pair<'_>,
    buffer_size: usize,
) -> DirectionStats {
    let mut buf = vec![0u8; buffer_size];
    let mut bytes: u64 = 0;
    let end = loop {
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
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            Err(err) => break End::ReadError(err),
        };
        if let Err(err) = dst.write_all(&buf[..n]) {
            break End::WriteError(err);
        }
        bytes += n as u64;
    };

    if end.is_error() {
        if pair.is_torn_down() {
            // The other direction already tore down; our error is a symptom.
            return DirectionStats {
                bytes,
                end: End::TornDown,
            };
        }
        debug!(%dir, reason = %end, "tearing down connection");
        pair.tear_down();
    } else {
        debug!(%dir, bytes, reason = %end, "direction finished");
    }
    DirectionStats { bytes, end }
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
        source: last_err.unwrap_or_else(|| std::io::Error::other("no upstream address")),
    })
}

//! Listener, accept loop, connection limiting and graceful shutdown.

use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::thread;
use std::time::Duration;

use tracing::{debug, error, info, info_span, warn};

use crate::config::{self, Config};
use crate::connection::{self, Connection};
use crate::error::{Error, Result};

/// Pause after an accept error that is likely to repeat immediately (for
/// example running out of file descriptors), so the loop does not spin.
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(100);
/// After forcing connections closed, how long to wait for their threads to
/// notice (they re-check every tick, at most 250ms) before giving up.
const FORCE_GRACE: Duration = Duration::from_secs(2);
/// Timeout for the loopback connection used to wake the blocking `accept`.
const WAKE_TIMEOUT: Duration = Duration::from_secs(1);

/// A bound TCP forwarder. Create it with [`Forwarder::bind`] and drive it
/// with [`Forwarder::run`]; stop it through a [`ShutdownHandle`].
#[derive(Debug)]
pub struct Forwarder {
    listener: TcpListener,
    local_addr: SocketAddr,
    config: Arc<Config>,
    limiter: Arc<ConnectionLimiter>,
    signal: Arc<ShutdownSignal>,
    next_id: AtomicU64,
}

impl Forwarder {
    /// Validates `config`, binds the listening socket and checks that the
    /// upstream address resolves, so that typos fail at startup instead of on
    /// the first client.
    ///
    /// # Errors
    ///
    /// Returns an error if the configuration is invalid, an address cannot be
    /// resolved, or no resolved listen address can be bound.
    pub fn bind(config: Config) -> Result<Self> {
        config.validate()?;
        let upstream = config::resolve("upstream", &config.upstream)?;
        debug!(upstream = ?upstream, "upstream address resolved");

        let mut last_err = None;
        for addr in config::resolve("listen", &config.listen)? {
            match TcpListener::bind(addr) {
                Ok(listener) => {
                    let local_addr = listener.local_addr()?;
                    return Ok(Self {
                        listener,
                        local_addr,
                        limiter: Arc::new(ConnectionLimiter::new(config.max_connections)),
                        signal: Arc::new(ShutdownSignal::default()),
                        config: Arc::new(config),
                        next_id: AtomicU64::new(1),
                    });
                }
                Err(err) => last_err = Some(err),
            }
        }
        Err(Error::Bind {
            addr: config.listen,
            source: last_err.unwrap_or_else(|| io::Error::other("no address to bind")),
        })
    }

    /// The address the listener is actually bound to (useful with port `0`).
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// The configuration this forwarder runs with.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// A cloneable handle that can stop the forwarder from another thread or
    /// a signal handler.
    pub fn shutdown_handle(&self) -> ShutdownHandle {
        ShutdownHandle {
            signal: Arc::clone(&self.signal),
            limiter: Arc::clone(&self.limiter),
            wake_addr: connection::connectable(self.local_addr),
        }
    }

    /// Accepts and forwards connections until [`ShutdownHandle::shutdown`] is
    /// called, then drains in-flight connections (see [`ShutdownHandle`]) and
    /// returns.
    ///
    /// # Errors
    ///
    /// Currently never returns an error: accept failures are logged and
    /// retried so a single bad connection cannot stop the listener.
    pub fn run(self) -> Result<()> {
        info!(
            listen = %self.local_addr,
            upstream = %self.config.upstream,
            max_connections = self.config.max_connections,
            "forwarder listening"
        );
        self.accept_loop();

        // Close the listening socket first so new clients are refused by the
        // kernel instead of queueing in a backlog nobody will drain.
        let Self {
            listener,
            config,
            limiter,
            signal,
            ..
        } = self;
        drop(listener);
        drain(&config, &limiter, &signal);
        Ok(())
    }

    fn accept_loop(&self) {
        while !self.signal.is_draining() {
            match self.listener.accept() {
                Ok((stream, peer)) => {
                    if self.signal.is_draining() {
                        // The wake-up connection, or a client that raced it.
                        drop(stream);
                        break;
                    }
                    self.dispatch(stream, peer);
                }
                Err(err) if self.signal.is_draining() => {
                    debug!(error = %err, "accept failed during shutdown");
                    break;
                }
                Err(err) => handle_accept_error(&err),
            }
        }
    }

    /// Hands an accepted client to its own thread, or rejects it when the
    /// connection limit is reached.
    fn dispatch(&self, client: TcpStream, client_addr: SocketAddr) {
        // Rejecting (accept + close) rather than leaving clients queued in
        // the kernel backlog gives them an immediate, unambiguous EOF.
        let Some(permit) = self.limiter.try_acquire() else {
            warn!(
                client = %client_addr,
                max_connections = self.limiter.max(),
                "connection limit reached; rejecting client"
            );
            drop(client);
            return;
        };

        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let span = info_span!("conn", id, client = %client_addr);
        span.in_scope(|| info!(active = self.limiter.active(), "accepted connection"));

        // Some platforms let accepted sockets inherit the listener's flags.
        if let Err(err) = client.set_nonblocking(false) {
            span.in_scope(|| warn!(error = %err, "cannot make client blocking; dropping"));
            return;
        }
        if let Err(err) = client.set_nodelay(true) {
            span.in_scope(|| debug!(error = %err, "failed to set TCP_NODELAY on client"));
        }

        let conn = Connection {
            client,
            config: Arc::clone(&self.config),
            signal: Arc::clone(&self.signal),
            permit,
        };
        let thread_span = span.clone();
        let spawned = thread::Builder::new()
            .name(format!("conn-{id}"))
            .spawn(move || {
                thread_span.in_scope(|| {
                    // The permit and sockets live inside `conn`, so they are
                    // released during unwinding; catching the panic only
                    // turns it into a structured log line.
                    if panic::catch_unwind(AssertUnwindSafe(|| conn.run())).is_err() {
                        error!("connection handler panicked; connection dropped");
                    }
                });
            });
        if let Err(err) = spawned {
            // The closure (and with it the client socket and permit) has been
            // dropped by `spawn`, so the client is closed and the slot freed.
            span.in_scope(|| error!(error = %err, "failed to spawn connection thread"));
        }
    }
}

/// Waits for in-flight connections after the listener has closed: first for
/// up to `shutdown_timeout` (or until forced), then forces the rest closed.
fn drain(config: &Config, limiter: &ConnectionLimiter, signal: &ShutdownSignal) {
    let active = limiter.active();
    if active > 0 {
        info!(
            active,
            timeout = ?config.shutdown_timeout,
            "shutting down: listener closed, waiting for active connections"
        );
    }
    if !limiter.wait_idle(config.shutdown_timeout, || signal.is_forced()) {
        warn!(
            active = limiter.active(),
            "forcing remaining connections closed"
        );
        signal.force.store(true, Ordering::Release);
        if !limiter.wait_idle(FORCE_GRACE, || false) {
            warn!(
                active = limiter.active(),
                "connections still open after forced close; exiting anyway"
            );
        }
    }
    info!("shutdown complete");
}

/// Logs an accept error and backs off when retrying immediately would spin.
fn handle_accept_error(err: &io::Error) {
    match err.kind() {
        // The client gave up before we accepted, or a signal interrupted the
        // call: nothing is wrong with the listener, retry right away.
        io::ErrorKind::ConnectionAborted
        | io::ErrorKind::ConnectionReset
        | io::ErrorKind::Interrupted => debug!(error = %err, "transient accept error"),
        _ => {
            // Typically EMFILE/ENFILE/ENOBUFS/ENOMEM: resource exhaustion that
            // will persist for a moment. Back off instead of busy-looping.
            error!(error = %err, "accept failed; backing off");
            thread::sleep(ACCEPT_ERROR_BACKOFF);
        }
    }
}

/// Process-wide shutdown state observed by the accept loop and connections.
#[derive(Debug, Default)]
pub(crate) struct ShutdownSignal {
    /// Stop accepting; let in-flight connections finish.
    draining: AtomicBool,
    /// Tear down in-flight connections at their next tick.
    force: AtomicBool,
}

impl ShutdownSignal {
    pub(crate) fn is_draining(&self) -> bool {
        self.draining.load(Ordering::Acquire)
    }

    pub(crate) fn is_forced(&self) -> bool {
        self.force.load(Ordering::Acquire)
    }
}

/// Stops a running [`Forwarder`]. Cheap to clone; safe to call from any
/// thread, including a signal-handling thread.
///
/// Shutdown is two-phase:
/// 1. [`shutdown`](Self::shutdown): stop accepting and close the listener;
///    in-flight connections keep forwarding for up to the configured
///    `shutdown_timeout`.
/// 2. [`force`](Self::force) (or the timeout expiring): every remaining
///    connection is torn down within one I/O tick (at most 250ms).
#[derive(Debug, Clone)]
pub struct ShutdownHandle {
    signal: Arc<ShutdownSignal>,
    limiter: Arc<ConnectionLimiter>,
    wake_addr: SocketAddr,
}

impl ShutdownHandle {
    /// Begins graceful shutdown. Idempotent.
    pub fn shutdown(&self) {
        if !self.signal.draining.swap(true, Ordering::AcqRel) {
            self.wake_accept();
        }
    }

    /// Skips the remaining grace period and closes all connections now.
    pub fn force(&self) {
        self.signal.force.store(true, Ordering::Release);
        self.limiter.notify();
        self.shutdown();
    }

    /// Whether [`shutdown`](Self::shutdown) has been requested.
    pub fn is_shutting_down(&self) -> bool {
        self.signal.is_draining()
    }

    /// Number of connections currently being forwarded.
    pub fn active_connections(&self) -> usize {
        self.limiter.active()
    }

    /// `accept` blocks and cannot be interrupted portably, so connect to
    /// ourselves; the accept loop sees the draining flag and exits.
    fn wake_accept(&self) {
        if let Err(err) = TcpStream::connect_timeout(&self.wake_addr, WAKE_TIMEOUT) {
            // The listener may already be gone (run() not started or done);
            // otherwise the next incoming client will wake it instead.
            debug!(error = %err, addr = %self.wake_addr, "could not wake accept loop");
        }
    }
}

/// Counting limiter bounding the number of simultaneous connections.
///
/// The count is held by RAII [`Permit`]s, so a slot is released no matter how
/// a connection ends, including when its thread panics and unwinds.
#[derive(Debug)]
pub(crate) struct ConnectionLimiter {
    active: AtomicUsize,
    max: usize,
    /// Paired with `idle` so shutdown can sleep until the count reaches 0.
    lock: Mutex<()>,
    idle: Condvar,
}

impl ConnectionLimiter {
    pub(crate) fn new(max: usize) -> Self {
        Self {
            active: AtomicUsize::new(0),
            max,
            lock: Mutex::new(()),
            idle: Condvar::new(),
        }
    }

    /// Takes a slot if one is free.
    pub(crate) fn try_acquire(self: &Arc<Self>) -> Option<Permit> {
        self.active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < self.max).then_some(n + 1)
            })
            .ok()
            .map(|_| Permit {
                limiter: Arc::clone(self),
            })
    }

    /// Number of connections currently holding a permit.
    pub(crate) fn active(&self) -> usize {
        self.active.load(Ordering::Acquire)
    }

    pub(crate) fn max(&self) -> usize {
        self.max
    }

    /// Blocks until no permits are held, `abort()` returns true, or
    /// `timeout` elapses. Returns whether the limiter is idle.
    pub(crate) fn wait_idle(&self, timeout: Duration, abort: impl Fn() -> bool) -> bool {
        let guard = self.lock.lock().unwrap_or_else(PoisonError::into_inner);
        let _guard = self
            .idle
            .wait_timeout_while(guard, timeout, |()| self.active() > 0 && !abort())
            .unwrap_or_else(PoisonError::into_inner);
        self.active() == 0
    }

    /// Wakes `wait_idle` callers so they re-evaluate their condition.
    fn notify(&self) {
        // Taking the lock orders this wake-up after a waiter's predicate
        // check, so the notification cannot be lost.
        let _guard = self.lock.lock().unwrap_or_else(PoisonError::into_inner);
        self.idle.notify_all();
    }
}

/// A held connection slot; dropping it frees the slot.
#[derive(Debug)]
pub(crate) struct Permit {
    limiter: Arc<ConnectionLimiter>,
}

impl Permit {
    /// Active connections once this permit is dropped. Used to log a final
    /// line *before* releasing the slot, so shutdown cannot complete (and the
    /// process exit) ahead of a connection's summary log.
    pub(crate) fn remaining(&self) -> usize {
        self.limiter.active().saturating_sub(1)
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.limiter.active.fetch_sub(1, Ordering::AcqRel);
        self.limiter.notify();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn limiter_bounds_and_releases_permits() {
        let limiter = Arc::new(ConnectionLimiter::new(2));
        let a = limiter.try_acquire().expect("first permit");
        let b = limiter.try_acquire().expect("second permit");
        assert!(limiter.try_acquire().is_none(), "limit must be enforced");
        assert_eq!(limiter.active(), 2);
        drop(a);
        assert_eq!(limiter.active(), 1);
        let _c = limiter.try_acquire().expect("slot freed by drop");
        drop(b);
        assert_eq!(limiter.active(), 1);
    }

    #[test]
    fn permit_is_released_when_holder_panics() {
        let limiter = Arc::new(ConnectionLimiter::new(1));
        let permit = limiter.try_acquire().unwrap();
        let result = thread::spawn(move || {
            let _held = permit;
            panic!("simulated connection handler panic");
        })
        .join();
        assert!(result.is_err());
        assert_eq!(limiter.active(), 0);
        assert!(limiter.try_acquire().is_some());
    }

    #[test]
    fn wait_idle_wakes_when_last_permit_drops() {
        let limiter = Arc::new(ConnectionLimiter::new(1));
        let permit = limiter.try_acquire().unwrap();
        let releaser = thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            drop(permit);
        });
        let start = Instant::now();
        assert!(limiter.wait_idle(Duration::from_secs(5), || false));
        assert!(start.elapsed() < Duration::from_secs(2));
        releaser.join().unwrap();
    }

    #[test]
    fn wait_idle_times_out_and_honours_abort() {
        let limiter = Arc::new(ConnectionLimiter::new(1));
        let _permit = limiter.try_acquire().unwrap();
        assert!(!limiter.wait_idle(Duration::from_millis(20), || false));
        assert!(!limiter.wait_idle(Duration::from_secs(5), || true));
    }
}

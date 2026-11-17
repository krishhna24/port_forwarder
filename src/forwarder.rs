//! Listener and accept loop.

use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::panic::{self, AssertUnwindSafe};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

use tracing::{debug, error, info, info_span, warn};

use crate::config::{self, Config};
use crate::connection::Connection;
use crate::error::{Error, Result};

/// Pause after an accept error that is likely to repeat immediately (for
/// example running out of file descriptors), so the loop does not spin.
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(100);

/// A bound TCP forwarder. Create it with [`Forwarder::bind`] and drive it
/// with [`Forwarder::run`].
#[derive(Debug)]
pub struct Forwarder {
    listener: TcpListener,
    local_addr: SocketAddr,
    config: Arc<Config>,
    limiter: Arc<ConnectionLimiter>,
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

    /// Accepts connections until the listener fails irrecoverably.
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
        loop {
            match self.listener.accept() {
                Ok((stream, peer)) => self.dispatch(stream, peer),
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

/// Counting limiter bounding the number of simultaneous connections.
///
/// The count is held by RAII [`Permit`]s, so a slot is released no matter how
/// a connection ends, including when its thread panics and unwinds.
#[derive(Debug)]
pub(crate) struct ConnectionLimiter {
    active: AtomicUsize,
    max: usize,
}

impl ConnectionLimiter {
    pub(crate) fn new(max: usize) -> Self {
        Self {
            active: AtomicUsize::new(0),
            max,
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
}

/// A held connection slot; dropping it frees the slot.
#[derive(Debug)]
pub(crate) struct Permit {
    limiter: Arc<ConnectionLimiter>,
}

impl Permit {
    /// Frees the slot now and returns how many connections remain active.
    pub(crate) fn release(self) -> usize {
        let limiter = Arc::clone(&self.limiter);
        drop(self);
        limiter.active()
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.limiter.active.fetch_sub(1, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}

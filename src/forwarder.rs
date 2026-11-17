//! Listener and accept loop.

use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use tracing::{debug, error, info};

use crate::config::{self, Config};
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
                        config: Arc::new(config),
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
            "forwarder listening"
        );
        loop {
            match self.listener.accept() {
                Ok((stream, peer)) => dispatch(stream, peer),
                Err(err) => handle_accept_error(&err),
            }
        }
    }
}

fn dispatch(stream: TcpStream, peer: SocketAddr) {
    info!(client = %peer, "accepted connection");
    // Connection handling is added in a later step; close politely.
    drop(stream);
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

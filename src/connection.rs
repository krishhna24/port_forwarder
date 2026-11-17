//! Per-connection lifecycle: upstream connect and teardown.

use std::net::{Shutdown, TcpStream};
use std::sync::Arc;

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

        // Forwarding is added in a later step.
        let _ = upstream.shutdown(Shutdown::Both);
        let _ = self.client.shutdown(Shutdown::Both);
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
        source: last_err.unwrap_or_else(|| std::io::Error::other("no upstream address")),
    })
}

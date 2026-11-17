//! Error types for the forwarder.

use std::io;

/// Errors that prevent the forwarder from starting or that are reported by
/// library entry points.
///
/// Per-connection I/O failures are *not* surfaced through this type: they are
/// logged and contained inside the connection that produced them so that one
/// misbehaving peer can never take down the listener.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A configuration value failed validation.
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    /// An address could not be resolved to any socket address.
    #[error("failed to resolve {role} address `{addr}`: {source}")]
    Resolve {
        /// Which address failed (`listen` or `upstream`).
        role: &'static str,
        /// The address as supplied by the user.
        addr: String,
        /// The underlying resolver error.
        #[source]
        source: io::Error,
    },

    /// The listening socket could not be bound.
    #[error("failed to bind listener on `{addr}`: {source}")]
    Bind {
        /// The address as supplied by the user.
        addr: String,
        /// The underlying OS error.
        #[source]
        source: io::Error,
    },

    /// No resolved upstream address accepted a connection.
    #[error("failed to connect to upstream `{addr}`: {source}")]
    Connect {
        /// The upstream address as supplied by the user.
        addr: String,
        /// The error from the last address that was tried.
        #[source]
        source: io::Error,
    },

    /// Any other I/O error.
    #[error(transparent)]
    Io(#[from] io::Error),
}

/// Convenience alias used throughout the crate.
pub type Result<T, E = Error> = std::result::Result<T, E>;

//! A lightweight, protocol-agnostic TCP port forwarder.
//!
//! The forwarder accepts TCP connections on a listen address, opens a separate
//! connection to a configured upstream for each client, and relays raw bytes in
//! both directions using blocking `std::net` I/O and one thread per direction.

pub mod cli;
pub mod config;
mod connection;
pub mod error;
pub mod forwarder;
pub mod logging;

pub use config::Config;
pub use error::{Error, Result};
pub use forwarder::Forwarder;

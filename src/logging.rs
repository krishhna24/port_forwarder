//! Tracing subscriber setup.

use std::io::IsTerminal;

use tracing_subscriber::EnvFilter;

/// Default filter when `RUST_LOG` is unset or invalid.
const DEFAULT_FILTER: &str = "info";

/// Installs a global `tracing` subscriber that writes to stderr.
///
/// The verbosity is taken from `RUST_LOG` (for example
/// `RUST_LOG=rust_tcp_forwarder=debug`) and defaults to `info`. Calling this
/// more than once is harmless: later calls leave the first subscriber in place.
pub fn init() {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(DEFAULT_FILTER));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_ansi(std::io::stderr().is_terminal())
        .with_thread_names(true)
        .try_init();
}

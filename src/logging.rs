//! Tracing subscriber setup.

use std::io::IsTerminal;

use tracing_subscriber::EnvFilter;

/// Default filter when `RUST_LOG` is unset or invalid.
const DEFAULT_FILTER: &str = "info";

/// Log output format.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, clap::ValueEnum)]
pub enum LogFormat {
    /// Human-readable lines with `key=value` fields.
    #[default]
    Text,
    /// One JSON object per line, for log shippers.
    Json,
}

/// Installs a global `tracing` subscriber that writes to stderr.
///
/// The verbosity is taken from `RUST_LOG` (for example
/// `RUST_LOG=rust_tcp_forwarder=debug`) and defaults to `info`. Calling this
/// more than once is harmless: later calls leave the first subscriber in place.
pub fn init(format: LogFormat) {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(DEFAULT_FILTER));
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_thread_names(true);
    let _ = match format {
        LogFormat::Text => builder
            .with_ansi(std::io::stderr().is_terminal())
            .try_init(),
        LogFormat::Json => builder
            .json()
            .with_current_span(true)
            .with_span_list(false)
            .try_init(),
    };
}

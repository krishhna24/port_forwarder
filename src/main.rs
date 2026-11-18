use std::process::ExitCode;

use clap::Parser;
use rust_tcp_forwarder::{Forwarder, cli::Args, logging};
use tracing::{error, info, warn};

/// Log under the library's target prefix so a single filter such as
/// `RUST_LOG=rust_tcp_forwarder=debug` also shows signal and fatal-error
/// messages from the binary (whose default target would be `tcp_forwarder`).
const LOG_TARGET: &str = "rust_tcp_forwarder::main";

fn main() -> ExitCode {
    let args = Args::parse();
    logging::init(args.log_format);

    match run(args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            error!(target: LOG_TARGET, error = %err, "fatal error");
            ExitCode::FAILURE
        }
    }
}

fn run(args: Args) -> rust_tcp_forwarder::Result<()> {
    let forwarder = Forwarder::bind(args.into_config()?)?;

    // SIGINT/SIGTERM (and SIGHUP) start a graceful shutdown; a second signal
    // forces all connections closed immediately.
    let handle = forwarder.shutdown_handle();
    let installed = ctrlc::set_handler(move || {
        if handle.is_shutting_down() {
            warn!(target: LOG_TARGET, "second signal received; forcing shutdown");
            handle.force();
        } else {
            info!(target: LOG_TARGET, "signal received; starting graceful shutdown (repeat to force)");
            handle.shutdown();
        }
    });
    if let Err(err) = installed {
        warn!(target: LOG_TARGET, error = %err, "could not install signal handler; graceful shutdown unavailable");
    }

    forwarder.run()
}

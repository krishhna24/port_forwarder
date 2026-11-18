use std::process::ExitCode;

use clap::Parser;
use rust_tcp_forwarder::{Forwarder, cli::Args, logging};
use tracing::{error, info, warn};

fn main() -> ExitCode {
    let args = Args::parse();
    logging::init(args.log_format);

    match run(args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            error!(error = %err, "fatal error");
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
            warn!("second signal received; forcing shutdown");
            handle.force();
        } else {
            info!("signal received; starting graceful shutdown (repeat to force)");
            handle.shutdown();
        }
    });
    if let Err(err) = installed {
        warn!(error = %err, "could not install signal handler; graceful shutdown unavailable");
    }

    forwarder.run()
}

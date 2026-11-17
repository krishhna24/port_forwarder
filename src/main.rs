use std::process::ExitCode;

use clap::Parser;
use rust_tcp_forwarder::{Forwarder, cli::Args, logging};
use tracing::error;

fn main() -> ExitCode {
    let args = Args::parse();
    logging::init();

    let result = args
        .into_config()
        .and_then(Forwarder::bind)
        .and_then(Forwarder::run);
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            error!(error = %err, "fatal error");
            ExitCode::FAILURE
        }
    }
}

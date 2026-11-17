use std::process::ExitCode;

use clap::Parser;
use rust_tcp_forwarder::cli::Args;

fn main() -> ExitCode {
    let args = Args::parse();
    let config = match args.into_config() {
        Ok(config) => config,
        Err(err) => {
            eprintln!("error: {err}");
            return ExitCode::FAILURE;
        }
    };
    println!("configuration: {config:?}");
    ExitCode::SUCCESS
}

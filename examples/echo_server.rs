//! Minimal std-only TCP echo server for manually testing the forwarder.
//!
//! Every byte a client sends is written straight back. When the client
//! half-closes its write side, the server finishes echoing and then shuts
//! down its own write side, so TCP half-close is honoured end to end.
//!
//! # Usage
//!
//! ```text
//! cargo run --example echo_server -- [ADDR]    # default ADDR: 127.0.0.1:9000
//! ```
//!
//! Point the forwarder at the echo server (for example listen on
//! `127.0.0.1:8080`, forward to `127.0.0.1:9000`), then talk to it through
//! the forwarder:
//!
//! ```text
//! echo hello | nc -N 127.0.0.1 8080
//! ```
//!
//! `-N` makes `nc` shut down its write side on stdin EOF, which exercises
//! the half-close path: the full echo still arrives before the connection
//! closes.

use std::env;
use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::process::ExitCode;
use std::thread;

const DEFAULT_ADDR: &str = "127.0.0.1:9000";

fn main() -> ExitCode {
    let mut args = env::args().skip(1);
    let addr = match args.next() {
        Some(arg) if arg == "-h" || arg == "--help" => {
            print_usage();
            return ExitCode::SUCCESS;
        }
        Some(arg) => arg,
        None => DEFAULT_ADDR.to_owned(),
    };
    if args.next().is_some() {
        print_usage();
        return ExitCode::FAILURE;
    }

    let listener = match TcpListener::bind(&addr) {
        Ok(listener) => listener,
        Err(err) => {
            eprintln!("echo_server: failed to bind {addr}: {err}");
            return ExitCode::FAILURE;
        }
    };
    match listener.local_addr() {
        Ok(local) => eprintln!("echo_server: listening on {local}"),
        Err(_) => eprintln!("echo_server: listening on {addr}"),
    }

    for incoming in listener.incoming() {
        match incoming {
            Ok(stream) => {
                thread::spawn(move || handle(stream));
            }
            Err(err) => eprintln!("echo_server: accept failed: {err}"),
        }
    }
    ExitCode::SUCCESS
}

fn print_usage() {
    println!("Usage: echo_server [ADDR]");
    println!();
    println!("Std-only TCP echo server. ADDR defaults to {DEFAULT_ADDR}.");
    println!("Echoes bytes until the client half-closes, then shuts down");
    println!("its write side.");
}

fn handle(stream: TcpStream) {
    let peer = stream
        .peer_addr()
        .map_or_else(|_| "<unknown>".to_owned(), |p: SocketAddr| p.to_string());
    eprintln!("echo_server: connect {peer}");
    match echo(stream) {
        Ok(bytes) => eprintln!("echo_server: close {peer} ({bytes} bytes echoed)"),
        Err((bytes, err)) => {
            eprintln!("echo_server: error {peer} after {bytes} bytes: {err}");
        }
    }
}

/// Echoes until the peer half-closes, then shuts down the write side.
/// Returns the number of bytes echoed, along with the error on failure.
fn echo(mut stream: TcpStream) -> Result<u64, (u64, io::Error)> {
    let mut buf = [0u8; 16 * 1024];
    let mut total: u64 = 0;
    loop {
        let n = match stream.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => n,
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            Err(err) => return Err((total, err)),
        };
        stream.write_all(&buf[..n]).map_err(|err| (total, err))?;
        total += n as u64;
    }
    stream.flush().map_err(|err| (total, err))?;
    stream
        .shutdown(Shutdown::Write)
        .map_err(|err| (total, err))?;
    Ok(total)
}

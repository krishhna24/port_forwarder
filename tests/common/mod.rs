//! Shared helpers for the integration tests.
//!
//! Every test binary uses a different subset of these helpers.
#![allow(dead_code)]

use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use rust_tcp_forwarder::{Config, Forwarder, ShutdownHandle, Stats};

/// Default timeout applied to every client socket so that a forwarder bug
/// turns into a test failure instead of a hung test run.
pub const IO_TIMEOUT: Duration = Duration::from_secs(5);
/// Default upper bound for `wait_until` style assertions.
pub const WAIT: Duration = Duration::from_secs(5);

/// A forwarder running on its own thread. Dropping the guard forces the
/// forwarder down and joins its thread, so a failing test never leaks one.
pub struct ForwarderGuard {
    pub addr: SocketAddr,
    pub handle: ShutdownHandle,
    join: Option<JoinHandle<()>>,
    done: Receiver<rust_tcp_forwarder::Result<()>>,
}

impl ForwarderGuard {
    /// Current statistics snapshot.
    pub fn stats(&self) -> Stats {
        self.handle.stats()
    }

    /// Waits up to `timeout` for `run()` to return and yields its result, or
    /// `None` if it is still running.
    pub fn wait_exit(&self, timeout: Duration) -> Option<rust_tcp_forwarder::Result<()>> {
        match self.done.recv_timeout(timeout) {
            Ok(result) => Some(result),
            Err(RecvTimeoutError::Timeout) => None,
            Err(RecvTimeoutError::Disconnected) => panic!("forwarder thread panicked"),
        }
    }

    /// Waits until the forwarder has accepted `n` connections in total.
    pub fn wait_accepted(&self, n: u64) {
        assert!(
            wait_until(WAIT, || self.stats().accepted >= n),
            "forwarder did not accept {n} connections: {:?}",
            self.stats()
        );
    }

    /// Waits until no connection is active.
    pub fn wait_idle(&self) {
        assert!(
            wait_until(WAIT, || self.stats().active == 0),
            "connections still active: {:?}",
            self.stats()
        );
    }
}

impl Drop for ForwarderGuard {
    fn drop(&mut self) {
        self.handle.force();
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

/// A configuration listening on an ephemeral loopback port and forwarding to
/// `upstream`.
pub fn config_for(upstream: SocketAddr) -> Config {
    Config::new("127.0.0.1:0", upstream.to_string())
}

/// Binds `config` and runs the forwarder on a background thread.
pub fn spawn_forwarder(config: Config) -> ForwarderGuard {
    let forwarder = Forwarder::bind(config).expect("bind forwarder");
    let addr = forwarder.local_addr();
    let handle = forwarder.shutdown_handle();
    let (tx, done) = mpsc::channel();
    let join = thread::Builder::new()
        .name("forwarder".into())
        .spawn(move || {
            let _ = tx.send(forwarder.run());
        })
        .expect("spawn forwarder thread");
    ForwarderGuard {
        addr,
        handle,
        join: Some(join),
        done,
    }
}

/// Spawns a forwarder with default tuning in front of `upstream`.
pub fn spawn_forwarder_to(upstream: SocketAddr) -> ForwarderGuard {
    spawn_forwarder(config_for(upstream))
}

/// Starts a scripted upstream: every accepted stream is handed to `handler`
/// on its own thread. The listener lives for the rest of the test process.
pub fn spawn_upstream<F>(handler: F) -> SocketAddr
where
    F: Fn(TcpStream) + Send + Sync + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind upstream");
    let addr = listener.local_addr().expect("upstream addr");
    let handler = Arc::new(handler);
    thread::Builder::new()
        .name("upstream-accept".into())
        .spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { continue };
                let handler = Arc::clone(&handler);
                thread::spawn(move || handler(stream));
            }
        })
        .expect("spawn upstream thread");
    addr
}

/// Echoes everything back; on EOF from the peer it half-closes its own
/// write side, so the peer receives the full echo followed by EOF.
pub fn echo(mut stream: TcpStream) {
    let _ = stream.set_read_timeout(Some(Duration::from_secs(30)));
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        match stream.read(&mut buf) {
            Ok(0) => {
                let _ = stream.shutdown(Shutdown::Write);
                return;
            }
            Ok(n) => {
                if stream.write_all(&buf[..n]).is_err() {
                    return;
                }
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(_) => return,
        }
    }
}

/// Starts an echo upstream.
pub fn spawn_echo() -> SocketAddr {
    spawn_upstream(echo)
}

/// Connects to `addr` with read and write timeouts set.
pub fn connect(addr: SocketAddr) -> TcpStream {
    let stream = TcpStream::connect(addr).expect("connect");
    stream.set_read_timeout(Some(IO_TIMEOUT)).unwrap();
    stream.set_write_timeout(Some(IO_TIMEOUT)).unwrap();
    stream
}

/// Polls `cond` every few milliseconds until it holds or `timeout` elapses.
/// Returns whether the condition was observed to hold.
pub fn wait_until(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if cond() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        thread::sleep(Duration::from_millis(5));
    }
}

/// Deterministic pseudo-random bytes (xorshift64*), distinct per seed.
pub fn payload(seed: u64, len: usize) -> Vec<u8> {
    // Mix the seed so that small consecutive seeds diverge immediately; the
    // state must never be zero.
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut out = Vec::with_capacity(len + 8);
    while out.len() < len {
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        out.extend_from_slice(&state.wrapping_mul(0x2545_F491_4F6C_DD1D).to_le_bytes());
    }
    out.truncate(len);
    out
}

/// FNV-1a hash, used to compare large buffers with a readable failure message.
pub fn fnv1a(data: &[u8]) -> u64 {
    data.iter().fold(0xcbf2_9ce4_8422_2325, |h, &b| {
        (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
    })
}

/// Whether `err` is an error a peer that closed abruptly may produce.
pub fn is_reset(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::BrokenPipe
    )
}

/// Asserts that the next read reports the connection closed: either a clean
/// EOF or a reset. Fails on data or on a timeout.
pub fn expect_closed(stream: &mut TcpStream) {
    let mut buf = [0u8; 1024];
    match stream.read(&mut buf) {
        Ok(0) => {}
        Ok(n) => panic!("expected EOF/reset, got {n} bytes"),
        Err(e) if is_reset(&e) => {}
        Err(e) => panic!("expected EOF/reset, got error {e} ({:?})", e.kind()),
    }
}

/// Reads until EOF, treating a reset after data as the end of the stream.
/// Panics on any other error (including a read timeout).
pub fn read_until_closed(stream: &mut TcpStream) -> Vec<u8> {
    let mut out = Vec::new();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        match stream.read(&mut buf) {
            Ok(0) => return out,
            Ok(n) => out.extend_from_slice(&buf[..n]),
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) if is_reset(&e) => return out,
            Err(e) => panic!("read failed after {} bytes: {e}", out.len()),
        }
    }
}

/// Sends `data` through `addr` (expected to lead to an echo upstream) using a
/// separate writer thread, half-closes, and returns everything read back
/// until EOF.
pub fn echo_roundtrip(addr: SocketAddr, data: Arc<Vec<u8>>) -> Vec<u8> {
    let mut stream = connect(addr);
    let mut writer = stream.try_clone().expect("clone client");
    let sender = thread::spawn(move || {
        writer.write_all(&data).expect("write payload");
        writer.shutdown(Shutdown::Write).expect("half-close");
    });
    let mut received = Vec::new();
    stream.read_to_end(&mut received).expect("read echo");
    sender.join().expect("writer thread");
    received
}

/// Writes `msg` and reads exactly `msg.len()` bytes back.
pub fn exchange(stream: &mut TcpStream, msg: &[u8]) -> Vec<u8> {
    stream.write_all(msg).expect("write");
    let mut back = vec![0u8; msg.len()];
    stream.read_exact(&mut back).expect("read reply");
    back
}

/// Returns a loopback address that nothing is listening on.
pub fn closed_port() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind probe");
    listener.local_addr().expect("probe addr")
}

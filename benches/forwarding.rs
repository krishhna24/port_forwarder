//! End-to-end benchmarks for the forwarder, using only `std`.
//!
//! Run with `cargo bench --bench forwarding`. Tunables (environment):
//!
//! * `BENCH_MIB` – MiB transferred per throughput measurement (default 256).
//! * `BENCH_ROUNDTRIPS` – round trips per latency measurement (default 10000).
//!
//! Every measurement is preceded by a warmup and repeated [`RUNS`] times; the
//! median is reported. Everything runs over loopback in a single process, so
//! the "direct" rows are the baseline the forwarder's overhead is compared
//! against.

use std::env;
use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, Barrier};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use rust_tcp_forwarder::{Config, Forwarder, ShutdownHandle};

const MIB: u64 = 1024 * 1024;
const RUNS: usize = 3;
const DEFAULT_MIB: u64 = 256;
const DEFAULT_ROUNDTRIPS: usize = 10_000;
/// Size of each client `write` call in throughput tests.
const WRITE_CHUNK: usize = 256 * 1024;
/// Payload size of one latency round trip.
const PING_SIZE: usize = 64;
const BUFFER_SIZES: [usize; 3] = [16 * 1024, 64 * 1024, 256 * 1024];
const CONCURRENCY: [usize; 3] = [1, 16, 64];

fn main() {
    let mib = env_or("BENCH_MIB", DEFAULT_MIB).max(1);
    let roundtrips = env_or("BENCH_ROUNDTRIPS", DEFAULT_ROUNDTRIPS).max(1);
    let total = mib * MIB;

    print_environment(mib, roundtrips);

    let sink = spawn_server(handle_sink);
    let echo = spawn_server(handle_echo);

    // One forwarder per buffer size in front of the sink; the default-sized
    // one is reused for the concurrency test.
    let sink_forwarders: Vec<(usize, RunningForwarder)> = BUFFER_SIZES
        .iter()
        .map(|&size| (size, RunningForwarder::start(sink, size)))
        .collect();
    let default_fwd = sink_forwarders
        .iter()
        .find(|(size, _)| *size == rust_tcp_forwarder::config::DEFAULT_BUFFER_SIZE)
        .map_or(sink, |(_, f)| f.addr);
    let echo_fwd = RunningForwarder::start(echo, rust_tcp_forwarder::config::DEFAULT_BUFFER_SIZE);

    // 1. Throughput.
    println!("## Throughput (one-way, client -> sink, {mib} MiB per run)\n");
    println!("| path | buffer | MiB/s (median of {RUNS}) | vs direct |");
    println!("|---|---|---:|---:|");
    let direct = measure_throughput(sink, total);
    println!("| direct | - | {direct:.1} | - |");
    for (size, fwd) in &sink_forwarders {
        let via = measure_throughput(fwd.addr, total);
        println!(
            "| forwarder | {} KiB | {via:.1} | {} |",
            size / 1024,
            overhead(direct, via)
        );
    }

    // 2. Latency.
    println!("\n## Latency ({PING_SIZE}-byte ping-pong, {roundtrips} round trips per run)\n");
    println!("| path | mean us | p50 us | p99 us |");
    println!("|---|---:|---:|---:|");
    for (name, addr) in [("direct", echo), ("forwarder", echo_fwd.addr)] {
        let l = measure_latency(addr, roundtrips);
        println!("| {name} | {:.1} | {:.1} | {:.1} |", l.mean, l.p50, l.p99);
    }

    // 3. Concurrency.
    println!(
        "\n## Concurrency (forwarder, 64 KiB buffer, {mib} MiB total split across connections)\n"
    );
    println!("| connections | aggregate MiB/s (median of {RUNS}) |");
    println!("|---:|---:|");
    for conns in CONCURRENCY {
        let rate = measure_concurrent(default_fwd, total, conns);
        println!("| {conns} | {rate:.1} |");
    }

    for (_, fwd) in sink_forwarders {
        fwd.stop();
    }
    echo_fwd.stop();
    println!("\nAll forwarders shut down.");
}

fn print_environment(mib: u64, roundtrips: usize) {
    let cpus = thread::available_parallelism().map_or(0, std::num::NonZero::get);
    let profile = if cfg!(debug_assertions) {
        "debug (unoptimized; numbers are not representative)"
    } else {
        "optimized (release/bench)"
    };
    println!("# rust-tcp-forwarder benchmarks\n");
    println!("- CPUs available: {cpus}");
    println!("- build profile: {profile}");
    println!("- transport: TCP over 127.0.0.1, single process");
    println!("- BENCH_MIB={mib}, BENCH_ROUNDTRIPS={roundtrips}");
    println!("- each figure is the median of {RUNS} runs after one warmup run\n");
}

fn env_or<T: std::str::FromStr>(key: &str, default: T) -> T {
    env::var(key)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

// ---------------------------------------------------------------------------
// Upstream servers
// ---------------------------------------------------------------------------

/// Binds `127.0.0.1:0` and serves every connection with `handler` on its own
/// thread. The accept loop lives for the rest of the process.
fn spawn_server(handler: fn(TcpStream) -> io::Result<()>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind upstream server");
    let addr = listener.local_addr().expect("upstream local addr");
    thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            thread::spawn(move || {
                let _ = stream.set_nodelay(true);
                let _ = handler(stream);
            });
        }
    });
    addr
}

/// Reads until EOF, then replies with the byte count (u64 LE) so the client
/// knows every byte has actually arrived.
fn handle_sink(mut stream: TcpStream) -> io::Result<()> {
    let mut buf = vec![0u8; 256 * 1024];
    let mut count: u64 = 0;
    loop {
        match stream.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => count += n as u64,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    stream.write_all(&count.to_le_bytes())?;
    stream.shutdown(Shutdown::Write)
}

/// Echoes everything back until EOF.
fn handle_echo(mut stream: TcpStream) -> io::Result<()> {
    let mut buf = vec![0u8; 16 * 1024];
    loop {
        match stream.read(&mut buf) {
            Ok(0) => return Ok(()),
            Ok(n) => stream.write_all(&buf[..n])?,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
}

// ---------------------------------------------------------------------------
// Forwarder lifecycle
// ---------------------------------------------------------------------------

struct RunningForwarder {
    addr: SocketAddr,
    handle: ShutdownHandle,
    thread: JoinHandle<()>,
}

impl RunningForwarder {
    fn start(upstream: SocketAddr, buffer_size: usize) -> Self {
        let mut config = Config::new("127.0.0.1:0", upstream.to_string());
        config.buffer_size = buffer_size;
        config.idle_timeout = None;
        let forwarder = Forwarder::bind(config).expect("bind forwarder");
        let addr = forwarder.local_addr();
        let handle = forwarder.shutdown_handle();
        let thread = thread::spawn(move || forwarder.run().expect("forwarder run"));
        Self {
            addr,
            handle,
            thread,
        }
    }

    fn stop(self) {
        self.handle.force();
        self.thread.join().expect("forwarder thread panicked");
    }
}

// ---------------------------------------------------------------------------
// Measurements
// ---------------------------------------------------------------------------

/// Sends `bytes` to the sink at `addr` and waits for its acknowledgement.
fn transfer(addr: SocketAddr, bytes: u64) {
    let mut stream = TcpStream::connect(addr).expect("connect for transfer");
    let _ = stream.set_nodelay(true);
    let chunk = vec![0xA5u8; WRITE_CHUNK];
    let mut left = bytes;
    while left > 0 {
        let n = usize::try_from(left).map_or(WRITE_CHUNK, |l| l.min(WRITE_CHUNK));
        stream.write_all(&chunk[..n]).expect("write to sink");
        left -= n as u64;
    }
    stream.shutdown(Shutdown::Write).expect("half-close");
    let mut ack = [0u8; 8];
    stream.read_exact(&mut ack).expect("read sink ack");
    assert_eq!(u64::from_le_bytes(ack), bytes, "sink received wrong count");
}

/// Median single-connection throughput in MiB/s.
fn measure_throughput(addr: SocketAddr, total: u64) -> f64 {
    transfer(addr, (total / 8).max(MIB)); // warmup
    median(
        (0..RUNS)
            .map(|_| {
                let start = Instant::now();
                transfer(addr, total);
                mib_per_sec(total, start.elapsed())
            })
            .collect(),
    )
}

/// Median aggregate throughput in MiB/s with `conns` parallel connections
/// sharing `total` bytes.
fn measure_concurrent(addr: SocketAddr, total: u64, conns: usize) -> f64 {
    let per_conn = (total / conns as u64).max(1);
    let run = || {
        let barrier = Arc::new(Barrier::new(conns + 1));
        let workers: Vec<_> = (0..conns)
            .map(|_| {
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    transfer(addr, per_conn);
                })
            })
            .collect();
        barrier.wait();
        let start = Instant::now();
        for w in workers {
            w.join().expect("transfer worker panicked");
        }
        mib_per_sec(per_conn * conns as u64, start.elapsed())
    };
    run(); // warmup
    median((0..RUNS).map(|_| run()).collect())
}

struct Latency {
    mean: f64,
    p50: f64,
    p99: f64,
}

/// Round-trip latency over one connection; each statistic is the median of
/// that statistic across runs.
fn measure_latency(addr: SocketAddr, roundtrips: usize) -> Latency {
    let mut stream = TcpStream::connect(addr).expect("connect for latency");
    stream.set_nodelay(true).expect("set TCP_NODELAY");
    let request = [0x5Au8; PING_SIZE];
    let mut reply = [0u8; PING_SIZE];
    let mut round = |samples: &mut Vec<f64>| {
        let start = Instant::now();
        stream.write_all(&request).expect("write ping");
        stream.read_exact(&mut reply).expect("read pong");
        samples.push(start.elapsed().as_secs_f64() * 1e6);
    };

    let mut scratch = Vec::with_capacity(roundtrips);
    for _ in 0..roundtrips.min(1000) {
        round(&mut scratch); // warmup
    }

    let (mut means, mut p50s, mut p99s) = (Vec::new(), Vec::new(), Vec::new());
    for _ in 0..RUNS {
        let mut samples = Vec::with_capacity(roundtrips);
        for _ in 0..roundtrips {
            round(&mut samples);
        }
        samples.sort_by(f64::total_cmp);
        means.push(mean(&samples));
        p50s.push(percentile(&samples, 0.50));
        p99s.push(percentile(&samples, 0.99));
    }
    stream.shutdown(Shutdown::Write).expect("half-close");
    Latency {
        mean: median(means),
        p50: median(p50s),
        p99: median(p99s),
    }
}

// ---------------------------------------------------------------------------
// Statistics helpers
// ---------------------------------------------------------------------------

#[allow(clippy::cast_precision_loss)] // byte counts stay far below 2^52
fn mib_per_sec(bytes: u64, elapsed: Duration) -> f64 {
    bytes as f64 / MIB as f64 / elapsed.as_secs_f64()
}

#[allow(clippy::cast_precision_loss)] // sample counts are small
fn mean(values: &[f64]) -> f64 {
    values.iter().sum::<f64>() / values.len() as f64
}

fn median(mut values: Vec<f64>) -> f64 {
    values.sort_by(f64::total_cmp);
    values[values.len() / 2]
}

/// Nearest-rank percentile of an ascending, non-empty slice.
#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
fn percentile(sorted: &[f64], q: f64) -> f64 {
    let rank = (q * sorted.len() as f64).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1]
}

fn overhead(direct: f64, via: f64) -> String {
    format!("{:+.1}%", (via - direct) / direct * 100.0)
}

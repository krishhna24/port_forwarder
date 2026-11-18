# rust-tcp-forwarder

A lightweight, protocol-agnostic TCP port forwarder written in Rust, built on
blocking `std::net` I/O.

## Project overview

`tcp-forwarder` listens on one TCP address and, for every client it accepts,
opens a separate connection to a single configured upstream. It then copies raw
bytes in both directions until both sides are done. It does not parse
the bytes it relays, so it works with any TCP protocol: HTTP, Redis, PostgreSQL,
SSH, or a custom binary protocol.

The design choices are deliberate:

- **Blocking I/O with a thread per direction.** There is no async runtime. Each
  connection is served by two OS threads.
- **Correct TCP half-close.** When one side finishes sending, the forwarder
  passes that FIN on and keeps the other direction open.
- **Bounded resources.** A connection limit, an idle timeout, and two-phase
  graceful shutdown keep slow or misbehaving peers from piling up.
- **Observable.** Structured `tracing` logs (text or JSON) with a per-connection
  span and a summary line when each connection closes.

The crate is `rust-tcp-forwarder` (library `rust_tcp_forwarder`) and the binary
is `tcp-forwarder`.

## Features

- Forwards any TCP protocol byte for byte, in both directions at once.
- Supports TCP half-close: EOF in one direction becomes `shutdown(Write)` on
  the other socket, so request/response protocols that rely on half-close
  work end to end.
- Resolves the upstream address again for every new connection, so DNS
  changes take effect. Each resolved address is tried in turn, each with its
  own connect timeout.
- Fails fast at startup: the configuration is validated, the listen address
  is bound, and the upstream must resolve before any client is accepted.
- Limits simultaneous connections (`--max-connections`). A client over the
  limit is accepted and closed at once, not left waiting in the kernel
  backlog.
- Closes connections where no bytes move in either direction for the idle
  timeout (`--idle-timeout`, default 300s; `0` disables it).
- Shuts down in two phases on SIGINT, SIGTERM or SIGHUP. The listener closes
  first, in-flight connections get up to `--shutdown-timeout` to finish, and
  any that remain are forced closed. A second signal forces closure at once.
- Contains failures to their connection. Per-connection I/O errors, and even
  panics, are logged and close only that connection; the listener keeps
  running.
- Sets `TCP_NODELAY` on both the client and upstream sockets.
- Writes structured logs to stderr as human-readable text or one JSON object
  per line (`--log-format json`). Verbosity is set with `RUST_LOG`.
- The library exposes a `Stats` snapshot (active, accepted, rejected,
  upstream failures, completed, byte totals) through `ShutdownHandle::stats`.
- No `unsafe` code (`unsafe_code = "forbid"`).

## Architecture diagram

```text
                          tcp-forwarder process
                 +--------------------------------------------------+
                 |                                                  |
  client A ----->|  listener (--listen)                             |
  client B ----->|      |                                           |
  client C ----->|      v                                           |
                 |  accept loop (main thread)                       |
                 |      |  try_acquire permit (--max-connections)   |
                 |      |    full -> close client, count "rejected" |
                 |      |                                           |
                 |      +--> thread "conn-1" ------------------+    |
                 |      |      connect upstream (re-resolve,   |    |
                 |      |      --connect-timeout per address)  |    |
                 |      |                                      |    |
                 |      |      [conn-1]      client -> upstream ======> upstream
                 |      |      [conn-1-down] upstream -> client <====== (--upstream)
                 |      |                                      |    |
                 |      +--> thread "conn-2" (same pair) ------+    |
                 |      +--> ...                                    |
                 |                                                  |
                 |  ctrl-c thread: SIGINT/SIGTERM/SIGHUP ->         |
                 |                 ShutdownHandle::shutdown/force   |
                 +--------------------------------------------------+
```

Each connection gets a thread named `conn-<id>`. That thread connects to the
upstream, copies client -> upstream itself, and starts a scoped helper thread
`conn-<id>-down` for upstream -> client. Both threads log inside a
`conn{id, client}` tracing span.

## TCP connection lifecycle

1. **Accept.** The main thread blocks in `accept()`. Transient errors
   (`ConnectionAborted`, `ConnectionReset`, `Interrupted`) are retried at once.
   Any other error, usually file-descriptor or memory exhaustion, is logged at
   `error` and the loop sleeps 100ms before trying again.
2. **Limit check.** The accept loop tries to take a permit from the connection
   limiter. If `--max-connections` permits are already held, the client is
   closed immediately, a `connection limit reached; rejecting client` warning
   is logged, and the `rejected` counter goes up. Otherwise the connection gets
   an id and `accepted connection active=<n>` is logged.
3. **Socket setup and spawn.** The client socket is made blocking and given
   `TCP_NODELAY`, then handed to a new `conn-<id>` thread together with its
   permit. If the thread cannot be spawned, the client socket and permit are
   dropped, which closes the client and frees the slot.
4. **Upstream connect.** The connection thread resolves `--upstream` again
   (DNS changes are picked up) and tries each resolved address with
   `TcpStream::connect_timeout(--connect-timeout)`. On success it logs
   `upstream connected` with `upstream` and `connect_ms`. `connect_ms` is
   measured from accept, so it includes resolution. If every address fails,
   the client is shut down and closed, `upstream_failures` goes up, and
   `upstream connection failed; closed client` is logged at `warn`. If a
   forced shutdown arrived while the connect was in progress, the connection
   is closed before any forwarding starts.
5. **Forwarding.** Both sockets get read and write timeouts of one *tick*
   (see [Architecture and concurrency decisions](#architecture-and-concurrency-decisions)).
   Two threads copy data, one per direction, each with its own
   `--buffer-size` buffer. A direction stops on EOF (half-close), an error,
   the idle timeout, a forced shutdown, or a teardown started by the other
   direction.
6. **Close summary.** When both directions have finished, both sockets are
   closed and one `connection closed` line is logged:

   ```text
   INFO conn-1 conn{id=1 client=127.0.0.1:42670}: rust_tcp_forwarder::connection: connection closed upstream=127.0.0.1:9000 bytes_client_to_upstream=79 bytes_upstream_to_client=918 duration_ms=16 client_to_upstream=eof upstream_to_client=eof active=0
   ```

   `client_to_upstream` and `upstream_to_client` give the reason each
   direction ended: `eof`, `torn down`, `idle timeout`, `shutdown`,
   `read error: ...`, `write error: ...` or `panicked`. `duration_ms` is
   measured from accept. `active` is the number of connections still open
   after this one. The summary is logged *before* the permit is released, so
   graceful shutdown cannot finish (and the process cannot exit) ahead of a
   connection's last log line. The byte totals are then added to the global
   counters.

## TCP half-close

**What it is.** A TCP connection is two independent byte streams. A peer can
call `shutdown(SHUT_WR)` to send a FIN, which says "I have nothing more to
send", and still read everything the other side sends back. This is a
*half-close*. Many real protocols rely on it: `nc -N`, HTTP/1.0-style clients
that signal the end of a request body, `rsync`/`ssh` pipes, and
`tar ... | nc`-style transfers.

**Why naive forwarders break it.** A simple forwarder copies until one side
returns EOF and then closes *both* sockets, or it waits for the first copy
loop to end and drops everything. When the client half-closes, that forwarder
cuts off the upstream's response while it is still being sent. The client sees
a truncated reply or an empty connection. Another common mistake is never
passing the FIN on at all. Then the upstream waits forever for the end of the
request and the connection hangs.

**How this forwarder handles it.**

- **EOF is a half-close.** When a direction reads EOF from its source, it calls
  `shutdown(Write)` on the destination only, which passes the FIN on, and that
  direction ends with reason `eof`. The opposite direction keeps running until
  it reaches EOF too.
- **Errors tear down the whole connection.** A read or write error, the idle
  timeout, or a forced shutdown makes the thread that noticed it call
  `shutdown(Both)` on *both* sockets. This wakes the other thread from a
  blocked `read` (it sees EOF) or `write` (it gets an error), so it exits right
  away and reports `torn down`. Neither thread waits on a peer that will never
  send again.

```text
 client                 forwarder                  upstream
   |   request bytes     |                            |
   |-------------------->|------ request bytes ------>|
   |   FIN (SHUT_WR)     |                            |
   |-------------------->|  read()=0 on client        |
   |                     |  upstream.shutdown(Write)  |
   |                     |------------ FIN ---------->|   client->upstream: eof
   |                     |                            |   (upstream->client still open)
   |                     |<------ response bytes -----|
   |<-- response bytes --|                            |
   |                     |<----------- FIN -----------|
   |                     |  read()=0 on upstream      |
   |<------- FIN --------|  client.shutdown(Write)    |   upstream->client: eof
   |                     |                            |
   |            both directions done: sockets closed, |
   |            "connection closed" summary logged    |
```

You can see this with the bundled echo server:
`echo hello | nc -N 127.0.0.1 8080` sends `hello`, half-closes, and still
receives the full echo before the connection closes.

## Prerequisites

- Rust **1.85 or newer** (the crate uses `edition = "2024"` and declares
  `rust-version = "1.85"`). Install it with [rustup](https://rustup.rs/).
- Linux is the tested platform (development happens on WSL2). Other
  platforms are untested; see [Known limitations](#known-limitations).
- Optional, for the examples: `python3`, `curl`, and an `nc` that supports
  `-N` (OpenBSD netcat).

## Installation

From a checkout of this repository:

```sh
cargo install --path .
```

This installs the `tcp-forwarder` binary into `~/.cargo/bin`.

## Build

```sh
cargo build              # debug build: target/debug/tcp-forwarder
cargo build --release    # optimized build (thin LTO, 1 codegen unit): target/release/tcp-forwarder
```

## Usage examples

### Forward an HTTP server

```sh
# Terminal 1: an upstream to forward to
python3 -m http.server 9000 --bind 127.0.0.1

# Terminal 2: forward 127.0.0.1:8080 -> 127.0.0.1:9000
tcp-forwarder --listen 127.0.0.1:8080 --upstream 127.0.0.1:9000

# Terminal 3: talk to the upstream through the forwarder
curl -i http://127.0.0.1:8080/
```

The forwarder logs one line when it accepts the connection, one when the
upstream is connected, and one summary line when it closes:

```text
INFO main rust_tcp_forwarder::forwarder: forwarder listening listen=127.0.0.1:8080 upstream=127.0.0.1:9000 max_connections=1024
INFO main conn{id=1 client=127.0.0.1:42670}: rust_tcp_forwarder::forwarder: accepted connection active=1
INFO conn-1 conn{id=1 client=127.0.0.1:42670}: rust_tcp_forwarder::connection: upstream connected upstream=127.0.0.1:9000 connect_ms=3
INFO conn-1 conn{id=1 client=127.0.0.1:42670}: rust_tcp_forwarder::connection: connection closed upstream=127.0.0.1:9000 bytes_client_to_upstream=79 bytes_upstream_to_client=918 duration_ms=16 client_to_upstream=eof upstream_to_client=eof active=0
```

Press Ctrl+C once for a graceful shutdown and a second time to force it.

### Echo server and half-close

The repository includes a std-only echo server (`examples/echo_server.rs`,
default address `127.0.0.1:9000`). When the client half-closes, it finishes
echoing and then shuts down its own write side.

```sh
cargo run --example echo_server                      # listens on 127.0.0.1:9000
cargo run -- -l 127.0.0.1:8080 -u 127.0.0.1:9000     # in another terminal
echo hello | nc -N 127.0.0.1 8080                    # prints "hello"
```

`-N` makes `nc` shut down its write side when stdin reaches EOF, which
exercises the half-close path.

### JSON logs

```sh
tcp-forwarder -l 127.0.0.1:8080 -u 127.0.0.1:9000 --log-format json
```

Each line is one JSON object. Event fields go under `fields`, and the
connection span goes under `span`:

```json
{"timestamp":"2026-11-18T10:40:56.738663Z","level":"INFO","fields":{"message":"connection closed","upstream":"127.0.0.1:9000","bytes_client_to_upstream":79,"bytes_upstream_to_client":959,"duration_ms":2,"client_to_upstream":"eof","upstream_to_client":"eof","active":0},"target":"rust_tcp_forwarder::connection","span":{"client":"127.0.0.1:42534","id":1,"name":"conn"},"threadName":"conn-1"}
```

### Debug logging

```sh
RUST_LOG=rust_tcp_forwarder=debug tcp-forwarder -l 127.0.0.1:8080 -u 127.0.0.1:9000
```

At `debug` level you also get the resolved upstream addresses, a line each
time one direction finishes (`direction finished dir=upstream->client bytes=959 reason=eof`),
failures for individual upstream addresses, and transient accept errors.

The filter above enables **only** the library target. The binary logs its
signal-handling and `fatal error` messages under the target `tcp_forwarder`,
so they are hidden by that filter. To keep them, add a default level:

```sh
RUST_LOG=info,rust_tcp_forwarder=debug tcp-forwarder -l 127.0.0.1:8080 -u 127.0.0.1:9000
```

## CLI reference

Output of `tcp-forwarder --help`:

```text
A lightweight, protocol-agnostic TCP port forwarder built on blocking std::net I/O

Usage: tcp-forwarder [OPTIONS] --listen <HOST:PORT> --upstream <HOST:PORT>

Options:
  -l, --listen <HOST:PORT>
          Address to listen on (host:port). Hostnames are resolved at startup

  -u, --upstream <HOST:PORT>
          Upstream address to forward to (host:port). Re-resolved per connection

      --max-connections <N>
          Maximum simultaneous connections; extra clients are closed immediately
          
          [default: 1024]

      --connect-timeout <DURATION>
          Timeout for each upstream connection attempt (e.g. 500ms, 5s, 1m)
          
          [default: 5s]

      --idle-timeout <DURATION>
          Tear down a connection after no bytes move in either direction for this long; 0 disables
          
          [default: 300s]

      --shutdown-timeout <DURATION>
          On SIGINT/SIGTERM, wait this long for in-flight connections before forcing them closed
          
          [default: 30s]

      --buffer-size <SIZE>
          Per-direction copy buffer size (e.g. 16KiB, 64KiB, 1MiB)
          
          [default: 64KiB]

      --log-format <LOG_FORMAT>
          Log output format; verbosity is controlled with `RUST_LOG` (default: info)

          Possible values:
          - text: Human-readable lines with `key=value` fields
          - json: One JSON object per line, for log shippers
          
          [default: text]

  -h, --help
          Print help (see a summary with '-h')

  -V, --version
          Print version
```

### Value formats and validation

| Kind | Accepted input | Notes |
| ---- | -------------- | ----- |
| `HOST:PORT` | `127.0.0.1:8080`, `localhost:9000`, `[::1]:8080` | IPv6 hosts must be in brackets. Port `0` is allowed for `--listen` (the OS picks a free port) but not for `--upstream`. |
| `DURATION` | A non-negative integer followed by `ms`, `s`, `m` or `h`, e.g. `250ms`, `5s`, `2m`, `1h`. A bare number means seconds. | No fractions (`1.5s` is rejected) and no spaces inside. `--connect-timeout` must be greater than 0. `--idle-timeout 0` disables the idle timeout. `--shutdown-timeout 0` skips the grace period and forces closure at once. |
| `SIZE` | An integer with an optional binary unit: none or `B` (bytes), `K`/`KiB` (x1024), `M`/`MiB` (x1024^2). Units are case-insensitive. | `--buffer-size` must be between 1024 bytes and 16 MiB. |
| `N` | A positive integer | `--max-connections` must be at least 1. |

### Exit codes

| Code | Meaning |
| ---- | ------- |
| `0` | Clean shutdown after a signal (graceful or forced), and also for `--help` and `--version`. |
| `1` | Runtime or configuration error, logged as `fatal error`. Examples: a malformed address (`upstream address 'nope' must be in host:port form`), a value out of range, a listen or upstream address that does not resolve, or a listen address that cannot be bound. All of these are found at startup. Once the forwarder is running, errors stay inside their connection and never stop the process. |
| `2` | Usage error reported by clap, such as a missing required flag, an unknown flag, or a value that fails to parse (`--connect-timeout 5x`). |

If the signal handler cannot be installed, the forwarder logs a warning and
keeps running without graceful shutdown.

## Testing

```sh
cargo test                                    # unit tests + integration tests
cargo test --test half_close                  # a single integration test file
cargo clippy --all-targets -- -D warnings     # lints (pedantic set, see Cargo.toml)
cargo fmt --all -- --check                    # formatting
```

Unit tests in `src/` cover argument and value parsing, configuration
validation, address resolution, the connection limiter (bounds, permit release
on panic, idle waiting), tick clamping, mapping unspecified addresses to
loopback, and the teardown classification of end reasons.

The integration tests in `tests/` run a real forwarder on an ephemeral port
against in-process upstream servers:

| File | Covers |
| ---- | ------ |
| `tests/forwarding.rs` | Basic forwarding, bidirectional traffic, large payloads, empty connections, slow and stalled clients. |
| `tests/half_close.rs` | Half-close in both directions: client-first FIN with a full response afterwards, and upstream-first FIN while the client keeps sending. |
| `tests/concurrent_clients.rs` | Many simultaneous clients, enforcement of `--max-connections`, and release of slots and resources (cleanup) after connections end. |
| `tests/error_handling.rs` | Upstream unavailable, client and upstream disconnects mid-stream (reset and clean close), idle timeout (and an active connection *not* being treated as idle), graceful shutdown draining in-flight connections, the shutdown timeout forcing connections closed, immediate forced shutdown, and startup failures (address in use, invalid config). |

See [docs/development.md](docs/development.md) for debugging tips.

## Configuration examples

```sh
# Expose a local service on all interfaces
tcp-forwarder -l 0.0.0.0:8080 -u 127.0.0.1:3000

# Forward to a hostname; it is resolved again for every connection
tcp-forwarder -l 127.0.0.1:5433 -u db.internal.example:5432

# IPv6 listener
tcp-forwarder -l '[::1]:8080' -u 127.0.0.1:9000

# Long-lived, mostly idle connections (e.g. SSH, database pools): no idle timeout
tcp-forwarder -l 127.0.0.1:2222 -u 10.0.0.5:22 --idle-timeout 0

# Small, constrained deployment: fewer slots, smaller buffers, quick failover
tcp-forwarder -l 127.0.0.1:8080 -u 127.0.0.1:9000 \
  --max-connections 128 --buffer-size 16KiB --connect-timeout 500ms

# Bulk transfer: larger buffers, short drain on restart
tcp-forwarder -l 127.0.0.1:8080 -u 127.0.0.1:9000 \
  --buffer-size 1MiB --shutdown-timeout 5s

# JSON logs with per-direction debug detail
RUST_LOG=info,rust_tcp_forwarder=debug \
  tcp-forwarder -l 127.0.0.1:8080 -u 127.0.0.1:9000 --log-format json
```

Memory for copy buffers grows with active connections: each connection
allocates two `--buffer-size` buffers (one per direction). With the defaults
(64 KiB, 1024 connections) that is up to 128 MiB of buffers, plus thread
stacks.

The library can also be embedded:

```rust
use rust_tcp_forwarder::{Config, Forwarder};

fn main() -> rust_tcp_forwarder::Result<()> {
    let mut config = Config::new("127.0.0.1:0", "127.0.0.1:9000");
    config.max_connections = 64;
    let forwarder = Forwarder::bind(config)?;
    let handle = forwarder.shutdown_handle(); // shutdown(), force(), stats()
    println!("listening on {}", forwarder.local_addr());
    // Call handle.shutdown() from another thread to stop; run() returns
    // once the listener is closed and in-flight connections have drained.
    std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_secs(60));
        handle.shutdown();
    });
    forwarder.run()
}
```

## Performance

See `cargo bench --bench forwarding`; measured results are recorded below.

<!-- PERF_RESULTS -->

## Architecture and concurrency decisions

A deeper walkthrough is in [docs/architecture.md](docs/architecture.md).

**Thread-per-connection, two threads per connection.** Each connection runs on
its own `conn-<id>` thread, which copies client -> upstream, plus a scoped
`conn-<id>-down` thread that copies upstream -> client. With blocking I/O this
is the simplest model where both directions move at the same time: a response
can stream back while the request is still being sent, and each direction
blocks only on its own source. The code reads straight down, with no state
machines or async runtime, and the only dependencies are `clap`, `ctrlc`,
`thiserror` and `tracing`.

**Bounded by `--max-connections`, rejecting at the limit.** Threads and buffers
cost memory, so concurrency is capped by a counting limiter. Each connection
holds an RAII permit, so its slot is freed however it ends. A client over the
limit is *accepted and closed at once* instead of being left in the kernel's
listen backlog. The client gets an immediate, clear close, not an
indefinite hang or a slow connect timeout, and the rejection is logged and
counted.

**Tick-based timeouts.** A blocking `read` or `write` in `std::net` cannot be
cancelled from another thread. Instead, every socket gets a short read/write
timeout, the *tick*: a quarter of the idle timeout, clamped to between 10ms and
250ms (250ms when the idle timeout is disabled). A call that times out is not
an error. The loop checks for teardown, forced shutdown and the idle deadline,
then retries. Writes use a hand-rolled `write_all` that resumes after a timeout
and counts exactly the bytes delivered.

**Idle time is measured across the whole connection.** Both directions update
one shared "last activity" timestamp, on every successful read and every
partial write. A connection is idle only when *no* bytes have moved in
*either* direction for `--idle-timeout`. A download that streams one way for
an hour while the other way is silent is never closed for being idle. A
destination that stops reading makes no write progress, so it eventually
trips the timeout.

**Waking `accept` with a self-connect.** `accept()` also blocks and cannot be
interrupted portably. `ShutdownHandle::shutdown` sets a draining flag and then
opens a loopback connection to the listener (`0.0.0.0`/`::` become
`127.0.0.1`/`::1`). The accept loop wakes, sees the flag, drops that
connection and exits.

**`TCP_NODELAY` on both sockets.** The forwarder writes whatever it has just
read. The original sender has already decided how to batch its data, so
Nagle's algorithm on the forwarder's sockets would only add latency for
small, interactive messages.

**Stats are added at close.** Each copy loop counts bytes in a local variable.
The global byte counters (`Stats::bytes_client_to_upstream`,
`Stats::bytes_upstream_to_client`) are updated once when a connection closes.
This keeps shared atomics out of the copy path, at the cost of long-lived
connections not showing up in byte totals until they end.

## Known limitations

- **Two threads and two buffers per connection.** At the default limit of 1024
  connections this means about 2048 threads and up to 128 MiB of copy buffers.
- **Periodic wakeups while idle.** With the default 250ms tick, each idle
  connection wakes up to 8 times per second (2 threads x 4 ticks). A shorter
  `--idle-timeout` gives a shorter tick and more wakeups.
- **Shutdown and force latency.** A forced shutdown takes effect at each
  thread's next tick, so up to 250ms. A connection that is still in its
  upstream connect is not interrupted. It finishes the connect (up to
  `--connect-timeout`) and then closes before forwarding. After forcing, the
  forwarder waits at most 2 seconds before logging `connections still open
  after forced close; exiting anyway` and returning.
- **Out of scope:** TLS termination or origination, the PROXY protocol, load
  balancing, health checks, and multiple upstreams. There is exactly one
  `--upstream`. Its resolved addresses are tried in order for failover on
  connect, not for balancing.
- **The client IP is not preserved.** The upstream sees connections coming
  from the forwarder's address. The original client address appears only in
  the forwarder's logs (the `client` span field).
- **Byte statistics are recorded only on close.** Open connections do not
  contribute to the byte totals.
- **`--idle-timeout 0` disables the only liveness check.** With it disabled, a
  peer that goes silent, or a client that stops reading, keeps its connection
  slot until it closes or the forwarder shuts down.
- **Connect timeout has no integration test.** Testing it needs an address that
  silently drops SYN packets. Such "blackhole" addresses proved unreliable in
  the development environment (WSL), where they failed fast instead of
  hanging. Each connect attempt still uses `TcpStream::connect_timeout`.
- **Windows is untested.** The code treats both `WouldBlock` and `TimedOut` as
  timeouts (Unix and Windows report a timed-out socket call differently), but
  it has only been built and tested on Linux.
- **Thread-per-connection does not scale to tens of thousands of
  connections.** Above a few thousand threads, an async design (epoll/kqueue
  through `mio`, or `tokio`) becomes the better choice:
  - *Memory per thread.* Each thread reserves its own stack (2 MiB of virtual
    address space by default for Rust threads, with some of it committed) as
    well as kernel task structures. An event loop keeps a few hundred bytes of
    state per connection.
  - *Context switches.* Each wakeup, whether data arriving or a tick
    expiring, is a kernel context switch into a separate thread, with the
    cache and TLB misses that come with it. An event loop handles many ready
    sockets in one `epoll_wait` call on a few threads.
  - *Scheduler overhead.* Thousands of runnable or periodically waking threads
    give the scheduler more work, raise tail latency, and can run into
    per-process thread limits (`ulimit -u`, `kernel.threads-max`). Readiness
    notification removes the need for timeout ticks: idle sockets cost nothing
    until they become readable.

## Future improvements

- Optional async back end (`mio` or `tokio`) for very high connection counts.
- Incremental byte counters, or periodic stats logging, for long-lived
  connections.
- Make an in-progress upstream connect cancellable on forced shutdown.
- PROXY protocol v1/v2 so the upstream can learn the original client address.
- Multiple upstreams with health checks and a balancing policy.
- A metrics endpoint (for example Prometheus) built on the existing `Stats`
  snapshot.
- Configurable TCP keepalive and socket buffer sizes.
- CI on Windows and macOS.

## License

Licensed under the MIT License. See [LICENSE](LICENSE).

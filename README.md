# rust-tcp-forwarder

A lightweight, protocol-agnostic TCP port forwarder in Rust, built on blocking
`std::net` I/O with one thread per direction and no async runtime.

```text
client ──► tcp-forwarder (listen) ──► upstream
           per connection: 2 threads
           client→upstream  |  upstream→client
```

## Features

- Forwards raw bytes in both directions at once, for any TCP protocol.
- **Half-close aware:** EOF in one direction becomes `shutdown(Write)` on the
  other socket, and the reverse direction keeps flowing.
- **Teardown on failure:** a read/write error, idle timeout or forced shutdown
  shuts down *both* sockets, which unblocks the other thread immediately.
- Bounded concurrency (`--max-connections`). Clients over the limit are
  accepted and closed at once.
- Connection-wide idle timeout, which also catches clients that stop reading.
- Upstream is re-resolved for every connection, and each address is tried
  with a connect timeout.
- Graceful shutdown on SIGINT/SIGTERM: stop accepting, drain for up to
  `--shutdown-timeout`, then force-close. A second signal forces immediately.
- Structured logs (text or JSON). Each connection has a summary line with
  bytes per direction, duration and close reason.

## Quick start

Requires Rust 1.85+ (edition 2024).

```sh
cargo build --release
python3 -m http.server 9000 --bind 127.0.0.1 &
./target/release/tcp-forwarder --listen 127.0.0.1:8080 --upstream 127.0.0.1:9000
curl -v http://127.0.0.1:8080/
```

Half-close check with the bundled echo server:

```sh
cargo run --example echo_server -- 127.0.0.1:9000
echo hello | nc -N 127.0.0.1 8080
```

## CLI

| Flag | Default | Description |
|---|---|---|
| `-l, --listen <HOST:PORT>` | required | Address to listen on |
| `-u, --upstream <HOST:PORT>` | required | Upstream address (hostnames allowed) |
| `--max-connections <N>` | `1024` | Simultaneous connection limit |
| `--connect-timeout <DURATION>` | `5s` | Per-address upstream connect timeout |
| `--idle-timeout <DURATION>` | `300s` | Close after no traffic either way; `0` disables |
| `--shutdown-timeout <DURATION>` | `30s` | Grace period for in-flight connections |
| `--buffer-size <SIZE>` | `64KiB` | Per-direction copy buffer (1KiB–16MiB) |
| `--log-format <text\|json>` | `text` | Log output format |

Durations accept `ms`, `s`, `m`, `h`. Sizes accept `B`, `KiB`, `MiB`.
Verbosity is set with `RUST_LOG` (default `info`), for example
`RUST_LOG=rust_tcp_forwarder=debug`. Exit codes: `0` clean shutdown, `1`
startup error (invalid config, resolve or bind failure), `2` usage error.

## Design notes

- **Thread per connection:** simple and easy to reason about, but each
  connection costs two threads and two buffers. Above a few thousand
  connections, an event loop (`mio`/`tokio`) scales better because it avoids
  per-thread stacks, context switches and scheduler load.
- **Deadlines with blocking I/O:** sockets use a short read/write timeout (at
  most 250ms). A timeout is not an error. The loop checks the idle and
  shutdown deadlines, then retries.
- **Panic safety:** connection slots are RAII permits, and a panicking
  direction tears down its peer, so nothing leaks or hangs.

## Testing

```sh
cargo test                                               # unit + integration
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
cargo bench --bench forwarding                           # throughput/latency
```

The integration tests in `tests/` use loopback sockets and cover forwarding,
large payloads, empty and slow clients, half-close in both directions,
concurrency and limits, cleanup, upstream failures, disconnects, idle timeout,
and graceful/forced shutdown.

One benchmark run on WSL2 over loopback (indicative only): direct 4320 MiB/s
vs forwarder 2153 MiB/s with a 64 KiB buffer (3483 MiB/s with 256 KiB).
Ping-pong p50 was 45µs direct and 88µs through the forwarder.

## Limitations

- Up to 250ms latency for forced shutdown. An in-progress upstream connect is
  not interrupted.
- Byte statistics are recorded when a connection closes.
- No TLS, PROXY protocol, load balancing or multiple upstreams. The upstream
  sees the forwarder's IP, not the client's.
- Tested on Linux only.

## License

MIT

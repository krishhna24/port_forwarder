# Development

## Environment setup

1. Install Rust 1.85 or newer with [rustup](https://rustup.rs/). The crate uses
   `edition = "2024"` and declares `rust-version = "1.85"`.

   ```sh
   rustup toolchain install stable
   rustup component add clippy rustfmt
   ```

2. Optional tools for manual testing: `python3`, `curl`, OpenBSD `nc` (for
   `-N`), and `ss` (from iproute2).

The project is developed and tested on Linux (including WSL2).

## Build and test commands

```sh
cargo build                          # debug build
cargo build --release                # release build (thin LTO, codegen-units = 1)
cargo run -- -l 127.0.0.1:8080 -u 127.0.0.1:9000
cargo run --example echo_server      # std-only echo upstream on 127.0.0.1:9000

cargo test                           # unit + integration tests
cargo test --lib                     # unit tests in src/ only
cargo test --test error_handling     # one integration test file
cargo test half_close -- --nocapture # filter by name, show output
cargo bench --bench forwarding       # throughput benchmark
```

Integration tests live in `tests/` (`forwarding.rs`, `half_close.rs`,
`concurrent_clients.rs`, `error_handling.rs`). They start a forwarder on
`127.0.0.1:0` through the library API and use its `local_addr()` and
`ShutdownHandle`, so they need no fixed ports and can run in parallel.

## Formatting and linting

```sh
cargo fmt --all                               # format
cargo fmt --all -- --check                    # verify formatting (CI-style)
cargo clippy --all-targets -- -D warnings     # lint everything, warnings as errors
```

- `rustfmt.toml` sets `edition = "2024"`, `max_width = 100` and
  `use_field_init_shorthand = true`.
- Lints are configured in `Cargo.toml` under `[lints]`:
  - `[lints.rust]`: `unsafe_code = "forbid"`,
    `missing_debug_implementations = "warn"`.
  - `[lints.clippy]`: the `all` and `pedantic` groups at `warn`, with
    `module_name_repetitions` and `must_use_candidate` allowed.
- `clippy.toml` sets `msrv = "1.85"`, so clippy does not suggest APIs newer
  than the declared `rust-version`.

## Contribution workflow

1. Create a topic branch from the main branch:
   `git switch -c fix/idle-timeout-race`.
2. Make focused changes. Keep behaviour and documentation in sync: if you
   change a flag, a default or a log field, update `README.md` and
   `docs/architecture.md` too.
3. Run all checks before committing:

   ```sh
   cargo fmt --all -- --check && \
   cargo clippy --all-targets -- -D warnings && \
   cargo test
   ```

4. Commit using [Conventional Commits](https://www.conventionalcommits.org/),
   as the existing history does (`feat:`, `fix:`, `chore:`, and also `docs:`,
   `test:`, `refactor:`, `perf:`). Use an imperative subject, wrap the body at
   72 columns, and explain *why* the change is needed:

   ```text
   fix: count partial writes before a teardown

   A direction that was torn down mid-buffer under-reported the bytes it
   had already delivered, so the close summary disagreed with what the
   peer received.
   ```

5. Open a pull request that describes the behaviour change and how you tested
   it.

## Debugging

### Log levels

Logs go to stderr. Verbosity comes from `RUST_LOG` (default `info`):

```sh
RUST_LOG=rust_tcp_forwarder=debug cargo run -- -l 127.0.0.1:8080 -u 127.0.0.1:9000
RUST_LOG=info,rust_tcp_forwarder::connection=trace cargo run -- -l 127.0.0.1:8080 -u 127.0.0.1:9000
RUST_LOG=rust_tcp_forwarder::connection=debug cargo run -- ...
```

The library logs under `rust_tcp_forwarder::forwarder` and
`rust_tcp_forwarder::connection`. The binary's own messages (signal handling,
`fatal error`) use `rust_tcp_forwarder::main`, so a filter on the
`rust_tcp_forwarder` prefix covers all of them.

| Level | What you get |
| ----- | ------------ |
| `error` | Accept failures with backoff, thread spawn failures, handler panics. |
| `warn` | Rejections at the connection limit, upstream connect failures, forced shutdown, an unavailable signal handler. |
| `info` | Listening, accepted (`active`), upstream connected (`upstream`, `connect_ms`), connection closed (`upstream`, `bytes_client_to_upstream`, `bytes_upstream_to_client`, `duration_ms`, `client_to_upstream`, `upstream_to_client`, `active`), non-reset teardowns, shutdown progress. |
| `debug` | Resolved upstream addresses, individual upstream address failures, per-direction `direction finished` lines (`dir`, `bytes`, `reason`), peer resets, half-close propagation failures, transient accept errors, wake-up failures. |

Each connection's lines are in a `conn{id=<n> client=<addr>}` span, and
thread names (`conn-<id>`, `conn-<id>-down`) appear in the output. Use
`grep 'id=42 '` to follow a single connection.

### JSON logs

```sh
cargo run -- -l 127.0.0.1:8080 -u 127.0.0.1:9000 --log-format json 2>&1 \
  | jq -c 'select(.fields.message == "connection closed") | .fields'
```

Event fields are under `.fields`, and the connection span (`id`, `client`) is
under `.span`.

### Manual end-to-end checks

```sh
# Upstream: the bundled echo server
cargo run --example echo_server                     # 127.0.0.1:9000

# Forwarder
cargo run -- -l 127.0.0.1:8080 -u 127.0.0.1:9000

# Half-close with nc: sends, shuts down its write side, still gets the echo
echo hello | nc -N 127.0.0.1 8080

# Half-close with Python, no nc needed
python3 -c "import socket;s=socket.create_connection(('127.0.0.1',8080));s.sendall(b'half-close works\n');s.shutdown(socket.SHUT_WR);print(s.makefile('rb').read())"

# HTTP upstream
python3 -m http.server 9000 --bind 127.0.0.1
curl -i http://127.0.0.1:8080/
```

A working half-close shows `client_to_upstream=eof upstream_to_client=eof` in
the summary, and the client receives the full echo.

### Inspecting sockets

```sh
ss -tnp state established '( sport = :8080 or dport = :9000 )'
ss -tnp | grep tcp-forwarder
ss -tn state fin-wait-2     # sockets that have sent FIN and are waiting for the peer's
ss -tn state close-wait     # sockets that received FIN but have not closed yet
```

After a half-close you should briefly see the client-side socket in
`FIN-WAIT-2`/`CLOSE-WAIT` while the reverse direction finishes. Sockets that
stay in `CLOSE-WAIT` long after a peer has gone suggest a direction that was
never torn down.

To stop a background forwarder or echo server, kill it by PID:

```sh
cargo run -- -l 127.0.0.1:8080 -u 127.0.0.1:9000 & FWD=$!
kill -INT "$FWD"    # graceful; send it again to force
```

Avoid `pkill -f`, which can match unrelated processes (including test
binaries and editors whose command lines contain the same words).

### A test that hangs

A hanging integration test almost always means a blocking `read` on a
connection the test expected to close, for example because half-close was not
propagated or a teardown did not happen. To debug it:

- Give every test socket a read timeout, e.g.
  `stream.set_read_timeout(Some(Duration::from_secs(5)))`, so a missing FIN
  fails the test with `WouldBlock`/`TimedOut` instead of hanging the run.
- Run the single test with output and debug logs:
  `RUST_LOG=rust_tcp_forwarder=debug cargo test --test half_close <name> -- --nocapture`.
  Forwarder logs appear only if the test installs a subscriber (for example
  by calling `rust_tcp_forwarder::logging::init`). `--nocapture` shows the
  test's own `println!` output alongside them.
- Use `--test-threads=1` to rule out interference between tests.
- While it hangs, run `ss -tnp` to see which socket is still open and in which
  state.

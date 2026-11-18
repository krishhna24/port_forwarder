# Architecture

This document describes how `tcp-forwarder` is built: which module does what,
how a connection moves through the system, how threads cooperate, and how
errors, resources and shutdown are handled. Everything here describes the
current implementation in `src/`.

## Component responsibilities

| Module | Responsibility |
| ------ | -------------- |
| `main.rs` | Binary entry point. Parses `cli::Args`, initialises logging, builds the `Config`, binds the `Forwarder`, installs the SIGINT/SIGTERM/SIGHUP handler (`ctrlc` with the `termination` feature) and runs the forwarder. Maps `Ok` to exit code 0 and any `Error` to exit code 1 after logging `fatal error`. clap exits with code 2 on usage errors before `main` gets that far. The signal handler calls `ShutdownHandle::shutdown` the first time and `ShutdownHandle::force` if shutdown is already in progress. |
| `lib.rs` | Library root. Re-exports `Config`, `Error`, `Result`, `Forwarder`, `ShutdownHandle`, `Stats` and `LogFormat`. `connection` is a private module. |
| `cli.rs` | clap `Args` definition, `parse_duration` (`ms`/`s`/`m`/`h`, a bare number means seconds) and `parse_size` (`B`, `K`/`KiB`, `M`/`MiB`, case-insensitive). `Args::into_config` turns `--idle-timeout 0` into `None` and validates the result. A unit test keeps the CLI default strings in sync with the constants in `config.rs`. |
| `config.rs` | `Config` and its defaults (`max_connections` 1024, `connect_timeout` 5s, `idle_timeout` 300s, `shutdown_timeout` 30s, `buffer_size` 64 KiB, allowed buffer range 1 KiB to 16 MiB). `Config::validate` checks the syntax and ranges without touching the network: `host:port` form, bracketed IPv6, port 0 allowed only for `listen`, non-zero limits and timeouts. `resolve` turns `host:port` into socket addresses and treats an empty result as an error. |
| `error.rs` | The `Error` enum (`InvalidConfig`, `Resolve`, `Bind`, `Connect`, `Io`), built with `thiserror`. It is used for startup failures and inside a connection for upstream connect failures. Per-connection I/O errors are never passed out through it. |
| `forwarder.rs` | `Forwarder` (bind, accept loop, dispatch, drain), the `ConnectionLimiter` with its RAII `Permit`, `ShutdownSignal` (the `draining` and `force` flags), `ShutdownHandle` (shutdown, force, stats, wake-by-self-connect), `Counters` and the public `Stats` snapshot. |
| `connection.rs` | Everything after a client is accepted: upstream connect (re-resolution, connect timeout, `TCP_NODELAY`), the two-thread forwarding pair, the copy loop with tick-based deadlines, half-close versus teardown policy, `End` reasons, panic containment (`TearDownOnPanic`) and the close summary log. |
| `logging.rs` | `LogFormat` (`text`/`json`) and `init`, which installs a `tracing-subscriber` writing to stderr with thread names. The filter comes from `RUST_LOG` and falls back to `info`. Text output uses ANSI colours only when stderr is a terminal. JSON output includes the current span (`conn{id, client}`) but not the full span list. |

## Connection lifecycle

```text
accept() ──► limiter.try_acquire()
               │ None ──► warn "connection limit reached; rejecting client",
               │          rejected += 1, drop(client)
               ▼ Some(permit)
            accepted += 1, id = next_id, span conn{id, client}
            client.set_nonblocking(false), set_nodelay(true)
            spawn thread "conn-<id>"  (spawn failure: client + permit dropped)
               │
               ▼  (in conn-<id>, inside catch_unwind)
            connect_upstream(): resolve(upstream) again,
               for each addr: TcpStream::connect_timeout(addr, connect_timeout)
               │ all failed ──► client.shutdown(Both), upstream_failures += 1,
               │                warn "upstream connection failed; closed client"
               ▼ connected (TCP_NODELAY set) ──► info "upstream connected" connect_ms
            if force flag set ──► info "shutdown in progress; closed connection before forwarding"
               ▼
            forward(): set read/write timeout = tick on both sockets,
               try_clone both sockets, spawn scoped "conn-<id>-down"
               conn-<id>       : copy(client  -> upstream)
               conn-<id>-down  : copy(upstream -> client)
               join both
               ▼
            drop sockets, completed += 1, add byte totals,
            info "connection closed" (logged before the permit is dropped)
               ▼
            permit dropped ──► active -= 1, notify drain waiters
```

`connect_ms` and `duration_ms` are both measured from the moment the client was
accepted. If `forward` fails before copying starts, because setting socket
timeouts or `try_clone` failed, the thread logs `failed to start forwarding`
instead of the summary.

## Data flow

Bytes are never parsed, buffered across reads, or changed. Each direction
runs the same loop:

1. `read` from the source into a private buffer of `--buffer-size` bytes,
   allocated once per direction.
2. On `Ok(n)`, update the shared activity timestamp, then write `buf[..n]` to
   the destination with a hand-rolled `write_all`. Each partial write adds to
   the direction's byte count and updates the timestamp. A write that times
   out is retried after a deadline check. `Ok(0)` from `write` is a
   `WriteZero` error.
3. On `Ok(0)` (EOF), pass the half-close on with `dst.shutdown(Write)` and stop
   with `End::Eof`. A `NotConnected` error from that shutdown is ignored, and
   other errors are logged at `debug`. If the pair has already been torn down,
   the EOF is just a side effect of the teardown and the direction ends with
   `End::TornDown`.
4. On a timeout (`WouldBlock` on Unix, `TimedOut` on Windows), check the
   deadlines and either stop or keep waiting. `Interrupted` is retried.
   Any other error ends the direction with `ReadError` or `WriteError`.

The byte count is exactly the number of bytes written to the destination, not
the number read. If a direction ends partway through a buffer, the bytes not
yet written are not counted.

## Threading model

| Thread | Count | Role |
| ------ | ----- | ---- |
| `main` | 1 | Binds, runs the accept loop and `dispatch`, then runs `drain` during shutdown. |
| `ctrl-c` | 1 | Created by the `ctrlc` crate. Runs the signal handler closure. |
| `conn-<id>` | 1 per connection | Upstream connect, then client -> upstream copy, then join and log the summary. |
| `conn-<id>-down` | 1 per connection | A `thread::scope` helper for upstream -> client. Borrows the sockets and the shared `Pair` state. |

Shared state, and how it is synchronised:

- **Per process:** `Arc<Config>`; `Arc<ShutdownSignal>` (two `AtomicBool`s,
  `Acquire`/`Release`); `Arc<Counters>` (`AtomicU64`s, `Relaxed`, updated only
  on accept, reject, upstream failure and close); `Arc<ConnectionLimiter>`
  (an `AtomicUsize` count updated with a `fetch_update` compare-and-swap, plus a
  `Mutex<()>`/`Condvar` pair so shutdown can sleep until the count reaches 0).
  The id counter is an `AtomicU64` owned by the `Forwarder`.
- **Per connection (`Pair`):** references to both sockets, a `torn_down`
  `AtomicBool`, and `last_activity_ms`, an `AtomicU64` holding milliseconds
  since forwarding started. Both directions update the timestamp, which is what
  makes the idle timeout connection-wide.
- **Sockets:** each direction reads from a `try_clone` of its source, which is
  a duplicated file descriptor for the same kernel socket. So a `shutdown` by
  either thread, and the timeouts set before cloning, apply to both handles.

The `conn-<id>-down` thread does not inherit the tracing span automatically,
so the parent passes it in explicitly and log lines from both directions carry
`conn{id, client}`.

### The tick

Blocking calls cannot be cancelled, so each socket gets a read and write
timeout:

```text
tick = clamp(idle_timeout / 4, 10ms, 250ms)    (250ms when idle timeout is disabled)
```

Each time a call times out, `Pair::deadline_reached` checks, in order:
whether the pair is torn down (`TornDown`), whether the force flag is set
(`Shutdown`), and whether the connection has been idle for at least
`idle_timeout` (`IdleTimeout`). If none apply, the call is retried. Because the
tick is at most a quarter of the idle timeout, the timeout fires at most
about one tick late.

## Error handling

**Fatal at startup (exit code 1).** `Forwarder::bind` validates the config,
resolves the upstream (only to catch typos; the result is not cached), resolves
the listen address and tries to bind each resolved address in turn. Any of
these can fail with `InvalidConfig`, `Resolve`, `Bind` or `Io`, and `main` then
logs `fatal error` and exits 1. clap value-parse failures exit 2 before this
point.

**Not fatal while running.** `Forwarder::run` returns `Ok(())` in every case.
Once the listener is up, nothing stops the process except a shutdown request:

- **Accept errors.** `ConnectionAborted`, `ConnectionReset` and `Interrupted`
  are logged at `debug` and retried at once. Anything else, typically EMFILE,
  ENFILE, ENOBUFS or ENOMEM, is logged at `error` (`accept failed; backing off`)
  and followed by a **100ms sleep**, so resource exhaustion does not become a
  busy loop. While draining, an accept error simply ends the loop.
- **Setup errors on a single connection.** If the client cannot be made
  blocking, it is dropped. A `TCP_NODELAY` failure is logged at `debug` and
  ignored. A thread spawn failure is logged at `error`, and the client and
  permit are dropped.
- **Upstream connect failure.** Logged at `warn` and counted in
  `upstream_failures`. The client is shut down and closed.
- **I/O errors while forwarding.** These end the connection through the
  teardown policy below. Resets, aborts and broken pipes are logged at `debug`
  (`peer reset; tearing down connection`). Other causes, including idle timeout
  and shutdown, are logged at `info` (`tearing down connection`).
- **Panics.** Contained to the connection (see below).

## Resource management

- **Connection slot (RAII `Permit`).** `ConnectionLimiter::try_acquire`
  returns a `Permit` that is moved into the `Connection`. Dropping it decrements
  the active count and notifies the drain condvar. Because it is a value owned
  by the connection thread, the slot is freed on every exit path: normal
  return, early return after an upstream failure, a spawn failure (the closure
  is dropped), or a panic that unwinds the thread. Before dropping the permit,
  `Permit::remaining()` gives the `active` value for the final log line, so
  the summary is always written before shutdown can see the slot as free.
- **Sockets.** `TcpStream` closes on drop. The client and upstream handles,
  and their read clones, are owned or borrowed within `Connection::run` and
  `forward`. `thread::scope` guarantees the helper thread has finished before
  the sockets go out of scope. The sockets are explicitly dropped before the
  summary is logged.
- **Buffers.** Each direction allocates one `Vec<u8>` of `buffer_size` bytes
  when its copy loop starts and frees it when the loop returns. Nothing else is
  allocated per read.
- **Panic handling.** There are two layers:
  1. `TearDownOnPanic` is a guard held by each direction. Its `Drop` calls
     `Pair::tear_down()` if `thread::panicking()`. This matters because
     `thread::scope` waits for *all* scoped threads before passing a panic on.
     Without the guard, a panic in one direction would leave the other blocked
     on a peer that may never send, and the connection would hang. When
     `conn-<id>-down` panics, the parent records `End::Panicked` for that
     direction with 0 bytes and logs `forwarding thread panicked`. A panic in
     the parent direction unwinds through the scope instead.
  2. `catch_unwind` around `Connection::run` in the `conn-<id>` thread turns
     any panic that escapes into an `error` log line
     (`connection handler panicked; connection dropped`). The permit and
     sockets have already been released during unwinding.

## TCP shutdown semantics

Two kinds of ending are distinguished:

- **Half-close:** only the write side of the destination is shut down
  (`shutdown(Write)`), which passes the FIN on. The other direction carries on
  undisturbed.
- **Teardown:** `Pair::tear_down` sets `torn_down` and calls `shutdown(Both)` on
  **both** sockets. The other thread wakes from a blocked `read` (it sees EOF)
  or `write` (it gets an error), notices `torn_down`, and ends with
  `TornDown`. Any error it hit at that point is treated as a symptom, logged
  at `debug` as `direction finished after teardown`, and not reported as the
  cause.

`End` reasons, as they appear in the `client_to_upstream` and
`upstream_to_client` fields of the summary:

| Reason (log value) | Trigger | Action | Teardown? |
| ------------------ | ------- | ------ | --------- |
| `eof` | `read` returned 0 and the pair was not torn down | `dst.shutdown(Write)` (half-close) | No |
| `torn down` | The other direction tore the pair down (seen as EOF, a deadline check, or an error after the teardown) | None; the sockets are already shut down | No (it is the result of a teardown) |
| `idle timeout` | No bytes moved in either direction for `idle_timeout` | `shutdown(Both)` on both sockets | Yes |
| `shutdown` | The force flag was set (forced shutdown or grace period expired) | `shutdown(Both)` on both sockets | Yes |
| `read error: <err>` | `read` failed with something other than a timeout or `Interrupted` | `shutdown(Both)` on both sockets | Yes |
| `write error: <err>` | `write` failed, or returned `Ok(0)` (`WriteZero`) | `shutdown(Both)` on both sockets | Yes |
| `panicked` | The `conn-<id>-down` thread panicked | Torn down by `TearDownOnPanic` during unwinding | Yes |

The usual clean close is `client_to_upstream=eof upstream_to_client=eof`.
When one side resets, that direction shows the error (for example
`read error: Connection reset by peer (os error 104)`) and the other shows
`torn down`.

## Graceful shutdown phases

```text
signal #1 ─► ShutdownHandle::shutdown()
               draining = true (only the first call does the following)
               wake_accept(): TcpStream::connect_timeout(listener addr, 1s)
                              (0.0.0.0 / :: replaced by loopback)
             accept loop wakes, sees draining, drops the wake connection, exits
             listener dropped  ─► new clients are refused by the kernel
             drain():
               log "shutting down: listener closed, waiting for active connections"
                   (only if active > 0)
               wait_idle(shutdown_timeout, abort = force flag)
                 └─ all permits released ─► "shutdown complete", run() returns Ok
                 └─ timeout or force    ─► force = true
                                           "forcing remaining connections closed"
                                           wait_idle(2s)
                                           └─ still active ─► "connections still open
                                                after forced close; exiting anyway"
                                           "shutdown complete", run() returns Ok
signal #2 ─► ShutdownHandle::force(): force = true, notify drain waiters, shutdown()
```

- **Phase 1, drain.** The listener closes first, so clients are refused
  instead of queueing in a backlog that nobody drains. In-flight connections
  keep forwarding normally, for up to `--shutdown-timeout`.
- **Phase 2, force.** The `force` flag is checked by every copy loop at its
  next tick, so forwarding connections end with reason `shutdown` within about
  250ms. A connection still in its upstream connect finishes or times out the
  connect first, then closes without forwarding. The forwarder waits at most
  2 seconds (`FORCE_GRACE`) after forcing, then returns whether or not every
  connection has closed.
- If the wake-up connect fails (for example because the listener is already
  gone), it is logged at `debug`. The next client to arrive wakes the accept
  loop instead.
- The process exit code is 0 in every shutdown case.

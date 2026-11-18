//! Many clients at once, the connection limit, and resource cleanup.

mod common;

use std::net::Shutdown;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use common::{
    config_for, connect, echo_roundtrip, exchange, expect_closed, payload, spawn_echo,
    spawn_forwarder, spawn_forwarder_to, wait_until,
};

#[test]
fn fifty_concurrent_clients_get_their_own_data() {
    const CLIENTS: u64 = 64;
    let fwd = spawn_forwarder_to(spawn_echo());
    let addr = fwd.addr;
    // Release every client at once to maximise overlap.
    let barrier = Arc::new(Barrier::new(usize::try_from(CLIENTS).unwrap()));

    let workers: Vec<_> = (0..CLIENTS)
        .map(|i| {
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                let len = 32 * 1024 + usize::try_from(i).unwrap() * 997;
                let data = Arc::new(payload(i + 1, len));
                barrier.wait();
                let received = echo_roundtrip(addr, Arc::clone(&data));
                assert!(received == *data, "client {i} received someone else's data");
                len as u64
            })
        })
        .collect();
    let total: u64 = workers.into_iter().map(|w| w.join().unwrap()).sum();

    fwd.wait_accepted(CLIENTS);
    fwd.wait_idle();
    let stats = fwd.stats();
    assert_eq!(stats.completed, CLIENTS);
    assert_eq!(stats.rejected, 0);
    assert_eq!(stats.bytes_client_to_upstream, total);
    assert_eq!(stats.bytes_upstream_to_client, total);
}

#[test]
fn connection_limit_rejects_and_recovers() {
    let mut config = config_for(spawn_echo());
    config.max_connections = 2;
    let fwd = spawn_forwarder(config);

    // Round-trips prove both connections hold a slot.
    let mut first = connect(fwd.addr);
    let mut second = connect(fwd.addr);
    assert_eq!(exchange(&mut first, b"one"), b"one");
    assert_eq!(exchange(&mut second, b"two"), b"two");
    assert_eq!(fwd.stats().active, 2);

    let mut third = connect(fwd.addr);
    expect_closed(&mut third);
    assert!(
        wait_until(common::WAIT, || fwd.stats().rejected == 1),
        "{:?}",
        fwd.stats()
    );
    assert_eq!(fwd.stats().accepted, 2);

    // Free a slot and wait until the forwarder has noticed.
    first.shutdown(Shutdown::Write).unwrap();
    expect_closed(&mut first);
    drop(first);
    assert!(wait_until(common::WAIT, || fwd.stats().active == 1));

    let mut fourth = connect(fwd.addr);
    assert_eq!(exchange(&mut fourth, b"four"), b"four");
    assert_eq!(exchange(&mut second, b"still alive"), b"still alive");
    let stats = fwd.stats();
    assert_eq!(stats.rejected, 1);
    assert_eq!(stats.accepted, 3);
}

#[test]
fn all_connections_are_cleaned_up() {
    const SEQUENTIAL: u64 = 20;
    const PARALLEL: u64 = 30;
    let fwd = spawn_forwarder_to(spawn_echo());
    let addr = fwd.addr;

    for i in 0..SEQUENTIAL {
        let data = Arc::new(payload(i, 1000));
        assert!(echo_roundtrip(addr, Arc::clone(&data)) == *data);
    }
    // Mix in some clients that just connect and disconnect.
    let workers: Vec<_> = (0..PARALLEL)
        .map(|i| {
            thread::spawn(move || {
                if i % 3 == 0 {
                    drop(connect(addr));
                } else {
                    let data = Arc::new(payload(100 + i, 4096));
                    assert!(echo_roundtrip(addr, Arc::clone(&data)) == *data);
                }
            })
        })
        .collect();
    for w in workers {
        w.join().unwrap();
    }

    fwd.wait_accepted(SEQUENTIAL + PARALLEL);
    assert!(
        wait_until(Duration::from_secs(5), || {
            let s = fwd.stats();
            s.active == 0 && s.completed == SEQUENTIAL + PARALLEL
        }),
        "connections leaked: {:?}",
        fwd.stats()
    );
    let stats = fwd.stats();
    assert_eq!(stats.accepted, SEQUENTIAL + PARALLEL);
    assert_eq!(stats.rejected, 0);
    assert_eq!(stats.upstream_failures, 0);
    assert_eq!(fwd.handle.active_connections(), 0);
}

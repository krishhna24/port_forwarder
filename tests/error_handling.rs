//! Failure paths: unreachable upstream, abrupt peers, timeouts, shutdown and
//! startup errors.

mod common;

use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use rust_tcp_forwarder::{Config, Error, Forwarder};

use common::{
    closed_port, config_for, connect, exchange, expect_closed, is_reset, spawn_echo,
    spawn_forwarder, spawn_forwarder_to, spawn_upstream, wait_until,
};

#[test]
fn unavailable_upstream_closes_client_promptly() {
    let fwd = spawn_forwarder_to(closed_port());

    for attempt in 1..=2 {
        let mut client = connect(fwd.addr);
        client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let start = Instant::now();
        expect_closed(&mut client);
        assert!(start.elapsed() < Duration::from_secs(2));
        assert!(
            wait_until(common::WAIT, || fwd.stats().upstream_failures == attempt),
            "{:?}",
            fwd.stats()
        );
    }
    fwd.wait_idle();
    let stats = fwd.stats();
    assert_eq!(stats.accepted, 2);
    assert_eq!(stats.completed, 0);
    assert!(!fwd.handle.is_shutting_down());
}

#[test]
fn client_disconnect_mid_stream() {
    let (tx, rx) = mpsc::channel();
    let upstream = spawn_upstream(move |mut stream| {
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        stream.write_all(&vec![1u8; 64 * 1024]).unwrap();
        let mut buf = Vec::new();
        let result = stream.read_to_end(&mut buf).map(|_| buf.len());
        tx.send(result).unwrap();
    });
    let fwd = spawn_forwarder_to(upstream);

    let mut client = connect(fwd.addr);
    client.write_all(b"partial request").unwrap();
    // Read a little of the response, then vanish with unread data pending,
    // which makes the kernel reset the connection.
    let mut first = [0u8; 10];
    client.read_exact(&mut first).unwrap();
    drop(client);

    match rx
        .recv_timeout(Duration::from_secs(5))
        .expect("upstream never observed the disconnect")
    {
        Ok(n) => assert_eq!(n, 15, "upstream should see the request then EOF"),
        Err(e) => assert!(is_reset(&e), "unexpected upstream error {e}"),
    }
    fwd.wait_accepted(1);
    fwd.wait_idle();
    assert_eq!(fwd.stats().completed, 1);
}

#[test]
fn upstream_disconnect_mid_stream() {
    let upstream = spawn_upstream(|mut stream| {
        stream.write_all(b"partial response").unwrap();
        // Wait until the client's data has arrived but leave it unread, so
        // closing the socket sends a reset rather than a clean FIN.
        let mut buf = [0u8; 64];
        let ready = wait_until(Duration::from_secs(5), || {
            stream.peek(&mut buf).is_ok_and(|n| n >= 5)
        });
        assert!(ready);
        drop(stream);
    });
    let fwd = spawn_forwarder_to(upstream);

    let mut client = connect(fwd.addr);
    let mut first = [0u8; 16];
    client.read_exact(&mut first).unwrap();
    assert_eq!(&first, b"partial response");
    client.write_all(b"hello").unwrap();

    // The reset must tear the whole connection down even though the client
    // keeps its socket open.
    expect_closed(&mut client);
    fwd.wait_accepted(1);
    fwd.wait_idle();
    assert_eq!(fwd.stats().completed, 1);
    drop(client);
}

#[test]
fn upstream_clean_close_mid_stream() {
    let upstream = spawn_upstream(|mut stream| {
        let _ = stream.write_all(b"bye");
    });
    let fwd = spawn_forwarder_to(upstream);

    let mut client = connect(fwd.addr);
    let mut data = Vec::new();
    client.read_to_end(&mut data).unwrap();
    assert_eq!(data, b"bye");
    drop(client);
    fwd.wait_accepted(1);
    fwd.wait_idle();
    assert_eq!(fwd.stats().completed, 1);
}

#[test]
fn silent_connection_hits_idle_timeout() {
    let mut config = config_for(spawn_echo());
    config.idle_timeout = Some(Duration::from_millis(300));
    let fwd = spawn_forwarder(config);

    let mut client = connect(fwd.addr);
    let start = Instant::now();
    expect_closed(&mut client);
    let elapsed = start.elapsed();
    assert!(
        elapsed >= Duration::from_millis(250) && elapsed < Duration::from_millis(1500),
        "idle connection closed after {elapsed:?}"
    );
    fwd.wait_idle();
    assert_eq!(fwd.stats().completed, 1);
}

#[test]
fn active_connection_is_not_idle() {
    let mut config = config_for(spawn_echo());
    config.idle_timeout = Some(Duration::from_millis(300));
    let fwd = spawn_forwarder(config);

    let mut client = connect(fwd.addr);
    let start = Instant::now();
    let mut rounds = 0u32;
    while start.elapsed() < Duration::from_millis(1300) {
        let msg = format!("ping {rounds}");
        assert_eq!(exchange(&mut client, msg.as_bytes()), msg.as_bytes());
        rounds += 1;
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(rounds >= 10);
    assert_eq!(fwd.stats().active, 1);
    assert_eq!(fwd.stats().completed, 0);
}

#[test]
fn graceful_shutdown_drains_in_flight_connections() {
    let fwd = spawn_forwarder_to(spawn_echo());
    let mut client = connect(fwd.addr);
    assert_eq!(exchange(&mut client, b"before"), b"before");

    fwd.handle.shutdown();
    assert!(fwd.handle.is_shutting_down());
    // The listener closes, so new clients are refused by the kernel.
    assert!(
        wait_until(common::WAIT, || TcpStream::connect(fwd.addr).is_err()),
        "listener still accepting after shutdown"
    );

    // The in-flight connection keeps forwarding and run() waits for it.
    assert_eq!(exchange(&mut client, b"during"), b"during");
    assert!(fwd.wait_exit(Duration::from_millis(100)).is_none());
    assert_eq!(fwd.stats().active, 1);

    client.shutdown(Shutdown::Write).unwrap();
    expect_closed(&mut client);
    drop(client);
    let result = fwd
        .wait_exit(Duration::from_secs(3))
        .expect("run() did not return after the last connection closed");
    result.unwrap();
    let stats = fwd.stats();
    assert_eq!(stats.active, 0);
    assert_eq!(stats.completed, 1);
}

#[test]
fn shutdown_timeout_forces_idle_connections_closed() {
    let mut config = config_for(spawn_echo());
    config.shutdown_timeout = Duration::from_millis(300);
    config.idle_timeout = None;
    let fwd = spawn_forwarder(config);
    let mut client = connect(fwd.addr);
    assert_eq!(exchange(&mut client, b"hold"), b"hold");

    let start = Instant::now();
    fwd.handle.shutdown();
    let result = fwd
        .wait_exit(Duration::from_secs(3))
        .expect("run() did not return after shutdown_timeout");
    result.unwrap();
    let elapsed = start.elapsed();
    assert!(
        elapsed >= Duration::from_millis(250),
        "returned before the grace period: {elapsed:?}"
    );
    assert!(elapsed < Duration::from_secs(2), "took {elapsed:?}");
    expect_closed(&mut client);
    assert_eq!(fwd.stats().active, 0);
}

#[test]
fn force_closes_connections_immediately() {
    let fwd = spawn_forwarder_to(spawn_echo());
    let mut client = connect(fwd.addr);
    assert_eq!(exchange(&mut client, b"hold"), b"hold");

    let start = Instant::now();
    fwd.handle.force();
    fwd.wait_exit(Duration::from_secs(2))
        .expect("run() did not return after force()")
        .unwrap();
    // Default shutdown_timeout is 30s, so this proves force skipped it.
    assert!(start.elapsed() < Duration::from_secs(1));
    expect_closed(&mut client);
    assert_eq!(fwd.stats().active, 0);
}

#[test]
fn bind_fails_when_address_in_use() {
    let taken = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = taken.local_addr().unwrap();
    let err = Forwarder::bind(Config::new(addr.to_string(), "127.0.0.1:9")).unwrap_err();
    match err {
        Error::Bind { source, .. } => assert_eq!(source.kind(), io::ErrorKind::AddrInUse),
        other => panic!("expected a bind error, got {other}"),
    }
}

#[test]
fn bind_rejects_invalid_config() {
    let base = || Config::new("127.0.0.1:0", "127.0.0.1:9");
    let mut cases = Vec::new();

    let mut c = base();
    c.max_connections = 0;
    cases.push(("max_connections = 0", c));
    let mut c = base();
    c.buffer_size = 1;
    cases.push(("tiny buffer", c));
    let mut c = base();
    c.connect_timeout = Duration::ZERO;
    cases.push(("zero connect timeout", c));
    let mut c = base();
    c.idle_timeout = Some(Duration::ZERO);
    cases.push(("zero idle timeout", c));
    let mut c = base();
    c.upstream = "127.0.0.1:0".into();
    cases.push(("upstream port 0", c));
    let mut c = base();
    c.listen = "not-an-address".into();
    cases.push(("malformed listen", c));

    for (name, config) in cases {
        match Forwarder::bind(config) {
            Err(Error::InvalidConfig(_)) => {}
            Err(other) => panic!("{name}: expected InvalidConfig, got {other}"),
            Ok(_) => panic!("{name}: invalid config was accepted"),
        }
    }
}

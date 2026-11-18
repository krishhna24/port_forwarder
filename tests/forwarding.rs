//! End-to-end data forwarding through a real forwarder.

mod common;

use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpStream};
use std::sync::Arc;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use common::{
    config_for, connect, echo_roundtrip, exchange, fnv1a, payload, spawn_echo, spawn_forwarder,
    spawn_forwarder_to, spawn_upstream, wait_until,
};

#[test]
fn basic_round_trip() {
    let fwd = spawn_forwarder_to(spawn_echo());
    let mut client = connect(fwd.addr);
    assert_eq!(
        exchange(&mut client, b"hello, forwarder"),
        b"hello, forwarder"
    );
    client.shutdown(Shutdown::Write).unwrap();
    let mut rest = Vec::new();
    client.read_to_end(&mut rest).unwrap();
    assert!(rest.is_empty());
    drop(client);

    fwd.wait_accepted(1);
    fwd.wait_idle();
    let stats = fwd.stats();
    assert_eq!(stats.completed, 1);
    assert_eq!(stats.bytes_client_to_upstream, 16);
    assert_eq!(stats.bytes_upstream_to_client, 16);
}

#[test]
fn interleaved_request_response_rounds() {
    // A tiny line protocol: the upstream answers each request line with a
    // response that depends on it, so the client must wait for every reply.
    let upstream = spawn_upstream(|stream| {
        let mut writer = stream.try_clone().unwrap();
        let reader = io::BufReader::new(stream);
        for line in io::BufRead::lines(reader) {
            let Ok(line) = line else { return };
            let reply = format!("ack {} {}\n", line.len(), line.to_uppercase());
            if writer.write_all(reply.as_bytes()).is_err() {
                return;
            }
        }
        let _ = writer.shutdown(Shutdown::Write);
    });
    let fwd = spawn_forwarder_to(upstream);
    let client = connect(fwd.addr);
    let mut writer = client.try_clone().unwrap();
    let mut reader = io::BufReader::new(client);

    for round in 0..50 {
        let request = format!("request number {round}");
        writer.write_all(format!("{request}\n").as_bytes()).unwrap();
        let mut reply = String::new();
        io::BufRead::read_line(&mut reader, &mut reply).unwrap();
        assert_eq!(
            reply,
            format!("ack {} {}\n", request.len(), request.to_uppercase())
        );
    }
    writer.shutdown(Shutdown::Write).unwrap();
    let mut rest = String::new();
    reader.read_to_string(&mut rest).unwrap();
    assert!(rest.is_empty());
    drop((writer, reader));

    fwd.wait_accepted(1);
    fwd.wait_idle();
    assert_eq!(fwd.stats().completed, 1);
}

#[test]
fn large_payload_is_forwarded_intact() {
    const LEN: usize = 32 * 1024 * 1024;
    let fwd = spawn_forwarder_to(spawn_echo());
    let data = Arc::new(payload(0x00C0_FFEE, LEN));
    let expected_hash = fnv1a(&data);

    let received = echo_roundtrip(fwd.addr, Arc::clone(&data));
    assert_eq!(received.len(), LEN);
    assert_eq!(fnv1a(&received), expected_hash, "payload corrupted");
    assert!(received == *data, "payload corrupted");

    fwd.wait_accepted(1);
    fwd.wait_idle();
    let stats = fwd.stats();
    assert_eq!(stats.completed, 1);
    assert_eq!(stats.bytes_client_to_upstream, LEN as u64);
    assert_eq!(stats.bytes_upstream_to_client, LEN as u64);
}

#[test]
fn empty_connection_propagates_eof() {
    let (tx, rx) = mpsc::channel();
    let upstream = spawn_upstream(move |mut stream| {
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut buf = Vec::new();
        let result = stream.read_to_end(&mut buf).map(|_| buf.len());
        let _ = tx.send(result.map_err(|e| e.kind()));
    });
    let fwd = spawn_forwarder_to(upstream);

    drop(connect(fwd.addr));

    let seen = rx
        .recv_timeout(Duration::from_secs(5))
        .expect("upstream never saw the connection end");
    assert_eq!(seen, Ok(0), "upstream should read a clean EOF and no data");
    fwd.wait_accepted(1);
    fwd.wait_idle();
    let stats = fwd.stats();
    assert_eq!(stats.completed, 1);
    assert_eq!(stats.bytes_client_to_upstream, 0);
    assert_eq!(stats.bytes_upstream_to_client, 0);
}

#[test]
fn slow_client_receives_everything_with_small_buffer() {
    const LEN: usize = 256 * 1024;
    let data = Arc::new(payload(7, LEN));
    let upstream_data = Arc::clone(&data);
    let upstream = spawn_upstream(move |mut stream| {
        let _ = stream.write_all(&upstream_data);
        let _ = stream.shutdown(Shutdown::Write);
        // Wait for the client to finish so no unread data triggers a reset.
        let _ = stream.read_to_end(&mut Vec::new());
    });
    let mut config = config_for(upstream);
    config.buffer_size = 1024;
    let fwd = spawn_forwarder(config);

    let mut client = connect(fwd.addr);
    let mut received = Vec::with_capacity(LEN);
    let mut chunk = [0u8; 700];
    let mut reads = 0u32;
    loop {
        let n = client.read(&mut chunk).unwrap();
        if n == 0 {
            break;
        }
        received.extend_from_slice(&chunk[..n]);
        reads += 1;
        if reads % 16 == 0 {
            thread::sleep(Duration::from_millis(2));
        }
    }
    assert!(received == *data, "slow client received corrupted data");
    drop(client);

    fwd.wait_accepted(1);
    fwd.wait_idle();
    assert_eq!(fwd.stats().bytes_upstream_to_client, LEN as u64);
}

#[test]
fn stalled_client_is_torn_down_by_idle_timeout() {
    let (tx, rx) = mpsc::channel();
    let upstream = spawn_upstream(move |mut stream| {
        stream
            .set_write_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let chunk = vec![0xAB; 64 * 1024];
        let mut pushed = 0usize;
        let end = loop {
            match stream.write(&chunk) {
                Ok(n) => pushed += n,
                Err(e) => break e,
            }
        };
        let _ = tx.send((pushed, end.kind()));
    });
    let mut config = config_for(upstream);
    config.idle_timeout = Some(Duration::from_millis(500));
    let fwd = spawn_forwarder(config);

    // Never read from this socket; keep it open until the end of the test.
    let client = TcpStream::connect(fwd.addr).unwrap();
    let start = Instant::now();
    fwd.wait_accepted(1);
    assert!(
        wait_until(Duration::from_secs(4), || fwd.stats().active == 0),
        "stalled connection was not torn down: {:?}",
        fwd.stats()
    );
    let elapsed = start.elapsed();
    assert!(
        elapsed >= Duration::from_millis(400),
        "torn down suspiciously early ({elapsed:?})"
    );

    let (pushed, kind) = rx
        .recv_timeout(Duration::from_secs(5))
        .expect("upstream writer was never stopped");
    assert!(pushed > 0);
    assert!(
        common::is_reset(&io::Error::from(kind)),
        "unexpected upstream write error {kind:?}"
    );
    assert_eq!(fwd.stats().completed, 1);
    drop(client);
}

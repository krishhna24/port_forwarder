//! Half-close (`shutdown(Write)`) propagation in both directions.

mod common;

use std::io::{Read, Write};
use std::net::Shutdown;
use std::sync::mpsc;
use std::time::Duration;

use common::{connect, spawn_forwarder_to, spawn_upstream};

#[test]
fn client_half_close_still_receives_response() {
    let (tx, rx) = mpsc::channel();
    let upstream = spawn_upstream(move |mut stream| {
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        // Read the whole request: this only returns once the FIN arrives.
        let mut request = Vec::new();
        stream.read_to_end(&mut request).unwrap();
        // Respond only after the client's EOF was observed.
        let mut response = b"response to: ".to_vec();
        response.extend_from_slice(&request);
        response.extend(std::iter::repeat_n(b'z', 100_000));
        stream.write_all(&response).unwrap();
        stream.shutdown(Shutdown::Write).unwrap();
        tx.send(request).unwrap();
    });
    let fwd = spawn_forwarder_to(upstream);

    let mut client = connect(fwd.addr);
    client.write_all(b"the request").unwrap();
    client.shutdown(Shutdown::Write).unwrap();

    let mut response = Vec::new();
    client.read_to_end(&mut response).unwrap();
    let request = rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(request, b"the request");
    assert_eq!(response.len(), 13 + 11 + 100_000);
    assert!(response.starts_with(b"response to: the request"));
    assert!(response[24..].iter().all(|&b| b == b'z'));

    fwd.wait_accepted(1);
    fwd.wait_idle();
    let stats = fwd.stats();
    assert_eq!(stats.completed, 1);
    assert_eq!(stats.bytes_client_to_upstream, 11);
    assert_eq!(stats.bytes_upstream_to_client, 100_024);
}

#[test]
fn upstream_half_close_lets_client_keep_sending() {
    let (tx, rx) = mpsc::channel();
    let upstream = spawn_upstream(move |mut stream| {
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        stream.write_all(b"banner, then silence").unwrap();
        stream.shutdown(Shutdown::Write).unwrap();
        // The read side must still work after our half-close.
        let mut from_client = Vec::new();
        let result = stream.read_to_end(&mut from_client).map(|_| from_client);
        tx.send(result.map_err(|e| e.kind())).unwrap();
    });
    let fwd = spawn_forwarder_to(upstream);

    let mut client = connect(fwd.addr);
    let mut banner = Vec::new();
    client.read_to_end(&mut banner).unwrap();
    assert_eq!(banner, b"banner, then silence");

    // The upstream->client direction is finished; client->upstream is not.
    client.write_all(b"late data ").unwrap();
    client.write_all(b"after EOF").unwrap();
    client.shutdown(Shutdown::Write).unwrap();

    let received = rx
        .recv_timeout(Duration::from_secs(5))
        .expect("upstream never finished reading");
    assert_eq!(received.as_deref(), Ok(&b"late data after EOF"[..]));
    drop(client);

    fwd.wait_accepted(1);
    fwd.wait_idle();
    let stats = fwd.stats();
    assert_eq!(stats.completed, 1);
    assert_eq!(stats.bytes_client_to_upstream, 19);
    assert_eq!(stats.bytes_upstream_to_client, 20);
}

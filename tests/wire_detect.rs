//! Selection of the transport: one detecting port for every wire, several endpoints at once,
//! and transports left out at compile time.

mod common;

use common::{DEV, free_port, server};
use std::time::Duration;
use trust::DeviceProxy;

async fn wait(port: u16) {
    while tokio::net::TcpStream::connect(("127.0.0.1", port)).await.is_err() {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[cfg(feature = "detect")]
mod detect {
    use super::*;
    use trust::Value;

    /// The client URLs reaching a `auto://127.0.0.1:{port}` server, one per compiled wire.
    fn clients(port: u16) -> Vec<String> {
        let mut urls = vec![format!("tcp://127.0.0.1:{port}"), format!("auto://127.0.0.1:{port}")];
        if cfg!(feature = "zmq") {
            urls.push(format!("zmq+tcp://127.0.0.1:{port}"));
        }
        if cfg!(feature = "grpc") {
            urls.push(format!("grpc://127.0.0.1:{port}"));
        }
        if cfg!(feature = "zenoh") {
            urls.push(format!("zenoh://tcp/127.0.0.1:{port}"));
        }
        urls
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn one_port_every_wire_one_device() {
        let port = free_port();
        tokio::spawn(server().run([format!("auto://127.0.0.1:{port}")]));
        wait(port).await;
        let mut proxies = vec![];
        for url in clients(port) {
            proxies.push(DeviceProxy::connect(&format!("{url}/{DEV}")).await.unwrap());
        }
        // each wire writes, every wire reads the value back: they share the device
        for i in 0..proxies.len() {
            let v = Value::String(format!("written by client {i}"));
            proxies[i].write_attribute("string_scalar", v.clone()).await.unwrap();
            for p in &mut proxies {
                assert_eq!(p.read_attribute("string_scalar").await.unwrap().value, v);
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn several_endpoints_at_once() {
    let ports: Vec<u16> = (0..4).map(|_| free_port()).collect();
    let mut endpoints = vec![];
    if cfg!(feature = "tcp") {
        endpoints.push(format!("tcp://127.0.0.1:{}", ports[0]));
    }
    if cfg!(feature = "zmq") {
        endpoints.push(format!("zmq+tcp://127.0.0.1:{}", ports[1]));
    }
    if cfg!(feature = "grpc") {
        endpoints.push(format!("grpc://127.0.0.1:{}", ports[2]));
    }
    if cfg!(feature = "zenoh") {
        endpoints.push(format!("zenoh://tcp/127.0.0.1:{}", ports[3]));
    }
    tokio::spawn(server().run(endpoints.clone()));
    for e in &endpoints {
        wait(e.rsplit(':').next().unwrap().parse().unwrap()).await;
        let mut dev = DeviceProxy::connect(&format!("{e}/{DEV}")).await.unwrap();
        assert_eq!(dev.name().await.unwrap(), DEV, "{e}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn unknown_scheme() {
    let err = DeviceProxy::connect(&format!("carrier+pigeon://coop/{DEV}")).await.err().unwrap();
    assert_eq!(err.reason(), "API_UnsupportedTransport");
}

/// Built without gRPC (`cargo test` with default features): the scheme is refused, with the
/// feature to enable, by the client and by the server before it binds anything.
#[cfg(not(feature = "grpc"))]
#[tokio::test(flavor = "multi_thread")]
async fn transport_not_compiled_in() {
    let err = DeviceProxy::connect(&format!("grpc://127.0.0.1:1/{DEV}")).await.err().unwrap();
    assert_eq!(err.reason(), "API_UnsupportedTransport");
    assert!(err.0[0].desc.contains("`grpc` feature"), "{err}");
    let err = server().run(["tcp://127.0.0.1:0", "grpc://127.0.0.1:0"]).await.unwrap_err();
    assert_eq!(err.reason(), "API_UnsupportedTransport");
}

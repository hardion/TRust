//! Wire-level tests: the TCP JSON Lines protocol driven without the Rust client.
#![cfg(feature = "tcp")]

mod common;

async fn start() -> String {
    common::start("tcp://127.0.0.1:{port}", "127.0.0.1:{port}").await // no scheme: TCP
}

use common::DEV;
use serde_json::{Value as Json, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use trust::DeviceProxy;

async fn raw(addr: &str, lines: &[Json]) -> Vec<Json> {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let payload: String =
        lines.iter().map(|l| if let Json::String(s) = l { format!("{s}\n") } else { format!("{l}\n") }).collect();
    stream.write_all(payload.as_bytes()).await.unwrap();
    let mut reader = BufReader::new(stream).lines();
    let mut out = vec![];
    for _ in lines {
        out.push(serde_json::from_str(&reader.next_line().await.unwrap().unwrap()).unwrap());
    }
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn raw_socket_requests_are_pipelined() {
    let addr = start().await;
    let replies = raw(
        &addr,
        &[
            json!({"v": 1, "id": 7, "device": DEV, "op": "DeviceName"}),
            json!({"v": 1, "id": 8, "device": DEV, "op": {"CommandExecution": {"name": "DevDouble", "arg": {"Double": 2.5}}}}),
            json!({"v": 1, "id": 9, "client": 42, "source": "CACHE_DEV", "device": DEV, "op": {"AttributesRead": {"names": ["State"]}}}),
        ],
    )
    .await;
    assert_eq!(replies[0], json!({"id": 7, "reply": {"Ok": {"String": DEV}}}));
    assert_eq!(replies[1], json!({"id": 8, "reply": {"Ok": {"Value": {"Double": 2.5}}}}));
    assert_eq!(replies[2]["reply"]["Ok"]["Attributes"][0]["Ok"]["value"], json!({"State": "ON"}));
}

#[tokio::test(flavor = "multi_thread")]
async fn raw_socket_protocol_errors() {
    let addr = start().await;
    let replies = raw(
        &addr,
        &[
            Json::String("this is not json".into()),
            json!({"v": 99, "id": 3, "device": DEV, "op": "DevicePing"}),
            json!({"v": 1, "id": 4, "device": DEV, "op": "DevicePing"}),
        ],
    )
    .await;
    assert_eq!(replies[0]["id"], 0);
    assert_eq!(replies[0]["reply"]["Err"][0]["reason"], "API_InvalidRequest");
    assert_eq!(replies[1]["id"], 3);
    assert_eq!(replies[1]["reply"]["Err"][0]["reason"], "API_UnsupportedProtocol");
    assert_eq!(replies[2], json!({"id": 4, "reply": {"Ok": "Void"}}), "connection survives errors");
}

#[tokio::test(flavor = "multi_thread")]
async fn tango_scheme_prefix() {
    let addr = start().await;
    let mut dev = DeviceProxy::connect(&format!("tango://{addr}/{DEV}")).await.unwrap();
    assert_eq!(dev.name().await.unwrap(), DEV);
}

#[tokio::test(flavor = "multi_thread")]
async fn black_box_records_tcp_peer() {
    let addr = start().await;
    let mut dev = DeviceProxy::connect(&format!("{addr}/{DEV}")).await.unwrap();
    assert!(dev.black_box(1).await.unwrap()[0].host.starts_with("127.0.0.1:"));
}

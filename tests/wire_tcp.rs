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
async fn tango_name_without_database() {
    let addr = start().await;
    let mut dev = DeviceProxy::connect(&format!("tango://{addr}/{DEV}#dbase=no")).await.unwrap();
    assert_eq!(dev.name().await.unwrap(), DEV);
}

#[tokio::test(flavor = "multi_thread")]
async fn black_box_records_tcp_peer() {
    let addr = start().await;
    let mut dev = DeviceProxy::connect(&format!("{addr}/{DEV}")).await.unwrap();
    assert!(dev.black_box(1).await.unwrap()[0].host.starts_with("127.0.0.1:"));
}

/// SPEC §9.2 on the wire: negotiation on the admin device, then a raw event connection; a
/// subscription that is never confirmed is dropped after 3 confirm periods.
#[tokio::test(flavor = "multi_thread")]
async fn raw_event_subscription_expires_without_confirmation() {
    use std::time::Duration;
    use trust::Value;
    let port = common::free_port();
    let server = common::server().event_confirm(Duration::from_millis(100));
    tokio::spawn(server.run([format!("tcp://127.0.0.1:{port}")]));
    while TcpStream::connect(("127.0.0.1", port)).await.is_err() {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let url = format!("tcp://127.0.0.1:{port}");
    let mut admin = DeviceProxy::connect(&format!("{url}/dserver/TgTest/test")).await.unwrap();
    let args = ["sys/tg/1", "ramp", "subscribe", "change", "1"].map(String::from).to_vec();
    let info = admin.command_inout("EventSubscriptionChange", Value::StringArray(args)).await.unwrap();
    let heartbeat = common::HEARTBEAT.as_millis() as i32;
    let channels = ["", "sys/tg/1/ramp.change", "dserver/tgtest/test.heartbeat"].map(String::from).to_vec();
    assert_eq!(info, Value::LongStringArray(vec![1, heartbeat, 100], channels));

    let mut events = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    events.write_all(b"{\"subscribe\": [\"sys/tg/1/ramp.change\"]}\n").await.unwrap();
    let mut lines = BufReader::new(events).lines();
    let mut dev = DeviceProxy::connect(&format!("{url}/{DEV}")).await.unwrap();
    dev.write_attribute("ramp", Value::Double(2.0)).await.unwrap();
    let line = tokio::time::timeout(Duration::from_secs(2), lines.next_line()).await.unwrap().unwrap().unwrap();
    let event: Json = serde_json::from_str(&line).unwrap();
    assert_eq!(event["channel"], "sys/tg/1/ramp.change");
    assert_eq!(event["data"]["Ok"]["Attr"]["value"], json!({"Double": 2.0}));

    tokio::time::sleep(Duration::from_millis(500)).await; // > 3 confirm periods, no EventConfirmSubscription
    dev.write_attribute("ramp", Value::Double(9.0)).await.unwrap();
    assert!(tokio::time::timeout(Duration::from_millis(300), lines.next_line()).await.is_err(), "subscription dropped");
}

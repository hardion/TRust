//! Wire-level tests: the protocol driven with a bare ZeroMQ DEALER socket, without the Rust client.

mod common;

use common::{DEV, start};
use serde_json::{Value as Json, json};
use zeromq::{DealerSocket, Socket, SocketRecv, SocketSend, ZmqMessage};

/// Sends every frame at once on a DEALER socket and returns the replies sorted by request id.
async fn raw(url: &str, frames: &[Json]) -> Vec<Json> {
    let mut socket = DealerSocket::new();
    socket.connect(url.strip_prefix("zmq+").unwrap()).await.unwrap();
    for f in frames {
        let bytes = if let Json::String(s) = f { s.clone().into_bytes() } else { f.to_string().into_bytes() };
        socket.send(ZmqMessage::from(bytes)).await.unwrap();
    }
    let mut out = vec![];
    for _ in frames {
        let msg = socket.recv().await.unwrap().into_vec();
        out.push(serde_json::from_slice::<Json>(msg.last().unwrap()).unwrap());
    }
    out.sort_by_key(|r| r["id"].as_u64());
    out
}

#[tokio::test]
async fn dealer_requests_are_pipelined() {
    let url = start().await;
    let replies = raw(
        &url,
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

#[tokio::test]
async fn protocol_errors() {
    let url = start().await;
    let replies = raw(
        &url,
        &[
            Json::String("this is not json".into()),
            json!({"v": 99, "id": 3, "device": DEV, "op": "DevicePing"}),
            json!({"v": 1, "id": 4, "device": DEV, "op": "DevicePing"}),
        ],
    )
    .await;
    assert_eq!(replies[0]["id"], 0);
    assert_eq!(replies[0]["reply"]["Err"][0]["reason"], "API_InvalidRequest");
    assert_eq!(replies[1]["reply"]["Err"][0]["reason"], "API_UnsupportedProtocol");
    assert_eq!(replies[2], json!({"id": 4, "reply": {"Ok": "Void"}}), "socket survives errors");
}

#[tokio::test]
async fn black_box_records_zmq_peer() {
    let url = start().await;
    let mut dev = trust::DeviceProxy::connect(&format!("{url}/{DEV}")).await.unwrap();
    assert!(dev.black_box(1).await.unwrap()[0].host.starts_with("zmq:"));
}

//! Runs the TgTest device server: `cargo run --example tgtest [endpoint]` (default tcp://127.0.0.1:10000).
//! Any ZeroMQ REQ/DEALER socket can talk to it, e.g. with pyzmq:
//! `s = zmq.Context().socket(zmq.REQ); s.connect("tcp://127.0.0.1:10000")`
//! `s.send_json({"v": 1, "id": 1, "device": "sys/tg/1", "op": "DeviceInfo"}); print(s.recv_json())`

#[path = "../tests/common/mod.rs"]
mod common;

#[tokio::main]
async fn main() -> trust::DevResult<()> {
    let endpoint = std::env::args().nth(1).unwrap_or_else(|| "tcp://127.0.0.1:10000".into());
    println!("TgTest: {} and dserver/TgTest/test bound to {endpoint}", common::DEV);
    common::server().run(&endpoint).await
}

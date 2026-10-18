//! Runs the TgTest device server on the endpoints given as arguments, e.g.
//! `cargo run --features full --example tgtest auto://127.0.0.1:10000 zenoh://`
//! (default: `auto://127.0.0.1:10000` with the `detect` feature, else `tcp://127.0.0.1:10000`).
//! With `auto://`, JSON Lines (`nc`), ZeroMQ, gRPC and Zenoh clients share the one port.

#[path = "../tests/common/mod.rs"]
mod common;

#[tokio::main]
async fn main() -> trust::DevResult<()> {
    let default = if cfg!(feature = "detect") { "auto://127.0.0.1:10000" } else { "tcp://127.0.0.1:10000" };
    let mut endpoints: Vec<String> = std::env::args().skip(1).collect();
    if endpoints.is_empty() {
        endpoints.push(default.into());
    }
    println!("TgTest: {} and dserver/TgTest/test on {endpoints:?}", common::DEV);
    common::server().run(endpoints).await
}

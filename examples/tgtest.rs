//! Runs the TgTest device server: `cargo run --example tgtest [addr]` (default 127.0.0.1:10000),
//! then e.g. `echo '{"v":1,"id":1,"device":"sys/tg/1","op":"DeviceInfo"}' | nc -q1 127.0.0.1 10000`

#[path = "../tests/common/mod.rs"]
mod common;

#[tokio::main]
async fn main() -> trust::DevResult<()> {
    let addr = std::env::args().nth(1).unwrap_or_else(|| "127.0.0.1:10000".into());
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    println!("TgTest: {} and dserver/TgTest/test listening on {addr}", common::DEV);
    common::server().run(listener).await
}

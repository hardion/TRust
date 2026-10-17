//! Runs the TgTest device server on Zenoh: `cargo run --example tgtest [locator]`
//! (default `tcp/127.0.0.1:7447`, multicast scouting on), then connect with
//! `DeviceProxy::connect("zenoh://tcp/127.0.0.1:7447/sys/tg/1")` or, on the same LAN,
//! `DeviceProxy::connect("zenoh:///sys/tg/1")`.

#[path = "../tests/common/mod.rs"]
mod common;

#[tokio::main]
async fn main() -> trust::DevResult<()> {
    let locator = std::env::args().nth(1).unwrap_or_else(|| "tcp/127.0.0.1:7447".into());
    let mut config = zenoh::Config::default();
    config.insert_json5("listen/endpoints", &format!(r#"["{locator}"]"#)).expect("valid locator");
    println!("TgTest: tango/{} and tango/dserver/tgtest/test on {locator}", common::DEV);
    common::server().run(config).await
}

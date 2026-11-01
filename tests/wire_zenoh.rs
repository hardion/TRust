//! Wire-level tests: the protocol driven with a bare Zenoh session, without `DeviceProxy`, and
//! location transparency: a client only knows a router, Zenoh finds the devices.
#![cfg(feature = "zenoh")]

mod common;

async fn start() -> String {
    common::start("zenoh://tcp/127.0.0.1:{port}", "zenoh://tcp/127.0.0.1:{port}").await
}

use common::{DEV, TgTest, free_port, server, zenoh_config};
use serde_json::{Value as Json, json};
use std::time::Duration;
use trust::{DeviceProxy, Server};

async fn session(url: &str) -> zenoh::Session {
    let mut c = zenoh::Config::default();
    c.insert_json5("mode", r#""client""#).unwrap();
    c.insert_json5("connect/endpoints", &format!(r#"["{}"]"#, url.strip_prefix("zenoh://").unwrap())).unwrap();
    c.insert_json5("scouting/multicast/enabled", "false").unwrap();
    zenoh::open(c).await.unwrap()
}

async fn get(s: &zenoh::Session, key: &str, payload: &[u8]) -> Option<Json> {
    let replies = s.get(key).payload(payload.to_vec()).timeout(Duration::from_secs(2)).await.unwrap();
    let reply = replies.recv_async().await.ok()?;
    Some(serde_json::from_slice(&reply.result().unwrap().payload().to_bytes()).unwrap())
}

#[tokio::test(flavor = "multi_thread")]
async fn bare_session_queries() {
    let s = session(&start().await).await;
    let key = format!("tango/{DEV}");
    let r = get(&s, &key, json!({"v": 1, "id": 7, "device": DEV, "op": "DeviceName"}).to_string().as_bytes()).await;
    assert_eq!(r, Some(json!({"id": 7, "reply": {"Ok": {"String": DEV}}})));
    let op = json!({"v": 1, "id": 8, "device": DEV, "op": {"CommandExecution": {"name": "DevDouble", "arg": {"Double": 2.5}}}});
    assert_eq!(get(&s, &key, op.to_string().as_bytes()).await.unwrap()["reply"]["Ok"]["Value"]["Double"], 2.5);
    let r = get(&s, &key, b"this is not json").await.unwrap();
    assert_eq!((r["id"].as_u64(), r["reply"]["Err"][0]["reason"].as_str()), (Some(0), Some("API_InvalidRequest")));
    assert_eq!(get(&s, "tango/no/such/device", b"{}").await, None, "nobody answers");
}

#[tokio::test(flavor = "multi_thread")]
async fn devices_are_found_through_a_router() {
    // a zenoh router, as zenohd would run it
    let router = format!("tcp/127.0.0.1:{}", free_port());
    let mut c = zenoh_config(&router);
    c.insert_json5("mode", r#""router""#).unwrap();
    let _router = zenoh::open(c).await.unwrap();
    // two device servers, each joining the router from its own port
    let join = || {
        let mut c = zenoh_config(&format!("tcp/127.0.0.1:{}", free_port()));
        c.insert_json5("connect/endpoints", &format!(r#"["{router}"]"#)).unwrap();
        c
    };
    tokio::spawn(server().run_zenoh(join()));
    tokio::spawn(Server::new("Other", "b").device("lab/motor/1", TgTest::default()).run_zenoh(join()));
    tokio::time::sleep(Duration::from_millis(500)).await;
    // the client only knows the router
    for dev in [DEV, "lab/motor/1", "dserver/Other/b"] {
        let mut proxy = DeviceProxy::connect(&format!("zenoh://{router}/{dev}")).await.unwrap();
        assert_eq!(proxy.name().await.unwrap(), dev);
    }
}

/// Needs multicast on the host network: `cargo test -- --ignored`.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn devices_are_discovered_by_multicast() {
    let mut c = zenoh::Config::default();
    c.insert_json5("listen/endpoints", &format!(r#"["tcp/127.0.0.1:{}"]"#, free_port())).unwrap();
    tokio::spawn(server().run_zenoh(c));
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut proxy = DeviceProxy::connect(&format!("zenoh:///{DEV}")).await.unwrap();
    assert_eq!(proxy.name().await.unwrap(), DEV);
}

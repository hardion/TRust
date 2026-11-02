//! Admin device (SPEC §8) and the attribute model completions (SPEC §8.1), over TCP.
#![cfg(feature = "tcp")]

mod common;

use common::{DEV, POLL};
use serde_json::{Value as Json, json};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use trust::*;

const ADMIN: &str = "dserver/TgTest/test";

async fn start() -> String {
    common::start("tcp://127.0.0.1:{port}", "tcp://127.0.0.1:{port}").await
}

async fn proxies() -> (String, DeviceProxy, DeviceProxy) {
    let url = start().await;
    let dev = DeviceProxy::connect(&format!("{url}/{DEV}")).await.unwrap();
    let admin = DeviceProxy::connect(&format!("{url}/{ADMIN}")).await.unwrap();
    (url, dev, admin)
}

fn sa(v: &[&str]) -> Value {
    Value::StringArray(v.iter().map(|s| s.to_string()).collect())
}

fn lsa(l: &[i32], s: &[&str]) -> Value {
    Value::LongStringArray(l.to_vec(), s.iter().map(|s| s.to_string()).collect())
}

async fn reason(r: DevResult<impl std::fmt::Debug>) -> String {
    r.expect_err("an error").reason().to_owned()
}

#[tokio::test(flavor = "multi_thread")]
async fn queries() {
    let (_, _, mut admin) = proxies().await;
    assert_eq!(admin.command_inout("QuerySubDevice", Value::Void).await.unwrap(), sa(&[]));
    assert_eq!(admin.command_inout("QueryWizardDevProperty", Value::String("TgTest".into())).await.unwrap(), sa(&[]));
    assert_eq!(admin.command_inout("QueryWizardClassProperty", Value::String("TgTest".into())).await.unwrap(), sa(&[]));
}

#[tokio::test(flavor = "multi_thread")]
async fn polling_commands() {
    let (_, mut dev, mut admin) = proxies().await;
    let obj = |p: i32| lsa(&[p], &[DEV, "attribute", "double_scalar"]);
    assert_eq!(reason(dev.attribute_history("double_scalar", 1).await).await, "API_AttrNotPolled");
    admin.command_inout("AddObjPolling", obj(30)).await.unwrap();
    assert_eq!(reason(admin.command_inout("AddObjPolling", obj(30)).await).await, "API_AlreadyPolled");
    tokio::time::sleep(POLL * 5).await;
    assert!(!dev.attribute_history("double_scalar", 3).await.unwrap().is_empty());
    let polled = admin.command_inout("PolledDevice", Value::Void).await.unwrap();
    assert_eq!(polled, sa(&[DEV]));

    admin.command_inout("UpdObjPollingPeriod", obj(70)).await.unwrap();
    let Value::StringArray(status) = admin.command_inout("DevPollStatus", Value::String(DEV.into())).await.unwrap()
    else {
        panic!()
    };
    let ds = status.iter().find(|s| s.contains("Polled attribute name = double_scalar")).expect("double_scalar polled");
    assert!(ds.contains("Polling period (mS) = 70"), "{ds}");
    assert!(status.iter().any(|s| s.contains("Polled command name = Counter")), "{status:?}");

    admin.command_inout("RemObjPolling", sa(&[DEV, "attribute", "double_scalar"])).await.unwrap();
    assert_eq!(reason(dev.attribute_history("double_scalar", 1).await).await, "API_AttrNotPolled");
    assert_eq!(reason(admin.command_inout("UpdObjPollingPeriod", obj(10)).await).await, "API_NotPolled");

    // StopPolling freezes every ring buffer, StartPolling resumes
    admin.command_inout("StopPolling", Value::Void).await.unwrap();
    tokio::time::sleep(POLL * 2).await;
    let frozen = dev.attribute_history("counter", 1).await.unwrap()[0].time;
    tokio::time::sleep(POLL * 5).await;
    assert_eq!(dev.attribute_history("counter", 1).await.unwrap()[0].time, frozen);
    admin.command_inout("StartPolling", Value::Void).await.unwrap();
    tokio::time::sleep(POLL * 5).await;
    assert!(dev.attribute_history("counter", 1).await.unwrap()[0].time > frozen);
}

#[tokio::test(flavor = "multi_thread")]
async fn restart() {
    let (_, mut dev, mut admin) = proxies().await;
    dev.write_attribute("string_scalar", Value::String("before".into())).await.unwrap();
    let mut interface = dev.event_stream("", EventType::InterfaceChange).await.unwrap();
    interface.recv().await.unwrap();
    admin.command_inout("DevRestart", Value::String(DEV.into())).await.unwrap();
    assert_eq!(dev.read_attribute("string_scalar").await.unwrap().value, Value::String("".into()), "a new device");
    let e = tokio::time::timeout(Duration::from_secs(2), interface.recv()).await.unwrap().unwrap();
    assert!(matches!(e.data, Ok(EventData::Interface { started: true, .. })));
    assert!(!dev.attribute_history("counter", 1).await.unwrap().is_empty(), "polling kept");

    dev.write_attribute("string_scalar", Value::String("again".into())).await.unwrap();
    admin.command_inout("RestartServer", Value::Void).await.unwrap();
    assert_eq!(dev.read_attribute("string_scalar").await.unwrap().value, Value::String("".into()));
    assert_eq!(
        reason(admin.command_inout("DevRestart", Value::String("no/such/device".into())).await).await,
        "API_DeviceNotFound"
    );
}

/// One request with an explicit client id, on a raw connection.
async fn raw(url: &str, client: u64, device: &str, op: Json) -> Json {
    let mut stream = TcpStream::connect(url.trim_start_matches("tcp://")).await.unwrap();
    let frame = json!({"v": 1, "id": 1, "client": client, "device": device, "op": op});
    stream.write_all(format!("{frame}\n").as_bytes()).await.unwrap();
    let line = BufReader::new(stream).lines().next_line().await.unwrap().unwrap();
    serde_json::from_str::<Json>(&line).unwrap()["reply"].clone()
}

fn cmd(name: &str, arg: Json) -> Json {
    json!({"CommandExecution": {"name": name, "arg": arg}})
}

#[tokio::test(flavor = "multi_thread")]
async fn locking() {
    let url = start().await;
    let write = |v: f64| json!({"AttributesWrite": {"values": [["double_scalar", {"Double": v}]]}});
    let lock = cmd("LockDevice", json!({"LongStringArray": [[1], [DEV]]}));
    assert!(raw(&url, 1, ADMIN, lock.clone()).await["Ok"].is_object());

    // client 2 can read but neither write nor run commands; client 1 can
    assert_eq!(raw(&url, 2, DEV, write(2.0)).await["Err"][0]["reason"], "API_DeviceLocked");
    assert_eq!(raw(&url, 2, DEV, cmd("DevVoid", json!("Void"))).await["Err"][0]["reason"], "API_DeviceLocked");
    assert!(raw(&url, 2, DEV, cmd("State", json!("Void"))).await["Ok"].is_object());
    assert!(raw(&url, 2, DEV, json!({"AttributesRead": {"names": ["double_scalar"]}})).await["Ok"].is_object());
    assert_eq!(raw(&url, 1, DEV, write(1.0)).await, json!({"Ok": "Void"}));

    let status = raw(&url, 2, ADMIN, cmd("DevLockStatus", json!({"String": DEV}))).await;
    let longs = &status["Ok"]["Value"]["LongStringArray"][0];
    assert_eq!((&longs[0], &longs[1]), (&json!(1), &json!(1)), "locked, by client 1: {status}");
    let unlock = |force: i32| cmd("UnLockDevice", json!({"LongStringArray": [[force], [DEV]]}));
    assert_eq!(raw(&url, 2, ADMIN, unlock(0)).await["Err"][0]["reason"], "API_DeviceLocked");
    assert_eq!(raw(&url, 2, ADMIN, unlock(1)).await, json!({"Ok": {"Value": {"Long": 1}}}), "forced");
    assert_eq!(raw(&url, 2, DEV, write(3.0)).await, json!({"Ok": "Void"}));

    // ReLockDevices extends, the validity (1 s) then expires
    raw(&url, 1, ADMIN, lock).await;
    tokio::time::sleep(Duration::from_millis(600)).await;
    raw(&url, 1, ADMIN, cmd("ReLockDevices", json!({"StringArray": [DEV]}))).await;
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(raw(&url, 2, DEV, write(4.0)).await["Err"][0]["reason"], "API_DeviceLocked", "relocked");
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(raw(&url, 2, DEV, write(5.0)).await, json!({"Ok": "Void"}), "expired");

    // the client API locks for the proxy's client id
    let mut dev = DeviceProxy::connect(&format!("{url}/{DEV}")).await.unwrap();
    dev.lock(Duration::from_secs(10)).await.unwrap();
    assert_eq!(raw(&url, 2, DEV, write(6.0)).await["Err"][0]["reason"], "API_DeviceLocked");
    dev.write_attribute("double_scalar", Value::Double(7.0)).await.unwrap();
    dev.unlock().await.unwrap();
    assert_eq!(raw(&url, 2, DEV, write(8.0)).await, json!({"Ok": "Void"}));
}

#[tokio::test(flavor = "multi_thread")]
async fn logging() {
    let (_, mut dev, mut admin) = proxies().await;
    let log = std::env::temp_dir().join(format!("trust-log-{}.txt", std::process::id()));
    let _ = std::fs::remove_file(&log);
    let target = format!("file::{}", log.display());
    let read = || std::fs::read_to_string(&log).unwrap_or_default();

    assert_eq!(admin.command_inout("GetLoggingLevel", sa(&[DEV])).await.unwrap(), lsa(&[3], &[DEV]));
    admin.command_inout("AddLoggingTarget", sa(&[DEV, &target])).await.unwrap();
    assert_eq!(admin.command_inout("GetLoggingTarget", Value::String(DEV.into())).await.unwrap(), sa(&[&target]));
    dev.command_inout("LogMessage", Value::String("hello at info".into())).await.unwrap();
    dev.command_inout("DevVoid", Value::Void).await.unwrap();
    assert!(!read().contains("hello at info"), "INFO is below the default WARN level");

    admin.command_inout("SetLoggingLevel", lsa(&[5], &[DEV])).await.unwrap();
    dev.command_inout("LogMessage", Value::String("hello at info".into())).await.unwrap();
    dev.command_inout("DevVoid", Value::Void).await.unwrap();
    let text = read();
    assert!(text.contains("INFO") && text.contains("hello at info"), "{text}");
    assert!(text.contains("DEBUG") && text.contains("DevVoid"), "requests logged at DEBUG: {text}");

    admin.command_inout("StopLogging", Value::Void).await.unwrap();
    dev.command_inout("LogMessage", Value::String("while stopped".into())).await.unwrap();
    admin.command_inout("StartLogging", Value::Void).await.unwrap();
    admin.command_inout("RemoveLoggingTarget", sa(&[DEV, &target])).await.unwrap();
    dev.command_inout("LogMessage", Value::String("after removal".into())).await.unwrap();
    let text = read();
    assert!(!text.contains("while stopped") && !text.contains("after removal"), "{text}");
}

#[tokio::test(flavor = "multi_thread")]
async fn enumerations() {
    let (_, mut dev, _) = proxies().await;
    let cfg = dev.get_attribute_config(&["mode"]).await.unwrap().remove(0);
    assert_eq!(cfg.enum_labels, ["OFF", "ON", "AUTO"]);
    dev.write_attribute("mode", Value::Short(2)).await.unwrap();
    assert_eq!(dev.read_attribute("mode").await.unwrap().value, Value::Short(2));
    assert_eq!(reason(dev.write_attribute("mode", Value::Short(3)).await).await, "API_WAttrOutsideLimit");
    let labels = ["STOP", "RUN", "AUTO", "MANUAL"].map(String::from).to_vec();
    dev.set_attribute_config(vec![AttrInfo { enum_labels: labels.clone(), ..cfg }]).await.unwrap();
    assert_eq!(dev.get_attribute_config(&["mode"]).await.unwrap()[0].enum_labels, labels);
    dev.write_attribute("mode", Value::Short(3)).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn read_with_write() {
    let (_, mut dev, _) = proxies().await;
    dev.write_attribute("ramp", Value::Double(3.0)).await.unwrap();
    let v = dev.read_attribute("ramp_target").await.unwrap();
    assert_eq!((v.value, v.w_value), (Value::Double(3.0), Some(Value::Double(3.0))));
    assert_eq!(reason(dev.write_attribute("ramp_target", Value::Double(1.0)).await).await, "API_AttrNotWritable");
}

#[tokio::test(flavor = "multi_thread")]
async fn nested_pipe_blob() {
    let (_, mut dev, _) = proxies().await;
    let inner = PipeBlob { name: "inner".into(), elements: vec![("y".into(), Value::LongArray(vec![1, 2]))] };
    let blob = PipeBlob {
        name: "outer".into(),
        elements: vec![("x".into(), Value::Double(1.0)), ("sub".into(), Value::PipeBlob(inner))],
    };
    assert_eq!(dev.write_read_pipe("blob", blob.clone()).await.unwrap().blob, blob);
}

#[tokio::test(flavor = "multi_thread")]
async fn one_error_per_failed_attribute() {
    let (_, mut dev, _) = proxies().await;
    let err = dev
        .write_attributes(&[
            ("nope", Value::Void),
            ("double_scalar", Value::Double(1.0)),
            ("long_spectrum", Value::Void),
        ])
        .await
        .unwrap_err();
    let origins: Vec<_> = err.0.iter().map(|e| e.origin.as_str()).collect();
    assert_eq!(origins, ["nope", "long_spectrum"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn dynamic_attributes() {
    let (_, mut dev, _) = proxies().await;
    let mut interface = dev.event_stream("", EventType::InterfaceChange).await.unwrap();
    interface.recv().await.unwrap();
    dev.command_inout("AddDynamic", Value::String("dyn_1".into())).await.unwrap();
    let e = tokio::time::timeout(Duration::from_secs(2), interface.recv()).await.unwrap().unwrap();
    let Ok(EventData::Interface { attributes, started: false, .. }) = e.data else { panic!("{e:?}") };
    assert!(attributes.iter().any(|a| a.name == "dyn_1"));
    assert_eq!(dev.read_attribute("dyn_1").await.unwrap().value, Value::Double(0.0));

    // the configuration of the attributes already known is kept
    let mut cfg = dev.get_attribute_config(&["ramp"]).await.unwrap().remove(0);
    cfg.label = "kept".into();
    dev.set_attribute_config(vec![cfg]).await.unwrap();
    dev.command_inout("RemoveDynamic", Value::String("dyn_1".into())).await.unwrap();
    assert_eq!(reason(dev.read_attribute("dyn_1").await).await, "API_AttrNotFound");
    assert_eq!(dev.get_attribute_config(&["ramp"]).await.unwrap()[0].label, "kept");
}

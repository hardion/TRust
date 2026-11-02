//! Device server life cycle with a Database (SPEC §6, §7.1, §7.3): devices created from the
//! Database, properties, export with the real port, name resolution, unexport on Kill.

mod common;

use common::*;
use std::time::Duration;
use trust::{DevState, DeviceProxy, Value};

/// Registers MotorServer/lab with `devices` and their `Port` property.
async fn register(host: &str, devices: &[(&str, Option<&str>)]) -> DeviceProxy {
    let mut db = db(host).await;
    for (dev, port) in devices {
        cmd(&mut db, "DbAddDevice", sa(&["MotorServer/lab", dev, "Motor"])).await;
        if let Some(p) = port {
            cmd(&mut db, "DbPutDeviceProperty", sa(&[dev, "1", "Port", "1", p])).await;
        }
    }
    db
}

async fn read(dev: &mut DeviceProxy, attr: &str) -> Value {
    dev.read_attribute(attr).await.unwrap().value
}

#[tokio::test(flavor = "multi_thread")]
async fn devices_and_properties_come_from_the_database() {
    let host = start_db().await;
    let mut db =
        register(&host, &[("lab/motor/1", Some("COM1")), ("lab/motor/2", None), ("lab/motor/3", Some("X"))]).await;
    cmd(&mut db, "DbPutDeviceProperty", sa(&["lab/motor/1", "1", "Speed", "1", "3"])).await;
    cmd(&mut db, "DbPutClassProperty", sa(&["Motor", "1", "Speed", "1", "7"])).await;
    cmd(&mut db, "DbPutDeviceAttributeProperty2", sa(&["lab/motor/1", "1", "position", "1", "label", "1", "Pos"]))
        .await;
    tokio::spawn(motor_server(&host).run(["auto://127.0.0.1:0"]));
    wait_exported(&mut db, "lab/motor/1", true).await;

    let mut m1 = DeviceProxy::connect(&format!("tango://{host}/lab/motor/1")).await.unwrap();
    assert_eq!(read(&mut m1, "speed").await, Value::Double(3.0), "device property");
    assert_eq!(read(&mut m1, "port").await, Value::String("COM1".into()));
    let mut m3 = DeviceProxy::connect(&format!("tango://{host}/lab/motor/3")).await.unwrap();
    assert_eq!(read(&mut m3, "speed").await, Value::Double(7.0), "class property over the default");
    cmd(&mut db, "DbDeleteClassProperty", sa(&["Motor", "Speed"])).await;
    m3.command_inout("Init", Value::Void).await.unwrap();
    assert_eq!(read(&mut m3, "speed").await, Value::Double(1.5), "default, re-read at Init");

    // a missing mandatory property puts the device in FAULT
    let mut m2 = DeviceProxy::connect(&format!("tango://{host}/lab/motor/2")).await.unwrap();
    assert_eq!(m2.command_inout("State", Value::Void).await.unwrap(), Value::State(DevState::Fault));
    let Value::String(status) = m2.command_inout("Status", Value::Void).await.unwrap() else { panic!() };
    assert!(status.contains("Port"), "{status}");

    // attribute properties: from the Database at export, back to it on set config
    let mut cfg = m1.get_attribute_config(&["position"]).await.unwrap().remove(0);
    assert_eq!(cfg.label, "Pos");
    cfg.label = "Position".into();
    cfg.unit = "mm".into();
    m1.set_attribute_config(vec![cfg]).await.unwrap();
    let get = sa(&["lab/motor/1", "position"]);
    for _ in 0..200 {
        let props = list(&mut db, "DbGetDeviceAttributeProperty2", get.clone()).await;
        if props.contains(&"Position".into()) && props.contains(&"mm".into()) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("attribute config not written back");
}

#[tokio::test(flavor = "multi_thread")]
async fn export_resolution_and_kill() {
    let host = start_db().await;
    let mut db = register(&host, &[("lab/motor/1", Some("COM1"))]).await;
    cmd(&mut db, "DbPutDeviceAlias", sa(&["lab/motor/1", "m1"])).await;
    let server = tokio::spawn(motor_server(&host).run(["auto://127.0.0.1:0"]));
    wait_exported(&mut db, "lab/motor/1", true).await;
    wait_exported(&mut db, "dserver/MotorServer/lab", true).await;

    // exported with the real port, not 0
    let Value::LongStringArray(l, s) = cmd(&mut db, "DbImportDevice", Value::String("lab/motor/1".into())).await else {
        panic!()
    };
    assert_eq!(l[1], std::process::id() as i32, "pid");
    assert!(s[1].starts_with("auto://127.0.0.1:") && !s[1].ends_with(":0"), "{}", s[1]);
    let direct = format!("tango://{}/lab/motor/1#dbase=no", s[1].trim_start_matches("auto://"));

    for url in [format!("tango://{host}/lab/motor/1"), format!("tango://{host}/m1"), direct] {
        let mut m = DeviceProxy::connect(&url).await.unwrap_or_else(|e| panic!("{url}: {e}"));
        assert_eq!(m.name().await.unwrap(), "lab/motor/1", "{url}");
    }
    assert_eq!(
        DeviceProxy::connect(&format!("tango://{host}/no/such/device")).await.err().unwrap().reason(),
        "DB_DeviceNotDefined"
    );

    // Kill: the server unexports its devices and returns
    let mut admin = DeviceProxy::connect(&format!("tango://{host}/dserver/MotorServer/lab")).await.unwrap();
    admin.command_inout("Kill", Value::Void).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), server).await.expect("server stops").unwrap().unwrap();
    wait_exported(&mut db, "lab/motor/1", false).await;
    let err = DeviceProxy::connect(&format!("tango://{host}/lab/motor/1")).await.err().unwrap();
    assert_eq!(err.reason(), "API_DeviceNotExported");
}

#[tokio::test(flavor = "multi_thread")]
async fn client_reimports_after_a_restart() {
    let host = start_db().await;
    let mut db = register(&host, &[("lab/motor/1", Some("COM1"))]).await;
    let first = tokio::spawn(motor_server(&host).run(["auto://127.0.0.1:0"]));
    wait_exported(&mut db, "lab/motor/1", true).await;
    let mut m = DeviceProxy::connect(&format!("tango://{host}/lab/motor/1")).await.unwrap();
    m.write_attribute("position", Value::Double(4.0)).await.unwrap();

    let mut admin = DeviceProxy::connect(&format!("tango://{host}/dserver/MotorServer/lab")).await.unwrap();
    admin.command_inout("Kill", Value::Void).await.unwrap();
    first.await.unwrap().unwrap();
    tokio::spawn(motor_server(&host).run(["auto://127.0.0.1:0"]));
    wait_exported(&mut db, "lab/motor/1", true).await;
    // same proxy, new port: one re-import
    assert_eq!(m.name().await.unwrap(), "lab/motor/1");
}

#[tokio::test(flavor = "multi_thread")]
async fn devices_added_in_code_are_registered() {
    let host = start_db().await;
    let mut db = db(&host).await;
    let server = motor_server(&host).device("lab/motor/9", Motor::default());
    tokio::spawn(server.run(["auto://127.0.0.1:0"]));
    wait_exported(&mut db, "lab/motor/9", true).await;
    assert_eq!(list(&mut db, "DbGetDeviceList", sa(&["MotorServer/lab", "Motor"])).await, ["lab/motor/9"]);
    assert!(list(&mut db, "DbGetHostList", Value::String("*".into())).await.len() == 1);
}

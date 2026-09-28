//! Validation tests: one group per row of the Tango Request-Reply cheatsheet (see docs/SPEC.md).

mod common;

use common::{DEV, POLL, start};
use serde_json::{Value as Json, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use trust::*;

async fn proxy() -> DeviceProxy {
    DeviceProxy::connect(&format!("{}/{DEV}", start().await)).await.unwrap()
}

async fn wait_polling() {
    tokio::time::sleep(POLL * 8).await;
}

// ---------------------------------------------------------------- device

#[tokio::test]
async fn device_identity() {
    let mut dev = proxy().await;
    assert_eq!(dev.name().await.unwrap(), DEV);
    assert_eq!(dev.description().await.unwrap(), "A TRust test device");
    assert_eq!(dev.adm_name().await.unwrap(), "dserver/TgTest/test");
}

#[tokio::test]
async fn device_names_are_case_insensitive() {
    let addr = start().await;
    let mut dev = DeviceProxy::connect(&format!("tango://{addr}/SYS/TG/1")).await.unwrap();
    assert_eq!(dev.name().await.unwrap(), DEV);
    assert_eq!(dev.command_inout("devdouble", Value::Double(1.5)).await.unwrap(), Value::Double(1.5));
    assert!(dev.read_attribute("Double_Scalar").await.is_ok());
}

#[tokio::test]
async fn ping() {
    let mut dev = proxy().await;
    assert!(dev.ping().await.unwrap() < std::time::Duration::from_secs(1));
}

#[tokio::test]
async fn black_box() {
    let mut dev = proxy().await;
    dev.name().await.unwrap();
    dev.ping().await.unwrap();
    let records = dev.black_box(3).await.unwrap();
    let ops: Vec<_> = records.iter().map(|r| r.operation.as_str()).collect();
    assert_eq!(ops, ["DeviceBlackbox", "DevicePing", "DeviceName"]);
    assert!(records.iter().all(|r| r.client_id == std::process::id() as u64 && r.source == Source::Dev));
    assert!(records[0].host.starts_with("127.0.0.1"));
    assert!(records[0].time >= records[1].time);
    assert_eq!(dev.black_box(100).await.unwrap().len(), 4, "limited to what is stored");
    assert_eq!(dev.black_box(0).await.unwrap_err().reason(), "API_BlackBoxArgument");
}

#[tokio::test]
async fn info() {
    let mut dev = proxy().await;
    let info = dev.info().await.unwrap();
    assert_eq!(info.dev_class, "TgTest");
    assert_eq!(info.server_id, "TgTest/test");
    assert_eq!(info.server_version, PROTOCOL_VERSION);
    assert!(!info.server_host.is_empty());
}

#[tokio::test]
async fn unknown_device() {
    let addr = start().await;
    let mut dev = DeviceProxy::connect(&format!("{addr}/no/such/device")).await.unwrap();
    assert_eq!(dev.ping().await.unwrap_err().reason(), "API_DeviceNotFound");
}

#[tokio::test]
async fn admin_device() {
    let addr = start().await;
    let mut dev = DeviceProxy::connect(&format!("{addr}/{DEV}")).await.unwrap();
    let adm = dev.adm_name().await.unwrap();
    let mut admin = DeviceProxy::connect(&format!("{addr}/{adm}")).await.unwrap();
    assert_eq!(admin.adm_name().await.unwrap(), adm);
    assert_eq!(admin.info().await.unwrap().dev_class, "DServer");
    assert_eq!(
        admin.command_inout("QueryClass", Value::Void).await.unwrap(),
        Value::StringArray(vec!["TgTest".into()])
    );
    assert_eq!(
        admin.command_inout("QueryDevice", Value::Void).await.unwrap(),
        Value::StringArray(vec![format!("TgTest::{DEV}")])
    );
}

// ---------------------------------------------------------------- commands

#[tokio::test]
async fn command_execution() {
    let mut dev = proxy().await;
    assert_eq!(dev.command_inout("DevVoid", Value::Void).await.unwrap(), Value::Void);
    assert_eq!(dev.command_inout("DevDouble", Value::Double(1.25)).await.unwrap(), Value::Double(1.25));
    assert_eq!(dev.command_inout("DevString", Value::String("hi".into())).await.unwrap(), Value::String("hi".into()));
    let longs = Value::LongArray(vec![1, -2, 3]);
    assert_eq!(dev.command_inout("DevVarLongArray", longs.clone()).await.unwrap(), longs);
}

#[tokio::test]
async fn command_errors() {
    let mut dev = proxy().await;
    let err = dev.command_inout("Throw", Value::Void).await.unwrap_err();
    assert_eq!(err.reason(), "TG_Thrown");
    assert_eq!(err.0[0].severity, ErrSeverity::Err);
    assert_eq!(dev.command_inout("Nope", Value::Void).await.unwrap_err().reason(), "API_CommandNotFound");
    let err = dev.command_inout("DevDouble", Value::Long(1)).await.unwrap_err();
    assert_eq!(err.reason(), "API_IncompatibleCmdArgumentType");
}

#[tokio::test]
async fn builtin_commands() {
    let mut dev = proxy().await;
    assert_eq!(dev.command_inout("State", Value::Void).await.unwrap(), Value::State(DevState::On));
    assert_eq!(
        dev.command_inout("Status", Value::Void).await.unwrap(),
        Value::String("The device is in ON state.".into())
    );
    assert_eq!(dev.command_inout("Init", Value::Void).await.unwrap(), Value::Void);
}

#[tokio::test]
async fn command_list_and_query() {
    let mut dev = proxy().await;
    let cmds = dev.command_list_query().await.unwrap();
    let names: Vec<_> = cmds.iter().map(|c| c.name.as_str()).collect();
    for n in ["Init", "State", "Status", "DevVoid", "DevDouble", "Throw", "Counter"] {
        assert!(names.contains(&n), "{n} missing in {names:?}");
    }
    let info = dev.command_query("DevVarLongArray").await.unwrap();
    assert_eq!((info.in_type, info.out_type), (DataType::LongArray, DataType::LongArray));
    let state = dev.command_query("State").await.unwrap();
    assert_eq!((state.in_type, state.out_type), (DataType::Void, DataType::State));
    assert_eq!(dev.command_query("Nope").await.unwrap_err().reason(), "API_CommandNotFound");
}

#[tokio::test]
async fn command_history() {
    let mut dev = proxy().await;
    wait_polling().await;
    let hist = dev.command_history("Counter", 3).await.unwrap();
    assert_eq!(hist.len(), 3);
    let values: Vec<_> = hist.iter().map(|h| h.value.clone().unwrap()).collect();
    let (Value::Long(a), Value::Long(b)) = (&values[0], &values[1]) else { panic!("{values:?}") };
    assert!(a > b, "newest first");
    assert!(hist[0].time >= hist[1].time);
    assert!(dev.command_history("Counter", 1000).await.unwrap().len() <= 10, "ring depth");
    assert_eq!(dev.command_history("DevVoid", 3).await.unwrap_err().reason(), "API_CmdNotPolled");
}

// ---------------------------------------------------------------- attributes

#[tokio::test]
async fn read_attributes() {
    let mut dev = proxy().await;
    let values =
        dev.read_attributes(&["State", "Status", "long_spectrum", "short_image", "nope", "faulty"]).await.unwrap();
    assert_eq!(values.len(), 6);
    let v = |i: usize| values[i].as_ref().unwrap();
    assert_eq!(v(0).value, Value::State(DevState::On));
    assert_eq!((v(0).dim_x, v(0).dim_y), (1, 0));
    assert_eq!(v(1).value, Value::String("The device is in ON state.".into()));
    assert_eq!(v(2).value, Value::LongArray(vec![1, 2, 3]));
    assert_eq!((v(2).dim_x, v(2).dim_y), (3, 0));
    assert_eq!((v(3).dim_x, v(3).dim_y), (2, 2));
    assert_eq!(v(2).name, "long_spectrum");
    assert_eq!(v(2).quality, AttrQuality::Valid);
    assert_eq!(values[4].as_ref().unwrap_err().reason(), "API_AttrNotFound");
    assert_eq!(values[5].as_ref().unwrap_err().reason(), "TG_ReadFailed");
}

#[tokio::test]
async fn write_attributes() {
    let mut dev = proxy().await;
    dev.write_attributes(&[("double_scalar", Value::Double(12.5)), ("string_scalar", Value::String("x".into()))])
        .await
        .unwrap();
    let v = dev.read_attribute("double_scalar").await.unwrap();
    assert_eq!(v.value, Value::Double(12.5));
    assert_eq!(v.w_value, Some(Value::Double(12.5)));
    assert_eq!(dev.read_attribute("string_scalar").await.unwrap().value, Value::String("x".into()));
}

#[tokio::test]
async fn write_errors() {
    let mut dev = proxy().await;
    let reason = |e: DevFailed| e.reason().to_owned();
    let w = |name: &'static str, v: Value| (name, v);
    assert_eq!(
        reason(dev.write_attribute("long_spectrum", Value::LongArray(vec![1])).await.unwrap_err()),
        "API_AttrNotWritable"
    );
    assert_eq!(
        reason(dev.write_attribute("State", Value::State(DevState::Off)).await.unwrap_err()),
        "API_AttrNotWritable"
    );
    assert_eq!(
        reason(dev.write_attribute("double_scalar", Value::Long(1)).await.unwrap_err()),
        "API_IncompatibleAttrDataType"
    );
    assert_eq!(
        reason(dev.write_attribute("double_scalar", Value::Double(1e3)).await.unwrap_err()),
        "API_WAttrOutsideLimit"
    );
    assert_eq!(reason(dev.write_attribute("nope", Value::Double(1.0)).await.unwrap_err()), "API_AttrNotFound");
    // errors are aggregated, valid writes are applied
    let err = dev
        .write_attributes(&[
            w("nope", Value::Void),
            w("double_scalar", Value::Double(7.0)),
            w("long_spectrum", Value::Void),
        ])
        .await
        .unwrap_err();
    assert_eq!(err.0.len(), 2);
    assert_eq!(dev.read_attribute("double_scalar").await.unwrap().value, Value::Double(7.0));
}

#[tokio::test]
async fn write_read_attributes() {
    let mut dev = proxy().await;
    let values = dev
        .write_read_attributes(&[("double_scalar", Value::Double(-3.0))], &["double_scalar", "long_spectrum"])
        .await
        .unwrap();
    assert_eq!(values[0].as_ref().unwrap().value, Value::Double(-3.0));
    assert_eq!(values[1].as_ref().unwrap().value, Value::LongArray(vec![1, 2, 3]));
    let err = dev.write_read_attributes(&[("long_spectrum", Value::Void)], &["double_scalar"]).await.unwrap_err();
    assert_eq!(err.reason(), "API_AttrNotWritable");
}

#[tokio::test]
async fn attribute_quality_and_alarm_state() {
    let mut dev = proxy().await;
    for (v, quality, state) in [
        (10.0, AttrQuality::Valid, DevState::On),
        (45.0, AttrQuality::Warning, DevState::Alarm),
        (60.0, AttrQuality::Alarm, DevState::Alarm),
    ] {
        dev.write_attribute("double_scalar", Value::Double(v)).await.unwrap();
        assert_eq!(dev.read_attribute("double_scalar").await.unwrap().quality, quality);
        assert_eq!(dev.read_attribute("State").await.unwrap().value, Value::State(state));
        assert_eq!(dev.command_inout("State", Value::Void).await.unwrap(), Value::State(state));
    }
}

#[tokio::test]
async fn attribute_get_config() {
    let mut dev = proxy().await;
    let all = dev.get_attribute_config(&[]).await.unwrap();
    let names: Vec<_> = all.iter().map(|a| a.name.as_str()).collect();
    for n in ["State", "Status", "double_scalar", "long_spectrum", "short_image", "counter"] {
        assert!(names.contains(&n), "{n} missing in {names:?}");
    }
    let [ds, state] = &dev.get_attribute_config(&["double_scalar", "State"]).await.unwrap()[..] else { panic!() };
    assert_eq!(
        (ds.data_type, ds.data_format, ds.writable),
        (DataType::Double, DataFormat::Scalar, Writable::ReadWrite)
    );
    assert_eq!((ds.unit.as_str(), ds.max_alarm), ("mm", Some(50.0)));
    assert_eq!((state.data_type, state.writable), (DataType::State, Writable::Read));
    assert_eq!(dev.get_attribute_config(&["nope"]).await.unwrap_err().reason(), "API_AttrNotFound");
}

#[tokio::test]
async fn attribute_set_config() {
    let mut dev = proxy().await;
    let mut cfg = dev.get_attribute_config(&["double_scalar"]).await.unwrap().remove(0);
    cfg.label = "Position".into();
    cfg.max_value = Some(1000.0);
    cfg.max_alarm = None;
    cfg.max_warning = None;
    cfg.writable = Writable::Read; // static metadata: ignored
    let new = dev.set_attribute_config(vec![cfg.clone()]).await.unwrap().remove(0);
    assert_eq!(new.label, "Position");
    assert_eq!(new.writable, Writable::ReadWrite);
    assert_eq!(dev.get_attribute_config(&["double_scalar"]).await.unwrap()[0], new);
    // new limits are enforced
    dev.write_attribute("double_scalar", Value::Double(500.0)).await.unwrap();
    assert_eq!(dev.read_attribute("double_scalar").await.unwrap().quality, AttrQuality::Valid);
    cfg.min_value = Some(10.0);
    cfg.max_value = Some(0.0);
    assert_eq!(dev.set_attribute_config(vec![cfg]).await.unwrap_err().reason(), "API_IncoherentValues");
}

#[tokio::test]
async fn attribute_history() {
    let mut dev = proxy().await;
    wait_polling().await;
    let hist = dev.attribute_history("counter", 4).await.unwrap();
    assert_eq!(hist.len(), 4);
    let v: Vec<_> = hist.iter().map(|h| h.value.as_ref().unwrap().value.clone()).collect();
    let (Value::Long(a), Value::Long(b)) = (&v[0], &v[1]) else { panic!("{v:?}") };
    assert!(a > b, "newest first");
    assert!(dev.attribute_history("counter", 1000).await.unwrap().len() <= 10, "ring depth");
    let faulty = dev.attribute_history("faulty", 2).await.unwrap();
    assert_eq!(faulty[0].value.as_ref().unwrap_err().reason(), "TG_ReadFailed");
    assert_eq!(dev.attribute_history("double_scalar", 2).await.unwrap_err().reason(), "API_AttrNotPolled");
}

// ---------------------------------------------------------------- source

#[tokio::test]
async fn source_cache() {
    let mut dev = proxy().await;
    wait_polling().await;
    dev.set_source(Source::Cache);
    let v = dev.read_attributes(&["counter", "double_scalar"]).await.unwrap();
    assert!(v[0].is_ok());
    assert_eq!(v[1].as_ref().unwrap_err().reason(), "API_AttrNotPolled");
    assert!(matches!(dev.command_inout("Counter", Value::Void).await.unwrap(), Value::Long(_)));
    assert_eq!(dev.command_inout("DevVoid", Value::Void).await.unwrap_err().reason(), "API_CmdNotPolled");
    // the cached value is the last polled one
    let cached = dev.read_attribute("counter").await.unwrap().value;
    let last = dev.attribute_history("counter", 1).await.unwrap().remove(0).value.unwrap().value;
    assert!(cached == last || matches!((&cached, &last), (Value::Long(c), Value::Long(l)) if c + 1 == *l));

    dev.set_source(Source::CacheDev);
    assert!(dev.read_attributes(&["counter", "double_scalar"]).await.unwrap().iter().all(Result::is_ok));
    assert_eq!(dev.command_inout("DevVoid", Value::Void).await.unwrap(), Value::Void);
}

// ---------------------------------------------------------------- pipes

#[tokio::test]
async fn pipe_read_write() {
    let mut dev = proxy().await;
    let p = dev.read_pipe("blob").await.unwrap();
    assert_eq!(p.name, "blob");
    assert_eq!(p.blob.elements[0], ("x".into(), Value::Double(1.0)));
    let blob = PipeBlob { name: "new".into(), elements: vec![("y".into(), Value::LongArray(vec![4, 5]))] };
    dev.write_pipe("blob", blob.clone()).await.unwrap();
    assert_eq!(dev.read_pipe("blob").await.unwrap().blob, blob);
    let blob2 = PipeBlob { name: "wr".into(), elements: vec![] };
    assert_eq!(dev.write_read_pipe("blob", blob2.clone()).await.unwrap().blob, blob2);
    assert_eq!(dev.write_pipe("ro_pipe", blob).await.unwrap_err().reason(), "API_PipeNotWritable");
    assert_eq!(dev.read_pipe("nope").await.unwrap_err().reason(), "API_PipeNotFound");
}

#[tokio::test]
async fn pipe_config() {
    let mut dev = proxy().await;
    assert_eq!(dev.get_pipe_config(&[]).await.unwrap().len(), 2);
    let mut cfg = dev.get_pipe_config(&["blob"]).await.unwrap().remove(0);
    assert!(cfg.writable);
    cfg.label = "Blob".into();
    cfg.description = "a blob".into();
    cfg.writable = false; // static: ignored
    let new = dev.set_pipe_config(vec![cfg]).await.unwrap().remove(0);
    assert_eq!((new.label.as_str(), new.description.as_str(), new.writable), ("Blob", "a blob", true));
    assert_eq!(dev.get_pipe_config(&["nope"]).await.unwrap_err().reason(), "API_PipeNotFound");
}

// ---------------------------------------------------------------- raw socket protocol

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

#[tokio::test]
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

#[tokio::test]
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

#[tokio::test]
async fn concurrent_clients() {
    let addr = start().await;
    let tasks: Vec<_> = (0..10)
        .map(|i| {
            let addr = addr.clone();
            tokio::spawn(async move {
                let mut dev = DeviceProxy::connect(&format!("{addr}/{DEV}")).await.unwrap();
                for j in 0..20 {
                    let v = Value::Double((i * 100 + j) as f64);
                    assert_eq!(dev.command_inout("DevDouble", v.clone()).await.unwrap(), v);
                }
            })
        })
        .collect();
    for t in tasks {
        t.await.unwrap();
    }
}

// Event tests (SPEC §9), included by tests/events.rs once per transport: `start()` returns the
// endpoint URL of a fresh TgTest server (heartbeat `HEARTBEAT`).

use crate::common::{DEV, HEARTBEAT, POLL};
use std::time::Duration;
use trust::*;

async fn proxy() -> DeviceProxy {
    DeviceProxy::connect(&format!("{}/{DEV}", start().await)).await.unwrap()
}

async fn next(s: &mut EventStream) -> Event {
    tokio::time::timeout(Duration::from_secs(3), s.recv()).await.expect("event in time").expect("stream open")
}

async fn quiet(s: &mut EventStream) {
    if let Ok(e) = tokio::time::timeout(POLL * 8, s.recv()).await {
        panic!("unexpected event {e:?}");
    }
}

fn value(e: &Event) -> Value {
    match e.data.as_ref() {
        Ok(EventData::Attr(a)) => a.value.clone(),
        other => panic!("expected an attribute value, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn change_event() {
    let mut dev = proxy().await;
    let mut s = dev.event_stream("ramp", EventType::Change).await.unwrap();
    let first = next(&mut s).await;
    assert_eq!((first.seq, value(&first)), (0, Value::Double(0.0)), "synchronous first event");
    assert_eq!((first.channel.as_str(), first.event_type), (format!("{DEV}/ramp.change").as_str(), EventType::Change));
    dev.write_attribute("ramp", Value::Double(0.5)).await.unwrap();
    quiet(&mut s).await; // below abs_change = 1
    dev.write_attribute("ramp", Value::Double(2.0)).await.unwrap();
    let e = next(&mut s).await;
    assert_eq!(value(&e), Value::Double(2.0));
    dev.write_attribute("ramp", Value::Double(3.5)).await.unwrap();
    let e2 = next(&mut s).await;
    assert_eq!((value(&e2), e2.seq), (Value::Double(3.5), e.seq + 1));
}

#[tokio::test(flavor = "multi_thread")]
async fn periodic_event() {
    let mut dev = proxy().await;
    let mut s = dev.event_stream("counter", EventType::Periodic).await.unwrap();
    next(&mut s).await;
    let (a, b) = (next(&mut s).await, next(&mut s).await);
    assert_eq!(b.seq, a.seq + 1);
    assert!(b.time - a.time >= 0.08, "period 100 ms, got {}", b.time - a.time);
    assert!(matches!(value(&b), Value::Long(_)));
}

#[tokio::test(flavor = "multi_thread")]
async fn archive_event() {
    let mut dev = proxy().await;
    let mut s = dev.event_stream("ramp", EventType::Archive).await.unwrap();
    next(&mut s).await;
    dev.write_attribute("ramp", Value::Double(5.0)).await.unwrap();
    quiet(&mut s).await; // below archive_abs_change = 10
    dev.write_attribute("ramp", Value::Double(12.0)).await.unwrap();
    assert_eq!(value(&next(&mut s).await), Value::Double(12.0));
}

#[tokio::test(flavor = "multi_thread")]
async fn events_pushed_by_device_code() {
    let mut dev = proxy().await;
    let mut user = dev.event_stream("ramp", EventType::User).await.unwrap();
    let mut ready = dev.event_stream("ramp", EventType::DataReady).await.unwrap();
    let mut pipe = dev.event_stream("blob", EventType::Pipe).await.unwrap();
    assert_eq!(value(&next(&mut user).await), Value::Double(0.0));
    assert!(matches!(next(&mut pipe).await.data, Ok(EventData::Pipe(_))));

    dev.command_inout("PushUser", Value::Void).await.unwrap();
    assert_eq!(value(&next(&mut user).await), Value::String("pushed".into()));
    dev.command_inout("PushDataReady", Value::Long(7)).await.unwrap();
    match next(&mut ready).await.data {
        Ok(EventData::DataReady { name, counter, .. }) => assert_eq!((name.as_str(), counter), ("ramp", 7)),
        other => panic!("{other:?}"),
    }
    dev.command_inout("PushPipe", Value::Void).await.unwrap();
    match next(&mut pipe).await.data {
        Ok(EventData::Pipe(p)) => assert_eq!(p.blob.name, "data"),
        other => panic!("{other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn attribute_configuration_event() {
    let mut dev = proxy().await;
    let mut s = dev.event_stream("ramp", EventType::AttrConf).await.unwrap();
    let Ok(EventData::Conf(mut cfg)) = next(&mut s).await.data else { panic!() };
    assert_eq!(cfg.abs_change, Some(1.0));
    cfg.label = "Ramp".into();
    dev.set_attribute_config(vec![cfg]).await.unwrap();
    let Ok(EventData::Conf(cfg)) = next(&mut s).await.data else { panic!() };
    assert_eq!(cfg.label, "Ramp");
}

#[tokio::test(flavor = "multi_thread")]
async fn interface_change_event() {
    let mut dev = proxy().await;
    let mut s = dev.event_stream("", EventType::InterfaceChange).await.unwrap();
    let Ok(EventData::Interface { started: false, .. }) = next(&mut s).await.data else { panic!() };
    dev.command_inout("Init", Value::Void).await.unwrap();
    let Ok(EventData::Interface { commands, attributes, started }) = next(&mut s).await.data else { panic!() };
    assert!(started);
    assert!(commands.iter().any(|c| c.name == "PushUser"));
    assert!(attributes.iter().any(|a| a.name == "ramp"));
}

#[tokio::test(flavor = "multi_thread")]
async fn subscription_errors() {
    let mut dev = proxy().await;
    let reason = |r: DevResult<EventStream>| r.err().map(|e| e.reason().to_owned());
    assert_eq!(reason(dev.event_stream("double_scalar", EventType::Change).await).as_deref(), Some("API_AttributePollingNotStarted"));
    assert_eq!(reason(dev.event_stream("counter", EventType::Change).await).as_deref(), Some("API_EventPropertiesNotSet"));
    assert_eq!(reason(dev.event_stream("nope", EventType::Change).await).as_deref(), Some("API_AttrNotFound"));
    assert_eq!(reason(dev.event_stream("nope", EventType::Pipe).await).as_deref(), Some("API_PipeNotFound"));
}

#[tokio::test(flavor = "multi_thread")]
async fn callback_and_unsubscribe() {
    let mut dev = proxy().await;
    let (tx, rx) = std::sync::mpsc::channel();
    let sub = dev.subscribe_event("ramp", EventType::Change, move |e: Event| drop(tx.send(e))).await.unwrap();
    assert_eq!(value(&rx.recv_timeout(Duration::from_secs(3)).unwrap()), Value::Double(0.0));
    dev.write_attribute("ramp", Value::Double(4.0)).await.unwrap();
    assert_eq!(value(&rx.recv_timeout(Duration::from_secs(3)).unwrap()), Value::Double(4.0));
    sub.unsubscribe().await.unwrap();
    dev.write_attribute("ramp", Value::Double(9.0)).await.unwrap();
    tokio::time::sleep(POLL * 8).await;
    assert!(rx.try_recv().is_err(), "no event after unsubscribe");
}

#[tokio::test(flavor = "multi_thread")]
async fn heartbeat_loss_is_reported() {
    let url = start().await;
    let mut dev = DeviceProxy::connect(&format!("{url}/{DEV}")).await.unwrap();
    let mut s = dev.event_stream("ramp", EventType::Change).await.unwrap();
    next(&mut s).await;
    tokio::time::sleep(HEARTBEAT * 4).await;
    quiet(&mut s).await; // heartbeats keep the subscription alive silently
    let adm = dev.adm_name().await.unwrap();
    let mut admin = DeviceProxy::connect(&format!("{url}/{adm}")).await.unwrap();
    admin.command_inout("Kill", Value::Void).await.unwrap();
    let e = tokio::time::timeout(HEARTBEAT * 20, s.recv()).await.expect("timeout reported").expect("stream open");
    assert_eq!(e.data.unwrap_err().reason(), "API_EventTimeout");
}

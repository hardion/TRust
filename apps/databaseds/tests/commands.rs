//! Validation tests of the Database device: one group per RFC-6 command group, with the argument
//! layouts of the C++ DataBaseds (see docs/SPEC.md §7.2).

mod common;

use common::*;
use trust::{DevState, DeviceProxy, Value};

async fn fresh() -> DeviceProxy {
    db(&start_db().await).await
}

async fn err(db: &mut DeviceProxy, name: &str, arg: Value) -> String {
    db.command_inout(name, arg).await.expect_err(name).reason().to_owned()
}

async fn add_tgtest(db: &mut DeviceProxy) {
    cmd(db, "DbAddServer", sa(&["TgTest/test", "sys/tg/1", "TgTest", "sys/tg/2", "TgTest"])).await;
    cmd(db, "DbAddDevice", sa(&["TgTest/test", "sys/tg/3", "Motor"])).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn self_registered() {
    let host = start_db().await;
    let mut db = db(&host).await;
    assert_eq!(db.command_inout("State", Value::Void).await.unwrap(), Value::State(DevState::On));
    assert!(list(&mut db, "DbGetServerList", Value::String("*".into())).await.contains(&"DataBaseds/2".into()));
    let Value::LongStringArray(l, s) = cmd(&mut db, "DbImportDevice", Value::String("sys/database/2".into())).await
    else {
        panic!()
    };
    assert_eq!(l[0], 1, "exported");
    assert_eq!(s[1], format!("auto://{host}"));
    assert_eq!(list(&mut db, "DbGetCSDbServerList", Value::Void).await, [format!("auto://{host}")]);
    // resolution through the Database itself
    let mut via_db = DeviceProxy::connect(&format!("tango://{host}/sys/database/2")).await.unwrap();
    assert_eq!(via_db.name().await.unwrap(), "sys/database/2");
}

#[tokio::test(flavor = "multi_thread")]
async fn devices_and_servers() {
    let mut db = fresh().await;
    add_tgtest(&mut db).await;
    let s = |v: &str| Value::String(v.into());
    assert!(list(&mut db, "DbGetServerList", s("*")).await.contains(&"TgTest/test".into()));
    assert_eq!(list(&mut db, "DbGetServerNameList", s("Tg*")).await, ["TgTest"]);
    assert_eq!(list(&mut db, "DbGetInstanceNameList", s("TgTest")).await, ["test"]);
    assert_eq!(list(&mut db, "DbGetDeviceList", sa(&["TgTest/test", "TgTest"])).await, ["sys/tg/1", "sys/tg/2"]);
    assert_eq!(
        list(&mut db, "DbGetDeviceClassList", s("TgTest/test")).await,
        ["dserver/TgTest/test", "DServer", "sys/tg/1", "TgTest", "sys/tg/2", "TgTest", "sys/tg/3", "Motor"]
    );
    assert_eq!(list(&mut db, "DbGetDeviceServerClassList", s("TgTest/test")).await, ["DServer", "Motor", "TgTest"]);
    let classes = list(&mut db, "DbGetClassList", s("*")).await;
    assert!(classes.contains(&"TgTest".into()) && classes.contains(&"Motor".into()));
    assert_eq!(cmd(&mut db, "DbGetClassForDevice", s("SYS/TG/3")).await, s("Motor"));
    assert_eq!(list(&mut db, "DbGetClassInheritanceForDevice", s("sys/tg/3")).await, ["Motor"]);
    let domains = list(&mut db, "DbGetDeviceDomainList", s("*")).await;
    assert!(domains.contains(&"sys".into()) && domains.contains(&"dserver".into()));
    let families = list(&mut db, "DbGetDeviceFamilyList", s("sys/*")).await;
    assert!(families.contains(&"tg".into()) && families.contains(&"database".into()));
    assert_eq!(list(&mut db, "DbGetDeviceMemberList", s("sys/tg/*")).await, ["1", "2", "3"]);
    assert_eq!(list(&mut db, "DbGetDeviceWideList", s("sys/tg/*")).await, ["sys/tg/1", "sys/tg/2", "sys/tg/3"]);
    let Value::LongStringArray(l, info) = cmd(&mut db, "DbGetDeviceInfo", s("sys/tg/1")).await else { panic!() };
    assert_eq!(l, [0, 0]);
    assert_eq!((info[0].as_str(), info[3].as_str(), info[7].as_str()), ("sys/tg/1", "TgTest/test", "TgTest"));

    cmd(&mut db, "DbDeleteDevice", s("sys/tg/3")).await;
    assert_eq!(err(&mut db, "DbGetClassForDevice", s("sys/tg/3")).await, "DB_DeviceNotDefined");
    cmd(&mut db, "DbRenameServer", sa(&["TgTest/test", "TgTest/renamed"])).await;
    assert_eq!(list(&mut db, "DbGetDeviceList", sa(&["TgTest/renamed", "TgTest"])).await, ["sys/tg/1", "sys/tg/2"]);
    cmd(&mut db, "DbDeleteServer", s("TgTest/renamed")).await;
    assert!(list(&mut db, "DbGetDeviceWideList", s("sys/tg/*")).await.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn naming_rules() {
    let mut db = fresh().await;
    for bad in ["not-a-device", "a/b", "a/b/c/d", &format!("{}/b/c", "x".repeat(86))] {
        assert_eq!(err(&mut db, "DbAddDevice", sa(&["S/i", bad, "C"])).await, "DB_IncorrectDeviceName", "{bad}");
    }
    assert_eq!(err(&mut db, "DbAddDevice", sa(&["no-instance", "a/b/c", "C"])).await, "DB_IncorrectServerName");
    cmd(&mut db, "DbAddDevice", sa(&["S/i", "a/b/c", "C"])).await;
    let put = sa(&["a/b/c", "1", "1bad", "1", "v"]);
    assert_eq!(err(&mut db, "DbPutDeviceProperty", put).await, "API_InvalidPropertyName");
}

#[tokio::test(flavor = "multi_thread")]
async fn device_properties() {
    let mut db = fresh().await;
    add_tgtest(&mut db).await;
    cmd(&mut db, "DbPutDeviceProperty", sa(&["sys/tg/1", "2", "host", "1", "myhost", "ports", "2", "10", "20"])).await;
    assert_eq!(
        list(&mut db, "DbGetDeviceProperty", sa(&["sys/tg/1", "host", "ports", "nope"])).await,
        ["sys/tg/1", "3", "host", "1", "myhost", "ports", "2", "10", "20", "nope", "0"]
    );
    assert_eq!(list(&mut db, "DbGetDevicePropertyList", sa(&["sys/tg/1", "*"])).await, ["host", "ports"]);
    cmd(&mut db, "DbPutDeviceProperty", sa(&["sys/tg/1", "1", "host", "1", "other"])).await;
    cmd(&mut db, "DbDeleteDeviceProperty", sa(&["sys/tg/1", "host"])).await;
    assert_eq!(list(&mut db, "DbGetDeviceProperty", sa(&["sys/tg/1", "host"])).await, ["sys/tg/1", "1", "host", "0"]);
    // history, oldest first: [name, date, count, values...]
    let h = list(&mut db, "DbGetDevicePropertyHist", sa(&["sys/tg/1", "host"])).await;
    let entries = history(&h);
    assert_eq!(entries, [vec!["myhost".to_string()], vec!["other".into()], vec![]]);
}

/// Splits `[name, date, count, values...]*` into the values of each entry.
fn history(h: &[String]) -> Vec<Vec<String>> {
    let (mut out, mut i) = (vec![], 0);
    while i < h.len() {
        let n: usize = h[i + 2].parse().unwrap();
        assert!(!h[i + 1].is_empty(), "date");
        out.push(h[i + 3..i + 3 + n].to_vec());
        i += 3 + n;
    }
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn class_properties() {
    let mut db = fresh().await;
    cmd(&mut db, "DbPutClassProperty", sa(&["TgTest", "1", "speed", "1", "3"])).await;
    assert_eq!(list(&mut db, "DbGetClassProperty", sa(&["TgTest", "speed"])).await, ["TgTest", "1", "speed", "1", "3"]);
    assert_eq!(list(&mut db, "DbGetClassPropertyList", Value::String("TgTest".into())).await, ["speed"]);
    cmd(&mut db, "DbDeleteClassProperty", sa(&["TgTest", "speed"])).await;
    let h = list(&mut db, "DbGetClassPropertyHist", sa(&["TgTest", "speed"])).await;
    assert_eq!(history(&h), [vec!["3".to_string()], vec![]]);
}

#[tokio::test(flavor = "multi_thread")]
async fn free_properties() {
    let mut db = fresh().await;
    cmd(&mut db, "DbPutProperty", sa(&["Global", "1", "site", "2", "lab", "hall"])).await;
    assert_eq!(
        list(&mut db, "DbGetProperty", sa(&["Global", "site"])).await,
        ["Global", "1", "site", "2", "lab", "hall"]
    );
    assert_eq!(list(&mut db, "DbGetPropertyList", sa(&["Global", "*"])).await, ["site"]);
    assert_eq!(list(&mut db, "DbGetObjectList", Value::String("*".into())).await, ["Global"]);
    cmd(&mut db, "DbDeleteProperty", sa(&["Global", "site"])).await;
    let h = list(&mut db, "DbGetPropertyHist", sa(&["Global", "site"])).await;
    assert_eq!(history(&h), [vec!["lab".to_string(), "hall".into()], vec![]]);
}

/// The attribute-like kinds share one layout: (Put, Get, List, Hist, DeleteProperty, DeleteAll, DeleteObject).
async fn attribute_like(db: &mut DeviceProxy, owner: &str, obj: &str, c: [&str; 7]) {
    let [put, get, lst, hist, del, del_all, del_obj] = c;
    cmd(db, put, sa(&[owner, "1", obj, "2", "label", "1", "Position", "min_value", "1", "-5"])).await;
    assert_eq!(
        list(db, get, sa(&[owner, obj, "nope"])).await,
        [owner, "2", obj, "2", "label", "1", "Position", "min_value", "1", "-5", "nope", "0"]
    );
    assert_eq!(list(db, lst, sa(&[owner, "*"])).await, [obj]);
    cmd(db, put, sa(&[owner, "1", obj, "1", "label", "1", "Pos"])).await;
    let h = list(db, hist, sa(&[owner, obj, "label"])).await;
    // [attribute, property, date, count, values...]
    assert_eq!((h[0].as_str(), h[1].as_str(), h[3].as_str(), h[4].as_str()), (obj, "label", "1", "Position"));
    cmd(db, del, sa(&[owner, obj, "label"])).await;
    assert_eq!(list(db, get, sa(&[owner, obj])).await, [owner, "1", obj, "1", "min_value", "1", "-5"]);
    if !del_all.is_empty() {
        cmd(db, del_all, sa(&[owner, obj])).await;
        assert_eq!(list(db, get, sa(&[owner, obj])).await, [owner, "1", obj, "0"]);
        cmd(db, put, sa(&[owner, "1", obj, "1", "unit", "1", "mm"])).await;
    }
    cmd(db, del_obj, sa(&[owner, obj])).await;
    assert!(list(db, lst, sa(&[owner, "*"])).await.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn attribute_and_pipe_properties() {
    let mut db = fresh().await;
    add_tgtest(&mut db).await;
    let device_attr = [
        "DbPutDeviceAttributeProperty2",
        "DbGetDeviceAttributeProperty2",
        "DbGetDeviceAttributeList",
        "DbGetDeviceAttributePropertyHist",
        "DbDeleteDeviceAttributeProperty",
        "DbDeleteAllDeviceAttributeProperty",
        "DbDeleteDeviceAttribute",
    ];
    attribute_like(&mut db, "sys/tg/1", "double_scalar", device_attr).await;
    let class_attr = [
        "DbPutClassAttributeProperty2",
        "DbGetClassAttributeProperty2",
        "DbGetClassAttributeList",
        "DbGetClassAttributePropertyHist",
        "DbDeleteClassAttributeProperty",
        "",
        "DbDeleteClassAttribute",
    ];
    attribute_like(&mut db, "TgTest", "double_scalar", class_attr).await;
    let device_pipe = [
        "DbPutDevicePipeProperty",
        "DbGetDevicePipeProperty",
        "DbGetDevicePipeList",
        "DbGetDevicePipePropertyHist",
        "DbDeleteDevicePipeProperty",
        "DbDeleteAllDevicePipeProperty",
        "DbDeleteDevicePipe",
    ];
    attribute_like(&mut db, "sys/tg/1", "blob", device_pipe).await;
    let class_pipe = [
        "DbPutClassPipeProperty",
        "DbGetClassPipeProperty",
        "DbGetClassPipeList",
        "DbGetClassPipePropertyHist",
        "DbDeleteClassPipeProperty",
        "",
        "DbDeleteClassPipe",
    ];
    attribute_like(&mut db, "TgTest", "blob", class_pipe).await;

    // version 1 layouts: one value per property, no value count
    cmd(&mut db, "DbPutDeviceAttributeProperty", sa(&["sys/tg/1", "1", "position", "2", "unit", "mm", "label", "Pos"]))
        .await;
    assert_eq!(
        list(&mut db, "DbGetDeviceAttributeProperty", sa(&["sys/tg/1", "position"])).await,
        ["sys/tg/1", "1", "position", "2", "label", "Pos", "unit", "mm"]
    );
    cmd(&mut db, "DbPutClassAttributeProperty", sa(&["TgTest", "1", "position", "1", "unit", "m"])).await;
    assert_eq!(
        list(&mut db, "DbGetClassAttributeProperty", sa(&["TgTest", "position"])).await,
        ["TgTest", "1", "position", "1", "unit", "m"]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn aliases() {
    let mut db = fresh().await;
    add_tgtest(&mut db).await;
    let s = |v: &str| Value::String(v.into());
    cmd(&mut db, "DbPutDeviceAlias", sa(&["sys/tg/1", "tg1"])).await;
    assert_eq!(cmd(&mut db, "DbGetDeviceAlias", s("sys/tg/1")).await, s("tg1"));
    assert_eq!(cmd(&mut db, "DbGetAliasDevice", s("TG1")).await, s("sys/tg/1"));
    assert_eq!(list(&mut db, "DbGetDeviceAliasList", s("tg*")).await, ["tg1"]);
    assert_eq!(err(&mut db, "DbPutDeviceAlias", sa(&["sys/tg/2", "tg1"])).await, "DB_AliasAlreadyDefined");
    cmd(&mut db, "DbDeleteDeviceAlias", s("tg1")).await;
    assert_eq!(err(&mut db, "DbGetAliasDevice", s("tg1")).await, "DB_AliasNotDefined");

    cmd(&mut db, "DbPutAttributeAlias", sa(&["sys/tg/1/double_scalar", "pos"])).await;
    assert_eq!(cmd(&mut db, "DbGetAttributeAlias", s("pos")).await, s("sys/tg/1/double_scalar"));
    assert_eq!(cmd(&mut db, "DbGetAliasAttribute", s("pos")).await, s("sys/tg/1/double_scalar"));
    assert_eq!(cmd(&mut db, "DbGetAttributeAlias2", s("sys/tg/1/double_scalar")).await, s("pos"));
    assert_eq!(list(&mut db, "DbGetAttributeAliasList", s("*")).await, ["pos"]);
    cmd(&mut db, "DbDeleteAttributeAlias", s("pos")).await;
    assert!(list(&mut db, "DbGetAttributeAliasList", s("*")).await.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn hosts_and_server_info() {
    let mut db = fresh().await;
    add_tgtest(&mut db).await;
    let s = |v: &str| Value::String(v.into());
    cmd(&mut db, "DbPutServerInfo", sa(&["TgTest/test", "hosta", "1", "2"])).await;
    assert_eq!(list(&mut db, "DbGetServerInfo", s("TgTest/test")).await, ["TgTest/test", "hosta", "1", "2"]);
    assert!(list(&mut db, "DbGetHostList", s("*")).await.contains(&"hosta".into()));
    assert_eq!(list(&mut db, "DbGetHostServerList", s("hosta")).await, ["TgTest/test"]);
    assert_eq!(list(&mut db, "DbGetHostServersInfo", s("hosta")).await, ["TgTest/test", "1", "2"]);
    cmd(&mut db, "DbDeleteServerInfo", s("TgTest/test")).await;
    assert_eq!(list(&mut db, "DbGetServerInfo", s("TgTest/test")).await, ["TgTest/test", "", "0", "0"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn export_and_import() {
    let mut db = fresh().await;
    add_tgtest(&mut db).await;
    let s = |v: &str| Value::String(v.into());
    cmd(&mut db, "DbExportDevice", sa(&["sys/tg/1", "tcp://h:1", "hosta", "1234", "1"])).await;
    let imported = cmd(&mut db, "DbImportDevice", s("sys/tg/1")).await;
    let expected = |e: i32| {
        let s = ["sys/tg/1", "tcp://h:1", "1", "TgTest/test", "hosta", "TgTest"];
        Value::LongStringArray(vec![e, 1234], s.iter().map(|x| x.to_string()).collect())
    };
    assert_eq!(imported, expected(1));
    assert_eq!(list(&mut db, "DbGetDeviceExportedList", s("sys/tg/*")).await, ["sys/tg/1"]);
    assert_eq!(list(&mut db, "DbGetExportedDeviceListForClass", s("TgTest")).await, ["sys/tg/1"]);
    let Value::LongStringArray(l, info) = cmd(&mut db, "DbGetDeviceInfo", s("sys/tg/1")).await else { panic!() };
    assert_eq!(l, [1, 1234]);
    assert!(!info[5].is_empty(), "started date");
    cmd(&mut db, "DbUnExportDevice", s("sys/tg/1")).await;
    assert_eq!(cmd(&mut db, "DbImportDevice", s("sys/tg/1")).await, expected(0));
    cmd(&mut db, "DbExportDevice", sa(&["sys/tg/1", "tcp://h:1", "hosta", "1234", "1"])).await;
    cmd(&mut db, "DbUnExportServer", s("TgTest/test")).await;
    assert_eq!(cmd(&mut db, "DbImportDevice", s("sys/tg/1")).await, expected(0));
    assert_eq!(err(&mut db, "DbImportDevice", s("no/such/device")).await, "DB_DeviceNotDefined");
    assert_eq!(
        err(&mut db, "DbExportDevice", sa(&["no/such/device", "x", "h", "1", "1"])).await,
        "DB_DeviceNotDefined"
    );

    cmd(&mut db, "DbExportEvent", sa(&["sys/tg/1", "tcp://h:2", "hosta", "1234", "1"])).await;
    let ev = |e: i32| {
        let s = ["sys/tg/1", "tcp://h:2", "1", "hosta"];
        Value::LongStringArray(vec![e, 1234], s.iter().map(|x| x.to_string()).collect())
    };
    assert_eq!(cmd(&mut db, "DbImportEvent", s("sys/tg/1")).await, ev(1));
    cmd(&mut db, "DbUnExportEvent", s("sys/tg/1")).await;
    assert_eq!(cmd(&mut db, "DbImportEvent", s("sys/tg/1")).await, ev(0));
}

#[tokio::test(flavor = "multi_thread")]
async fn miscellaneous() {
    let mut db = fresh().await;
    add_tgtest(&mut db).await;
    let s = |v: &str| Value::String(v.into());
    assert!(list(&mut db, "DbInfo", Value::Void).await.iter().any(|l| l.contains("SQLite")));
    let Value::LongStringArray(l, rows) =
        cmd(&mut db, "DbMySqlSelect", s("SELECT name FROM device WHERE name LIKE 'sys/tg/%' ORDER BY name")).await
    else {
        panic!()
    };
    assert_eq!((l, rows), (vec![3, 1], vec!["sys/tg/1".to_string(), "sys/tg/2".into(), "sys/tg/3".into()]));
    assert_eq!(err(&mut db, "DbMySqlSelect", s("DELETE FROM device")).await, "DB_SQLError");
    assert!(list(&mut db, "DbGetForwardedAttributeListForDevice", s("sys/tg/1")).await.is_empty());
    assert_eq!(cmd(&mut db, "ResetTimingValues", Value::Void).await, Value::Void);
    let cache = list(&mut db, "DbGetDataForServerCache", sa(&["TgTest/test", "hosta"])).await;
    assert!(cache.contains(&"sys/tg/1".into()), "{cache:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn data_survives_a_restart() {
    let file = db_file();
    {
        let mut db = trust_databaseds::DataBase::open(&file, "auto://h:1").unwrap();
        trust::Device::execute(&mut db, "DbAddServer", sa(&["S/i", "a/b/c", "C"])).unwrap();
    }
    let mut db = trust_databaseds::DataBase::open(&file, "auto://h:1").unwrap();
    let devices = trust::Device::execute(&mut db, "DbGetDeviceList", sa(&["S/i", "C"])).unwrap();
    assert_eq!(strings(devices), ["a/b/c"]);
}

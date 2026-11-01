//! Test helpers: an in-process Database on a temporary SQLite file, and a `Motor` device class with
//! typed, defaulted and mandatory properties.
#![allow(dead_code)]

use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use trust::{
    AttrInfo, DataType as T, DevResult, DevState, Device, DeviceProxy, PropInfo, Properties, Server, Value,
    Writable as W,
};

pub fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

/// A fresh SQLite file in the temp directory.
pub fn db_file() -> std::path::PathBuf {
    static N: AtomicU32 = AtomicU32::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    let p = std::env::temp_dir().join(format!("trust-db-{}-{n}.sqlite", std::process::id()));
    let _ = std::fs::remove_file(&p);
    p
}

/// Starts a Database server on a free port; returns its TANGO_HOST (`127.0.0.1:port`).
pub async fn start_db() -> String {
    loop {
        let port = free_port();
        let host = format!("127.0.0.1:{port}");
        let server = trust_databaseds::server(&db_file(), &format!("auto://{host}")).unwrap();
        let task = tokio::spawn(server.run([format!("auto://{host}")]));
        while !task.is_finished() && tokio::net::TcpStream::connect(&host).await.is_err() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        if !task.is_finished() {
            return host;
        }
    }
}

/// A direct connection to the Database device.
pub async fn db(host: &str) -> DeviceProxy {
    DeviceProxy::connect(&format!("tango://{host}/sys/database/2#dbase=no")).await.unwrap()
}

pub fn sa(v: &[&str]) -> Value {
    Value::StringArray(v.iter().map(|s| s.to_string()).collect())
}

pub fn strings(v: Value) -> Vec<String> {
    match v {
        Value::StringArray(v) => v,
        other => panic!("expected StringArray, got {other:?}"),
    }
}

pub async fn cmd(db: &mut DeviceProxy, name: &str, arg: Value) -> Value {
    db.command_inout(name, arg).await.unwrap_or_else(|e| panic!("{name}: {e}"))
}

pub async fn list(db: &mut DeviceProxy, name: &str, arg: Value) -> Vec<String> {
    strings(cmd(db, name, arg).await)
}

/// Waits until `device` is exported (or not) in the Database.
pub async fn wait_exported(db: &mut DeviceProxy, device: &str, exported: bool) {
    for _ in 0..400 {
        if let Ok(Value::LongStringArray(l, _)) = db.command_inout("DbImportDevice", Value::String(device.into())).await
            && (l[0] == 1) == exported
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("{device} exported != {exported}");
}

/// A motor: `Speed` (Double, default 1.5) and `Port` (String, mandatory) device properties.
#[derive(Default)]
pub struct Motor {
    speed: f64,
    port: String,
    position: f64,
}

impl Device for Motor {
    fn class_name(&self) -> String {
        "Motor".into()
    }
    fn properties(&self) -> Vec<PropInfo> {
        vec![
            PropInfo {
                name: "Speed".into(),
                data_type: T::Double,
                description: "max speed".into(),
                default: Some(vec!["1.5".into()]),
                mandatory: false,
            },
            PropInfo { name: "Port".into(), data_type: T::String, mandatory: true, ..Default::default() },
        ]
    }
    fn init(&mut self, props: &Properties) -> DevResult<()> {
        if let Some(Value::Double(s)) = props.get("Speed") {
            self.speed = *s;
        }
        if let Some(Value::String(p)) = props.get("Port") {
            self.port = p.clone();
        }
        Ok(())
    }
    fn state(&mut self) -> DevState {
        DevState::On
    }
    fn attributes(&self) -> Vec<AttrInfo> {
        vec![
            AttrInfo { name: "position".into(), data_type: T::Double, writable: W::ReadWrite, ..Default::default() },
            AttrInfo { name: "speed".into(), data_type: T::Double, ..Default::default() },
            AttrInfo { name: "port".into(), data_type: T::String, ..Default::default() },
        ]
    }
    fn read_attribute(&mut self, attr: &str) -> DevResult<Value> {
        Ok(match attr {
            "position" => Value::Double(self.position),
            "speed" => Value::Double(self.speed),
            _ => Value::String(self.port.clone()),
        })
    }
    fn write_attribute(&mut self, _attr: &str, value: Value) -> DevResult<()> {
        if let Value::Double(v) = value {
            self.position = v;
        }
        Ok(())
    }
}

pub fn motor_server(host: &str) -> Server {
    Server::new("MotorServer", "lab").database(host).class("Motor", || Box::new(Motor::default()))
}

//! `TgTest`: a TangoTest-like device shared by the validation tests and the example server.
#![allow(dead_code)]

use std::time::Duration;
use trust::{
    AttrInfo, CmdInfo, DataFormat as F, DataType as T, DevFailed, DevResult, DevState, Device, PipeBlob, PipeInfo,
    Server, Value, Writable as W,
};

pub const DEV: &str = "sys/tg/1";
pub const POLL: Duration = Duration::from_millis(20);

pub struct TgTest {
    double: f64,
    string: String,
    counter: i32,
    blob: PipeBlob,
}

impl Default for TgTest {
    fn default() -> Self {
        let elements =
            vec![("x".into(), Value::Double(1.0)), ("names".into(), Value::StringArray(vec!["a".into(), "b".into()]))];
        Self { double: 0.0, string: String::new(), counter: 0, blob: PipeBlob { name: "data".into(), elements } }
    }
}

fn cmd(name: &str, in_type: T, out_type: T) -> CmdInfo {
    CmdInfo { name: name.into(), in_type, out_type, ..Default::default() }
}

fn attr(name: &str, data_type: T, data_format: F, writable: W) -> AttrInfo {
    AttrInfo { name: name.into(), data_type, data_format, writable, ..Default::default() }
}

impl Device for TgTest {
    fn class_name(&self) -> String {
        "TgTest".into()
    }
    fn description(&self) -> String {
        "A TRust test device".into()
    }
    fn state(&mut self) -> DevState {
        DevState::On
    }
    fn commands(&self) -> Vec<CmdInfo> {
        vec![
            cmd("DevVoid", T::Void, T::Void),
            cmd("DevDouble", T::Double, T::Double),
            cmd("DevString", T::String, T::String),
            cmd("DevVarLongArray", T::LongArray, T::LongArray),
            cmd("Throw", T::Void, T::Void),
            cmd("Counter", T::Void, T::Long),
        ]
    }
    fn attributes(&self) -> Vec<AttrInfo> {
        vec![
            AttrInfo {
                min_value: Some(-100.0),
                max_value: Some(100.0),
                max_alarm: Some(50.0),
                max_warning: Some(40.0),
                unit: "mm".into(),
                ..attr("double_scalar", T::Double, F::Scalar, W::ReadWrite)
            },
            AttrInfo { max_dim_x: 10, ..attr("long_spectrum", T::Long, F::Spectrum, W::Read) },
            AttrInfo { max_dim_x: 2, max_dim_y: 2, ..attr("short_image", T::Short, F::Image, W::Read) },
            attr("string_scalar", T::String, F::Scalar, W::ReadWrite),
            attr("counter", T::Long, F::Scalar, W::Read),
            attr("faulty", T::Double, F::Scalar, W::Read),
        ]
    }
    fn pipes(&self) -> Vec<PipeInfo> {
        vec![
            PipeInfo { name: "blob".into(), writable: true, ..Default::default() },
            PipeInfo { name: "ro_pipe".into(), ..Default::default() },
        ]
    }
    fn execute(&mut self, cmd: &str, arg: Value) -> DevResult<Value> {
        match cmd {
            "DevVoid" => Ok(Value::Void),
            "Throw" => Err(DevFailed::new("TG_Thrown", "thrown on purpose", "TgTest::Throw")),
            "Counter" => {
                self.counter += 1;
                Ok(Value::Long(self.counter))
            }
            _ => Ok(arg), // echo commands
        }
    }
    fn read_attribute(&mut self, attr: &str) -> DevResult<Value> {
        Ok(match attr {
            "double_scalar" => Value::Double(self.double),
            "long_spectrum" => Value::LongArray(vec![1, 2, 3]),
            "short_image" => Value::ShortArray(vec![1, 2, 3, 4]),
            "string_scalar" => Value::String(self.string.clone()),
            "counter" => {
                self.counter += 1;
                Value::Long(self.counter)
            }
            _ => {
                return Err(DevFailed::new("TG_ReadFailed", "hardware not responding", "TgTest::faulty"));
            }
        })
    }
    fn write_attribute(&mut self, attr: &str, value: Value) -> DevResult<()> {
        match (attr, value) {
            ("double_scalar", Value::Double(v)) => self.double = v,
            ("string_scalar", Value::String(v)) => self.string = v,
            _ => unreachable!("type checked by the server"),
        }
        Ok(())
    }
    fn read_pipe(&mut self, pipe: &str) -> DevResult<PipeBlob> {
        Ok(match pipe {
            "blob" => self.blob.clone(),
            _ => PipeBlob { name: "ro".into(), elements: vec![] },
        })
    }
    fn write_pipe(&mut self, _pipe: &str, blob: PipeBlob) -> DevResult<()> {
        self.blob = blob;
        Ok(())
    }
}

pub fn server() -> Server {
    Server::new("TgTest", "test")
        .device(DEV, TgTest::default())
        .poll_attribute(DEV, "counter", POLL)
        .poll_attribute(DEV, "faulty", POLL)
        .poll_command(DEV, "Counter", POLL)
}

/// A free local TCP port.
pub fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

/// Starts a server on a random port and returns its endpoint URL (without the device name).
pub async fn start() -> String {
    let port = free_port();
    tokio::spawn(async move { server().run(&format!("tcp://127.0.0.1:{port}")).await.unwrap() });
    while tokio::net::TcpStream::connect(("127.0.0.1", port)).await.is_err() {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    format!("zmq+tcp://127.0.0.1:{port}")
}

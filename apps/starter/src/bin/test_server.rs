//! `TestServer <instance>`: one `TestDevice` per device registered for `TestServer/<instance>`.
//! Its `Crash` command exits the process, for the Starter's crash detection.

use trust::{AttrInfo, CmdInfo, DataType, DevResult, DevState, Device, Server, Value};

struct TestDevice;

impl Device for TestDevice {
    fn class_name(&self) -> String {
        "TestDevice".into()
    }
    fn state(&mut self) -> DevState {
        DevState::On
    }
    fn commands(&self) -> Vec<CmdInfo> {
        vec![CmdInfo { name: "Crash".into(), ..Default::default() }]
    }
    fn attributes(&self) -> Vec<AttrInfo> {
        vec![AttrInfo { name: "value".into(), data_type: DataType::Long, ..Default::default() }]
    }
    fn execute(&mut self, _cmd: &str, _arg: Value) -> DevResult<Value> {
        std::process::exit(3)
    }
    fn read_attribute(&mut self, _attr: &str) -> DevResult<Value> {
        Ok(Value::Long(42))
    }
}

#[tokio::main]
async fn main() -> DevResult<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let server = Server::from_args("TestServer", args.clone())?.class("TestDevice", || Box::new(TestDevice));
    println!("TestServer/{} started", args.first().map(String::as_str).unwrap_or_default());
    server.run_args().await
}

# TRust
PoC for new control system

A minimal Rust implementation of the [Tango Controls](https://www.tango-controls.org) device model
(Tango RFC-2/3/4/7/9) whose request-reply protocol (RFC-10) runs over a **plain TCP socket with
JSON Lines frames** instead of CORBA.

* [`docs/SPEC.md`](docs/SPEC.md) — protocol specification (the 21 requests of the Tango Request-Reply cheatsheet)
* [`docs/PLAN.md`](docs/PLAN.md) — plan, design choices and test matrix
* `tests/validation.rs` — validation tests, one group per cheatsheet row + raw socket tests

```sh
cargo test                              # run the validation suite
cargo run --example tgtest              # TgTest device server on 127.0.0.1:10000
echo '{"v":1,"id":1,"device":"sys/tg/1","op":{"CommandExecution":{"name":"DevDouble","arg":{"Double":2.5}}}}' \
  | nc -q1 127.0.0.1 10000
# {"id":1,"reply":{"Ok":{"Value":{"Double":2.5}}}}
```

Writing a device:

```rust
struct Motor { position: f64 }

impl trust::Device for Motor {
    fn class_name(&self) -> String { "Motor".into() }
    fn attributes(&self) -> Vec<AttrInfo> {
        vec![AttrInfo { name: "position".into(), data_type: DataType::Double,
                        writable: Writable::ReadWrite, ..Default::default() }]
    }
    fn read_attribute(&mut self, _: &str) -> DevResult<Value> { Ok(Value::Double(self.position)) }
    fn write_attribute(&mut self, _: &str, v: Value) -> DevResult<()> {
        if let Value::Double(p) = v { self.position = p }
        Ok(())
    }
}

Server::new("MotorServer", "lab").device("lab/motor/1", Motor { position: 0.0 }).run(listener).await?;
let mut m = DeviceProxy::connect("127.0.0.1:10000/lab/motor/1").await?;
m.write_attribute("position", Value::Double(3.0)).await?;
```

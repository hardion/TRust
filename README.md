# TRust
PoC for new control system

A minimal Rust implementation of the [Tango Controls](https://www.tango-controls.org) device model
(Tango RFC-2/3/4/7/9) whose request-reply protocol (RFC-10) runs over **ZeroMQ** (ROUTER server,
REQ/DEALER clients, one JSON frame per message) instead of CORBA.
This is the `claude/transport-zmq` branch; see `claude/transport-core` for the plain TCP version.

* [`docs/SPEC.md`](docs/SPEC.md) — protocol specification (the 21 requests of the Tango Request-Reply cheatsheet)
* [`docs/PLAN.md`](docs/PLAN.md) — plan, design choices and test matrix
* `tests/validation.rs` — transport independent validation tests, one group per cheatsheet row
* `tests/wire.rs` — the protocol driven with a bare DEALER socket

```sh
cargo test
cargo run --example tgtest tcp://127.0.0.1:10000
python3 -c 'import zmq; s=zmq.Context().socket(zmq.REQ); s.connect("tcp://127.0.0.1:10000");
s.send_json({"v":1,"id":1,"device":"sys/tg/1","op":"DeviceInfo"}); print(s.recv_json())'
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

Server::new("MotorServer", "lab").device("lab/motor/1", Motor { position: 0.0 }).run("tcp://0.0.0.0:10000").await?;
let mut m = DeviceProxy::connect("zmq+tcp://127.0.0.1:10000/lab/motor/1").await?;
m.write_attribute("position", Value::Double(3.0)).await?;
```

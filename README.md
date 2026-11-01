# TRust
PoC for new control system

A minimal Rust implementation of the [Tango Controls](https://www.tango-controls.org) device model
(Tango RFC-2/3/4/7/9) whose request-reply protocol (RFC-10) runs over **pluggable transports**:
JSON Lines over TCP, ZeroMQ, gRPC and Zenoh. You select them by Cargo feature, by URL scheme, or
by detection of the wire (one `auto://` port for every wire, client-side probing).

* [`docs/SPEC.md`](docs/SPEC.md): protocol specification (the 21 requests of the Tango Request-Reply cheatsheet)
* [`docs/TRANSPORTS.md`](docs/TRANSPORTS.md): the transports compared, latency, and the abstraction
* [`docs/PLAN.md`](docs/PLAN.md): the original plan and test matrix
* `tests/validation.rs`: the validation suite run over every compiled transport, directly and through detection
* `tests/wire_*.rs`: each wire driven without `DeviceProxy`, plus selection and detection tests

| Feature | Transport | Client URL |
|---|---|---|
| `tcp` (default) | JSON Lines over TCP | `tcp://host:10000/sys/tg/1` |
| `zmq` | ZeroMQ (pure Rust) | `zmq+tcp://host:5555/sys/tg/1` |
| `grpc` | gRPC (tonic, vendored protoc) | `grpc://host:50051/sys/tg/1` |
| `zenoh` | Zenoh | `zenoh://tcp/host:7447/sys/tg/1`, `zenoh:///sys/tg/1` (discovery) |
| `detect` | wire detection | `auto://host:10000/sys/tg/1` |
| `full` | all of the above | |

```sh
cargo test --features full                     # 264 validation tests + wire tests
cargo run --features full --example tgtest      # every wire on auto://127.0.0.1:10000
echo '{"v":1,"id":1,"device":"sys/tg/1","op":"DeviceName"}' | nc -q1 127.0.0.1 10000
cargo run --release --features full --example bench
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

Server::new("MotorServer", "lab").device("lab/motor/1", Motor { position: 0.0 })
    .run(["auto://0.0.0.0:10000", "zenoh://"]).await?;
let mut m = DeviceProxy::connect("grpc://127.0.0.1:10000/lab/motor/1").await?;
m.write_attribute("position", Value::Double(3.0)).await?;
```

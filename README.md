# TRust
PoC for new control system

A minimal Rust implementation of the [Tango Controls](https://www.tango-controls.org) device model
(Tango RFC-2/3/4/7/9) whose request-reply protocol (RFC-10) runs over **[Zenoh](https://zenoh.io)**
queries instead of CORBA: every device is the queryable `tango/<device>`, so Zenoh locates devices
by name, peer-to-peer or through routers, with optional multicast discovery.
This is the `claude/transport-zenoh` branch; see `claude/transport-core` for the plain TCP version.

* [`docs/SPEC.md`](docs/SPEC.md) — protocol specification (the 21 requests of the Tango Request-Reply cheatsheet)
* [`docs/PLAN.md`](docs/PLAN.md) — plan, design choices and test matrix
* `tests/validation.rs` — transport independent validation tests, one group per cheatsheet row
* `tests/wire.rs` — bare Zenoh session queries, two servers reached through a router, multicast discovery

```sh
cargo test                                  # `-- --ignored` adds the multicast discovery test
cargo run --example tgtest tcp/127.0.0.1:7447
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

Server::new("MotorServer", "lab").device("lab/motor/1", Motor { position: 0.0 }).run(zenoh::Config::default()).await?;
let mut m = DeviceProxy::connect("zenoh:///lab/motor/1").await?; // found by multicast scouting
m.write_attribute("position", Value::Double(3.0)).await?;
```

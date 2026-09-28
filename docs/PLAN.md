# TRust PoC — Plan

## Phases

1. **Specification** — `docs/SPEC.md`: wire protocol, the 21 cheatsheet
   requests and their semantics mapped onto the Tango RFCs.
2. **Validation tests** — `tests/`: black-box tests written against the public
   API *before* the implementation. One test (or group) per cheatsheet row,
   plus protocol-level tests using a raw `TcpStream` to prove the socket
   protocol is usable without the Rust client.
3. **Implementation** — smallest code that makes the tests pass.

## Technical choices

| Concern      | Choice                          | Why |
|--------------|---------------------------------|-----|
| Runtime      | `tokio`                         | de-facto async runtime, one task per connection |
| Encoding     | `serde` + `serde_json`, JSON Lines | zero hand-written codec, debuggable with `nc` |
| Device model | `trait Device` with default methods | a device only implements what it has |
| Concurrency  | one `std::sync::Mutex` per device | Tango "by device" serialisation, no lock held across `.await` |
| Errors       | one type `DevFailed` everywhere (client, server, I/O) | mirrors Tango exceptions |
| Data types   | one `macro_rules!` generating `Value`, `DataType` | the 30 RFC-9 types in a few lines |

## Crate layout

```
src/lib.rs       re-exports
src/types.rs     RFC-9 data types, DevFailed, metadata (CmdInfo, AttrInfo, PipeInfo, …)
src/protocol.rs  Request / Op / Reply / Data frames
src/server.rs    Device trait, Server builder, dispatcher, black box, polling
src/client.rs    DeviceProxy
tests/common     TgTest: a TangoTest-like device used by the tests & example
tests/*.rs       validation tests
examples/tgtest.rs  runnable server (port 10000)
```

## Public API sketch

```rust
// server side
pub trait Device: Send + 'static {
    fn class_name(&self) -> String;
    fn description(&self) -> String { .. }
    fn commands(&self) -> Vec<CmdInfo> { vec![] }
    fn attributes(&self) -> Vec<AttrInfo> { vec![] }
    fn pipes(&self) -> Vec<PipeInfo> { vec![] }
    fn init(&mut self) -> DevResult<()> { Ok(()) }
    fn state(&mut self) -> DevState { DevState::Unknown }
    fn status(&mut self) -> String { /* "The device is in X state." */ }
    fn execute(&mut self, cmd: &str, arg: Value) -> DevResult<Value>;
    fn read_attribute(&mut self, attr: &str) -> DevResult<Value>;
    fn write_attribute(&mut self, attr: &str, v: Value) -> DevResult<()>;
    fn read_pipe(&mut self, pipe: &str) -> DevResult<PipeBlob>;
    fn write_pipe(&mut self, pipe: &str, blob: PipeBlob) -> DevResult<()>;
}

Server::new("TgTest", "test")
    .device("sys/tg/1", TgTest::default())
    .poll("sys/tg/1", "double_scalar", Duration::from_millis(50))
    .run(listener).await?;

// client side
let mut dev = DeviceProxy::connect("127.0.0.1:10000/sys/tg/1").await?;
dev.command_inout("DevDouble", Value::Double(1.0)).await?;
dev.read_attribute("double_scalar").await?;
```

## Test matrix (phase 2)

| Cheatsheet row           | Test                                   |
|--------------------------|----------------------------------------|
| DEVICE NAME/DESCRIPTION/ADM_NAME | `device_identity`              |
| DEVICE PING              | `ping`                                 |
| DEVICE BLACKBOX          | `black_box`                            |
| DEVICE INFO              | `info`                                 |
| COMMAND EXECUTION        | `command_*` (types, void, errors, builtins) |
| COMMANDS LIST / INFO     | `command_list_and_query`               |
| COMMAND HISTORY          | `command_history_*`                    |
| ATTRIBUTES READ          | `read_*` (multi, State/Status, quality, dims, per-attr errors) |
| ATTRIBUTES WRITE         | `write_*` (ok, not writable, limits, type) |
| ATTRIBUTES WRITE READ    | `write_read`                           |
| ATTRIBUTE GET/SET CONFIG | `attribute_config*`                    |
| ATTRIBUTE HISTORY        | `attribute_history*`                   |
| PIPE *                   | `pipe_*`                               |
| Source Dev/Cache/CacheDev| `source_*`                             |
| Protocol                 | `raw_socket_*`, `unknown_device`, `concurrent_clients` |
| Admin device             | `admin_device`                         |

## Out of scope

See SPEC §6.

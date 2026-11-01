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
    fn status(&mut self) -> Option<String> { None } // None: "The device is in X state."
    fn execute(&mut self, cmd: &str, arg: Value) -> DevResult<Value>;
    fn read_attribute(&mut self, attr: &str) -> DevResult<Value>;
    fn write_attribute(&mut self, attr: &str, v: Value) -> DevResult<()>;
    fn read_pipe(&mut self, pipe: &str) -> DevResult<PipeBlob>;
    fn write_pipe(&mut self, pipe: &str, blob: PipeBlob) -> DevResult<()>;
}

Server::new("TgTest", "test")
    .device("sys/tg/1", TgTest::default())
    .poll_attribute("sys/tg/1", "counter", Duration::from_millis(50))
    .poll_command("sys/tg/1", "Counter", Duration::from_millis(50))
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

---

# Phase 2 — Tango compliance, Database, events, Starter, browser

Specification: `docs/SPEC.md` §6–§10 (properties, naming and Database, admin
device, events, Starter), `docs/BROWSER.md` (GUI and Starter interface),
`docs/COMPLIANCE.md` (status of every RFC requirement).

## Process

As in phase 1, each milestone is: spec section (done for all milestones in
this commit) → validation tests written first against the public API → the
smallest implementation that passes them. Each milestone is one PR from its
own branch, merged in order.

## Workspace

```
Cargo.toml            workspace + the `trust` library (unchanged layout)
apps/databaseds/      DataBaseds server: sys/database/2 on SQLite (rusqlite, bundled)
apps/starter/         Starter server: tango/admin/<host>
apps/browser/         trust-browser (eframe/egui, egui_plot, egui_kittest for tests)
```

Heavy dependencies (SQLite, egui) stay out of the library: the library only
gains the Database *client* (property access, name resolution, export) and
the event system.

## Milestones

| # | Content | Tests first |
|---|---|---|
| M1 | Properties (`PropInfo`, `Device::init(&Properties)`), Database device on SQLite with every RFC-6 command, Database-as-file, server life cycle (create from DB, export, unexport on Kill/SIGTERM), name resolution (`TANGO_HOST`, `tango://…`, `#dbase=no`, aliases) | one test group per RFC-6 command group, life-cycle tests (start a server, import it by name, kill it, see it unexported), property defaults/mandatory/types, file syntax parser |
| M2 | Event system: event properties in `AttrInfo`, triggers (change, periodic, archive, user, attr conf, data ready, interface change, pipe), admin negotiation, heartbeat/confirm, sequence numbers and missed events, one channel implementation per transport, `subscribe_event` / `event_stream` | the event suite instantiated over every transport (like `tests/validation.rs`): each event type, thresholds, first synchronous event, unsubscribe, heartbeat loss → re-subscribe, unconfirmed subscription dropped, overflow → missed events |
| M3 | Admin device completion: runtime polling commands, DevRestart/RestartServer/Kill, locking, logging (RFC-14); memorised attributes, enum labels, nested pipe blobs, READ_WITH_WRITE, MultiDevFailed, dynamic attributes for INTERFACE_CHANGE | one test per admin command; memorised value survives a restart through the Database |
| M4 | Starter device: process management from `StartDsPath`, startup levels, states, logs, events on `Servers`/`HostState` | start/stop/kill the TgTest example server through a Starter registered in a test Database; level ordering; crash detection |
| M5 | Browser: Database tree, device panel with generated forms, events-driven refresh, properties and attribute config editing, admin tab; Starter interface (hosts, servers by level, start/stop/log, server registration) | unit tests of the type → widget/value rules; `egui_kittest` UI tests against a live TgTest + Database + Starter |

## Decisions taken

| Question | Choice | Why |
|---|---|---|
| GUI toolkit | egui / eframe | generated forms are a few lines in immediate mode; native + WebAssembly |
| Database backend | SQLite (rusqlite, bundled) | one file, SQL like the MySQL reference, no server to run |
| Event channels | the transport of the request-reply connection | keeps "by compilation / by URL / by detection" unchanged for events |
| Database argument layouts | identical to the C++ `DataBaseds` | the Tango documentation and tooling knowledge apply as is |
| `tango://` scheme | now means "through the Database" (Tango semantics); direct access is `#dbase=no` or the transport schemes | follows the Tango naming RFC; phase 1 used it as a TCP alias |

## Risks

* **Scope**: about 100 Database commands. Most are the same get/put/delete
  pattern over seven property kinds, so one generic property store and a
  table that maps each command to it keep the code small.
* **Starter process control** is Unix-first (signals, process groups); on
  Windows only start and hard kill would work.
* **egui_kittest** UI tests need a GPU-less renderer; the plan is the
  accessibility-tree assertions only, without screenshot comparison.

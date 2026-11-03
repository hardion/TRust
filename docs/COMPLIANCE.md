# Tango RFC compliance matrix

Status per requirement. ✅ done (on `main`), 🔨 planned in phase 2 (milestone
of `docs/PLAN.md` §Phase 2), ⛔ out of scope (with reason).

## RFC-2 Device

| Requirement | Status |
|---|---|
| Device name `domain/family/member`, case insensitive | ✅ |
| Class with commands, attributes, pipes | ✅ |
| Init, State, Status commands; State, Status attributes | ✅ |
| Default state UNKNOWN; ON + attribute in ALARM/WARNING → ALARM | ✅ |
| Exported / not exported phases, init at export | ✅ |
| Device and class properties | ✅ |
| Dynamic attributes and commands (RFC-15) | ✅ `EventSink::interface_changed`, INTERFACE_CHANGE sent after the request |
| Reserved names `dserver/*`, classes `DServer`, `DataBase` | ✅ |

## RFC-3 Command

| Requirement | Status |
|---|---|
| Name, ARGIN/ARGOUT types, descriptions, display level | ✅ |
| All argument types of RFC-9 | ✅ |
| Fire and forget (asynchronous) | ⛔ the client API is already async; one-way calls add nothing here |

## RFC-4 Attribute

| Requirement | Status |
|---|---|
| Static metadata: type, format, writable, dims, display level | ✅ |
| Properties: label, unit, format, min/max value, alarms, warnings | ✅ |
| Event properties: abs/rel change, period, archive * | ✅ (stored in the Database) |
| Quality VALID/ALARM/WARNING | ✅ · CHANGING/INVALID set by device code ⛔ later phase (needs a quality in `read_attribute`) |
| `READ_WITH_WRITE` + writable attribute name | ✅ |
| Memorized attributes (`__value`) | ✅ stored on write, written back at export and DevRestart |
| Enum labels | ✅ range check, editable, stored (browser display 🔨 M5) |
| Forwarded attributes (`__root_att`) | ⛔ needs device-to-device proxying; later phase |
| Attribute aliases | ✅ (Database; client-side resolution of attribute aliases ⛔ later phase) |

## RFC-5 Property

| Requirement | Status |
|---|---|
| Class, device, attribute, free properties | ✅ |
| Default value, mandatory in Database, types | ✅ |
| Naming convention | ✅ |

## RFC-6 Database

| Requirement | Status |
|---|---|
| Implemented as a device server, pre-known address, State/Status | ✅ |
| SQL backend (SQLite) | ✅ |
| Every command of the RFC-6 list (§7.2 of SPEC) | ✅ (`DbGetDataForServerCache` simplified) |
| Property history | ✅ |
| Uniqueness and naming checks, field sizes | ✅ |
| Database as a file (read-only) | ✅ |
| High availability (3500 req/s) | 🔨 M1 measured with `examples/bench`; replication ⛔ |

## RFC-7 Pipe

| Requirement | Status |
|---|---|
| Metadata, read/write, blob key → value | ✅ |
| Nested blobs | ✅ `Value::PipeBlob` |
| Pipe events | ✅ |
| Pipes not polled | ✅ |

## RFC-8 Server

| Requirement | Status |
|---|---|
| Several devices and classes per server, unique instance | ✅ |
| Admin device name `dserver/<server>/<instance>` | ✅ |
| Black box | ✅ |
| Per-device serialisation | ✅ |
| Admin command table (polling, events, restart, kill, locking, logging) | ✅ queries and wizards, polling (persisted as `polled_attr`/`polled_cmd`), DevRestart/RestartServer/Kill, locking, logging, events (SPEC §8) |
| Export / unexport sequences with the Database | ✅ |

## RFC-9 Data types

| Requirement | Status |
|---|---|
| All scalar and array types, State, Encoded, Long/DoubleStringArray | ✅ |
| DevEnum (as Short) | ✅ with labels |
| DevPipeBlob | ✅ nested |
| DevFailed, MultiDevFailed | ✅ one error per failed attribute, origin = attribute name |

## RFC-10 Request-Reply

| Requirement | Status |
|---|---|
| The 21 operations of the cheatsheet | ✅ |
| Source DEV / CACHE / CACHE_DEV | ✅ |
| Transports | ✅ TCP, ZeroMQ, gRPC, Zenoh (RFC-11 CORBA ⛔: replaced by design) |

## RFC-12 Publisher-Subscriber

| Requirement | Status |
|---|---|
| Event types INTERFACE_CHANGE, PIPE, ATTR_CONF, CHANGE, PERIODIC, ARCHIVE, USER (+ DATA_READY) | ✅ (INTERFACE_CHANGE on Init, restart and dynamic attributes) |
| Negotiation through admin commands, SUBSCRIPTION_INFO | ✅ |
| Heartbeat every 9 s, confirmation within 600 s | ✅ (both configurable) |
| Re-subscription, missed-event and overflow notification | ✅ |
| Channels per transport (RFC-13 ZeroMQ and the others) | ✅ TCP, ZeroMQ PUB/SUB, gRPC streaming, Zenoh pub/sub |

## RFC-14 Logging

| Requirement | Status |
|---|---|
| Levels, targets, admin logging commands | ✅ OFF…DEBUG, `console::` and `file::` targets, `EventSink::log`, requests at DEBUG · device (`device::`) targets ⛔ |

## Services outside the RFCs

| Item | Status |
|---|---|
| Starter device (`tango/admin/<host>`) | 🔨 M4 |
| Device browser GUI with generic forms (egui) | 🔨 M5 |
| Starter interface in the browser (Astor-like) | 🔨 M5 |
| Access control (RFC-18), memorised attribute service (RFC-17), cache system (RFC-16) | ⛔ later phase |

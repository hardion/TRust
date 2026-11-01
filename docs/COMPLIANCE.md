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
| Exported / not exported phases, init at export | ✅ without Database · 🔨 M1 with Database |
| Device and class properties | 🔨 M1 |
| Dynamic attributes and commands (RFC-15) | 🔨 M3 (needed by INTERFACE_CHANGE) |
| Reserved names `dserver/*`, classes `DServer`, `DataBase` | ✅ `dserver` · 🔨 M1 `DataBase` |

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
| Event properties: abs/rel change, period, archive * | 🔨 M2 |
| Quality VALID/ALARM/WARNING | ✅ · CHANGING/INVALID set by device code 🔨 M3 |
| `READ_WITH_WRITE` + writable attribute name | 🔨 M3 |
| Memorized attributes (`__value`) | 🔨 M3 (with the Database) |
| Enum labels | 🔨 M3 (display/edit in the browser) |
| Forwarded attributes (`__root_att`) | ⛔ needs device-to-device proxying; later phase |
| Attribute aliases | 🔨 M1 |

## RFC-5 Property

| Requirement | Status |
|---|---|
| Class, device, attribute, free properties | 🔨 M1 |
| Default value, mandatory in Database, types | 🔨 M1 |
| Naming convention | 🔨 M1 |

## RFC-6 Database

| Requirement | Status |
|---|---|
| Implemented as a device server, pre-known address, State/Status | 🔨 M1 |
| SQL backend (SQLite) | 🔨 M1 |
| Every command of the RFC-6 list (§7.2 of SPEC) | 🔨 M1 |
| Property history | 🔨 M1 |
| Uniqueness and naming checks, field sizes | 🔨 M1 |
| Database as a file (read-only) | 🔨 M1 |
| High availability (3500 req/s) | 🔨 M1 measured with `examples/bench`; replication ⛔ |

## RFC-7 Pipe

| Requirement | Status |
|---|---|
| Metadata, read/write, blob key → value | ✅ |
| Nested blobs | 🔨 M3 |
| Pipe events | 🔨 M2 |
| Pipes not polled | ✅ |

## RFC-8 Server

| Requirement | Status |
|---|---|
| Several devices and classes per server, unique instance | ✅ |
| Admin device name `dserver/<server>/<instance>` | ✅ |
| Black box | ✅ |
| Per-device serialisation | ✅ |
| Admin command table (polling, events, restart, kill, locking, logging) | ✅ QueryClass/QueryDevice · 🔨 M1 lifecycle · 🔨 M2 events · 🔨 M3 the rest |
| Export / unexport sequences with the Database | 🔨 M1 |

## RFC-9 Data types

| Requirement | Status |
|---|---|
| All scalar and array types, State, Encoded, Long/DoubleStringArray | ✅ |
| DevEnum (as Short) | ✅ transport · 🔨 M3 labels |
| DevPipeBlob | ✅ flat · 🔨 M3 nested |
| DevFailed, MultiDevFailed | ✅ DevFailed · 🔨 M3 MultiDevFailed for multi-attribute writes |

## RFC-10 Request-Reply

| Requirement | Status |
|---|---|
| The 21 operations of the cheatsheet | ✅ |
| Source DEV / CACHE / CACHE_DEV | ✅ |
| Transports | ✅ TCP, ZeroMQ, gRPC, Zenoh (RFC-11 CORBA ⛔: replaced by design) |

## RFC-12 Publisher-Subscriber

| Requirement | Status |
|---|---|
| Event types INTERFACE_CHANGE, PIPE, ATTR_CONF, CHANGE, PERIODIC, ARCHIVE, USER (+ DATA_READY) | 🔨 M2 |
| Negotiation through admin commands, SUBSCRIPTION_INFO | 🔨 M2 |
| Heartbeat every 9 s, confirmation within 600 s | 🔨 M2 |
| Re-subscription, missed-event and overflow notification | 🔨 M2 |
| Channels per transport (RFC-13 ZeroMQ and the others) | 🔨 M2 |

## RFC-14 Logging

| Requirement | Status |
|---|---|
| Levels, targets, admin logging commands | 🔨 M3 |

## Services outside the RFCs

| Item | Status |
|---|---|
| Starter device (`tango/admin/<host>`) | 🔨 M4 |
| Device browser GUI with generic forms (egui) | 🔨 M5 |
| Starter interface in the browser (Astor-like) | 🔨 M5 |
| Access control (RFC-18), memorised attribute service (RFC-17), cache system (RFC-16) | ⛔ later phase |

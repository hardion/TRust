# TRust — Tango control system over pluggable transports

Status: **Draft / Proof of Concept** — protocol version `1`.

TRust implements the Tango device model as defined by the
[Tango RFCs](https://github.com/tango-controls/rfc) (RFC-2 Device, RFC-3
Command, RFC-4 Attribute, RFC-7 Pipe, RFC-9 Data Types) and provides an
implementation of **RFC-10 "The Request-Reply protocol"** that replaces the
CORBA transport (RFC-11) by pluggable transports: JSON Lines over TCP, ZeroMQ, gRPC and Zenoh, selected by Cargo feature, by URL scheme or by detection of the wire.

The set of low-level requests is taken from the *Tango Request-Reply
cheatsheet* ("Tango Request-Reply" spreadsheet, sheet `REQUEST-REPLY`).
Requests marked *DEPRECATED* in the cheatsheet (`ATTRIBUTE READ`,
`ATTRIBUTE WRITE`, `ATTRIBUTE WRITE READ`) are replaced by their plural
counterparts and are not part of this protocol.

The key words MUST, MUST NOT, SHALL, SHOULD, MAY are to be interpreted as
described in RFC 2119.

---

## 1. Transport

The frames of §2 are carried by one of several transports, selected by the
scheme of the endpoint URL and compiled in with Cargo features. The
comparison and the design live in [`TRANSPORTS.md`](TRANSPORTS.md).

| Scheme | Transport | Feature | Mapping |
|---|---|---|---|
| none, `tcp://`, `tango://` | JSON Lines over TCP | `tcp` (default) | one frame per `\n` terminated line, replies in request order |
| `zmq+tcp://`, `zmq+ipc://` | ZeroMQ | `zmq` | `ROUTER` server, `REQ`/`DEALER` client, frame = last message part |
| `grpc://` | gRPC | `grpc` | unary `tango.v1.Device/Call` (`proto/tango.proto`), protobuf envelope, JSON `op`/`reply` |
| `zenoh://<locator>` | Zenoh | `zenoh` | queryable `tango/<device in lower case>`, frame = query payload |
| `auto://` | wire detection | `detect` | server: one port for every compiled TCP-based wire; client: probe |

Common rules:

* A client URL is `<endpoint>/domain/family/member`; the device name is
  always the last three `/`-separated segments.
* A server MAY serve the same devices on several endpoints at once.
* Each request receives exactly one reply. Transports that allow
  concurrency (ZeroMQ `DEALER`, HTTP/2, Zenoh) MAY reorder replies, so
  clients match them by `id`.
* Access to one device SHALL be serialised (Tango "by device" serialisation
  model): two requests never execute concurrently on the same device.
* A scheme whose transport is not compiled in → `API_UnsupportedTransport`.
* The plain TCP wire is human readable and can be driven with `nc`:

```
$ echo '{"v":1,"id":1,"device":"sys/tg/1","op":"DeviceName"}' | nc localhost 10000
{"id":1,"reply":{"Ok":{"String":"sys/tg/1"}}}
```

## 2. Frames

### 2.1 Request

| Field    | Cheatsheet column | Type                        | Required | Description |
|----------|-------------------|-----------------------------|----------|-------------|
| `v`      | Version Protocol  | u32                         | yes      | Protocol version, MUST be `1`. |
| `id`     | Request ID        | u64                         | yes      | Chosen by the client, echoed in the reply. |
| `client` | Client ID         | u64                         | no (0)   | Identifies the client process (recorded in the black box). |
| `source` | Source            | `"DEV"`\|`"CACHE"`\|`"CACHE_DEV"` | no (`DEV`) | Data source (see §5). |
| `device` | —                 | string                      | yes      | Target device name `domain/family/member`. Needed because one socket serves many devices (in CORBA the object reference carried it). |
| `op`     | Request Type + Payload | `Op`                   | yes      | The operation (see §3). |

### 2.2 Reply

| Field   | Cheatsheet column | Type                         | Description |
|---------|-------------------|------------------------------|-------------|
| `id`    | Reply ID          | u64                          | Copy of the request `id` (`0` if the request could not be parsed). |
| `reply` | Payload           | `{"Ok": Data}` \| `{"Err": DevFailed}` | Result of the operation. |

`DevFailed` (RFC-9 Exceptions) is a non-empty array of
`{"reason", "desc", "origin", "severity": "WARN"|"ERR"|"PANIC"}`.

### 2.3 Encoding conventions

Rust `serde` external tagging is used everywhere: a unit variant is a bare
string (`"DevicePing"`), a variant with data is a one-key object
(`{"Double": 3.14}`, `{"CommandInfo": {"name": "DevDouble"}}`).
Tango enumerations use their Tango spelling: `"ON"`, `"SCALAR"`, `"READ_WRITE"`,
`"VALID"`, `"OPERATOR"`, `"CACHE_DEV"`, `"ERR"`.

## 3. Operations

All operations of the cheatsheet, with their payloads and replies.
`N` means "list of".

| # | Request Type (cheatsheet) | `op`                                                                 | Reply `Data`                    |
|---|---------------------------|----------------------------------------------------------------------|---------------------------------|
| 1 | DEVICE NAME               | `"DeviceName"`                                                       | `{"String": name}`              |
| 2 | DEVICE DESCRIPTION        | `"DeviceDescription"`                                                | `{"String": description}`       |
| 3 | DEVICE ADM_NAME           | `"DeviceAdmName"`                                                    | `{"String": "dserver/<exec>/<instance>"}` |
| 4 | DEVICE PING               | `"DevicePing"`                                                       | `"Void"`                        |
| 5 | DEVICE BLACKBOX           | `{"DeviceBlackbox": {"n": u32}}`                                     | `{"BlackBox": N BlackBoxRecord}`|
| 6 | DEVICE INFO               | `"DeviceInfo"`                                                       | `{"Info": DeviceInfo}`          |
| 7 | COMMAND EXECUTION         | `{"CommandExecution": {"name", "arg": Value}}`                       | `{"Value": Value}`              |
| 8 | COMMANDS LIST             | `"CommandsList"`                                                     | `{"Commands": N CmdInfo}`       |
| 9 | COMMAND INFO              | `{"CommandInfo": {"name"}}`                                          | `{"Commands": [CmdInfo]}`       |
|10 | COMMAND HISTORY           | `{"CommandHistory": {"name", "n": u32}}`                             | `{"CmdHistory": N CmdHistory}`  |
|11 | ATTRIBUTES READ           | `{"AttributesRead": {"names": N string}}`                            | `{"Attributes": N {"Ok": AttrValue} \| {"Err": DevFailed}}` |
|12 | ATTRIBUTES WRITE          | `{"AttributesWrite": {"values": N [name, Value]}}`                   | `"Void"`                        |
|13 | ATTRIBUTES WRITE READ     | `{"AttributesWriteRead": {"values": N [name, Value], "names": N string}}` | as ATTRIBUTES READ         |
|14 | ATTRIBUTE SET CONFIG      | `{"AttributeSetConfig": {"configs": N AttrInfo}}`                    | `{"AttrConfigs": N AttrInfo}`   |
|15 | ATTRIBUTE GET CONFIG      | `{"AttributeGetConfig": {"names": N string}}` (empty = all attributes) | `{"AttrConfigs": N AttrInfo}` |
|16 | ATTRIBUTE HISTORY         | `{"AttributeHistory": {"name", "n": u32}}`                           | `{"AttrHistory": N AttrHistory}`|
|17 | PIPE READ                 | `{"PipeRead": {"name"}}`                                             | `{"Pipe": PipeValue}`           |
|18 | PIPE WRITE                | `{"PipeWrite": {"name", "blob": PipeBlob}}`                          | `"Void"`                        |
|19 | PIPE WRITE READ           | `{"PipeWriteRead": {"name", "blob": PipeBlob}}`                      | `{"Pipe": PipeValue}`           |
|20 | PIPE GET CONFIG           | `{"PipeGetConfig": {"names": N string}}` (empty = all pipes)         | `{"PipeConfigs": N PipeInfo}`   |
|21 | PIPE SET CONFIG           | `{"PipeSetConfig": {"configs": N PipeInfo}}`                         | `{"PipeConfigs": N PipeInfo}`   |

The cheatsheet's multiple versions of a request (V3/V4/V5 history formats,
`"All attributes"`/`"All attributes_3"`) collapse into the single form above.

### 3.1 Semantics

General:
* Device, command, attribute and pipe names SHALL be matched
  case-insensitively.
* Unknown device → `API_DeviceNotFound`. Unsupported `v` →
  `API_UnsupportedProtocol`. Unparsable frame → reply with `id` 0 and
  `API_InvalidRequest`; the connection stays open.

Device (RFC-2):
* Every device SHALL expose the commands `Init` (Void→Void), `State`
  (Void→State), `Status` (Void→String) and the read-only scalar attributes
  `State` (State) and `Status` (String).
* Default state is `UNKNOWN`; default status is `"The device is in <STATE> state."`.
* If the device state is `ON` and at least one attribute has quality `ALARM`
  or `WARNING`, the reported state SHALL be `ALARM`.
* `Init` re-initialises the device; the server calls it once for each device
  before serving (Not Exported → Exported transition).
* **DEVICE PING** does nothing but prove the device is reachable.
* **DEVICE BLACKBOX**: every request addressed to a device is recorded, on
  reception, in a ring buffer (depth 50) of
  `{time, operation, source, client_id, host}`. The reply holds the
  `min(n, stored)` most recent records, newest first. `n == 0` →
  `API_BlackBoxArgument`.
* **DEVICE INFO**: `{dev_class, server_id ("<exec>/<instance>"), server_host,
  server_version (= protocol version), doc_url, dev_type}`.
* **Admin device**: every server SHALL export `dserver/<exec>/<instance>` of
  class `DServer`, with commands `QueryClass` (Void→StringArray) and
  `QueryDevice` (Void→StringArray of `"<class>::<device>"`).

Command (RFC-3):
* `CmdInfo = {name, in_type, out_type, in_desc, out_desc, disp_level}`.
* Unknown command → `API_CommandNotFound`. `arg` whose data type differs from
  `in_type` → `API_IncompatibleCmdArgumentType`. An error raised by the device
  code is returned unchanged.

Attribute (RFC-4):
* `AttrInfo` = static metadata `{name, data_type, data_format
  (SCALAR|SPECTRUM|IMAGE), writable (READ|WRITE|READ_WRITE|READ_WITH_WRITE),
  max_dim_x, max_dim_y, disp_level}` + properties `{description, label, unit,
  format, min_value, max_value, min_alarm, max_alarm, min_warning,
  max_warning}` (numbers or `null` = "Not specified").
* `data_type` is always the scalar RFC-9 type; SPECTRUM/IMAGE values are
  carried with the matching array type (`Double` → `DoubleArray`).
* `AttrValue = {name, value, w_value, quality, time, dim_x, dim_y}`;
  SCALAR: `dim_x=1, dim_y=0`; SPECTRUM: `dim_x=len, dim_y=0`;
  IMAGE: row-major, `dim_x=max_dim_x, dim_y=len/max_dim_x`.
  `w_value` is the last *set value* (`null` if never written).
* Quality: `ALARM` if a numeric element is outside `[min_alarm, max_alarm]`,
  else `WARNING` if outside `[min_warning, max_warning]`, else `VALID`.
* **ATTRIBUTES READ** never fails as a whole for an attribute problem; each
  element carries its own result (`API_AttrNotFound`, device error, …).
* **ATTRIBUTES WRITE**: each value is checked then written in order:
  not `WRITE`/`READ_WRITE` → `API_AttrNotWritable`; wrong type →
  `API_IncompatibleAttrDataType`; numeric element outside
  `[min_value, max_value]` → `API_WAttrOutsideLimit`. Successful writes are
  kept; all errors are aggregated in one `DevFailed`.
* **ATTRIBUTES WRITE READ** writes then reads; a write error fails the request.
* **ATTRIBUTE SET CONFIG** updates only the properties (static metadata is
  ignored); `min > max` for any pair → `API_IncoherentValues`. Returns the
  resulting configuration.
* **ATTRIBUTE GET CONFIG**: unknown name → `API_AttrNotFound`.

Pipe (RFC-7):
* `PipeInfo = {name, description, label, disp_level, writable}`.
* `PipeBlob = {name, elements: N [key, Value]}`;
  `PipeValue = {name, time, blob}`.
* Write on a non-writable pipe → `API_PipeNotWritable`; unknown pipe →
  `API_PipeNotFound`. **PIPE WRITE READ** writes then reads.
* **PIPE SET CONFIG** updates `description` and `label` only.
* Pipes are never polled (RFC-7).

## 4. Data types (RFC-9)

`Value` is one of (JSON tag → payload):

| Scalar             | Array                   |
|--------------------|-------------------------|
| `Void` (no payload)|                         |
| `Boolean` bool     | `BooleanArray` [bool]   |
| `UChar` u8         | `CharArray` [u8]        |
| `Short` i16        | `ShortArray` [i16]      |
| `UShort` u16       | `UShortArray` [u16]     |
| `Long` i32         | `LongArray` [i32]       |
| `ULong` u32        | `ULongArray` [u32]      |
| `Long64` i64       | `Long64Array` [i64]     |
| `ULong64` u64      | `ULong64Array` [u64]    |
| `Float` f32        | `FloatArray` [f32]      |
| `Double` f64       | `DoubleArray` [f64]     |
| `String` string    | `StringArray` [string]  |
| `State` DevState   | `StateArray` [DevState] |
| `Encoded` {format, data:[u8]} | `EncodedArray` [..] |
|                    | `LongStringArray` [[i32],[string]] |
|                    | `DoubleStringArray` [[f64],[string]] |

`DevState` ∈ `ON OFF CLOSE OPEN INSERT EXTRACT MOVING STANDBY FAULT INIT
RUNNING ALARM DISABLE UNKNOWN`. `DataType` uses the same tag names without
payload. `DevEnum` is transported as `Short` (RFC-9).

## 5. Polling, history and data source

* The server MAY poll attributes and Void-input commands at a fixed period
  (server configuration). As in Tango, the object kind (attribute or command)
  is given explicitly since an attribute and a command may share a name. Each poll result (value or error) is stored in a
  ring buffer of depth 10 (Tango `poll_ring_depth` default).
* **COMMAND/ATTRIBUTE HISTORY** returns the `min(n, stored, depth)` most
  recent entries, newest first: `CmdHistory = {time, value: {"Ok": Value} |
  {"Err": DevFailed}}`, `AttrHistory = {time, value: {"Ok": AttrValue} |
  {"Err": DevFailed}}`. Not polled → `API_CmdNotPolled` /
  `API_AttrNotPolled`.
* `source` applies to COMMAND EXECUTION and ATTRIBUTES READ:
  `DEV` = always the device; `CACHE` = last polled result, not polled →
  `API_CmdNotPolled`/`API_AttrNotPolled`; `CACHE_DEV` = cache if polled,
  device otherwise.

Time is a `f64` number of seconds since the Unix epoch.

## 6. Properties (RFC-5)

* A property is a named list of strings (a scalar is a list of one). Four
  kinds exist: **class**, **device**, **attribute** (of a class or of a device)
  and **free** (object + name). Pipe properties are attribute-like.
* A device class declares its device properties:
  `PropInfo = {name, data_type, description, default: [string] | null,
  mandatory: bool}` (`Device::properties()`). `data_type` is one of the
  RFC-5 recommended types (`Boolean`…`Double`, `String`, `*Array`).
* At export (and at each `Init`) the server reads the device properties from
  the Database, falls back to the declared default, fails the device
  (`FAULT`, status naming the property) if a *mandatory* property is
  missing, converts the strings to `data_type` and hands them to
  `Device::init(&Properties)`.
* Attribute properties stored in the Database (`description`, `label`,
  `unit`, `format`, `min_value` … `max_warning`, the event properties of §9.3,
  `__value` for memorised attributes) override the static ones at export;
  ATTRIBUTE SET CONFIG writes them back to the Database.
* Property names follow RFC-5 (`ALPHA *254(alphanum / "_")`; attribute
  properties may start with `_`); others → `API_InvalidPropertyName`.

## 7. Naming and Database (RFC-6)

### 7.1 Device names and resolution

```
full-name  = [ "tango://" db-host ":" db-port "/" ] device [ "#dbase=no" ]
device     = domain "/" family "/" member          ; RFC-2, case insensitive
db-host    = <TANGO_HOST host>
```

* `sys/tg/1` resolves through the Database at `$TANGO_HOST`
  (`host:port`, default port 10000).
* `tango://db:10000/sys/tg/1` resolves through that Database.
* `tango://host:port/sys/tg/1#dbase=no` is a direct connection, no Database
  (Tango syntax); the transport is detected (`auto://`).
* The endpoint URLs of §1 (`grpc://…/sys/tg/1`, …) stay valid and bypass
  the Database.
* Resolution = `DbImportDevice(device)` → `{exported, endpoint URL, …}`.
  Not exported → `API_DeviceNotExported`. The client caches the endpoint and
  re-imports once on `API_CommunicationFailed`.
* **Aliases**: a name without `/` is a device alias, resolved with
  `DbGetAliasDevice`; `alias/attribute` names an attribute.
  Attribute aliases resolve with `DbGetAliasAttribute`.

### 7.2 The Database device

* The Database service is a device server, `DataBaseds`, instance `2`,
  exporting the device `sys/database/2` (class `DataBase`) and its admin
  device. It listens on the pre-known address `$TANGO_HOST` (all transports
  through `auto://`). It has no class or device properties. It is ON while its
  backend is reachable, FAULT otherwise.
* Backend: **SQLite**, one file (`DataBaseds 2 -db=<path>`, default `tango.db`;
  `-endpoint=<url>`, default `auto://0.0.0.0:10000`). Tables: `device`
  (with alias and export columns), `server` (host, controlled mode, startup
  level), `attribute_alias`, `event`, and **one** `property` table for the
  seven property kinds (`device`, `class`, `free`, `device_attr`,
  `class_attr`, `device_pipe`, `class_pipe`) with its `property_hist`
  history. One table and one code path serve all the property commands.
* Names are unique and validated (§6, RFC-2); sizes: device 255,
  domain/family/member 85, alias 255.
* Every command listed by RFC-6 is implemented, with the Tango argument types
  (mostly `DevVarStringArray` and `DevVarLongStringArray` packed exactly as
  the C++ `DataBaseds` does, so that the argument layouts of the Tango
  documentation apply):

| Group | Commands |
|---|---|
| Devices | `DbAddDevice`, `DbDeleteDevice`, `DbGetClassForDevice`, `DbGetClassInheritanceForDevice`, `DbGetDeviceClassList`, `DbGetDeviceDomainList`, `DbGetDeviceFamilyList`, `DbGetDeviceMemberList`, `DbGetDeviceInfo`, `DbGetDeviceList`, `DbGetDeviceWideList`, `DbGetDeviceServerClassList` |
| Classes | `DbGetClassList` |
| Class properties | `DbGetClassProperty`, `DbPutClassProperty`, `DbDeleteClassProperty`, `DbGetClassPropertyHist`, `DbGetClassPropertyList` |
| Device properties | `DbGetDeviceProperty`, `DbPutDeviceProperty`, `DbDeleteDeviceProperty`, `DbGetDevicePropertyHist`, `DbGetDevicePropertyList` |
| Class attribute properties | `DbGetClassAttributeProperty`, `…2`, `DbPutClassAttributeProperty`, `…2`, `DbDeleteClassAttributeProperty`, `DbGetClassAttributePropertyHist` |
| Device attribute properties | `DbGetDeviceAttributeProperty`, `…2`, `DbPutDeviceAttributeProperty`, `…2`, `DbDeleteDeviceAttributeProperty`, `DbDeleteAllDeviceAttributeProperty`, `DbGetDeviceAttributePropertyHist` |
| Pipe properties | `DbGetClassPipeProperty`, `DbPutClassPipeProperty`, `DbDeleteClassPipeProperty`, `DbGetClassPipePropertyHist`, `DbGetDevicePipeProperty`, `DbPutDevicePipeProperty`, `DbDeleteDevicePipeProperty`, `DbDeleteAllDevicePipeProperty`, `DbGetDevicePipePropertyHist` |
| Free properties | `DbGetProperty`, `DbPutProperty`, `DbDeleteProperty`, `DbGetPropertyHist`, `DbGetPropertyList`, `DbGetObjectList` |
| Attributes, pipes | `DbGetClassAttributeList`, `DbDeleteClassAttribute`, `DbGetDeviceAttributeList`, `DbDeleteDeviceAttribute`, `DbGetClassPipeList`, `DbDeleteClassPipe`, `DbGetDevicePipeList`, `DbDeleteDevicePipe` |
| Aliases | `DbPutDeviceAlias`, `DbGetDeviceAlias`, `DbGetAliasDevice`, `DbDeleteDeviceAlias`, `DbGetDeviceAliasList`, `DbPutAttributeAlias`, `DbGetAttributeAlias`, `DbGetAttributeAlias2`, `DbGetAliasAttribute`, `DbDeleteAttributeAlias`, `DbGetAttributeAliasList` |
| Hosts, servers | `DbAddServer`, `DbDeleteServer`, `DbRenameServer`, `DbGetServerList`, `DbGetServerNameList`, `DbGetInstanceNameList`, `DbGetHostList`, `DbGetHostServerList`, `DbGetHostServersInfo`, `DbGetServerInfo`, `DbPutServerInfo`, `DbDeleteServerInfo` |
| Export | `DbExportDevice`, `DbUnExportDevice`, `DbUnExportServer`, `DbImportDevice`, `DbGetDeviceExportedList`, `DbGetExportedDeviceListForClass`, `DbExportEvent`, `DbImportEvent`, `DbUnExportEvent` |
| Miscellaneous | `DbInfo`, `DbGetDataForServerCache`, `DbGetForwardedAttributeListForDevice`, `DbGetCSDbServerList`, `ResetTimingValues`, `DbMySqlSelect` (a read-only `SELECT` on the SQLite schema) |

* Wildcards in list commands use `*` (translated to SQL `LIKE`). Names are
  matched case-insensitively.
* Argument layouts (C++ DataBaseds):

| Commands | Argument | Reply |
|---|---|---|
| `DbPut{Device,Class}Property`, `DbPutProperty` | `[obj, n, (name, count, values…)*]` | |
| `DbGet{Device,Class}Property`, `DbGetProperty` | `[obj, name…]` | `[obj, n, (name, count, values…)*]` (count 0 if unset) |
| `Db*PropertyHist` | `[obj, name wildcard]` | `[(name, date, count, values…)*]`, oldest first, last 10 per property, a deletion has count 0 |
| `DbPut{Device,Class}AttributeProperty2`, `Db*PipeProperty` puts | `[obj, n_attr, (attr, n_props, (prop, count, values…)*)*]` | |
| `DbGet{Device,Class}AttributeProperty2`, `Db*PipeProperty` gets | `[obj, attr…]` | `[obj, n_attr, (attr, n_props, (prop, count, values…)*)*]`, properties sorted |
| `Db{Put,Get}{Device,Class}AttributeProperty` (v1) | same without `count` (one value) | |
| `Db*AttributePropertyHist`, `Db*PipePropertyHist` | `[obj, attr, prop wildcard]` | `[(attr, prop, date, count, values…)*]` |
| `DbImportDevice` | `device` | `LongStringArray([exported, pid], [name, endpoint, version, server, host, class])` |
| `DbGetDeviceInfo` | `device` | `LongStringArray([exported, pid], [name, endpoint, version, server, host, started, stopped, class])` |
| `DbExportDevice`, `DbExportEvent` | `[name, endpoint, host, pid, version]` | |
| `DbImportEvent` | `name` | `LongStringArray([exported, pid], [name, endpoint, version, host])` |
| `DbAddServer` | `[server, (device, class)*]` (adds `dserver/<server>` too) | |
| `DbGetDeviceClassList` | `server` | `[(device, class)*]` |
| `DbPutServerInfo` / `DbGetServerInfo` | `[server, host, mode, level]` | |
| `DbGetHostServersInfo` | `host` | `[(server, mode, level)*]` |
| `DbMySqlSelect` | one `SELECT` | `LongStringArray([rows, columns], values row by row)` |

* `DbExportDevice` stores, in the `ior` column of Tango, the **endpoint URL**
  of the device (e.g. `auto://host:10000`), with host, pid and version.
* `DbGetDataForServerCache` returns the server's `[(device, class)*]` (the
  C++ cache bundle has no meaning without CORBA).
* Errors: `DB_DeviceNotDefined`, `DB_IncorrectDeviceName`,
  `DB_IncorrectServerName`, `DB_AliasAlreadyDefined`, `DB_AliasNotDefined`,
  `DB_IncorrectArguments`, `DB_SQLError` (also for a non-`SELECT` in
  `DbMySqlSelect`), `API_InvalidPropertyName`.

### 7.3 Device server life cycle with a Database

`Server::new(exec, instance)` with `$TANGO_HOST` set:

1. `DbGetDeviceServerClassList` / `DbGetDeviceList(server, class)` give the
   devices to create (devices can also be added in code; they are then
   registered with `DbAddDevice`);
2. properties are read (§6) and each device is initialised;
3. `DbExportDevice` for each device and for the admin device, with the
   endpoint URL the server listens on;
4. on shutdown (`Kill`, SIGTERM, SIGINT): `DbUnExportServer`, then exit.

Without `$TANGO_HOST`, or with `-nodb`, the server runs as today. With
`-file=<path>` it reads devices and properties from a Database file (§7.4).

API: `Server::new(exec, instance)` with `.class(name, factory)` for the
classes whose devices come from the Database, `.device(name, device)` for
devices created in code, `.database(tango_host)`, `.nodb()`, `.db_file(path)`;
or `Server::from_args(exec, args)` for
`<instance> [-nodb] [-file=<path>] [-dbhost=<host:port>] [-endpoint=<url>]…`
then `.run_args()`. `run` binds every endpoint first (port 0 picks a free port)
and exports the devices with the first one; the admin device gains `Kill`.

### 7.4 Database as a file

The RFC-6 file syntax is supported, read-only:
`<server>/<instance>/DEVICE/<class>: dev1, dev2`,
`<device>-><prop>: value`, `<device>/<attr>-><prop>: value`,
`CLASS/<class>-><prop>: value`, `#` comments, `\` line continuation,
`,` array separator, `"…"` quoted strings.

## 8. Admin device (RFC-8)

`dserver/<server>/<instance>`, class `DServer`, implements the RFC-8 command
table:

| Command | In → Out | Behaviour |
|---|---|---|
| `State`, `Status`, `Init` | | standard (Init re-initialises every device) |
| `QueryClass`, `QueryDevice`, `QuerySubDevice` | Void → StringArray | classes; `class::device`; sub-devices (empty) |
| `QueryWizardClassProperty`, `QueryWizardDevProperty` | String → StringArray | `[name, description, default]` triples from `PropInfo` |
| `AddObjPolling`, `RemObjPolling`, `UpdObjPollingPeriod` | LongStringArray / StringArray | runtime polling configuration, persisted as the `polled_attr` / `polled_cmd` device properties |
| `PolledDevice`, `DevPollStatus` | | polled devices; per object: period, ring depth, last read time and duration, last error |
| `StartPolling`, `StopPolling` | Void → Void | |
| `EventSubscriptionChange`, `ZmqEventSubscriptionChange`, `EventConfirmSubscription` | §9.2 | event negotiation |
| `DevRestart` | String → Void | re-creates one device (unexport, drop, create, init, export) |
| `RestartServer` | Void → Void | re-creates every device |
| `Kill` | Void → Void | replies, then shuts the server down (§7.3 step 4) |
| `LockDevice`, `UnLockDevice`, `ReLockDevices`, `DevLockStatus` | | device locking: a locked device refuses writes and commands from other clients (`API_DeviceLocked`); a lock expires after its validity |
| `AddLoggingTarget`, `RemoveLoggingTarget`, `GetLoggingTarget`, `GetLoggingLevel`, `SetLoggingLevel`, `StartLogging`, `StopLogging` | | per-device log levels and targets (`console::`, `file::<path>`, `device::<name>`), RFC-14 levels OFF…DEBUG |

## 9. Events (RFC-12)

### 9.1 Event types and triggers

| Event type | Fired when (RFC-4 properties) |
|---|---|
| `CHANGE_EVENT` | polled value moves by more than `abs_change` or `rel_change` (%) since the last event, or quality changes, or a read error appears or clears |
| `PERIODIC_EVENT` | every `period` ms (default 1000) of polling |
| `ARCHIVE_EVENT` | `archive_abs_change` / `archive_rel_change` exceeded, or `archive_period` elapsed |
| `USER_EVENT` | pushed by device code (`EventSink`, below) |
| `ATTR_CONF_EVENT` | ATTRIBUTE SET CONFIG (or a property change in the Database) |
| `DATA_READY_EVENT` | pushed by device code, carries a counter |
| `INTERFACE_CHANGE_EVENT` | device (re)started, attributes/commands added or removed |
| `PIPE_EVENT` | pushed by device code |

* CHANGE, PERIODIC and ARCHIVE need the attribute to be polled, unless the
  device declares it pushes them itself (`AttrInfo.events_by_code`).
  Subscribing otherwise → `API_AttributePollingNotStarted`. A change or
  archive subscription with none of the thresholds set →
  `API_EventPropertiesNotSet`.
* `abs_change`, `rel_change`, `period`, `archive_*` are new optional
  attribute properties (`AttrInfo`), settable with ATTRIBUTE SET CONFIG.
* On subscription the subscriber immediately receives one event holding the
  current value (Tango "synchronous first event"). As in Tango, the client
  produces it by reading the attribute (or its configuration, or the
  interface) once its event connection is up, with `seq` 0, so it cannot be
  lost.
* Device code pushes USER, DATA_READY, PIPE events (and CHANGE/ARCHIVE for
  `events_by_code` attributes) through the `EventSink` the server hands to
  `Device::attach(sink)` at export: `sink.push_value(attr, EventType::User,
  value)`, `sink.push_data_ready(attr, counter)`, `sink.push_pipe(pipe, blob)`.
  Init fires INTERFACE_CHANGE (`started: true`).
* Change and archive thresholds: an event fires when, for any element,
  `|new - old| >= abs_change`, or `|new - old| / |old| * 100 >= rel_change`,
  `old` being the value of the last event sent; also when the value changes
  for a non-numeric type, when the quality or dimensions change, or when a
  read error appears or clears.

### 9.2 Negotiation and channels

1. The client executes on the admin device
   `EventSubscriptionChange([device, attribute, "subscribe", event_type, client_version])`
   (`ZmqEventSubscriptionChange` is accepted as a synonym).
2. The admin replies `SUBSCRIPTION_INFO` as a `LongStringArray`:
   `[version, heartbeat_period_ms, confirm_period_ms],
   [zmq_endpoint, event_channel, heartbeat_channel]`.
3. The client receives the events over the *same transport* as its
   request-reply connection (table below): on its request endpoint itself,
   except ZeroMQ, which uses the PUB endpoint `zmq_endpoint` (empty when the
   server serves no ZeroMQ).
4. The server publishes a heartbeat on `heartbeat_channel` every
   `heartbeat_period` (**9 s**, `Server::heartbeat`). No heartbeat for 3
   periods → the client's stream receives an `API_EventTimeout` error event and
   the client re-subscribes.
5. The client calls `EventConfirmSubscription([device, attribute, event_type])`
   every `confirm_period` (**200 s**); the server drops subscriptions not
   confirmed for 3 confirm periods (**600 s**, `Server::event_confirm`).
6. `"unsubscribe"` as the action cancels; the server stops publishing when
   no subscriber is left.

Channel names: `<device>/<attribute>.<event_type in lower case>`, e.g.
`sys/tg/1/double_scalar.change`; heartbeat: `dserver/<server>/<instance>.heartbeat`.

| Transport | Event channel mapping |
|---|---|
| TCP JSON Lines | the client opens a second connection to the same endpoint whose first line is `{"subscribe": [channel, …]}`; the server then streams one JSON `Event` per line |
| ZeroMQ | one `PUB` socket per server (bound next to the ROUTER, port chosen by the system); the ZeroMQ topic is the channel name; message = `[channel, json Event]` |
| gRPC | server-streaming `rpc Subscribe(SubscribeRequest{repeated string channels}) returns (stream EventFrame{bytes event})` on the same service |
| Zenoh | the server puts each event on key `tango/event/<channel>` (e.g. `tango/event/sys/tg/1/ramp.change`); clients subscribe to those keys on their session |

### 9.3 Event payload

```
Event = { channel, seq: u64, type: EventType, time,
          data: {"Ok": AttrValue | AttrInfo | PipeValue
                       | {"DataReady": {name, data_type, counter}}
                       | {"Interface": {commands, attributes, started}} }
              | {"Err": DevFailed} }
```

* `seq` is per channel and increases by one: a gap tells the client it
  missed events, and its stream receives an `API_MissedEvents` error event
  before the next one.
* Buffers are bounded (1000 events per subscriber, Tango `zmq_sub_event_hwm`);
  overflow drops the oldest events, which the client then sees as a gap.

### 9.4 Client API

```rust
let mut events = dev.event_stream("double_scalar", EventType::Change).await?;
while let Some(e) = events.recv().await { … }        // dropping the stream unsubscribes
let sub = dev.subscribe_event("double_scalar", EventType::Change, |e: Event| { … }).await?;
drop(sub);                                           // or sub.unsubscribe().await
```

`INTERFACE_CHANGE` ignores the attribute name (use `""`); `PIPE` takes the
pipe name.

## 10. Starter (Tango `Starter` class)

* One Starter runs per host as the device `tango/admin/<host>` (class
  `Starter`, server `Starter/<host>`). It starts, stops and monitors the
  device servers registered for its host in the Database
  (`DbGetHostServersInfo`: server, controlled flag, startup level).
* Device properties: `StartDsPath` (directories holding the executables),
  `WaitForDriverStartup`, `InterStartupLevelWait` (s), `ServerStartupTimeout`
  (s), `StartServersAtStartup` (bool), `LogFileHome`, `KeepLogFiles`.
* A server is *running* when its admin device answers a ping, *starting*
  until then (up to `ServerStartupTimeout`), *stopped* otherwise; a
  controlled server that dies is reported (state `ALARM`) but not restarted
  (Tango behaviour).

| Command | In → Out | Behaviour |
|---|---|---|
| `DevStart` | String → Void | starts `<exec> <instance>` from `StartDsPath`, stdout/stderr to `LogFileHome/<exec>_<instance>.log` |
| `DevStop` | String → Void | `Kill` on the server's admin device |
| `HardKillServer` | String → Void | kills the process (SIGKILL) |
| `DevStartAll` | Short → Void | starts every controlled server of a startup level, waiting `InterStartupLevelWait` between levels |
| `DevStopAll` | Short → Void | stops every server of a level |
| `DevGetRunningServers`, `DevGetStopServers` | Boolean (controlled only) → StringArray | |
| `DevReadLog` | String → String | the server's log file (`"Starter"` = the Starter's own log) |
| `UpdateServersInfo` | Void → Void | re-reads the Database |

| Attribute | Type | Content |
|---|---|---|
| `HostState` | Short | 0 ON, 1 MOVING (a server starting), 2 ALARM (a controlled server stopped) |
| `Servers` | StringArray | `name\tstate\tcontrolled\tlevel\tnb_instances` per server |
| `RunningServers`, `StoppedServers` | StringArray | |

Changes of `Servers` and `HostState` are pushed as CHANGE events.

## 11. Out of scope

Wire compatibility with CORBA Tango clients and servers, forwarded and
dynamic attributes beyond what INTERFACE_CHANGE needs, enum labels editing,
access control (RFC-18), memorised attribute service beyond `__value`
(RFC-17), and a binary codec. See `docs/COMPLIANCE.md` for the status of
every RFC requirement.

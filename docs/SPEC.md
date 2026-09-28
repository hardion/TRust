# TRust — Tango Request-Reply over a simple socket protocol

Status: **Draft / Proof of Concept** — protocol version `1`.

TRust implements the Tango device model as defined by the
[Tango RFCs](https://github.com/tango-controls/rfc) (RFC-2 Device, RFC-3
Command, RFC-4 Attribute, RFC-7 Pipe, RFC-9 Data Types) and provides an
implementation of **RFC-10 "The Request-Reply protocol"** that replaces the
CORBA transport (RFC-11) by a plain TCP socket carrying line-delimited JSON.

The set of low-level requests is taken from the *Tango Request-Reply
cheatsheet* ("Tango Request-Reply" spreadsheet, sheet `REQUEST-REPLY`).
Requests marked *DEPRECATED* in the cheatsheet (`ATTRIBUTE READ`,
`ATTRIBUTE WRITE`, `ATTRIBUTE WRITE READ`) are replaced by their plural
counterparts and are not part of this protocol.

The key words MUST, MUST NOT, SHALL, SHOULD, MAY are to be interpreted as
described in RFC 2119.

---

## 1. Transport

* The server SHALL listen on a TCP port. One server process (Device Server,
  RFC-8) MAY host any number of devices on the same port.
* A frame SHALL be one UTF-8 JSON object terminated by a single `\n`
  (JSON Lines). JSON string escaping guarantees a frame never contains a raw
  newline.
* The client sends *Request* frames, the server answers each with exactly one
  *Reply* frame. On a given connection replies SHALL be sent in request order;
  a client MAY therefore pipeline requests.
* A server SHALL serve several connections concurrently.
* Access to one device SHALL be serialised (Tango "by device" serialisation
  model): two requests never execute concurrently on the same device.
* The protocol is human readable, a device can be driven with `nc`:

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
| `source` | Source            | `"Dev"`\|`"Cache"`\|`"CacheDev"` | no (`Dev`) | Data source (see §5). |
| `device` | —                 | string                      | yes      | Target device name `domain/family/member`. Needed because one socket serves many devices (in CORBA the object reference carried it). |
| `op`     | Request Type + Payload | `Op`                   | yes      | The operation (see §3). |

### 2.2 Reply

| Field   | Cheatsheet column | Type                         | Description |
|---------|-------------------|------------------------------|-------------|
| `id`    | Reply ID          | u64                          | Copy of the request `id` (`0` if the request could not be parsed). |
| `reply` | Payload           | `{"Ok": Data}` \| `{"Err": DevFailed}` | Result of the operation. |

`DevFailed` (RFC-9 Exceptions) is a non-empty array of
`{"reason", "desc", "origin", "severity": "Warn"|"Err"|"Panic"}`.

### 2.3 Encoding conventions

Rust `serde` external tagging is used everywhere: a unit variant is a bare
string (`"DevicePing"`), a variant with data is a one-key object
(`{"Double": 3.14}`, `{"CommandInfo": {"name": "DevDouble"}}`).

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
  (Scalar|Spectrum|Image), writable (Read|Write|ReadWrite|ReadWithWrite),
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
  not `Write`/`ReadWrite` → `API_AttrNotWritable`; wrong type →
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
  (server configuration). Each poll result (value or error) is stored in a
  ring buffer of depth 10 (Tango `poll_ring_depth` default).
* **COMMAND/ATTRIBUTE HISTORY** returns the `min(n, stored, depth)` most
  recent entries, newest first: `CmdHistory = {time, value: {"Ok": Value} |
  {"Err": DevFailed}}`, `AttrHistory = {time, value: {"Ok": AttrValue} |
  {"Err": DevFailed}}`. Not polled → `API_CmdNotPolled` /
  `API_AttrNotPolled`.
* `source` applies to COMMAND EXECUTION and ATTRIBUTES READ:
  `Dev` = always the device; `Cache` = last polled result, not polled →
  `API_CmdNotPolled`/`API_AttrNotPolled`; `CacheDev` = cache if polled,
  device otherwise.

Time is a `f64` number of seconds since the Unix epoch.

## 6. Out of scope for the PoC

Database device and properties (RFC-5/6), events (RFC-12), forwarded and
memorized attributes, dynamic attributes, enum labels, access control,
asynchronous (fire-and-forget) calls, binary encoding. The frame layer is
isolated so JSON can later be swapped for a binary codec (CBOR, MessagePack)
without touching the model.

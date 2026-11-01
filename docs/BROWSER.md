# TRust Browser — device browser and Starter interface

`trust-browser` is a native desktop application written in Rust with
**egui / eframe**; the same code also builds to WebAssembly. It talks to a
control system only through `DeviceProxy`, so it works over every transport.
Every form is **inferred from the device's interface** (`CmdInfo`, `AttrInfo`,
`PipeInfo`, `PropInfo`), so no device class needs GUI code.

## 1. Layout

```
┌───────────────┬─────────────────────────────────────────────────────────┐
│ Database      │  sys/tg/1   [TgTest]   ● ON   "The device is in ON…"    │
│ ▸ dserver     │ ┌ Attributes ┬ Commands ┬ Pipes ┬ Properties ┬ Admin ┐   │
│ ▾ sys         │ │ name         value        unit  quality   write     │   │
│   ▾ tg        │ │ double_scalar 12.50       mm    VALID   [____] [Set]│   │
│     ● 1       │ │ long_spectrum [1, 2, 3]         VALID   ⎯ plot      │   │
│ ▸ tango       │ │ short_image   2×2 ▦             VALID               │   │
│               │ └──────────────────────────────────────────────────────┘   │
│ Hosts         │                                                          │
│ ▸ host-a  ●   │  Event log / errors                                      │
└───────────────┴─────────────────────────────────────────────────────────┘
```

* **Left panel: Database tree.** domain → family → member
  (`DbGetDeviceDomainList`, `…FamilyList`, `…MemberList`), aliases, and a
  filter box (wildcards). Each leaf shows an exported/not-exported dot
  (`DbGetDeviceInfo`). Without a Database, a URL box opens a device directly
  (`grpc://host:50051/sys/tg/1`, `tango://host:port/dev#dbase=no`).
* **Hosts section** (Starter interface, §4): hosts from `DbGetHostList`, each
  with its Starter's `HostState` colour.
* **Device panel**: header (name, class, State as a colour, Status), then
  tabs.

## 2. Generic forms

Every widget comes from metadata alone, through one pure function per
type, so the rules below can be unit-tested without a GUI:

| Data type / format | Display | Input widget |
|---|---|---|
| `Void` | — | none (command button only) |
| `Boolean` | ✓ / ✗ | checkbox |
| integer types | number with `format` | drag value clamped to the type range and to `[min_value, max_value]` |
| `Float`, `Double` | number with `format` and `unit` | text field parsed as float, with range check |
| `String` | text | single-line text (multi-line if it contains `\n`) |
| `State` | coloured state name (Tango colours) | combo box of the 14 states |
| `DevEnum` (Short + `enum_labels`) | label | combo box of the labels |
| `Encoded` | format + byte count, hex preview | not writable from the GUI |
| `*Array`, SPECTRUM | inline list; plot (egui_plot) for numbers | comma-separated text, element-wise validation |
| IMAGE | dimensions + greyscale heat map | not writable from the GUI |
| `LongStringArray`, `DoubleStringArray` | two lists | two comma-separated fields |
| Pipe blob | tree of key → value | tree editor for writable pipes |

* **Attributes tab**: one row per attribute: name (tooltip = description and
  label), read value, `w_value`, unit, quality colour (VALID green, ALARM red,
  WARNING orange, CHANGING blue, INVALID grey), and a write widget for
  `WRITE`/`READ_WRITE` attributes. Values refresh by **CHANGE events** when the
  attribute supports them, otherwise by polling every second (configurable).
  Clicking a numeric attribute opens a trend plot.
* **Commands tab**: the command list with `in_type`/`out_type` and their
  descriptions, the input widget for `in_type`, an *Execute* button and the
  result rendered by the `out_type` rule. Errors show the full `DevFailed`
  stack.
* **Pipes tab**: read/refresh, and a write editor for writable pipes.
* **Properties tab**: device properties from the Database with the
  `PropInfo` description and default (`QueryWizardDevProperty`); edit, add,
  delete, and show the history (`DbGetDevicePropertyHist`).
* **Attribute config dialog**: the editable properties of `AttrInfo`
  (label, unit, format, limits, alarms, event thresholds) → ATTRIBUTE SET
  CONFIG.
* **Admin tab**: black box (DEVICE BLACKBOX), device info, polling status and
  configuration (`DevPollStatus`, `AddObjPolling`, …), logging level, Init,
  restart (`DevRestart` on the admin device).

## 3. Interaction rules

* Network calls never block the UI: each request runs on a tokio runtime
  owned by the app and its result comes back through a channel the UI drains
  every frame (`ctx.request_repaint()` on arrival).
* A failed call keeps the last value, greys it out, and logs the error in the
  bottom panel.
* Writes ask for confirmation when the value is outside
  `[min_alarm, max_alarm]`.

## 4. Starter interface (Astor-like)

* **Hosts view**: one row per host: Starter state (`HostState`), counts of
  running/stopped servers. Hosts without a Starter show "no Starter".
* **Host panel**: servers grouped by startup level (from the `Servers`
  attribute, refreshed by its CHANGE events), each with a state light
  (running / starting / stopped / not controlled) and buttons
  *Start* (`DevStart`), *Stop* (`DevStop`), *Kill* (`HardKillServer`),
  *Log* (`DevReadLog`, shown in a scrollable window), and per level
  *Start level* / *Stop level* (`DevStartAll` / `DevStopAll`).
* **Server panel**: its devices and classes (`QueryDevice` on its admin
  device); opening a device switches to the device panel.
* **Server registration**: a dialog to declare a new server on a host
  (`DbAddServer`, `DbPutServerInfo` with level and controlled flag), so that
  the Starter can start it.

## 5. Tests

* The type → widget and text → `Value` rules are pure functions with unit
  tests for every data type, including range errors.
* UI tests with `egui_kittest` run the app headless against a TgTest server,
  a Database and a Starter: open a device from the tree, write an attribute,
  execute a command, start and stop a server from the host panel. They
  assert on the accessibility tree, not on pixels.

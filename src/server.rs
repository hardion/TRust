//! Device server: the `Device` trait, the transport-independent request handler, black box and polling.

use crate::db::{Database, hostname, tango_host};
use crate::dbfile::DbFile;
use crate::events::{self, EventData, EventSink, EventType, Hub, LogLevel};
use crate::props::{PropInfo, Properties};
use crate::protocol::*;
use crate::store::{Raw, Store, apply_attribute};
use crate::transport::Scheme;
use crate::types::*;
use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};
use tokio::sync::{Notify, watch};

const BLACK_BOX_DEPTH: usize = 50;
const POLL_RING_DEPTH: usize = 10;

/// A Tango device (RFC-2). Only `class_name` is mandatory; `Init`, `State` and `Status`
/// commands and attributes are provided by the server.
pub trait Device: Send + 'static {
    fn class_name(&self) -> String;
    fn description(&self) -> String {
        "A Tango device".into()
    }
    fn doc_url(&self) -> String {
        "https://www.tango-controls.org".into()
    }
    fn commands(&self) -> Vec<CmdInfo> {
        vec![]
    }
    fn attributes(&self) -> Vec<AttrInfo> {
        vec![]
    }
    fn pipes(&self) -> Vec<PipeInfo> {
        vec![]
    }
    /// The device properties of the class (RFC-5), resolved before each `init`.
    fn properties(&self) -> Vec<PropInfo> {
        vec![]
    }
    /// Receives the sink through which the device pushes its own events, before the first `init`.
    fn attach(&mut self, _events: EventSink) {}
    /// (Re-)initialises the device with its property values: at export and on the Init command.
    fn init(&mut self, _props: &Properties) -> DevResult<()> {
        Ok(())
    }
    fn state(&mut self) -> DevState {
        DevState::Unknown
    }
    /// `None` gives the default status "The device is in <STATE> state."
    fn status(&mut self) -> Option<String> {
        None
    }
    fn execute(&mut self, cmd: &str, _arg: Value) -> DevResult<Value> {
        Err(unsupported(cmd))
    }
    fn read_attribute(&mut self, attr: &str) -> DevResult<Value> {
        Err(unsupported(attr))
    }
    fn write_attribute(&mut self, attr: &str, _value: Value) -> DevResult<()> {
        Err(unsupported(attr))
    }
    fn read_pipe(&mut self, pipe: &str) -> DevResult<PipeBlob> {
        Err(unsupported(pipe))
    }
    fn write_pipe(&mut self, pipe: &str, _blob: PipeBlob) -> DevResult<()> {
        Err(unsupported(pipe))
    }
}

fn unsupported(name: &str) -> DevFailed {
    DevFailed::new("API_NotSupported", format!("{name} is not implemented"), "Device")
}

trait Named {
    fn name(&self) -> &str;
}

macro_rules! named { ($($t:ty),*) => { $(impl Named for $t { fn name(&self) -> &str { &self.name } })* } }
named!(CmdInfo, AttrInfo, PipeInfo);

/// Case-insensitive lookup, returns the index or `reason`.
fn find<T: Named>(list: &[T], name: &str, reason: &str) -> DevResult<usize> {
    list.iter()
        .position(|x| x.name().eq_ignore_ascii_case(name))
        .ok_or_else(|| DevFailed::new(reason, format!("{name} not found"), "Device"))
}

/// The named items, or all of them if `names` is empty.
fn select<T: Named + Clone>(list: &[T], names: Vec<String>, reason: &str) -> DevResult<Vec<T>> {
    if names.is_empty() {
        return Ok(list.to_vec());
    }
    names.iter().map(|n| Ok(list[find(list, n, reason)?].clone())).collect()
}

type Ring<T> = HashMap<String, VecDeque<History<T>>>;

fn push<T>(ring: &mut VecDeque<T>, x: T, depth: usize) {
    ring.push_front(x);
    ring.truncate(depth);
}

/// `Ok(None)`: go to the device, `Ok(Some(_))`: served from the polling buffer.
fn cached<T: Clone>(ring: &Ring<T>, name: &str, source: Source, reason: &str) -> DevResult<Option<DevResult<T>>> {
    match (source, ring.get(name).and_then(VecDeque::front)) {
        (Source::Dev, _) | (Source::CacheDev, None) => Ok(None),
        (_, Some(h)) => Ok(Some(h.value.clone())),
        (Source::Cache, None) => Err(DevFailed::new(reason, format!("{name} is not polled"), "Cache")),
    }
}

fn history<T: Clone>(ring: &Ring<T>, name: &str, n: usize, reason: &str) -> DevResult<Vec<History<T>>> {
    let h = ring.get(name).ok_or_else(|| DevFailed::new(reason, format!("{name} is not polled"), "History"))?;
    Ok(h.iter().take(n).cloned().collect())
}

/// A device exported by the server with its runtime data.
struct Entry {
    name: String,
    class: String,
    dev: Box<dyn Device>,
    store: Store,
    /// Why the device is FAULT after its initialisation, if it is.
    fault: Option<String>,
    hub: Arc<Hub>,
    /// Per polled event channel: the value of the last event sent and when.
    last_events: HashMap<String, (Option<DevResult<AttrValue>>, f64)>,
    cmds: Vec<CmdInfo>,
    attrs: Vec<AttrInfo>,
    pipes: Vec<PipeInfo>,
    w_values: HashMap<String, Value>,
    black_box: VecDeque<BlackBoxRecord>,
    cmd_ring: Ring<Value>,
    attr_ring: Ring<AttrValue>,
    /// Per polled `(is_attribute, name)`: when it was last polled, how long it took (s), the error.
    poll_stats: HashMap<(bool, String), (f64, f64, Option<String>)>,
}

/// The commands and attributes of a device, those provided by the server included.
fn interface(dev: &dyn Device) -> (Vec<CmdInfo>, Vec<AttrInfo>) {
    let cmd = |name: &str, out_type| CmdInfo { name: name.into(), out_type, ..Default::default() };
    let attr = |name: &str, data_type| AttrInfo { name: name.into(), data_type, ..Default::default() };
    let cmds = [cmd("Init", DataType::Void), cmd("State", DataType::State), cmd("Status", DataType::String)];
    let attrs = [attr("State", DataType::State), attr("Status", DataType::String)];
    (cmds.into_iter().chain(dev.commands()).collect(), attrs.into_iter().chain(dev.attributes()).collect())
}

impl Entry {
    fn new(name: &str, dev: Box<dyn Device>) -> Self {
        let (cmds, attrs) = interface(&*dev);
        Self {
            name: name.into(),
            class: dev.class_name(),
            store: Store::None,
            fault: None,
            hub: Arc::new(Hub::new(events::HEARTBEAT, events::CONFIRM)),
            last_events: HashMap::new(),
            cmds,
            attrs,
            pipes: dev.pipes(),
            dev,
            w_values: HashMap::new(),
            black_box: VecDeque::new(),
            cmd_ring: HashMap::new(),
            attr_ring: HashMap::new(),
            poll_stats: HashMap::new(),
        }
    }

    fn property_names(&self) -> Vec<String> {
        self.dev.properties().into_iter().map(|p| p.name).collect()
    }

    /// Initialises the device from raw property values; failures put it in FAULT (RFC-5).
    fn init(&mut self, raw: Raw) {
        self.fault = match Properties::resolve(&self.dev.properties(), |n| raw.get(&n.to_lowercase()).cloned()) {
            Ok((_, missing)) if !missing.is_empty() => {
                Some(format!("Missing mandatory property: {}", missing.join(", ")))
            }
            Ok((props, _)) => self.dev.init(&props).err().map(|e| e.to_string()),
            Err(e) => Some(e.to_string()),
        };
    }

    /// The Init command: re-reads the properties.
    fn reinit(&mut self) -> DevResult<()> {
        let raw = self.store.properties_blocking(&self.name, &self.class, &self.property_names())?;
        self.init(raw);
        self.refresh_interface(true);
        Ok(())
    }

    /// Re-reads the interface of the device, keeping the configuration of the attributes already
    /// known, and sends INTERFACE_CHANGE (`started`: after an Init or a restart).
    fn refresh_interface(&mut self, started: bool) {
        let (cmds, attrs) = interface(&*self.dev);
        let known =
            |a: AttrInfo| self.attrs.iter().find(|o| o.name.eq_ignore_ascii_case(&a.name)).cloned().unwrap_or(a);
        let attrs = attrs.into_iter().map(known).collect();
        (self.cmds, self.attrs, self.pipes) = (cmds, attrs, self.dev.pipes());
        let interface = EventData::Interface { commands: self.cmds.clone(), attributes: self.attrs.clone(), started };
        let channel = events::channel(&self.name, "", EventType::InterfaceChange);
        self.hub.publish(&channel, EventType::InterfaceChange, Ok(interface));
    }

    /// Writes set values back to the device (memorized attributes), keeping those it accepts.
    fn restore(&mut self, values: Vec<(String, Value)>) {
        for (name, value) in values {
            if self.dev.write_attribute(&name, value.clone()).is_ok() {
                self.w_values.insert(name, value);
            }
        }
    }

    /// DevRestart: a new device of the class replaces this one; memorized set values, polling
    /// and attribute configuration are kept.
    fn restart(&mut self, mut dev: Box<dyn Device>, sink: EventSink) -> DevResult<()> {
        dev.attach(sink);
        self.dev = dev;
        let memorized: Vec<_> =
            self.w_values.drain().filter(|(n, _)| self.attrs.iter().any(|a| a.memorized && a.name == *n)).collect();
        self.last_events.clear();
        let raw = self.store.properties_blocking(&self.name, &self.class, &self.property_names())?;
        self.init(raw);
        self.restore(memorized);
        self.refresh_interface(true);
        Ok(())
    }

    /// RFC-2: an ON device with an attribute in ALARM or WARNING is in ALARM.
    fn state(&mut self) -> DevState {
        if self.fault.is_some() {
            return DevState::Fault;
        }
        let state = self.dev.state();
        if state != DevState::On {
            return state;
        }
        let watched: Vec<_> = self.attrs.iter().filter(|a| a.has_alarms()).map(|a| a.name.clone()).collect();
        let alarm = watched.iter().any(
            |n| matches!(self.read_attr(n), Ok(v) if matches!(v.quality, AttrQuality::Alarm | AttrQuality::Warning)),
        );
        if alarm { DevState::Alarm } else { state }
    }

    fn status(&mut self) -> String {
        if let Some(why) = &self.fault {
            return format!("The device is in FAULT state.\n{why}");
        }
        self.dev.status().unwrap_or_else(|| format!("The device is in {} state.", self.state()))
    }

    fn command(&mut self, name: &str, arg: Value, source: Source) -> DevResult<Value> {
        let info = &self.cmds[find(&self.cmds, name, "API_CommandNotFound")?];
        if arg.data_type() != info.in_type {
            let desc = format!("{} expects {:?}, got {:?}", info.name, info.in_type, arg.data_type());
            return Err(DevFailed::new("API_IncompatibleCmdArgumentType", desc, "Command"));
        }
        let name = info.name.clone();
        match cached(&self.cmd_ring, &name, source, "API_CmdNotPolled")? {
            Some(v) => v,
            None => self.execute(&name, arg),
        }
    }

    fn execute(&mut self, name: &str, arg: Value) -> DevResult<Value> {
        match name {
            "Init" => self.reinit().map(|_| Value::Void),
            "State" => Ok(Value::State(self.state())),
            "Status" => Ok(Value::String(self.status())),
            _ => self.dev.execute(name, arg),
        }
    }

    fn read_attr(&mut self, name: &str) -> DevResult<AttrValue> {
        let info = self.attrs[find(&self.attrs, name, "API_AttrNotFound")?].clone();
        let value = match info.name.as_str() {
            "State" => Value::State(self.state()),
            "Status" => Value::String(self.status()),
            name => self.dev.read_attribute(name)?,
        };
        // a READ_WITH_WRITE attribute reports the set value of its writable attribute
        let set = if info.writable_attr_name.is_empty() { &info.name } else { &info.writable_attr_name };
        let w_value = self.w_values.iter().find(|(n, _)| n.eq_ignore_ascii_case(set)).map(|(_, v)| v.clone());
        Ok(info.value(value, w_value))
    }

    fn read_attrs(&mut self, names: &[String], source: Source) -> Vec<DevResult<AttrValue>> {
        names.iter().map(|n| self.read_one(n, source)).collect()
    }

    fn read_one(&mut self, name: &str, source: Source) -> DevResult<AttrValue> {
        let name = self.attrs[find(&self.attrs, name, "API_AttrNotFound")?].name.clone();
        match cached(&self.attr_ring, &name, source, "API_AttrNotPolled")? {
            Some(v) => v,
            None => self.read_attr(&name),
        }
    }

    fn write_attrs(&mut self, values: Vec<(String, Value)>) -> DevResult<()> {
        // one error per failed attribute, its origin naming the attribute (MultiDevFailed)
        let mut errors = vec![];
        for (name, value) in values {
            if let Err(e) = self.write_attr(&name, value) {
                errors.extend(e.0.into_iter().map(|e| DevError { origin: name.clone(), ..e }));
            }
        }
        if errors.is_empty() { Ok(()) } else { Err(DevFailed(errors)) }
    }

    fn write_attr(&mut self, name: &str, value: Value) -> DevResult<()> {
        let info = &self.attrs[find(&self.attrs, name, "API_AttrNotFound")?];
        let fail =
            |reason: &str, desc: &str| Err(DevFailed::new(reason, format!("{}: {desc}", info.name), "WriteAttribute"));
        let expected = if info.data_format == DataFormat::Scalar { info.data_type } else { info.data_type.array() };
        if !matches!(info.writable, Writable::Write | Writable::ReadWrite) {
            return fail("API_AttrNotWritable", "attribute is not writable");
        }
        if value.data_type() != expected {
            return fail(
                "API_IncompatibleAttrDataType",
                &format!("expected {expected:?}, got {:?}", value.data_type()),
            );
        }
        if value.outside(info.min_value, info.max_value) {
            return fail("API_WAttrOutsideLimit", "value outside [min_value, max_value]");
        }
        let labels = info.enum_labels.len();
        if labels > 0 && matches!(value, Value::Short(v) if v < 0 || v as usize >= labels) {
            return fail("API_WAttrOutsideLimit", "value is not an index of enum_labels");
        }
        let (name, memorized) = (info.name.clone(), info.memorized);
        self.dev.write_attribute(&name, value.clone())?;
        if memorized {
            self.store.save_memorized(&self.name, &name, &value)?;
        }
        self.w_values.insert(name, value);
        Ok(())
    }

    fn set_attr_config(&mut self, c: AttrInfo) -> DevResult<AttrInfo> {
        let i = find(&self.attrs, &c.name, "API_AttrNotFound")?;
        for (lo, hi) in [(c.min_value, c.max_value), (c.min_alarm, c.max_alarm), (c.min_warning, c.max_warning)] {
            if let (Some(lo), Some(hi)) = (lo, hi)
                && lo > hi
            {
                return Err(DevFailed::new(
                    "API_IncoherentValues",
                    format!("{}: min {lo} > max {hi}", c.name),
                    "SetConfig",
                ));
            }
        }
        let a = &mut self.attrs[i];
        // only properties are updated, static metadata is kept (RFC-4)
        *a = AttrInfo {
            name: a.name.clone(),
            data_type: a.data_type,
            data_format: a.data_format,
            writable: a.writable,
            max_dim_x: a.max_dim_x,
            max_dim_y: a.max_dim_y,
            disp_level: a.disp_level,
            events_by_code: a.events_by_code,
            memorized: a.memorized,
            writable_attr_name: a.writable_attr_name.clone(),
            ..c
        };
        self.store.save_attribute(&self.name, a);
        let (a, channel) = (a.clone(), events::channel(&self.name, &a.name, EventType::AttrConf));
        self.hub.publish(&channel, EventType::AttrConf, Ok(EventData::Conf(a.clone())));
        Ok(a)
    }

    fn read_pipe(&mut self, name: &str) -> DevResult<PipeValue> {
        let name = self.pipes[find(&self.pipes, name, "API_PipeNotFound")?].name.clone();
        Ok(PipeValue { blob: self.dev.read_pipe(&name)?, name, time: now() })
    }

    fn write_pipe(&mut self, name: &str, blob: PipeBlob) -> DevResult<()> {
        let info = &self.pipes[find(&self.pipes, name, "API_PipeNotFound")?];
        if !info.writable {
            return Err(DevFailed::new("API_PipeNotWritable", format!("{} is not writable", info.name), "WritePipe"));
        }
        let name = info.name.clone();
        self.dev.write_pipe(&name, blob)
    }

    fn poll(&mut self, obj: &str, is_attr: bool) {
        let time = now();
        let error = if is_attr {
            let value = self.read_attr(obj);
            self.fire_polled(obj, &value);
            let error = value.as_ref().err().map(DevFailed::to_string);
            let Some(ring) = self.attr_ring.get_mut(obj) else { return };
            push(ring, History { time, value }, POLL_RING_DEPTH);
            error
        } else {
            let value = self.execute(obj, Value::Void);
            let error = value.as_ref().err().map(DevFailed::to_string);
            let Some(ring) = self.cmd_ring.get_mut(obj) else { return };
            push(ring, History { time, value }, POLL_RING_DEPTH);
            error
        };
        self.poll_stats.insert((is_attr, obj.into()), (time, now() - time, error));
    }

    /// CHANGE, PERIODIC and ARCHIVE events of a polled attribute (SPEC §9.1).
    fn fire_polled(&mut self, attr: &str, value: &DevResult<AttrValue>) {
        let Ok(i) = find(&self.attrs, attr, "") else { return };
        let a = &self.attrs[i];
        for t in [EventType::Change, EventType::Periodic, EventType::Archive] {
            let channel = events::channel(&self.name, &a.name, t);
            if !self.hub.active(&channel) {
                continue;
            }
            let (last, at) = self.last_events.get(&channel).cloned().unwrap_or((None, 0.0));
            let elapsed_ms = (now() - at) * 1000.0;
            let fire = match (t, &last) {
                (_, None) => true,
                (EventType::Change, Some(old)) => events::changed(old, value, a.abs_change, a.rel_change),
                (EventType::Periodic, _) => elapsed_ms >= a.period.unwrap_or(1000) as f64,
                (_, Some(old)) => {
                    events::changed(old, value, a.archive_abs_change, a.archive_rel_change)
                        || a.archive_period.is_some_and(|p| elapsed_ms >= p as f64)
                }
            };
            if fire {
                self.hub.publish(&channel, t, value.clone().map(EventData::Attr));
                self.last_events.insert(channel, (Some(value.clone()), now()));
            }
        }
    }

    /// Validates a subscription to `name` (SPEC §9.1) and returns its channel.
    fn event_channel(&self, name: &str, t: EventType) -> DevResult<String> {
        let fail = |reason: &str, desc: String| Err(DevFailed::new(reason, desc, "EventSubscriptionChange"));
        let name = match t {
            EventType::InterfaceChange => String::new(),
            EventType::Pipe => self.pipes[find(&self.pipes, name, "API_PipeNotFound")?].name.clone(),
            _ => {
                let a = &self.attrs[find(&self.attrs, name, "API_AttrNotFound")?];
                let polled = self.attr_ring.contains_key(&a.name) || a.events_by_code;
                let thresholds = match t {
                    EventType::Change => a.abs_change.or(a.rel_change).is_some(),
                    EventType::Archive => {
                        a.archive_abs_change.or(a.archive_rel_change).is_some() || a.archive_period.is_some()
                    }
                    _ => true,
                };
                if matches!(t, EventType::Change | EventType::Periodic | EventType::Archive) {
                    if !polled {
                        return fail("API_AttributePollingNotStarted", format!("{} is not polled", a.name));
                    }
                    if !thresholds && !a.events_by_code {
                        return fail(
                            "API_EventPropertiesNotSet",
                            format!("{} has no {} threshold", a.name, t.suffix()),
                        );
                    }
                }
                a.name.clone()
            }
        };
        Ok(events::channel(&self.name, &name, t))
    }

    fn handle(&mut self, op: Op, source: Source, ctx: &Ctx) -> DevResult<Data> {
        Ok(match op {
            Op::DeviceName => Data::String(self.name.clone()),
            Op::DeviceDescription => Data::String(self.dev.description()),
            Op::DeviceAdmName => Data::String(ctx.adm_name.clone()),
            Op::DevicePing => Data::Void,
            Op::DeviceBlackbox { n: 0 } => {
                return Err(DevFailed::new("API_BlackBoxArgument", "number of records must be > 0", "BlackBox"));
            }
            Op::DeviceBlackbox { n } => Data::BlackBox(self.black_box.iter().take(n).cloned().collect()),
            Op::DeviceInfo => Data::Info(DeviceInfo {
                dev_class: self.dev.class_name(),
                server_id: ctx.server_id.clone(),
                server_host: ctx.host.clone(),
                server_version: PROTOCOL_VERSION,
                doc_url: self.dev.doc_url(),
                dev_type: self.dev.class_name(),
            }),
            Op::CommandExecution { name, arg } => Data::Value(self.command(&name, arg, source)?),
            Op::CommandsList => Data::Commands(self.cmds.clone()),
            Op::CommandInfo { name } => Data::Commands(select(&self.cmds, vec![name], "API_CommandNotFound")?),
            Op::CommandHistory { name, n } => {
                let name = &self.cmds[find(&self.cmds, &name, "API_CommandNotFound")?].name;
                Data::CmdHistory(history(&self.cmd_ring, name, n, "API_CmdNotPolled")?)
            }
            Op::AttributesRead { names } => Data::Attributes(self.read_attrs(&names, source)),
            Op::AttributesWrite { values } => {
                self.write_attrs(values)?;
                Data::Void
            }
            Op::AttributesWriteRead { values, names } => {
                self.write_attrs(values)?;
                Data::Attributes(self.read_attrs(&names, Source::Dev))
            }
            Op::AttributeSetConfig { configs } => {
                Data::AttrConfigs(configs.into_iter().map(|c| self.set_attr_config(c)).collect::<DevResult<_>>()?)
            }
            Op::AttributeGetConfig { names } => Data::AttrConfigs(select(&self.attrs, names, "API_AttrNotFound")?),
            Op::AttributeHistory { name, n } => {
                let name = &self.attrs[find(&self.attrs, &name, "API_AttrNotFound")?].name;
                Data::AttrHistory(history(&self.attr_ring, name, n, "API_AttrNotPolled")?)
            }
            Op::PipeRead { name } => Data::Pipe(self.read_pipe(&name)?),
            Op::PipeWrite { name, blob } => {
                self.write_pipe(&name, blob)?;
                Data::Void
            }
            Op::PipeWriteRead { name, blob } => {
                self.write_pipe(&name, blob)?;
                Data::Pipe(self.read_pipe(&name)?)
            }
            Op::PipeGetConfig { names } => Data::PipeConfigs(select(&self.pipes, names, "API_PipeNotFound")?),
            Op::PipeSetConfig { configs } => {
                let mut out = vec![];
                for c in configs {
                    let i = find(&self.pipes, &c.name, "API_PipeNotFound")?;
                    let p = &mut self.pipes[i];
                    (p.description, p.label) = (c.description, c.label);
                    out.push(p.clone());
                }
                Data::PipeConfigs(out)
            }
        })
    }
}

/// The admin device `dserver/<exec>/<instance>` (class DServer).
struct Admin {
    classes: Vec<String>,
    devices: Vec<String>,
    shutdown: Arc<Notify>,
}

impl Device for Admin {
    fn class_name(&self) -> String {
        "DServer".into()
    }
    fn state(&mut self) -> DevState {
        DevState::On
    }
    fn commands(&self) -> Vec<CmdInfo> {
        use DataType::{Long, LongStringArray as Lsa, String as Str, StringArray as Sa, Void};
        let table = [
            ("QueryClass", Void, Sa),
            ("QueryDevice", Void, Sa),
            ("QuerySubDevice", Void, Sa),
            ("QueryWizardClassProperty", Str, Sa),
            ("QueryWizardDevProperty", Str, Sa),
            ("Kill", Void, Void),
            ("DevRestart", Str, Void),
            ("RestartServer", Void, Void),
            ("AddObjPolling", Lsa, Void),
            ("UpdObjPollingPeriod", Lsa, Void),
            ("RemObjPolling", Sa, Void),
            ("PolledDevice", Void, Sa),
            ("DevPollStatus", Str, Sa),
            ("StartPolling", Void, Void),
            ("StopPolling", Void, Void),
            ("LockDevice", Lsa, Void),
            ("ReLockDevices", Sa, Void),
            ("UnLockDevice", Lsa, Long),
            ("DevLockStatus", Str, Lsa),
            ("AddLoggingTarget", Sa, Void),
            ("RemoveLoggingTarget", Sa, Void),
            ("GetLoggingTarget", Str, Sa),
            ("SetLoggingLevel", Lsa, Void),
            ("GetLoggingLevel", Sa, Lsa),
            ("StartLogging", Void, Void),
            ("StopLogging", Void, Void),
            ("EventSubscriptionChange", Sa, Lsa),
            ("ZmqEventSubscriptionChange", Sa, Lsa),
            ("EventConfirmSubscription", Sa, Void),
        ];
        table
            .map(|(name, in_type, out_type)| CmdInfo { name: name.into(), in_type, out_type, ..Default::default() })
            .into()
    }
    /// QueryClass, QueryDevice and Kill; the others are handled by `Ctx::admin_command`.
    fn execute(&mut self, cmd: &str, _arg: Value) -> DevResult<Value> {
        Ok(match cmd {
            "QueryClass" => Value::StringArray(self.classes.clone()),
            "QueryDevice" => Value::StringArray(self.devices.clone()),
            "Kill" => {
                self.shutdown.notify_one(); // the reply leaves before the server stops
                Value::Void
            }
            _ => return Err(unsupported(cmd)),
        })
    }
}

/// Admin commands answered by the device itself (or by every device).
const DEVICE_COMMANDS: [&str; 6] = ["Init", "State", "Status", "QueryClass", "QueryDevice", "Kill"];

type Shared = Arc<Mutex<Entry>>;

fn lock(e: &Shared) -> MutexGuard<'_, Entry> {
    e.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

struct Ctx {
    devices: HashMap<String, Shared>,
    adm_name: String,
    server_id: String,
    host: String,
    hub: Arc<Hub>,
    stop: watch::Sender<bool>,
    store: Store,
    classes: HashMap<String, Factory>,
    rt: tokio::runtime::Handle,
    /// Per `(device, is_attribute, name)` in lower case.
    polls: Mutex<HashMap<(String, bool, String), Poll>>,
    /// StopPolling: the polling tasks skip their ticks.
    paused: Arc<AtomicBool>,
    /// Per device in lower case.
    locks: Mutex<HashMap<String, DevLock>>,
}

struct Poll {
    device: String,
    name: String,
    period: Duration,
    task: tokio::task::AbortHandle,
}

#[derive(Clone)]
struct DevLock {
    client: u64,
    host: String,
    validity: Duration,
    until: Instant,
}

fn guard<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

/// Requests a locked device refuses to other clients: commands other than State and Status,
/// writes and configuration changes.
fn mutating(op: &Op) -> bool {
    match op {
        Op::CommandExecution { name, .. } => !["State", "Status"].iter().any(|n| n.eq_ignore_ascii_case(name)),
        Op::AttributesWrite { .. }
        | Op::AttributesWriteRead { .. }
        | Op::AttributeSetConfig { .. }
        | Op::PipeWrite { .. }
        | Op::PipeWriteRead { .. }
        | Op::PipeSetConfig { .. } => true,
        _ => false,
    }
}

impl Ctx {
    fn handle(&self, req: Request, host: &str) -> DevResult<Data> {
        if req.v != PROTOCOL_VERSION {
            let desc = format!("protocol version {} not supported, expected {PROTOCOL_VERSION}", req.v);
            return Err(DevFailed::new("API_UnsupportedProtocol", desc, "Server"));
        }
        let key = req.device.to_lowercase();
        let entry = self.entry(&key)?;
        if mutating(&req.op) {
            self.check_lock(&key, req.client)?;
        }
        let op = format!("{:?}", req.op);
        let operation = op.split([' ', '(']).next().unwrap_or_default().to_string();
        let what = match &req.op {
            Op::CommandExecution { name, .. } => format!("{operation} {name}"),
            _ => operation.clone(),
        };
        self.hub.log(&key, LogLevel::Debug, &format!("{what} from {host}"));
        let mut e = lock(entry);
        let record =
            BlackBoxRecord { time: now(), operation, source: req.source, client_id: req.client, host: host.into() };
        push(&mut e.black_box, record, BLACK_BOX_DEPTH);
        if key == self.adm_name.to_lowercase()
            && let Op::CommandExecution { name, arg } = &req.op
            && !DEVICE_COMMANDS.iter().any(|c| c.eq_ignore_ascii_case(name))
        {
            let info = e.cmds[find(&e.cmds, name, "API_CommandNotFound")?].clone();
            drop(e); // the admin commands reach into the other devices
            if arg.data_type() != info.in_type {
                let desc = format!("{} expects {:?}, got {:?}", info.name, info.in_type, arg.data_type());
                return Err(DevFailed::new("API_IncompatibleCmdArgumentType", desc, "Command"));
            }
            return self.admin_command(&info.name, arg, req.client, host).map(Data::Value);
        }
        let result = e.handle(req.op, req.source, self);
        if self.hub.take_interface_changed(&e.name) {
            e.refresh_interface(false);
        }
        result
    }

    fn entry(&self, device: &str) -> DevResult<&Shared> {
        let not_found = || DevFailed::new("API_DeviceNotFound", format!("{device} not found"), "Server");
        self.devices.get(&device.to_lowercase()).ok_or_else(not_found)
    }

    /// The admin commands of SPEC §8 that act on the server rather than on one device.
    fn admin_command(&self, name: &str, arg: &Value, client: u64, host: &str) -> DevResult<Value> {
        let (longs, strings) = match arg {
            Value::LongStringArray(l, s) => (l.clone(), s.clone()),
            Value::StringArray(s) => (vec![], s.clone()),
            Value::String(s) => (vec![], vec![s.clone()]),
            _ => (vec![], vec![]),
        };
        let s = |i: usize| strings.get(i).map(String::as_str).unwrap_or_default();
        let long = |i: usize| longs.get(i).copied().unwrap_or_default();
        let invalid = |desc: &str| DevFailed::new("API_InvalidArgs", desc, name);
        let period = || match long(0) {
            p if p > 0 => Ok(Duration::from_millis(p as u64)),
            _ => Err(invalid("the polling period must be > 0 ms")),
        };
        let list = Value::StringArray;
        let void = |r: DevResult<()>| r.map(|_| Value::Void);
        match name {
            n if n.contains("Subscription") => self.event_command(n, arg),
            "QuerySubDevice" | "QueryWizardClassProperty" => Ok(list(vec![])),
            "QueryWizardDevProperty" => self.wizard(s(0)).map(list),
            "AddObjPolling" => void(self.add_poll(s(0), s(1), s(2), period()?, true)),
            "UpdObjPollingPeriod" => void(self.update_poll(s(0), s(1), s(2), Some(period()?))),
            "RemObjPolling" => void(self.update_poll(s(0), s(1), s(2), None)),
            "PolledDevice" => {
                let mut devices: Vec<_> = guard(&self.polls).values().map(|p| p.device.clone()).collect();
                devices.sort();
                devices.dedup();
                Ok(list(devices))
            }
            "DevPollStatus" => self.poll_status(s(0)).map(list),
            "StartPolling" | "StopPolling" => {
                self.paused.store(name == "StopPolling", Ordering::Relaxed);
                Ok(Value::Void)
            }
            "DevRestart" => void(self.restart(s(0))),
            "RestartServer" => {
                let mut devices: Vec<_> = self.devices.keys().filter(|d| **d != self.adm_name.to_lowercase()).collect();
                devices.sort();
                devices.into_iter().try_for_each(|d| self.restart(d)).map(|_| Value::Void)
            }
            "LockDevice" => void(self.lock_device(s(0), client, host, long(0))),
            "ReLockDevices" => void(strings.iter().try_for_each(|d| self.relock(d, client))),
            "UnLockDevice" => self.unlock(&strings, client, long(0) == 1),
            "DevLockStatus" => self.lock_status(s(0)),
            "AddLoggingTarget" | "RemoveLoggingTarget" => {
                for pair in strings.chunks(2) {
                    let [device, target] = pair else { return Err(invalid("expected device, target pairs")) };
                    self.entry(device)?;
                    let mut logging = guard(&self.hub.logging);
                    let targets = &mut logging.device(device).1;
                    targets.retain(|t| t != target);
                    if name == "AddLoggingTarget" {
                        targets.push(target.clone());
                    }
                }
                Ok(Value::Void)
            }
            "GetLoggingTarget" => {
                self.entry(s(0))?;
                Ok(list(guard(&self.hub.logging).device(s(0)).1.clone()))
            }
            "SetLoggingLevel" => {
                for (level, device) in longs.iter().zip(&strings) {
                    let level = LogLevel::from_i32(*level).ok_or_else(|| invalid("levels are 0 (OFF) to 5 (DEBUG)"))?;
                    self.entry(device)?;
                    guard(&self.hub.logging).device(device).0 = level;
                }
                Ok(Value::Void)
            }
            "GetLoggingLevel" => {
                let mut levels = vec![];
                for device in &strings {
                    self.entry(device)?;
                    levels.push(guard(&self.hub.logging).device(device).0 as i32);
                }
                Ok(Value::LongStringArray(levels, strings))
            }
            "StartLogging" | "StopLogging" => {
                guard(&self.hub.logging).enabled = name == "StartLogging";
                Ok(Value::Void)
            }
            _ => Err(unsupported(name)),
        }
    }

    /// `[name, description, default]` per device property of `class`.
    fn wizard(&self, class: &str) -> DevResult<Vec<String>> {
        let props = match self.classes.iter().find(|(c, _)| c.eq_ignore_ascii_case(class)) {
            Some((_, factory)) => factory().properties(),
            None => self
                .devices
                .values()
                .find_map(|e| {
                    let e = lock(e);
                    e.class.eq_ignore_ascii_case(class).then(|| e.dev.properties())
                })
                .ok_or_else(|| DevFailed::new("API_ClassNotFound", format!("{class} not found"), "QueryWizard"))?,
        };
        Ok(props.into_iter().flat_map(|p| [p.name, p.description, p.default.unwrap_or_default().join("\n")]).collect())
    }

    /// `(key, entry, name, is_attribute)` of a polling object `<device> attribute|command <name>`.
    fn poll_target(&self, device: &str, kind: &str, obj: &str) -> DevResult<((String, bool, String), Shared, String)> {
        let entry = self.entry(device)?;
        let e = lock(entry);
        let name = match kind.to_lowercase().as_str() {
            "attribute" => e.attrs[find(&e.attrs, obj, "API_AttrNotFound")?].name.clone(),
            "command" => e.cmds[find(&e.cmds, obj, "API_CommandNotFound")?].name.clone(),
            _ => return Err(DevFailed::new("API_NotSupported", format!("{kind}: attribute or command"), "Polling")),
        };
        let key = (device.to_lowercase(), kind.eq_ignore_ascii_case("attribute"), name.to_lowercase());
        Ok((key, entry.clone(), name))
    }

    fn add_poll(&self, device: &str, kind: &str, obj: &str, period: Duration, save: bool) -> DevResult<()> {
        let (key, entry, name) = self.poll_target(device, kind, obj)?;
        let mut polls = guard(&self.polls);
        if polls.contains_key(&key) {
            return Err(DevFailed::new("API_AlreadyPolled", format!("{name} is already polled"), "AddObjPolling"));
        }
        let device = {
            let mut e = lock(&entry);
            match key.1 {
                true => drop(e.attr_ring.insert(name.clone(), VecDeque::new())),
                false => drop(e.cmd_ring.insert(name.clone(), VecDeque::new())),
            }
            e.name.clone()
        };
        let task = self.spawn_poll(entry, name.clone(), key.1, period);
        polls.insert(key.clone(), Poll { device, name, period, task });
        drop(polls);
        if save { self.save_polling(&key.0) } else { Ok(()) }
    }

    /// A new period (UpdObjPollingPeriod), or `None` to stop polling (RemObjPolling).
    fn update_poll(&self, device: &str, kind: &str, obj: &str, period: Option<Duration>) -> DevResult<()> {
        let (key, entry, name) = self.poll_target(device, kind, obj)?;
        let mut polls = guard(&self.polls);
        let not_polled = || DevFailed::new("API_NotPolled", format!("{name} is not polled"), "Polling");
        let poll = polls.get_mut(&key).ok_or_else(not_polled)?;
        poll.task.abort();
        match period {
            Some(period) => {
                poll.period = period;
                poll.task = self.spawn_poll(entry, name, key.1, period);
            }
            None => {
                polls.remove(&key);
                let mut e = lock(&entry);
                match key.1 {
                    true => drop(e.attr_ring.remove(&name)),
                    false => drop(e.cmd_ring.remove(&name)),
                }
                e.poll_stats.remove(&(key.1, name));
            }
        }
        drop(polls);
        self.save_polling(&key.0)
    }

    fn spawn_poll(&self, entry: Shared, obj: String, is_attr: bool, period: Duration) -> tokio::task::AbortHandle {
        let (mut stopped, paused) = (self.stop.subscribe(), self.paused.clone());
        let task = self.rt.spawn(async move {
            let mut tick = tokio::time::interval(period);
            loop {
                tokio::select! {
                    _ = tick.tick() => if !paused.load(Ordering::Relaxed) {
                        lock(&entry).poll(&obj, is_attr)
                    },
                    _ = stopped.wait_for(|s| *s) => break,
                }
            }
        });
        task.abort_handle()
    }

    /// Persists the polling configuration of a device: `polled_attr`, `polled_cmd`.
    fn save_polling(&self, device: &str) -> DevResult<()> {
        let mut polled: Vec<_> = guard(&self.polls)
            .iter()
            .filter(|((d, ..), _)| d == device)
            .map(|((_, attr, _), p)| (*attr, p.name.clone(), p.period.as_millis().to_string()))
            .collect();
        polled.sort();
        let (mut attrs, mut cmds) = (vec![], vec![]);
        for (attr, name, period) in polled {
            (if attr { &mut attrs } else { &mut cmds }).extend([name, period]);
        }
        let name = lock(self.entry(device)?).name.clone();
        self.store.save_polling(&name, attrs, cmds)
    }

    fn poll_status(&self, device: &str) -> DevResult<Vec<String>> {
        let entry = self.entry(device)?;
        let mut polled: Vec<_> = guard(&self.polls)
            .iter()
            .filter(|((d, ..), _)| d.eq_ignore_ascii_case(device))
            .map(|((_, attr, _), p)| (*attr, p.name.clone(), p.period))
            .collect();
        polled.sort();
        let e = lock(entry);
        let status = |(attr, name, period): (bool, String, Duration)| {
            let kind = if attr { "attribute" } else { "command" };
            let mut s = format!(
                "Polled {kind} name = {name}\nPolling period (mS) = {}\nPolling ring buffer depth = {POLL_RING_DEPTH}",
                period.as_millis()
            );
            if let Some((time, took, error)) = e.poll_stats.get(&(attr, name)) {
                s += &format!("\nTime needed for the last {kind} reading (mS) = {:.3}", took * 1000.0);
                s += &format!("\nLast record taken {:.0} mS ago", (now() - time) * 1000.0);
                if let Some(error) = error {
                    s += &format!("\nLast {kind} read FAILED :\n{error}");
                }
            }
            s
        };
        Ok(polled.into_iter().map(status).collect())
    }

    fn restart(&self, device: &str) -> DevResult<()> {
        if device.eq_ignore_ascii_case(&self.adm_name) {
            return Err(DevFailed::new("API_NotSupported", "the admin device is not restarted", "DevRestart"));
        }
        let mut e = lock(self.entry(device)?);
        let factory = self.classes.get(&e.class).ok_or_else(|| {
            DevFailed::new("API_NotSupported", format!("no factory for class {}", e.class), "DevRestart")
        })?;
        let sink = EventSink { device: e.name.clone(), hub: self.hub.clone() };
        e.restart(factory(), sink)
    }

    /// The lock of a device, if any and still valid.
    fn holder(&self, device: &str) -> Option<DevLock> {
        let mut locks = guard(&self.locks);
        let device = device.to_lowercase();
        if locks.get(&device).is_some_and(|l| l.until <= Instant::now()) {
            locks.remove(&device);
        }
        locks.get(&device).cloned()
    }

    fn locked(device: &str, l: &DevLock) -> DevFailed {
        let desc = format!("{device} is locked by client {} on {}", l.client, l.host);
        DevFailed::new("API_DeviceLocked", desc, "Lock")
    }

    fn check_lock(&self, device: &str, client: u64) -> DevResult<()> {
        match self.holder(device) {
            Some(l) if l.client != client => Err(Self::locked(device, &l)),
            _ => Ok(()),
        }
    }

    fn lock_device(&self, device: &str, client: u64, host: &str, validity_s: i32) -> DevResult<()> {
        if device.eq_ignore_ascii_case(&self.adm_name) {
            return Err(DevFailed::new("API_DeviceUnlockable", "the admin device cannot be locked", "LockDevice"));
        }
        self.entry(device)?;
        self.check_lock(device, client)?;
        let validity = Duration::from_secs(validity_s.max(1) as u64);
        let l = DevLock { client, host: host.into(), validity, until: Instant::now() + validity };
        guard(&self.locks).insert(device.to_lowercase(), l);
        Ok(())
    }

    fn relock(&self, device: &str, client: u64) -> DevResult<()> {
        match self.holder(device) {
            Some(l) if l.client == client => {
                if let Some(l) = guard(&self.locks).get_mut(&device.to_lowercase()) {
                    l.until = Instant::now() + l.validity;
                }
                Ok(())
            }
            Some(l) => Err(Self::locked(device, &l)),
            None => Err(DevFailed::new("API_DeviceNotLocked", format!("{device} is not locked"), "ReLockDevices")),
        }
    }

    /// Unlocks the devices locked by `client` (any client with `force`); returns how many.
    fn unlock(&self, devices: &[String], client: u64, force: bool) -> DevResult<Value> {
        let mut n = 0;
        for device in devices {
            match self.holder(device) {
                Some(l) if l.client == client || force => {
                    guard(&self.locks).remove(&device.to_lowercase());
                    n += 1;
                }
                Some(l) => return Err(Self::locked(device, &l)),
                None => {}
            }
        }
        Ok(Value::Long(n))
    }

    /// `([locked, client, remaining ms], [status, host])`.
    fn lock_status(&self, device: &str) -> DevResult<Value> {
        self.entry(device)?;
        Ok(match self.holder(device) {
            Some(l) => {
                let left = l.until.saturating_duration_since(Instant::now()).as_millis() as i32;
                let status = format!("Device {device} is locked by client {} on {}", l.client, l.host);
                Value::LongStringArray(vec![1, l.client as i32, left], vec![status, l.host])
            }
            None => {
                Value::LongStringArray(vec![0, 0, 0], vec![format!("Device {device} is not locked"), String::new()])
            }
        })
    }
}

impl Ctx {
    /// The admin event commands (SPEC §9.2), handled here as they reach into other devices.
    fn event_command(&self, name: &str, arg: &Value) -> DevResult<Value> {
        let Value::StringArray(a) = arg else {
            return Err(DevFailed::new(
                "API_IncompatibleCmdArgumentType",
                format!("{name} expects a StringArray"),
                name,
            ));
        };
        let field = |i: usize| a.get(i).map(String::as_str).unwrap_or_default();
        let confirm = name == "EventConfirmSubscription";
        let t = EventType::parse(field(if confirm { 2 } else { 3 }))
            .ok_or_else(|| DevFailed::new("API_InvalidArgs", format!("unknown event type in {a:?}"), name))?;
        let entry = self.devices.get(&field(0).to_lowercase());
        let entry =
            entry.ok_or_else(|| DevFailed::new("API_DeviceNotFound", format!("{} not found", field(0)), name))?;
        let mut e = lock(entry);
        let channel = e.event_channel(field(1), t)?;
        if confirm {
            // the immediate heartbeat tells the client its event link is live
            self.hub.confirm(&channel);
            self.hub.beat(&events::heartbeat_channel(&self.adm_name));
            return Ok(Value::Void);
        }
        if field(2) == "unsubscribe" {
            self.hub.unsubscribe(&channel);
        } else {
            self.hub.subscribe(&channel);
            // the change rules start from the current polled value, which the client reads itself
            let polled = find(&e.attrs, field(1), "").ok().and_then(|i| e.attr_ring.get(&e.attrs[i].name));
            if let Some(last) = polled.and_then(VecDeque::front).map(|h| h.value.clone()) {
                e.last_events.insert(channel.clone(), (Some(last), now()));
            }
        }
        let ms = |d: Duration| d.as_millis() as i32;
        let zmq = self.hub.zmq_endpoint.lock().unwrap_or_else(|p| p.into_inner()).clone();
        let info = vec![zmq, channel, events::heartbeat_channel(&self.adm_name)];
        Ok(Value::LongStringArray(vec![1, ms(self.hub.heartbeat), ms(self.hub.confirm)], info))
    }
}

/// Transport-independent request handler: one encoded request frame in, one reply frame out.
#[derive(Clone)]
pub struct Handler(Arc<Ctx>);

impl Handler {
    /// `peer` identifies the client in the black box.
    pub fn frame(&self, frame: &[u8], peer: &str) -> Vec<u8> {
        let reply = match serde_json::from_slice::<Request>(frame) {
            Ok(req) => self.call(req, peer),
            Err(e) => Reply { id: 0, reply: Err(e.into()) },
        };
        serde_json::to_vec(&reply).expect("a reply is always serialisable")
    }

    /// Typed entry point, for transports that carry the envelope themselves.
    pub fn call(&self, req: Request, peer: &str) -> Reply {
        Reply { id: req.id, reply: self.0.handle(req, peer) }
    }

    /// The events every transport forwards to its subscribers.
    pub(crate) fn events(&self) -> tokio::sync::broadcast::Receiver<Arc<events::Event>> {
        self.0.hub.receiver()
    }

    #[cfg(feature = "zmq")]
    pub(crate) fn hub(&self) -> &Hub {
        &self.0.hub
    }

    /// Resolves once the server shuts down (Kill, signal).
    pub(crate) async fn stopped(&self) {
        let mut stop = self.0.stop.subscribe();
        let _ = stop.wait_for(|s| *s).await;
    }

    /// Names (lower case) of the exported devices, the admin device included.
    pub fn devices(&self) -> Vec<String> {
        self.0.devices.keys().cloned().collect()
    }
}

type Factory = Box<dyn Fn() -> Box<dyn Device> + Send + Sync>;

/// Where the devices and their properties come from.
enum Config {
    Auto,
    NoDb,
    Database(String),
    File(PathBuf),
}

/// A device server (RFC-8): hosts devices on one or several endpoints.
pub struct Server {
    exec: String,
    instance: String,
    devices: Vec<(String, Box<dyn Device>)>,
    classes: HashMap<String, Factory>,
    polling: Vec<(String, String, bool, Duration)>,
    config: Config,
    endpoints: Vec<String>,
    heartbeat: Duration,
    confirm: Duration,
}

/// What `start` hands back to `run`: the handler and what the export needs.
pub struct Started {
    pub handler: Handler,
    store: Store,
    devices: Vec<String>,
    shutdown: Arc<Notify>,
}

impl Server {
    pub fn new(exec: &str, instance: &str) -> Self {
        Self {
            exec: exec.into(),
            instance: instance.into(),
            devices: vec![],
            classes: HashMap::new(),
            polling: vec![],
            config: Config::Auto,
            endpoints: vec![],
            heartbeat: events::HEARTBEAT,
            confirm: events::CONFIRM,
        }
    }

    /// The event heartbeat period (default 9 s, RFC-12).
    pub fn heartbeat(mut self, period: Duration) -> Self {
        self.heartbeat = period;
        self
    }

    /// The period clients confirm their event subscriptions at (default 200 s); a subscription not
    /// confirmed for 3 periods is dropped.
    pub fn event_confirm(mut self, period: Duration) -> Self {
        self.confirm = period;
        self
    }

    /// From command-line arguments, Tango style: `<instance> [-nodb] [-file=<path>]
    /// [-dbhost=<host:port>] [-endpoint=<url>]...`. Without option, `$TANGO_HOST` is used if set.
    pub fn from_args(exec: &str, args: impl IntoIterator<Item = impl Into<String>>) -> DevResult<Self> {
        let mut server = Self::new(exec, "");
        for arg in args.into_iter().map(Into::into) {
            if arg == "-nodb" {
                server.config = Config::NoDb;
            } else if let Some(p) = arg.strip_prefix("-file=") {
                server.config = Config::File(p.into());
            } else if let Some(h) = arg.strip_prefix("-dbhost=") {
                server.config = Config::Database(h.into());
            } else if let Some(e) = arg.strip_prefix("-endpoint=") {
                server.endpoints.push(e.into());
            } else if server.instance.is_empty() && !arg.starts_with('-') {
                server.instance = arg;
            } else {
                return Err(DevFailed::new("API_InvalidArgs", format!("unknown argument {arg}"), "Server::from_args"));
            }
        }
        if server.instance.is_empty() {
            let usage =
                format!("usage: {exec} <instance> [-nodb] [-file=<path>] [-dbhost=<host:port>] [-endpoint=<url>]");
            return Err(DevFailed::new("API_InvalidArgs", usage, "Server::from_args"));
        }
        Ok(server)
    }

    /// A device created in code; registered in the Database if there is one.
    pub fn device(mut self, name: &str, device: impl Device) -> Self {
        self.devices.push((name.into(), Box::new(device)));
        self
    }

    /// A device class: one device is created for each device of this class registered for the
    /// server in the Database (or Database file).
    pub fn class(mut self, name: &str, factory: impl Fn() -> Box<dyn Device> + Send + Sync + 'static) -> Self {
        self.classes.insert(name.into(), Box::new(factory));
        self
    }

    /// Uses the Database at `tango_host` (default: `$TANGO_HOST`).
    pub fn database(mut self, tango_host: &str) -> Self {
        self.config = Config::Database(tango_host.into());
        self
    }

    /// Ignores `$TANGO_HOST`: devices and properties come from code only.
    pub fn nodb(mut self) -> Self {
        self.config = Config::NoDb;
        self
    }

    /// Reads devices and properties from a Database file (RFC-6), read-only.
    pub fn db_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.config = Config::File(path.into());
        self
    }

    /// Polls an attribute every `period`.
    pub fn poll_attribute(mut self, device: &str, attr: &str, period: Duration) -> Self {
        self.polling.push((device.to_lowercase(), attr.into(), true, period));
        self
    }

    /// Polls a command without input every `period`.
    pub fn poll_command(mut self, device: &str, cmd: &str, period: Duration) -> Self {
        self.polling.push((device.to_lowercase(), cmd.into(), false, period));
        self
    }

    /// `run` on the `-endpoint=` arguments, default `auto://0.0.0.0:0` (`tcp://` without `detect`).
    pub async fn run_args(mut self) -> DevResult<()> {
        let endpoints = std::mem::take(&mut self.endpoints);
        match endpoints.is_empty() {
            true if cfg!(feature = "detect") => self.run(["auto://0.0.0.0:0"]).await,
            true => self.run(["tcp://0.0.0.0:0"]).await,
            false => self.run(endpoints).await,
        }
    }

    /// Serves the devices on every endpoint URL at once until `Kill`, SIGINT or SIGTERM:
    /// `tcp://host:port`, `zmq+tcp://host:port`, `grpc://host:port`, `zenoh://<locator>`,
    /// `auto://host:port` (see `Scheme`); port 0 picks a free port. With a Database, the devices
    /// are exported with the first endpoint and unexported on the way out.
    pub async fn run(self, endpoints: impl IntoIterator<Item = impl AsRef<str>>) -> DevResult<()> {
        let endpoints: Vec<String> = endpoints.into_iter().map(|e| e.as_ref().to_owned()).collect();
        for e in &endpoints {
            Scheme::parse(e)?;
        }
        let mut listeners = vec![];
        for e in &endpoints {
            listeners.push(crate::transport::listen(e).await?);
        }
        let export = listeners.first().map(|(url, _)| url.replace("0.0.0.0", &hostname())).unwrap_or_default();
        let server_name = format!("{}/{}", self.exec, self.instance);
        let started = self.start(&export).await?;
        if let Store::Db(db, _) = &started.store {
            for dev in &started.devices {
                db.export(dev, &export, &hostname()).await?;
            }
        }
        let mut servers = tokio::task::JoinSet::new();
        for (_, listener) in listeners {
            servers.spawn(crate::transport::serve(listener, started.handler.clone()));
        }
        let result = tokio::select! {
            r = servers.join_next() => match r {
                Some(r) => r.map_err(|e| DevFailed::new("API_InternalError", e.to_string(), "Server::run")).and_then(|r| r),
                None => Ok(()),
            },
            _ = started.shutdown.notified() => {
                tokio::time::sleep(Duration::from_millis(100)).await; // let the Kill reply leave
                Ok(())
            }
            _ = terminated() => Ok(()),
        };
        let _ = started.handler.0.stop.send(true);
        if let Store::Db(db, _) = &started.store {
            db.unexport_server(&server_name).await?;
        }
        result
    }

    /// Serves the devices on a Zenoh session with a full Zenoh configuration (routers to join,
    /// TLS, ...), when the `zenoh://<locator>` shorthand of `run` is not enough.
    #[cfg(feature = "zenoh")]
    pub async fn run_zenoh(self, config: zenoh::Config) -> DevResult<()> {
        let session = crate::transport::zenoh::open(config).await?;
        let started = self.start(&session.zid().to_string()).await?;
        crate::transport::zenoh::serve(session, started.handler).await
    }

    /// Creates and initialises the devices, starts polling and returns the request handler.
    /// `endpoint` is the URL the devices will be exported with. `run` uses it; a custom
    /// transport can use it directly.
    pub async fn start(self, endpoint: &str) -> DevResult<Started> {
        let server = format!("{}/{}", self.exec, self.instance);
        let store = match self.config {
            Config::NoDb => Store::None,
            Config::Database(h) => Store::Db(Database::connect(&h).await?, h),
            Config::File(p) => Store::File(Arc::new(DbFile::read(&p)?)),
            Config::Auto => match tango_host() {
                Some(h) => Store::Db(Database::connect(&h).await?, h),
                None => Store::None,
            },
        };
        let mut devices = self.devices;
        if let Store::Db(db, _) = &store {
            let registered: Vec<(String, String)> = devices.iter().map(|(n, d)| (n.clone(), d.class_name())).collect();
            for (name, class) in registered {
                db.add_device(&server, &name, &class).await?;
            }
        }
        for (name, class) in store.devices(&server).await? {
            if devices.iter().any(|(n, _)| n.eq_ignore_ascii_case(&name)) {
                continue;
            }
            match self.classes.get(&class) {
                Some(factory) => devices.push((name, factory())),
                None => eprintln!("{server}: no class {class} for device {name}, skipped"),
            }
        }

        let adm_name = format!("dserver/{server}");
        let mut classes: Vec<_> = devices.iter().map(|(_, d)| d.class_name()).collect();
        classes.sort();
        classes.dedup();
        let shutdown = Arc::new(Notify::new());
        let names = devices.iter().map(|(n, d)| format!("{}::{n}", d.class_name())).collect();
        let admin: Box<dyn Device> = Box::new(Admin { classes, devices: names, shutdown: shutdown.clone() });

        let hub = Arc::new(Hub::new(self.heartbeat, self.confirm));
        let (stop, _) = watch::channel(false);
        let mut entries = HashMap::new();
        let mut exported = vec![];
        let mut polling: Vec<_> = self
            .polling
            .into_iter()
            .map(|(dev, obj, attr, period)| (dev, if attr { "attribute" } else { "command" }, obj, period, true))
            .collect();
        for (name, dev) in devices.into_iter().chain([(adm_name.clone(), admin)]) {
            let mut entry = Entry::new(&name, dev);
            entry.store = store.clone();
            entry.hub = hub.clone();
            entry.dev.attach(EventSink { device: name.clone(), hub: hub.clone() });
            let attrs: Vec<String> = entry.attrs.iter().map(|a| a.name.clone()).collect();
            let stored = store.attribute_properties(&name, &attrs).await?;
            for a in &mut entry.attrs {
                if let Some(props) = stored.get(&a.name.to_lowercase()) {
                    apply_attribute(a, props);
                }
            }
            let (class, props) = (entry.class.clone(), entry.property_names());
            let raw = store.properties(&name, &class, &props).await?;
            entry.init(raw);
            // memorized set values are written back after the initialisation (RFC-4)
            let memorized = entry.attrs.iter().filter(|a| a.memorized).filter_map(|a| {
                let json = stored.get(&a.name.to_lowercase())?.get("__value")?.first()?;
                Some((a.name.clone(), serde_json::from_str(json).ok()?))
            });
            let memorized = memorized.collect();
            entry.restore(memorized);
            // the polling configuration saved by AddObjPolling & co
            let saved = ["polled_attr", "polled_cmd"].map(String::from);
            let saved = store.properties(&name, &class, &saved).await?;
            for (kind, prop) in [("attribute", "polled_attr"), ("command", "polled_cmd")] {
                for pair in saved.get(prop).into_iter().flatten().collect::<Vec<_>>().chunks(2) {
                    if let [obj, ms] = pair
                        && let Ok(ms) = ms.parse::<u64>()
                    {
                        polling.push((name.clone(), kind, obj.to_string(), Duration::from_millis(ms.max(1)), false));
                    }
                }
            }
            exported.push(name.clone());
            entries.insert(name.to_lowercase(), Arc::new(Mutex::new(entry)));
        }
        let ctx = Ctx {
            devices: entries,
            adm_name: adm_name.clone(),
            server_id: server,
            host: endpoint_host(endpoint),
            hub: hub.clone(),
            stop,
            store: store.clone(),
            classes: self.classes,
            rt: tokio::runtime::Handle::current(),
            polls: Mutex::default(),
            paused: Arc::default(),
            locks: Mutex::default(),
        };
        for (dev, kind, obj, period, configured) in polling {
            // a saved configuration may name what the code polls already, or what no longer exists
            if let Err(e) = ctx.add_poll(&dev, kind, &obj, period, false)
                && configured
            {
                return Err(e);
            }
        }
        let (beating, channel, mut stopped) = (hub, events::heartbeat_channel(&adm_name), ctx.stop.subscribe());
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(beating.heartbeat);
            loop {
                tokio::select! {
                    _ = tick.tick() => beating.beat(&channel),
                    _ = stopped.wait_for(|s| *s) => break,
                }
            }
        });

        Ok(Started { handler: Handler(Arc::new(ctx)), store, devices: exported, shutdown })
    }
}

/// The host part of an endpoint URL, for DEVICE INFO.
fn endpoint_host(endpoint: &str) -> String {
    let rest = endpoint.split_once("://").map_or(endpoint, |(_, r)| r);
    rest.rsplit_once(':').map_or(rest, |(h, _)| h).to_string()
}

/// SIGINT or SIGTERM.
async fn terminated() {
    #[cfg(unix)]
    let term = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut s) => drop(s.recv().await),
            Err(_) => std::future::pending().await,
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term => {}
    }
}

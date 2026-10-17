//! Device server: the `Device` trait, the transport-independent request handler, black box and polling.

use crate::protocol::*;
use crate::types::*;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

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
    fn init(&mut self) -> DevResult<()> {
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
    dev: Box<dyn Device>,
    cmds: Vec<CmdInfo>,
    attrs: Vec<AttrInfo>,
    pipes: Vec<PipeInfo>,
    w_values: HashMap<String, Value>,
    black_box: VecDeque<BlackBoxRecord>,
    cmd_ring: Ring<Value>,
    attr_ring: Ring<AttrValue>,
}

impl Entry {
    fn new(name: &str, dev: Box<dyn Device>) -> Self {
        let cmd = |name: &str, out_type| CmdInfo { name: name.into(), out_type, ..Default::default() };
        let attr = |name: &str, data_type| AttrInfo { name: name.into(), data_type, ..Default::default() };
        let cmds = [cmd("Init", DataType::Void), cmd("State", DataType::State), cmd("Status", DataType::String)];
        let attrs = [attr("State", DataType::State), attr("Status", DataType::String)];
        Self {
            name: name.into(),
            cmds: cmds.into_iter().chain(dev.commands()).collect(),
            attrs: attrs.into_iter().chain(dev.attributes()).collect(),
            pipes: dev.pipes(),
            dev,
            w_values: HashMap::new(),
            black_box: VecDeque::new(),
            cmd_ring: HashMap::new(),
            attr_ring: HashMap::new(),
        }
    }

    /// RFC-2: an ON device with an attribute in ALARM or WARNING is in ALARM.
    fn state(&mut self) -> DevState {
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
            "Init" => self.dev.init().map(|_| Value::Void),
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
        Ok(info.value(value, self.w_values.get(&info.name).cloned()))
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
        let errors: Vec<_> =
            values.into_iter().filter_map(|(n, v)| self.write_attr(&n, v).err()).flat_map(|e| e.0).collect();
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
        let name = info.name.clone();
        self.dev.write_attribute(&name, value.clone())?;
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
            ..c
        };
        Ok(a.clone())
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

    fn poll(&mut self, obj: &str) {
        let time = now();
        if self.attr_ring.contains_key(obj) {
            let value = self.read_attr(obj);
            push(self.attr_ring.get_mut(obj).unwrap(), History { time, value }, POLL_RING_DEPTH);
        } else {
            let value = self.execute(obj, Value::Void);
            push(self.cmd_ring.get_mut(obj).unwrap(), History { time, value }, POLL_RING_DEPTH);
        }
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
}

impl Device for Admin {
    fn class_name(&self) -> String {
        "DServer".into()
    }
    fn state(&mut self) -> DevState {
        DevState::On
    }
    fn commands(&self) -> Vec<CmdInfo> {
        let cmd = |name: &str| CmdInfo { name: name.into(), out_type: DataType::StringArray, ..Default::default() };
        vec![cmd("QueryClass"), cmd("QueryDevice")]
    }
    fn execute(&mut self, cmd: &str, _arg: Value) -> DevResult<Value> {
        Ok(Value::StringArray(if cmd == "QueryClass" { self.classes.clone() } else { self.devices.clone() }))
    }
}

type Shared = Arc<Mutex<Entry>>;

fn lock(e: &Shared) -> MutexGuard<'_, Entry> {
    e.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

struct Ctx {
    devices: HashMap<String, Shared>,
    adm_name: String,
    server_id: String,
    host: String,
}

impl Ctx {
    fn handle(&self, req: Request, host: &str) -> DevResult<Data> {
        if req.v != PROTOCOL_VERSION {
            let desc = format!("protocol version {} not supported, expected {PROTOCOL_VERSION}", req.v);
            return Err(DevFailed::new("API_UnsupportedProtocol", desc, "Server"));
        }
        let entry = self.devices.get(&req.device.to_lowercase());
        let entry =
            entry.ok_or_else(|| DevFailed::new("API_DeviceNotFound", format!("{} not found", req.device), "Server"))?;
        let mut e = lock(entry);
        let op = format!("{:?}", req.op);
        let record = BlackBoxRecord {
            time: now(),
            operation: op.split([' ', '(']).next().unwrap_or_default().into(),
            source: req.source,
            client_id: req.client,
            host: host.into(),
        };
        push(&mut e.black_box, record, BLACK_BOX_DEPTH);
        e.handle(req.op, req.source, self)
    }
}

/// Transport-independent request handler: one encoded request frame in, one reply frame out.
#[derive(Clone)]
pub struct Handler(Arc<Ctx>);

impl Handler {
    /// `peer` identifies the client in the black box.
    pub fn frame(&self, frame: &[u8], peer: &str) -> Vec<u8> {
        let reply = match serde_json::from_slice::<Request>(frame) {
            Ok(req) => Reply { id: req.id, reply: self.0.handle(req, peer) },
            Err(e) => Reply { id: 0, reply: Err(e.into()) },
        };
        serde_json::to_vec(&reply).expect("a reply is always serialisable")
    }

    /// Names (lower case) of the exported devices, the admin device included.
    pub fn devices(&self) -> Vec<String> {
        self.0.devices.keys().cloned().collect()
    }
}

/// A device server (RFC-8): hosts devices on one Zenoh session.
pub struct Server {
    exec: String,
    instance: String,
    devices: Vec<(String, Box<dyn Device>)>,
    polling: Vec<(String, String, bool, Duration)>,
}

impl Server {
    pub fn new(exec: &str, instance: &str) -> Self {
        Self { exec: exec.into(), instance: instance.into(), devices: vec![], polling: vec![] }
    }

    pub fn device(mut self, name: &str, device: impl Device) -> Self {
        self.devices.push((name.into(), Box::new(device)));
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

    /// Joins the Zenoh network described by `config` and serves every device as the queryable
    /// `tango/<device>` forever.
    pub async fn run(self, config: zenoh::Config) -> DevResult<()> {
        let session = crate::zenoh::open(config).await?;
        let handler = self.start(&session.zid().to_string())?;
        crate::zenoh::serve(session, handler).await
    }

    /// Initialises and exports the devices, starts polling and returns the request handler.
    /// `host` is reported in DEVICE INFO. Must be called within a tokio runtime.
    pub fn start(self, host: &str) -> DevResult<Handler> {
        let adm_name = format!("dserver/{}/{}", self.exec, self.instance);
        let mut classes: Vec<_> = self.devices.iter().map(|(_, d)| d.class_name()).collect();
        classes.dedup();
        let devices = self.devices.iter().map(|(n, d)| format!("{}::{n}", d.class_name())).collect();
        let admin: Box<dyn Device> = Box::new(Admin { classes, devices });

        let mut entries = HashMap::new();
        for (name, dev) in self.devices.into_iter().chain([(adm_name.clone(), admin)]) {
            let mut entry = Entry::new(&name, dev);
            entry.dev.init()?;
            entries.insert(name.to_lowercase(), Arc::new(Mutex::new(entry)));
        }
        for (dev, obj, is_attr, period) in self.polling {
            let not_found = || DevFailed::new("API_DeviceNotFound", format!("{dev} not found"), "Server::poll");
            let entry = entries.get(&dev).ok_or_else(not_found)?.clone();
            let obj = {
                let mut e = lock(&entry);
                if is_attr {
                    let name = e.attrs[find(&e.attrs, &obj, "API_AttrNotFound")?].name.clone();
                    e.attr_ring.insert(name.clone(), VecDeque::new());
                    name
                } else {
                    let name = e.cmds[find(&e.cmds, &obj, "API_CommandNotFound")?].name.clone();
                    e.cmd_ring.insert(name.clone(), VecDeque::new());
                    name
                }
            };
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(period);
                loop {
                    tick.tick().await;
                    lock(&entry).poll(&obj);
                }
            });
        }

        let server_id = format!("{}/{}", self.exec, self.instance);
        Ok(Handler(Arc::new(Ctx { devices: entries, adm_name, server_id, host: host.into() })))
    }
}

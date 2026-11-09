//! Events (RFC-12): types, payloads, channel names, the server's publication hub, the sink handed
//! to device code, and the client-side stream.

use crate::types::*;
use serde::{Deserialize, Serialize};
#[cfg(not(target_arch = "wasm32"))]
use std::collections::{HashMap, HashSet};
#[cfg(not(target_arch = "wasm32"))]
use std::io::Write;
#[cfg(not(target_arch = "wasm32"))]
use std::sync::{Arc, Mutex};
#[cfg(not(target_arch = "wasm32"))]
use std::time::{Duration, Instant};
#[cfg(not(target_arch = "wasm32"))]
use tokio::sync::broadcast;
use tokio::sync::mpsc;

#[cfg(not(target_arch = "wasm32"))]
pub const HEARTBEAT: Duration = Duration::from_secs(9);
#[cfg(not(target_arch = "wasm32"))]
pub const CONFIRM: Duration = Duration::from_secs(200);
/// Events buffered per subscriber before the oldest are dropped (Tango `zmq_sub_event_hwm`).
pub(crate) const BUFFER: usize = 1000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EventType {
    Change,
    Periodic,
    Archive,
    User,
    AttrConf,
    DataReady,
    InterfaceChange,
    Pipe,
}

impl EventType {
    pub const ALL: [EventType; 8] = [
        Self::Change,
        Self::Periodic,
        Self::Archive,
        Self::User,
        Self::AttrConf,
        Self::DataReady,
        Self::InterfaceChange,
        Self::Pipe,
    ];

    /// The lower-case name used in channels and admin commands: `change`, `attr_conf`, …
    pub fn suffix(self) -> &'static str {
        match self {
            Self::Change => "change",
            Self::Periodic => "periodic",
            Self::Archive => "archive",
            Self::User => "user",
            Self::AttrConf => "attr_conf",
            Self::DataReady => "data_ready",
            Self::InterfaceChange => "interface_change",
            Self::Pipe => "pipe",
        }
    }

    /// Accepts `change`, `CHANGE_EVENT`, …
    pub fn parse(name: &str) -> Option<Self> {
        let n = name.to_lowercase();
        let n = n.strip_suffix("_event").unwrap_or(&n);
        Self::ALL.into_iter().find(|t| t.suffix() == n)
    }
}

// events travel behind an `Arc`: the size of the largest variant costs one allocation per event
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum EventData {
    Attr(AttrValue),
    Conf(AttrInfo),
    Pipe(PipeValue),
    DataReady {
        name: String,
        data_type: DataType,
        counter: i32,
    },
    Interface {
        commands: Vec<CmdInfo>,
        attributes: Vec<AttrInfo>,
        started: bool,
    },
    /// On the server's heartbeat channel, every heartbeat period.
    Heartbeat,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Event {
    pub channel: String,
    pub seq: u64,
    pub event_type: EventType,
    pub time: f64,
    pub data: DevResult<EventData>,
}

#[cfg(not(target_arch = "wasm32"))]
/// `<device>/<name>.<type>`, or `<device>.interface_change`, in lower case.
pub fn channel(device: &str, name: &str, t: EventType) -> String {
    match t {
        EventType::InterfaceChange => format!("{device}.{}", t.suffix()),
        _ => format!("{device}/{name}.{}", t.suffix()),
    }
    .to_lowercase()
}

#[cfg(not(target_arch = "wasm32"))]
pub fn heartbeat_channel(admin: &str) -> String {
    format!("{}.heartbeat", admin.to_lowercase())
}

/// RFC-4 change rule between the value of the last event and a new one (SPEC §9.1).
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn changed(
    old: &DevResult<AttrValue>,
    new: &DevResult<AttrValue>,
    abs: Option<f64>,
    rel: Option<f64>,
) -> bool {
    let (Ok(old), Ok(new)) = (old, new) else { return old.is_ok() != new.is_ok() };
    if old.quality != new.quality || (old.dim_x, old.dim_y) != (new.dim_x, new.dim_y) {
        return true;
    }
    let (a, b) = (old.value.numbers(), new.value.numbers());
    if a.is_empty() || a.len() != b.len() {
        return old.value != new.value;
    }
    a.iter().zip(&b).any(|(o, n)| {
        abs.is_some_and(|t| (n - o).abs() >= t)
            || rel.is_some_and(|t| *o != 0.0 && (n - o).abs() / o.abs() * 100.0 >= t)
    })
}

#[cfg(not(target_arch = "wasm32"))]
struct Channel {
    seq: u64,
    subscribers: usize,
    confirmed: Instant,
}

#[cfg(not(target_arch = "wasm32"))]
/// The server's publication point: every transport forwards what it broadcasts.
pub(crate) struct Hub {
    tx: broadcast::Sender<Arc<Event>>,
    channels: Mutex<HashMap<String, Channel>>,
    pub(crate) heartbeat: Duration,
    pub(crate) confirm: Duration,
    /// The ZeroMQ PUB endpoint, when the server serves ZeroMQ.
    pub(crate) zmq_endpoint: Mutex<String>,
    heartbeats: Mutex<u64>,
    /// Devices whose code announced a change of interface (dynamic attributes and commands).
    interface_changed: Mutex<HashSet<String>>,
    pub(crate) logging: Mutex<Logging>,
}

/// RFC-14 logging levels; a message is emitted if its level is at most the device's level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Serialize, Deserialize)]
pub enum LogLevel {
    Off = 0,
    Fatal = 1,
    Error = 2,
    #[default]
    Warn = 3,
    Info = 4,
    Debug = 5,
}

impl LogLevel {
    pub fn from_i32(n: i32) -> Option<Self> {
        [Self::Off, Self::Fatal, Self::Error, Self::Warn, Self::Info, Self::Debug]
            .get(usize::try_from(n).ok()?)
            .copied()
    }

    pub fn name(self) -> &'static str {
        ["OFF", "FATAL", "ERROR", "WARN", "INFO", "DEBUG"][self as usize]
    }
}

#[cfg(not(target_arch = "wasm32"))]
/// The logging configuration of the server (RFC-14): per device a level and targets,
/// `console::` or `file::<path>`.
pub(crate) struct Logging {
    pub(crate) enabled: bool,
    pub(crate) devices: HashMap<String, (LogLevel, Vec<String>)>,
}

#[cfg(not(target_arch = "wasm32"))]
impl Logging {
    pub(crate) fn device(&mut self, device: &str) -> &mut (LogLevel, Vec<String>) {
        self.devices.entry(device.to_lowercase()).or_default()
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Hub {
    pub(crate) fn new(heartbeat: Duration, confirm: Duration) -> Self {
        let (tx, _) = broadcast::channel(BUFFER);
        Self {
            tx,
            channels: Mutex::default(),
            heartbeat,
            confirm,
            zmq_endpoint: Mutex::default(),
            heartbeats: Mutex::new(0),
            interface_changed: Mutex::default(),
            logging: Mutex::new(Logging { enabled: true, devices: HashMap::new() }),
        }
    }

    /// Emits `<time> LEVEL <device> <message>` on the targets of `device` if its level allows.
    pub(crate) fn log(&self, device: &str, level: LogLevel, message: &str) {
        let targets = {
            let mut logging = self.logging.lock().unwrap_or_else(|p| p.into_inner());
            if !logging.enabled || level == LogLevel::Off {
                return;
            }
            match logging.device(device) {
                (max, targets) if level <= *max && !targets.is_empty() => targets.clone(),
                _ => return,
            }
        };
        let line = format!("{:.6} {} {device} {message}\n", now(), level.name());
        for t in targets {
            if let Some(path) = t.strip_prefix("file::") {
                let file = std::fs::OpenOptions::new().create(true).append(true).open(path);
                let _ = file.and_then(|mut f| f.write_all(line.as_bytes()));
            } else {
                eprint!("{line}");
            }
        }
    }

    pub(crate) fn interface_changed(&self, device: &str) {
        self.interface_changed.lock().unwrap_or_else(|p| p.into_inner()).insert(device.to_lowercase());
    }

    /// Whether `device` announced a change of interface since the last call.
    pub(crate) fn take_interface_changed(&self, device: &str) -> bool {
        self.interface_changed.lock().unwrap_or_else(|p| p.into_inner()).remove(&device.to_lowercase())
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Channel>> {
        self.channels.lock().unwrap_or_else(|p| p.into_inner())
    }

    pub(crate) fn receiver(&self) -> broadcast::Receiver<Arc<Event>> {
        self.tx.subscribe()
    }

    pub(crate) fn subscribe(&self, channel: &str) {
        let mut c = self.lock();
        let ch = c.entry(channel.into()).or_insert(Channel { seq: 0, subscribers: 0, confirmed: Instant::now() });
        ch.subscribers += 1;
        ch.confirmed = Instant::now();
    }

    pub(crate) fn unsubscribe(&self, channel: &str) {
        let mut c = self.lock();
        if let Some(ch) = c.get_mut(channel) {
            ch.subscribers = ch.subscribers.saturating_sub(1);
            if ch.subscribers == 0 {
                c.remove(channel);
            }
        }
    }

    pub(crate) fn confirm(&self, channel: &str) {
        if let Some(ch) = self.lock().get_mut(channel) {
            ch.confirmed = Instant::now();
        }
    }

    pub(crate) fn active(&self, channel: &str) -> bool {
        self.lock().contains_key(channel)
    }

    /// Publishes on a subscribed channel; does nothing otherwise.
    pub(crate) fn publish(&self, channel: &str, event_type: EventType, data: DevResult<EventData>) {
        let seq = match self.lock().get_mut(channel) {
            Some(ch) => {
                ch.seq += 1;
                ch.seq
            }
            None => return,
        };
        let _ = self.tx.send(Arc::new(Event { channel: channel.into(), seq, event_type, time: now(), data }));
    }

    /// One heartbeat; drops the channels not confirmed for 3 confirm periods.
    pub(crate) fn beat(&self, channel: &str) {
        self.lock().retain(|_, ch| ch.confirmed.elapsed() < self.confirm * 3);
        let seq = {
            let mut n = self.heartbeats.lock().unwrap_or_else(|p| p.into_inner());
            *n += 1;
            *n
        };
        let event = Event {
            channel: channel.into(),
            seq,
            event_type: EventType::Periodic,
            time: now(),
            data: Ok(EventData::Heartbeat),
        };
        let _ = self.tx.send(Arc::new(event));
    }
}

#[cfg(not(target_arch = "wasm32"))]
/// Handed to device code (`Device::attach`) to push events itself.
#[derive(Clone)]
pub struct EventSink {
    pub(crate) device: String,
    pub(crate) hub: Arc<Hub>,
}

#[cfg(not(target_arch = "wasm32"))]
impl EventSink {
    pub fn push(&self, name: &str, event_type: EventType, data: DevResult<EventData>) -> DevResult<()> {
        self.hub.publish(&channel(&self.device, name, event_type), event_type, data);
        Ok(())
    }

    /// A value of attribute `attr` (USER, or CHANGE/ARCHIVE for `events_by_code` attributes).
    pub fn push_value(&self, attr: &str, event_type: EventType, value: Value) -> DevResult<()> {
        let dim_x = value.len().max(1);
        let v = AttrValue {
            name: attr.into(),
            value,
            w_value: None,
            quality: AttrQuality::Valid,
            time: now(),
            dim_x,
            dim_y: 0,
        };
        self.push(attr, event_type, Ok(EventData::Attr(v)))
    }

    pub fn push_data_ready(&self, attr: &str, counter: i32) -> DevResult<()> {
        let data = EventData::DataReady { name: attr.into(), data_type: DataType::Void, counter };
        self.push(attr, EventType::DataReady, Ok(data))
    }

    /// Logs a message of the device (RFC-14), on its targets if its level allows.
    pub fn log(&self, level: LogLevel, message: &str) {
        self.hub.log(&self.device, level, message);
    }

    /// Announces that `commands()`/`attributes()`/`pipes()` changed (dynamic attributes and
    /// commands, RFC-15): the server re-reads them after the current request and sends
    /// INTERFACE_CHANGE.
    pub fn interface_changed(&self) {
        self.hub.interface_changed(&self.device);
    }

    pub fn push_pipe(&self, pipe: &str, blob: PipeBlob) -> DevResult<()> {
        self.push(pipe, EventType::Pipe, Ok(EventData::Pipe(PipeValue { name: pipe.into(), time: now(), blob })))
    }
}

/// Events of one subscription, in order; dropping it unsubscribes.
pub struct EventStream {
    pub(crate) rx: mpsc::Receiver<Event>,
}

impl EventStream {
    /// The next event, `None` once the subscription ended.
    pub async fn recv(&mut self) -> Option<Event> {
        self.rx.recv().await
    }
}

/// A callback subscription (`DeviceProxy::subscribe_event`); dropping it unsubscribes.
pub struct Subscription(pub(crate) crate::rt::AbortHandle);

impl Subscription {
    pub async fn unsubscribe(self) -> DevResult<()> {
        self.0.abort();
        Ok(())
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Detects gaps in the sequence numbers of a channel.
#[derive(Default)]
pub(crate) struct Sequence(Option<u64>);

impl Sequence {
    /// The number of missed events before `seq` (0 if none).
    pub(crate) fn missed(&mut self, seq: u64) -> u64 {
        let missed = match self.0 {
            Some(next) if seq > next => seq - next,
            _ => 0,
        };
        self.0 = Some(seq + 1);
        missed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn attr(v: f64) -> DevResult<AttrValue> {
        Ok(AttrValue {
            name: "a".into(),
            value: Value::Double(v),
            w_value: None,
            quality: AttrQuality::Valid,
            time: 0.0,
            dim_x: 1,
            dim_y: 0,
        })
    }

    #[test]
    fn change_rule() {
        assert!(!changed(&attr(0.0), &attr(0.5), Some(1.0), None));
        assert!(changed(&attr(0.0), &attr(1.0), Some(1.0), None));
        assert!(changed(&attr(10.0), &attr(11.0), None, Some(10.0)));
        assert!(!changed(&attr(10.0), &attr(10.5), None, Some(10.0)));
        assert!(changed(&attr(1.0), &Err(DevFailed::new("X", "", "")), Some(1.0), None), "error appears");
        let mut alarm = attr(0.0).unwrap();
        alarm.quality = AttrQuality::Alarm;
        assert!(changed(&attr(0.0), &Ok(alarm), Some(1.0), None), "quality changes");
    }

    #[test]
    fn missed_events() {
        let mut s = Sequence::default();
        assert_eq!([1, 2, 5, 6].map(|n| s.missed(n)), [0, 0, 2, 0]);
    }

    #[test]
    fn names() {
        assert_eq!(EventType::parse("CHANGE_EVENT"), Some(EventType::Change));
        assert_eq!(EventType::parse("attr_conf"), Some(EventType::AttrConf));
        assert_eq!(channel("Sys/TG/1", "Ramp", EventType::Change), "sys/tg/1/ramp.change");
        assert_eq!(channel("sys/tg/1", "", EventType::InterfaceChange), "sys/tg/1.interface_change");
    }
}

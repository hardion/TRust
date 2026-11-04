//! Client side: `DeviceProxy`, one connection to one device.

use crate::db::{Database, database_endpoint, tango_host};
use crate::events::{Event, EventData, EventStream, EventType, Sequence, Subscription};
use crate::protocol::*;
use crate::rt::{self, Instant};
use crate::transport::{EventLink, Link};
use crate::types::*;
use std::time::Duration;

pub struct DeviceProxy {
    device: String,
    source: Source,
    next_id: u64,
    link: Link,
    /// The resolved endpoint URL of the link (no `auto://`), reused for events.
    endpoint: String,
    /// `(TANGO_HOST, device)` when the device was resolved through a Database.
    resolved: Option<(String, String)>,
}

/// A live subscription: its admin device, event link and timings (SPEC §9.2).
struct Watch {
    admin: DeviceProxy,
    link: Option<EventLink>,
    url: String,
    zmq: String,
    channel: String,
    beat: String,
    heartbeat: Duration,
    confirm: Duration,
    subscribe: Value,
    unsubscribe: Value,
    confirm_args: Value,
    event_type: EventType,
    seq: Sequence,
}

impl Watch {
    /// Connects the event link and waits until it carries a heartbeat (the confirm command makes
    /// the server send one), so that no event published from now on can be missed. Returns the
    /// events of the channel seen meanwhile.
    async fn connect(&mut self) -> DevResult<Vec<Event>> {
        let mut link = EventLink::connect(&self.url, &self.zmq, vec![self.channel.clone(), self.beat.clone()]).await?;
        let mut early = vec![];
        for _ in 0..100 {
            self.admin.command_inout("EventConfirmSubscription", self.confirm_args.clone()).await?;
            let deadline = Instant::now() + Duration::from_millis(50);
            while let Some(e) = rt::timeout(deadline.saturating_duration_since(Instant::now()), link.next()).await {
                let e = e?;
                if e.channel == self.beat {
                    self.link = Some(link);
                    return Ok(early);
                }
                early.push(e);
            }
        }
        Err(DevFailed::new("API_EventTimeout", format!("no heartbeat on {}", self.beat), "event_stream"))
    }

    fn error(&self, reason: &str, desc: String) -> Event {
        let data = Err(DevFailed::new(reason, desc, &self.channel));
        Event { channel: self.channel.clone(), seq: 0, event_type: self.event_type, time: now(), data }
    }

    /// Sends an event to the stream, preceded by an `API_MissedEvents` error after a gap.
    async fn deliver(&mut self, tx: &tokio::sync::mpsc::Sender<Event>, e: Event) -> bool {
        let missed = self.seq.missed(e.seq);
        if missed > 0 && tx.send(self.error("API_MissedEvents", format!("{missed} events missed"))).await.is_err() {
            return false;
        }
        tx.send(e).await.is_ok()
    }

    /// Delivers events, watches heartbeats, confirms, re-subscribes; unsubscribes once the stream
    /// is dropped.
    async fn run(mut self, tx: tokio::sync::mpsc::Sender<Event>) {
        let mut last_beat = Instant::now();
        let mut next_confirm = Instant::now() + self.confirm;
        loop {
            let next = async {
                match self.link.as_mut() {
                    Some(link) => link.next().await,
                    None => std::future::pending().await,
                }
            };
            let lost_at = last_beat + self.heartbeat * 3;
            let wake = lost_at.min(next_confirm).saturating_duration_since(Instant::now());
            tokio::select! {
                biased;
                _ = tx.closed() => break,
                e = next => match e {
                    Ok(e) if e.channel == self.beat => last_beat = Instant::now(),
                    Ok(e) => if !self.deliver(&tx, e).await { break },
                    Err(_) => self.link = None, // wait for the heartbeat timeout
                },
                _ = rt::sleep(wake) => {
                    if Instant::now() >= lost_at {
                        let lost = self.error("API_EventTimeout", format!("no heartbeat on {}", self.beat));
                        if tx.send(lost).await.is_err() {
                            break;
                        }
                        last_beat = Instant::now();
                        self.link = None;
                        if self.admin.command_inout("EventSubscriptionChange", self.subscribe.clone()).await.is_ok()
                            && let Ok(early) = self.connect().await
                        {
                            for e in early {
                                self.deliver(&tx, e).await;
                            }
                        }
                    }
                    if Instant::now() >= next_confirm {
                        let _ = self.admin.command_inout("EventConfirmSubscription", self.confirm_args.clone()).await;
                        next_confirm = Instant::now() + self.confirm;
                    }
                }
            }
        }
        let _ = self.admin.command_inout("EventSubscriptionChange", self.unsubscribe.clone()).await;
    }
}

pub(crate) fn bad_reply(what: impl std::fmt::Debug) -> DevFailed {
    DevFailed::new("API_CommunicationFailed", format!("unexpected reply {what:?}"), "DeviceProxy")
}

fn strings(names: &[&str]) -> Vec<String> {
    names.iter().map(|n| n.to_string()).collect()
}

fn pairs(values: &[(&str, Value)]) -> Vec<(String, Value)> {
    values.iter().map(|(n, v)| (n.to_string(), v.clone())).collect()
}

fn invalid(url: &str) -> DevFailed {
    DevFailed::new("API_InvalidArgs", format!("expected tango://host:port/domain/family/member, got {url}"), "url")
}

/// `(device, endpoint)` of a device or alias, through the Database at `host`.
async fn import(host: &str, name: &str) -> DevResult<(String, String)> {
    let db = Database::connect(host).await?;
    let device = if name.contains('/') { name.to_string() } else { db.alias_device(name).await? };
    match db.import(&device).await? {
        (true, endpoint) => Ok((device, endpoint)),
        (false, _) => Err(DevFailed::new("API_DeviceNotExported", format!("{device} is not exported"), host)),
    }
}

/// Splits `<endpoint>/domain/family/member`: the device name is always the last three segments.
pub(crate) fn split_url(url: &str) -> DevResult<(&str, &str)> {
    let cut = url.char_indices().filter(|(_, c)| *c == '/').map(|(i, _)| i).rev().nth(2);
    match cut {
        Some(i) if i > 0 => Ok((&url[..i], &url[i + 1..])),
        _ => Err(DevFailed::new(
            "API_InvalidArgs",
            format!("expected <endpoint>/domain/family/member, got {url}"),
            "url",
        )),
    }
}

/// Sends `$op` and extracts the `Data::$variant` payload (`()` for `Data::Void`).
macro_rules! call {
    ($self:ident, $op:expr) => {
        match $self.call($op).await? {
            Data::Void => Ok(()),
            other => Err(bad_reply(other)),
        }
    };
    ($self:ident, $op:expr, $variant:ident) => {
        match $self.call($op).await? {
            Data::$variant(x) => Ok(x),
            other => Err(bad_reply(other)),
        }
    };
}

impl DeviceProxy {
    /// Connects to a device (SPEC §7.1):
    /// * `tango://dbhost:port/domain/family/member` (or an alias) resolves through that Database,
    /// * `tango://host:port/domain/family/member#dbase=no` connects directly,
    /// * `domain/family/member` (or an alias) resolves through the Database at `$TANGO_HOST`,
    /// * `<endpoint>/domain/family/member` connects directly with the endpoint's transport:
    ///   `tcp://host:port`, `zmq+tcp://host:port`, `grpc://host:port`, `zenoh://tcp/host:7447`,
    ///   `zenoh://` (discovery), `auto://host:port` (probe), or `host:port` (TCP), see `Scheme`.
    pub async fn connect(url: &str) -> DevResult<Self> {
        if let Some(rest) = url.strip_prefix("tango://") {
            if let Some(rest) = rest.strip_suffix("#dbase=no") {
                let (host, device) = rest.split_once('/').ok_or_else(|| invalid(url))?;
                return Self::direct(&database_endpoint(host), device).await;
            }
            let (host, name) = rest.split_once('/').ok_or_else(|| invalid(url))?;
            return Self::resolve(host, name).await;
        }
        if !url.contains("://") && url.matches('/').count() <= 2 {
            let host =
                tango_host().ok_or_else(|| DevFailed::new("API_TangoHostNotSet", "TANGO_HOST is not set", url))?;
            return Self::resolve(&host, url).await;
        }
        let (endpoint, device) = split_url(url)?;
        Self::direct(endpoint, device).await
    }

    /// Connects to `device` at `endpoint`, without a Database.
    pub(crate) async fn direct(endpoint: &str, device: &str) -> DevResult<Self> {
        let endpoint = crate::transport::resolve(endpoint).await?;
        let link = Link::connect(&endpoint, device).await?;
        Ok(Self { device: device.into(), source: Source::Dev, next_id: 0, link, endpoint, resolved: None })
    }

    async fn resolve(host: &str, name: &str) -> DevResult<Self> {
        let (device, endpoint) = import(host, name).await?;
        let mut proxy = Self::direct(&endpoint, &device).await?;
        proxy.resolved = Some((host.into(), device));
        Ok(proxy)
    }

    /// Subscribes to the events of `name` (an attribute, a pipe for PIPE, `""` for
    /// INTERFACE_CHANGE), SPEC §9. The first event is the current value; dropping the stream
    /// unsubscribes.
    pub async fn event_stream(&mut self, name: &str, event_type: EventType) -> DevResult<EventStream> {
        let adm = self.adm_name().await?;
        let mut admin = Self::direct(&self.endpoint, &adm).await?;
        let args = |action: &str| {
            let mut a =
                vec![self.device.clone(), name.to_string(), action.to_string(), event_type.suffix().to_string()];
            if action != "confirm" {
                a.push("1".into());
            } else {
                a.remove(2);
            }
            Value::StringArray(a)
        };
        let Value::LongStringArray(l, s) = admin.command_inout("EventSubscriptionChange", args("subscribe")).await?
        else {
            return Err(bad_reply("SUBSCRIPTION_INFO"));
        };
        let (Some(heartbeat), Some(confirm), [zmq, channel, beat]) = (l.get(1), l.get(2), &s[..]) else {
            return Err(bad_reply(s));
        };
        let ms = |v: i32| Duration::from_millis(v.max(1) as u64);
        let mut sub = Watch {
            admin,
            link: None,
            url: self.endpoint.clone(),
            zmq: zmq.clone(),
            channel: channel.clone(),
            beat: beat.clone(),
            heartbeat: ms(*heartbeat),
            confirm: ms(*confirm),
            subscribe: args("subscribe"),
            unsubscribe: args("unsubscribe"),
            confirm_args: args("confirm"),
            event_type,
            seq: Sequence::default(),
        };
        let early = sub.connect().await?;
        let (tx, rx) = tokio::sync::mpsc::channel(crate::events::BUFFER);
        if let Some(first) = self.first_event(name, event_type, channel).await {
            sub.seq.missed(first.seq);
            let _ = tx.send(first).await;
        }
        for e in early {
            sub.deliver(&tx, e).await;
        }
        rt::spawn(sub.run(tx));
        Ok(EventStream { rx })
    }

    /// `event_stream` with a callback, called on a task of its own; dropping the subscription
    /// unsubscribes.
    pub async fn subscribe_event(
        &mut self,
        name: &str,
        event_type: EventType,
        mut callback: impl FnMut(Event) + Send + 'static,
    ) -> DevResult<Subscription> {
        let mut events = self.event_stream(name, event_type).await?;
        Ok(Subscription(rt::spawn(async move {
            while let Some(e) = events.recv().await {
                callback(e);
            }
        })))
    }

    /// The synchronous first event: the current value, configuration, pipe or interface.
    async fn first_event(&mut self, name: &str, event_type: EventType, channel: &str) -> Option<Event> {
        let data = match event_type {
            EventType::DataReady => return None,
            EventType::AttrConf => self.get_attribute_config(&[name]).await.map(|mut c| EventData::Conf(c.remove(0))),
            EventType::Pipe => self.read_pipe(name).await.map(EventData::Pipe),
            EventType::InterfaceChange => match (self.command_list_query().await, self.get_attribute_config(&[]).await)
            {
                (Ok(commands), Ok(attributes)) => Ok(EventData::Interface { commands, attributes, started: false }),
                (Err(e), _) | (_, Err(e)) => Err(e),
            },
            _ => self.read_attribute(name).await.map(EventData::Attr),
        };
        Some(Event { channel: channel.into(), seq: 0, event_type, time: now(), data })
    }

    pub fn set_source(&mut self, source: Source) {
        self.source = source;
    }

    /// Sends `op`; a device resolved through a Database is re-imported once if its server is gone.
    async fn call(&mut self, op: Op) -> DevResult<Data> {
        match self.send(op.clone()).await {
            Err(e) if e.reason() == "API_CommunicationFailed" && self.resolved.is_some() => {
                let (host, device) = self.resolved.clone().expect("checked");
                let (_, endpoint) = Box::pin(import(&host, &device)).await?; // boxed: import calls back into here
                self.endpoint = crate::transport::resolve(&endpoint).await?;
                self.link = Link::connect(&self.endpoint, &device).await?;
                self.send(op).await
            }
            result => result,
        }
    }

    async fn send(&mut self, op: Op) -> DevResult<Data> {
        self.next_id += 1;
        let (id, client, source, device) = (self.next_id, rt::client_id(), self.source, self.device.clone());
        let reply = self.link.call(Request { v: PROTOCOL_VERSION, id, client, source, device, op }).await?;
        if reply.id != id {
            return Err(bad_reply(reply));
        }
        reply.reply
    }

    pub async fn name(&mut self) -> DevResult<String> {
        call!(self, Op::DeviceName, String)
    }

    pub async fn description(&mut self) -> DevResult<String> {
        call!(self, Op::DeviceDescription, String)
    }

    pub async fn adm_name(&mut self) -> DevResult<String> {
        call!(self, Op::DeviceAdmName, String)
    }

    /// Locks the device for this client (process) for `validity` (at least 1 s), renewed by
    /// another `lock`: other clients can only read it and run State and Status (RFC-8).
    pub async fn lock(&mut self, validity: Duration) -> DevResult<()> {
        let arg = Value::LongStringArray(vec![validity.as_secs().max(1) as i32], vec![self.device.clone()]);
        self.admin_command("LockDevice", arg).await.map(drop)
    }

    pub async fn unlock(&mut self) -> DevResult<()> {
        self.admin_command("UnLockDevice", Value::LongStringArray(vec![0], vec![self.device.clone()])).await.map(drop)
    }

    /// A command of the device's admin device, sent with this proxy's client id.
    async fn admin_command(&mut self, cmd: &str, arg: Value) -> DevResult<Value> {
        let adm = self.adm_name().await?;
        Self::direct(&self.endpoint, &adm).await?.command_inout(cmd, arg).await
    }

    /// Round trip time.
    pub async fn ping(&mut self) -> DevResult<Duration> {
        let start = Instant::now();
        call!(self, Op::DevicePing)?;
        Ok(start.elapsed())
    }

    pub async fn black_box(&mut self, n: usize) -> DevResult<Vec<BlackBoxRecord>> {
        call!(self, Op::DeviceBlackbox { n }, BlackBox)
    }

    pub async fn info(&mut self) -> DevResult<DeviceInfo> {
        call!(self, Op::DeviceInfo, Info)
    }

    pub async fn command_inout(&mut self, name: &str, arg: Value) -> DevResult<Value> {
        call!(self, Op::CommandExecution { name: name.into(), arg }, Value)
    }

    pub async fn command_list_query(&mut self) -> DevResult<Vec<CmdInfo>> {
        call!(self, Op::CommandsList, Commands)
    }

    pub async fn command_query(&mut self, name: &str) -> DevResult<CmdInfo> {
        let mut infos = call!(self, Op::CommandInfo { name: name.into() }, Commands)?;
        infos.pop().ok_or_else(|| bad_reply("no command info"))
    }

    pub async fn command_history(&mut self, name: &str, n: usize) -> DevResult<Vec<CmdHistory>> {
        call!(self, Op::CommandHistory { name: name.into(), n }, CmdHistory)
    }

    pub async fn read_attributes(&mut self, names: &[&str]) -> DevResult<Vec<DevResult<AttrValue>>> {
        call!(self, Op::AttributesRead { names: strings(names) }, Attributes)
    }

    pub async fn read_attribute(&mut self, name: &str) -> DevResult<AttrValue> {
        self.read_attributes(&[name]).await?.pop().ok_or_else(|| bad_reply("no attribute value"))?
    }

    pub async fn write_attributes(&mut self, values: &[(&str, Value)]) -> DevResult<()> {
        call!(self, Op::AttributesWrite { values: pairs(values) })
    }

    pub async fn write_attribute(&mut self, name: &str, value: Value) -> DevResult<()> {
        self.write_attributes(&[(name, value)]).await
    }

    pub async fn write_read_attributes(
        &mut self,
        values: &[(&str, Value)],
        names: &[&str],
    ) -> DevResult<Vec<DevResult<AttrValue>>> {
        call!(self, Op::AttributesWriteRead { values: pairs(values), names: strings(names) }, Attributes)
    }

    /// Empty `names` returns every attribute.
    pub async fn get_attribute_config(&mut self, names: &[&str]) -> DevResult<Vec<AttrInfo>> {
        call!(self, Op::AttributeGetConfig { names: strings(names) }, AttrConfigs)
    }

    pub async fn set_attribute_config(&mut self, configs: Vec<AttrInfo>) -> DevResult<Vec<AttrInfo>> {
        call!(self, Op::AttributeSetConfig { configs }, AttrConfigs)
    }

    pub async fn attribute_history(&mut self, name: &str, n: usize) -> DevResult<Vec<AttrHistory>> {
        call!(self, Op::AttributeHistory { name: name.into(), n }, AttrHistory)
    }

    pub async fn read_pipe(&mut self, name: &str) -> DevResult<PipeValue> {
        call!(self, Op::PipeRead { name: name.into() }, Pipe)
    }

    pub async fn write_pipe(&mut self, name: &str, blob: PipeBlob) -> DevResult<()> {
        call!(self, Op::PipeWrite { name: name.into(), blob })
    }

    pub async fn write_read_pipe(&mut self, name: &str, blob: PipeBlob) -> DevResult<PipeValue> {
        call!(self, Op::PipeWriteRead { name: name.into(), blob }, Pipe)
    }

    /// Empty `names` returns every pipe.
    pub async fn get_pipe_config(&mut self, names: &[&str]) -> DevResult<Vec<PipeInfo>> {
        call!(self, Op::PipeGetConfig { names: strings(names) }, PipeConfigs)
    }

    pub async fn set_pipe_config(&mut self, configs: Vec<PipeInfo>) -> DevResult<Vec<PipeInfo>> {
        call!(self, Op::PipeSetConfig { configs }, PipeConfigs)
    }
}

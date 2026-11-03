//! The browser window (BROWSER.md §1–§4): the Database tree and the hosts on the left, the device
//! or host panel in the middle, errors at the bottom.
//!
//! Network calls never block the UI: they run on the app's tokio runtime and send back a closure
//! that updates the UI state, applied at the next frame.

use crate::form::{self, Input};
use eframe::egui::{self, Color32, RichText, Ui};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use trust::*;

type Update = Box<dyn FnOnce(&mut Browser) + Send>;
type Proxy = Arc<Mutex<DeviceProxy>>;

/// Sends updates to the UI and wakes it up.
#[derive(Clone)]
struct Notifier {
    tx: mpsc::Sender<Update>,
    ctx: Arc<std::sync::Mutex<Option<egui::Context>>>,
}

impl Notifier {
    fn send(&self, update: impl FnOnce(&mut Browser) + Send + 'static) {
        let _ = self.tx.send(Box::new(update));
        if let Some(ctx) = self.ctx.lock().unwrap_or_else(|p| p.into_inner()).as_ref() {
            ctx.request_repaint();
        }
    }
}

/// Values refresh this often when they have no CHANGE events.
const REFRESH: Duration = Duration::from_secs(1);

pub struct Browser {
    rt: tokio::runtime::Runtime,
    notifier: Notifier,
    rx: mpsc::Receiver<Update>,
    tango_host: Option<String>,
    filter: String,
    url: String,
    devices: Vec<String>,
    exported: HashSet<String>,
    hosts: Vec<(String, Option<i16>)>,
    view: View,
    log: Vec<String>,
    loaded: bool,
}

enum View {
    Empty,
    Device(Box<DevicePanel>),
    Host(Box<HostPanel>),
}

#[derive(Clone, Copy, PartialEq)]
enum Tab {
    Attributes,
    Commands,
    Pipes,
    Properties,
    Admin,
}

/// Aborts the background tasks of a panel when it closes.
#[derive(Default)]
struct Tasks(Vec<tokio::task::AbortHandle>);

impl Drop for Tasks {
    fn drop(&mut self) {
        self.0.iter().for_each(tokio::task::AbortHandle::abort);
    }
}

struct DevicePanel {
    name: String,
    url: String,
    proxy: Option<Proxy>,
    error: Option<String>,
    tab: Tab,
    class: String,
    state: Option<DevState>,
    status: String,
    attrs: Vec<AttrInfo>,
    values: BTreeMap<String, Result<AttrValue, String>>,
    writes: HashMap<String, String>,
    /// Attributes refreshed by CHANGE events.
    evented: HashSet<String>,
    cmds: Vec<CmdInfo>,
    args: HashMap<String, String>,
    results: HashMap<String, Result<String, String>>,
    pipes: Vec<PipeInfo>,
    pipe_text: HashMap<String, String>,
    pipe_values: HashMap<String, Result<PipeValue, String>>,
    /// Device properties: name → one value per line.
    props: BTreeMap<String, String>,
    /// From QueryWizardDevProperty: name → (description, default).
    wizard: BTreeMap<String, (String, String)>,
    new_prop: (String, String),
    admin: Vec<String>,
    poll: (String, String),
    log_level: i32,
    config: Option<AttrInfo>,
    confirm: Option<(String, Value)>,
    refreshed: Option<Instant>,
    busy: bool,
    tasks: Tasks,
}

/// One line of the Starter's `Servers` attribute.
#[derive(Clone, Debug)]
struct ServerLine {
    name: String,
    state: String,
    controlled: bool,
    level: i16,
}

struct HostPanel {
    host: String,
    proxy: Option<Proxy>,
    error: Option<String>,
    servers: Vec<ServerLine>,
    devices: HashMap<String, Vec<String>>,
    log: Option<(String, String)>,
    register: Option<Register>,
    refreshed: Option<Instant>,
    evented: bool,
    busy: bool,
    tasks: Tasks,
}

#[derive(Default)]
struct Register {
    server: String,
    class: String,
    devices: String,
    level: String,
    controlled: bool,
}

/// `*` matches any text; without `*`, a substring match. Case-insensitive.
pub fn matches(pattern: &str, name: &str) -> bool {
    let (p, n) = (pattern.to_lowercase(), name.to_lowercase());
    if !p.contains('*') {
        return n.contains(&p);
    }
    let parts: Vec<&str> = p.split('*').collect();
    let (first, last) = (parts[0], parts[parts.len() - 1]);
    if !n.starts_with(first) || !n[first.len()..].ends_with(last) {
        return false;
    }
    let mut rest = &n[first.len()..n.len() - last.len()];
    for part in &parts[1..parts.len() - 1] {
        match rest.find(part) {
            Some(i) => rest = &rest[i + part.len()..],
            None => return false,
        }
    }
    true
}

/// The device name of a URL: its last three segments.
fn device_of(url: &str) -> String {
    let url = url.split('#').next().unwrap_or(url);
    let parts: Vec<&str> = url.rsplitn(4, '/').collect();
    match parts.as_slice() {
        [m, f, d, ..] => format!("{d}/{f}/{m}"),
        _ => url.to_string(),
    }
}

fn err(e: DevFailed) -> String {
    e.to_string().trim_end().to_string()
}

impl Browser {
    /// A browser on the Database at `tango_host` (`None`: direct URLs only).
    pub fn new(tango_host: Option<String>) -> Self {
        let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().expect("a runtime");
        let (tx, rx) = mpsc::channel();
        let notifier = Notifier { tx, ctx: Arc::default() };
        Self {
            rt,
            notifier,
            rx,
            tango_host,
            filter: String::new(),
            url: String::new(),
            devices: vec![],
            exported: HashSet::new(),
            hosts: vec![],
            view: View::Empty,
            log: vec![],
            loaded: false,
        }
    }

    /// Runs `f` in the background; its result updates the UI.
    fn spawn<F, U>(&self, f: F) -> tokio::task::AbortHandle
    where
        F: Future<Output = Box<U>> + Send + 'static,
        U: FnOnce(&mut Browser) + Send + 'static,
    {
        let notifier = self.notifier.clone();
        self.rt.spawn(async move { notifier.send(*f.await) }).abort_handle()
    }

    fn report(&mut self, message: String) {
        self.log.push(message);
        let n = self.log.len();
        if n > 200 {
            self.log.drain(..n - 200);
        }
    }

    fn db_url(&self, device: &str) -> Option<String> {
        self.tango_host.as_ref().map(|h| format!("tango://{h}/{device}"))
    }

    /// Re-reads the device tree and the hosts.
    fn load(&mut self) {
        let Some(host) = self.tango_host.clone() else { return };
        self.spawn(async move {
            let result = async {
                let db = Database::connect(&host).await?;
                let list = |cmd: &'static str| {
                    let db = db.clone();
                    async move {
                        match db.command(cmd, Value::String("*".into())).await? {
                            Value::StringArray(v) => Ok(v),
                            other => Err(DevFailed::new("API_BadReply", format!("{other:?}"), cmd)),
                        }
                    }
                };
                let (devices, exported, hosts) = (
                    list("DbGetDeviceWideList").await?,
                    list("DbGetDeviceExportedList").await?,
                    list("DbGetHostList").await?,
                );
                let mut states = vec![];
                for h in hosts {
                    let url = format!("tango://{host}/tango/admin/{}", h.to_lowercase());
                    let state = async {
                        let mut starter = DeviceProxy::connect(&url).await?;
                        match starter.read_attribute("HostState").await?.value {
                            Value::Short(s) => Ok(s),
                            _ => Err(DevFailed::new("API_BadReply", "HostState", "Starter")),
                        }
                    };
                    let state = tokio::time::timeout(Duration::from_secs(2), state).await.ok().and_then(Result::ok);
                    states.push((h, state));
                }
                Ok::<_, DevFailed>((devices, exported, states))
            }
            .await;
            Box::new(move |b: &mut Browser| match result {
                Ok((devices, exported, hosts)) => {
                    b.devices = devices;
                    b.exported = exported.into_iter().map(|d| d.to_lowercase()).collect();
                    b.hosts = hosts;
                }
                Err(e) => b.report(format!("Database: {}", err(e))),
            })
        });
    }

    pub fn open_device(&mut self, name: &str, url: String) {
        let panel = DevicePanel {
            name: name.into(),
            url: url.clone(),
            proxy: None,
            error: None,
            tab: Tab::Attributes,
            class: String::new(),
            state: None,
            status: String::new(),
            attrs: vec![],
            values: BTreeMap::new(),
            writes: HashMap::new(),
            evented: HashSet::new(),
            cmds: vec![],
            args: HashMap::new(),
            results: HashMap::new(),
            pipes: vec![],
            pipe_text: HashMap::new(),
            pipe_values: HashMap::new(),
            props: BTreeMap::new(),
            wizard: BTreeMap::new(),
            new_prop: Default::default(),
            admin: vec![],
            poll: (String::new(), "1000".into()),
            log_level: 3,
            config: None,
            confirm: None,
            refreshed: None,
            busy: true,
            tasks: Tasks::default(),
        };
        self.view = View::Device(Box::new(panel));
        let name = name.to_string();
        self.spawn(async move {
            let opened = async {
                let mut p = DeviceProxy::connect(&url).await?;
                let info = p.info().await?;
                let cmds = p.command_list_query().await?;
                let attrs = p.get_attribute_config(&[]).await?;
                let pipes = p.get_pipe_config(&[]).await?;
                Ok::<_, DevFailed>((p, info, cmds, attrs, pipes))
            }
            .await;
            Box::new(move |b: &mut Browser| {
                let View::Device(d) = &mut b.view else { return };
                if d.name != name {
                    return;
                }
                d.busy = false;
                match opened {
                    Ok((p, info, cmds, attrs, pipes)) => {
                        d.proxy = Some(Arc::new(Mutex::new(p)));
                        d.class = info.dev_class;
                        d.cmds = cmds;
                        d.attrs = attrs.into_iter().filter(|a| a.name != "State" && a.name != "Status").collect();
                        d.pipes = pipes;
                        b.subscribe();
                    }
                    Err(e) => d.error = Some(err(e)),
                }
            })
        });
    }

    /// CHANGE events for the attributes that have them, on a connection of their own.
    fn subscribe(&mut self) {
        let View::Device(d) = &mut self.view else { return };
        let (url, name, attrs) =
            (d.url.clone(), d.name.clone(), d.attrs.iter().map(|a| a.name.clone()).collect::<Vec<_>>());
        let notifier = self.notifier.clone();
        let task = self.rt.spawn(async move {
            let Ok(mut events) = DeviceProxy::connect(&url).await else { return };
            let mut streams = tokio::task::JoinSet::new();
            for attr in attrs {
                let Ok(mut stream) = events.event_stream(&attr, EventType::Change).await else { continue };
                let (notifier, name) = (notifier.clone(), name.clone());
                notifier.send({
                    let (name, attr) = (name.clone(), attr.clone());
                    move |b| {
                        if let View::Device(d) = &mut b.view
                            && d.name == name
                        {
                            d.evented.insert(attr);
                        }
                    }
                });
                streams.spawn(async move {
                    while let Some(e) = stream.recv().await {
                        let value = match e.data {
                            Ok(EventData::Attr(v)) => Ok(v),
                            Ok(_) => continue,
                            Err(e) => Err(err(e)),
                        };
                        let (name, attr) = (name.clone(), attr.clone());
                        notifier.send(move |b| {
                            if let View::Device(d) = &mut b.view
                                && d.name == name
                            {
                                d.values.insert(attr, value);
                            }
                        });
                    }
                });
            }
            while streams.join_next().await.is_some() {}
        });
        if let View::Device(d) = &mut self.view {
            d.tasks.0.push(task.abort_handle());
        }
    }

    /// Reads State, Status and the attributes without events.
    fn refresh_device(&mut self) {
        let View::Device(d) = &mut self.view else { return };
        let Some(proxy) = d.proxy.clone() else { return };
        if d.busy || d.refreshed.is_some_and(|t| t.elapsed() < REFRESH) {
            return;
        }
        d.busy = true;
        d.refreshed = Some(Instant::now());
        let names: Vec<String> = d.attrs.iter().map(|a| a.name.clone()).filter(|a| !d.evented.contains(a)).collect();
        let name = d.name.clone();
        self.spawn(async move {
            let mut p = proxy.lock().await;
            let state = p.command_inout("State", Value::Void).await;
            let status = p.command_inout("Status", Value::Void).await;
            let refs: Vec<&str> = names.iter().map(String::as_str).collect();
            let values = if refs.is_empty() { Ok(vec![]) } else { p.read_attributes(&refs).await };
            Box::new(move |b: &mut Browser| {
                let View::Device(d) = &mut b.view else { return };
                if d.name != name {
                    return;
                }
                d.busy = false;
                match (state, status) {
                    (Ok(Value::State(s)), Ok(Value::String(t))) => (d.state, d.status, d.error) = (Some(s), t, None),
                    (Err(e), _) | (_, Err(e)) => d.error = Some(err(e)),
                    _ => {}
                }
                match values {
                    Ok(values) => {
                        for (n, v) in names.into_iter().zip(values) {
                            d.values.insert(n, v.map_err(err));
                        }
                    }
                    Err(e) => d.error = Some(err(e)),
                }
            })
        });
    }

    /// Runs `call` on the device proxy, then stores its result under `key`.
    fn device_call<F, Fut>(&mut self, key: String, call: F)
    where
        F: FnOnce(Proxy) -> Fut + Send + 'static,
        Fut: Future<Output = DevResult<String>> + Send,
    {
        let View::Device(d) = &mut self.view else { return };
        let Some(proxy) = d.proxy.clone() else { return };
        d.results.insert(key.clone(), Ok("...".into()));
        d.refreshed = None; // re-read after the call
        let name = d.name.clone();
        self.spawn(async move {
            let result = call(proxy).await.map_err(err);
            Box::new(move |b: &mut Browser| {
                if let Err(e) = &result {
                    b.report(format!("{name} {key}: {e}"));
                }
                if let View::Device(d) = &mut b.view
                    && d.name == name
                {
                    d.results.insert(key, result);
                }
            })
        });
    }

    /// Runs a command on another device (Database, admin device, Starter) and stores its result
    /// as text under `key` of the current panel.
    fn remote<F>(&mut self, url: String, key: String, then: F, cmd: &'static str, arg: Value)
    where
        F: FnOnce(&mut Browser, &Value) + Send + 'static,
    {
        self.spawn(async move {
            let result = async { DeviceProxy::connect(&url).await?.command_inout(cmd, arg).await }.await;
            Box::new(move |b: &mut Browser| match result {
                Ok(v) => {
                    then(b, &v);
                    let text = form::display(&v, "", "", &[]);
                    match &mut b.view {
                        View::Device(d) => {
                            let _ = d.results.insert(key, Ok(text));
                        }
                        View::Host(h) => h.error = None,
                        View::Empty => {}
                    }
                }
                Err(e) => {
                    let e = err(e);
                    b.report(format!("{cmd}: {e}"));
                    match &mut b.view {
                        View::Device(d) => {
                            let _ = d.results.insert(key, Err(e));
                        }
                        View::Host(h) => h.error = Some(e),
                        View::Empty => {}
                    }
                }
            })
        });
    }

    fn load_properties(&mut self) {
        let View::Device(d) = &self.view else { return };
        let (Some(db), name, class, url) =
            (self.db_url(DATABASE_DEVICE), d.name.clone(), d.class.clone(), d.url.clone())
        else {
            return;
        };
        let Some(proxy) = d.proxy.clone() else { return };
        self.spawn(async move {
            let result = async {
                let mut db = DeviceProxy::connect(&db).await?;
                let arg = Value::StringArray(vec![name.clone(), "*".into()]);
                let Value::StringArray(names) = db.command_inout("DbGetDevicePropertyList", arg).await? else {
                    return Ok((BTreeMap::new(), BTreeMap::new()));
                };
                let mut props = BTreeMap::new();
                if !names.is_empty() {
                    let arg = Value::StringArray([vec![name.clone()], names].concat());
                    if let Value::StringArray(l) = db.command_inout("DbGetDeviceProperty", arg).await? {
                        // [device, n, (name, count, values...)...]
                        let mut at = 2;
                        while at + 1 < l.len() {
                            let n: usize = l[at + 1].parse().unwrap_or(0);
                            props.insert(l[at].clone(), l[at + 2..(at + 2 + n).min(l.len())].join("\n"));
                            at += 2 + n;
                        }
                    }
                }
                let adm = proxy.lock().await.adm_name().await?;
                let url = url.replace(&name, &adm);
                let mut wizard = BTreeMap::new();
                if let Ok(mut admin) = DeviceProxy::connect(&url).await
                    && let Ok(Value::StringArray(w)) =
                        admin.command_inout("QueryWizardDevProperty", Value::String(class)).await
                {
                    for t in w.chunks(3) {
                        if let [n, desc, default] = t {
                            wizard.insert(n.clone(), (desc.clone(), default.clone()));
                        }
                    }
                }
                Ok::<_, DevFailed>((props, wizard))
            }
            .await;
            Box::new(move |b: &mut Browser| match result {
                Ok((props, wizard)) => {
                    if let View::Device(d) = &mut b.view {
                        (d.props, d.wizard) = (props, wizard);
                    }
                }
                Err(e) => b.report(format!("properties: {}", err(e))),
            })
        });
    }

    pub fn open_host(&mut self, host: &str) {
        let Some(url) = self.db_url(&format!("tango/admin/{}", host.to_lowercase())) else { return };
        self.view = View::Host(Box::new(HostPanel {
            host: host.into(),
            proxy: None,
            error: None,
            servers: vec![],
            devices: HashMap::new(),
            log: None,
            register: None,
            refreshed: None,
            evented: false,
            busy: true,
            tasks: Tasks::default(),
        }));
        let host = host.to_string();
        let notifier = self.notifier.clone();
        let task = self.rt.spawn(async move {
            let proxy = async {
                let mut p = DeviceProxy::connect(&url).await?;
                p.ping().await?;
                Ok::<_, DevFailed>(p)
            }
            .await;
            let proxy = match proxy {
                Ok(p) => p,
                Err(e) => {
                    let e = format!("no Starter: {}", err(e));
                    return notifier.send(move |b| {
                        if let View::Host(h) = &mut b.view {
                            (h.error, h.busy) = (Some(e), false);
                        }
                    });
                }
            };
            let stream = DeviceProxy::connect(&url).await;
            let proxy = Arc::new(Mutex::new(proxy));
            notifier.send({
                let (proxy, host) = (proxy.clone(), host.clone());
                move |b| {
                    if let View::Host(h) = &mut b.view
                        && h.host == host
                    {
                        (h.proxy, h.busy) = (Some(proxy), false);
                    }
                }
            });
            // the Servers attribute, refreshed by its CHANGE events
            let Ok(mut stream) = stream else { return };
            let Ok(mut events) = stream.event_stream("Servers", EventType::Change).await else { return };
            notifier.send({
                let host = host.clone();
                move |b| {
                    if let View::Host(h) = &mut b.view
                        && h.host == host
                    {
                        h.evented = true;
                    }
                }
            });
            while let Some(e) = events.recv().await {
                if let Ok(EventData::Attr(v)) = e.data {
                    let (lines, host) = (servers(&v.value), host.clone());
                    notifier.send(move |b| {
                        if let View::Host(h) = &mut b.view
                            && h.host == host
                        {
                            h.servers = lines;
                        }
                    });
                }
            }
        });
        if let View::Host(h) = &mut self.view {
            h.tasks.0.push(task.abort_handle());
        }
    }

    /// Reads `Servers` when it has no events.
    fn refresh_host(&mut self) {
        let View::Host(h) = &mut self.view else { return };
        let Some(proxy) = h.proxy.clone() else { return };
        if h.busy || h.evented || h.refreshed.is_some_and(|t| t.elapsed() < REFRESH) {
            return;
        }
        (h.busy, h.refreshed) = (true, Some(Instant::now()));
        let host = h.host.clone();
        self.spawn(async move {
            let value = proxy.lock().await.read_attribute("Servers").await;
            Box::new(move |b: &mut Browser| {
                if let View::Host(h) = &mut b.view
                    && h.host == host
                {
                    h.busy = false;
                    match value {
                        Ok(v) => h.servers = servers(&v.value),
                        Err(e) => h.error = Some(err(e)),
                    }
                }
            })
        });
    }

    fn starter_command(&mut self, cmd: &'static str, arg: Value) {
        let View::Host(h) = &mut self.view else { return };
        let Some(proxy) = h.proxy.clone() else { return };
        let host = h.host.clone();
        h.refreshed = None;
        self.spawn(async move {
            let result = proxy.lock().await.command_inout(cmd, arg).await;
            Box::new(move |b: &mut Browser| {
                let View::Host(h) = &mut b.view else { return };
                if h.host != host {
                    return;
                }
                match result {
                    Ok(Value::String(log)) if cmd == "DevReadLog" => {
                        h.log = Some((cmd.into(), log));
                    }
                    Ok(_) => h.error = None,
                    Err(e) => {
                        let e = err(e);
                        h.error = Some(e.clone());
                        b.report(format!("{cmd}: {e}"));
                    }
                }
            })
        });
    }

    /// The whole window.
    pub fn show(&mut self, ui: &mut Ui) {
        let ctx = ui.ctx().clone();
        *self.notifier.ctx.lock().unwrap_or_else(|p| p.into_inner()) = Some(ctx.clone());
        while let Ok(update) = self.rx.try_recv() {
            update(self);
        }
        if !self.loaded {
            self.loaded = true;
            self.load();
        }
        self.refresh_device();
        self.refresh_host();
        ctx.request_repaint_after(REFRESH);

        egui::Panel::left("tree").resizable(true).default_size(260.0).show(ui, |ui| self.tree(ui));
        egui::Panel::bottom("log").resizable(true).default_size(90.0).show(ui, |ui| {
            egui::ScrollArea::vertical().stick_to_bottom(true).show(ui, |ui| {
                for line in &self.log {
                    ui.label(RichText::new(line).small().color(Color32::LIGHT_RED));
                }
            });
        });
        egui::CentralPanel::default().show(ui, |ui| match &self.view {
            View::Empty => {
                ui.label("Open a device from the tree, or a URL.");
            }
            View::Device(_) => self.device_ui(ui),
            View::Host(_) => self.host_ui(ui),
        });
    }

    fn tree(&mut self, ui: &mut Ui) {
        ui.horizontal(|ui| {
            ui.text_edit_singleline(&mut self.url).accessible_name("device URL");
            if ui.button("Open").clicked() && !self.url.is_empty() {
                let url = self.url.clone();
                let name = device_of(&url);
                self.open_device(&name, url);
            }
        });
        if self.tango_host.is_none() {
            return;
        }
        ui.horizontal(|ui| {
            ui.text_edit_singleline(&mut self.filter).accessible_name("filter");
            if ui.button("Reload").on_hover_text("reload the Database").clicked() {
                self.load();
            }
        });
        let mut open = None;
        egui::ScrollArea::vertical().show(ui, |ui| {
            ui.heading("Database");
            let mut tree: BTreeMap<&str, BTreeMap<&str, Vec<&str>>> = BTreeMap::new();
            for d in self.devices.iter().filter(|d| self.filter.is_empty() || matches(&self.filter, d)) {
                let mut parts = d.splitn(3, '/');
                if let (Some(a), Some(b), Some(c)) = (parts.next(), parts.next(), parts.next()) {
                    tree.entry(a).or_default().entry(b).or_default().push(c);
                }
            }
            let filtering = !self.filter.is_empty();
            for (domain, families) in tree {
                egui::CollapsingHeader::new(domain).default_open(filtering).show(ui, |ui| {
                    for (family, members) in families {
                        egui::CollapsingHeader::new(family).id_salt((domain, family)).default_open(filtering).show(
                            ui,
                            |ui| {
                                for member in members {
                                    let name = format!("{domain}/{family}/{member}");
                                    let on = self.exported.contains(&name.to_lowercase());
                                    ui.horizontal(|ui| {
                                        dot(ui, if on { Color32::GREEN } else { Color32::GRAY });
                                        let selected = matches!(&self.view, View::Device(d) if d.name == name);
                                        if ui.selectable_label(selected, member).on_hover_text(&name).clicked() {
                                            open = Some(name);
                                        }
                                    });
                                }
                            },
                        );
                    }
                });
            }
            ui.separator();
            ui.heading("Hosts");
            let mut host = None;
            for (h, state) in &self.hosts {
                ui.horizontal(|ui| {
                    let (colour, tip) = match state {
                        Some(0) => (Color32::GREEN, "ON"),
                        Some(1) => (Color32::LIGHT_BLUE, "MOVING"),
                        Some(_) => (Color32::from_rgb(255, 140, 0), "ALARM"),
                        None => (Color32::GRAY, "no Starter"),
                    };
                    dot(ui, colour).on_hover_text(tip);
                    if ui.selectable_label(false, h).clicked() {
                        host = Some(h.clone());
                    }
                });
            }
            if let Some(h) = host {
                self.open_host(&h);
            }
        });
        if let Some(name) = open
            && let Some(url) = self.db_url(&name)
        {
            self.open_device(&name, url);
        }
    }

    fn device_ui(&mut self, ui: &mut Ui) {
        let View::Device(d) = &mut self.view else { return };
        ui.horizontal(|ui| {
            ui.heading(&d.name);
            ui.label(format!("[{}]", d.class));
            if let Some(s) = d.state {
                dot(ui, form::state_color(s));
                ui.label(s.to_string());
            }
            ui.label(&d.status);
        });
        if let Some(e) = &d.error {
            ui.colored_label(Color32::LIGHT_RED, e);
        }
        ui.horizontal(|ui| {
            for (tab, label) in [
                (Tab::Attributes, "Attributes"),
                (Tab::Commands, "Commands"),
                (Tab::Pipes, "Pipes"),
                (Tab::Properties, "Properties"),
                (Tab::Admin, "Admin"),
            ] {
                ui.selectable_value(&mut d.tab, tab, label);
            }
        });
        ui.separator();
        let tab = d.tab;
        egui::ScrollArea::vertical().show(ui, |ui| match tab {
            Tab::Attributes => self.attributes_ui(ui),
            Tab::Commands => self.commands_ui(ui),
            Tab::Pipes => self.pipes_ui(ui),
            Tab::Properties => self.properties_ui(ui),
            Tab::Admin => self.admin_ui(ui),
        });
        self.dialogs(ui.ctx());
    }

    fn attributes_ui(&mut self, ui: &mut Ui) {
        let View::Device(d) = &mut self.view else { return };
        let mut write = None;
        egui::Grid::new("attributes").striped(true).show(ui, |ui| {
            for h in ["name", "value", "set value", "quality", "write", ""] {
                ui.strong(h);
            }
            ui.end_row();
            for a in &d.attrs {
                let tip = format!("{}\n{}", a.label, a.description);
                ui.label(&a.name).on_hover_text(tip);
                let value = d.values.get(&a.name);
                match value {
                    Some(Ok(v)) if a.data_format == DataFormat::Image => {
                        ui.label(format!("{}x{} image", v.dim_x, v.dim_y));
                    }
                    Some(Ok(v)) => {
                        let text = form::display(&v.value, &a.format, &a.unit, &a.enum_labels);
                        let numbers = v.value.numbers();
                        if a.data_format == DataFormat::Spectrum && !numbers.is_empty() {
                            ui.vertical(|ui| {
                                ui.label(text);
                                let points: egui_plot::PlotPoints =
                                    numbers.iter().enumerate().map(|(i, y)| [i as f64, *y]).collect();
                                egui_plot::Plot::new(("plot", &a.name)).height(80.0).width(240.0).show(ui, |p| {
                                    p.line(egui_plot::Line::new(&a.name, points));
                                });
                            });
                        } else {
                            ui.label(text);
                        }
                    }
                    Some(Err(e)) => {
                        ui.colored_label(Color32::GRAY, e.lines().next().unwrap_or_default());
                    }
                    None => {
                        ui.label("...");
                    }
                }
                let w = value.and_then(|v| v.as_ref().ok()).and_then(|v| v.w_value.as_ref());
                ui.label(w.map(|w| form::display(w, &a.format, &a.unit, &a.enum_labels)).unwrap_or_default());
                match value {
                    Some(Ok(v)) => {
                        ui.label(RichText::new(format!("{:?}", v.quality)).color(form::quality_color(v.quality)));
                    }
                    _ => {
                        ui.label("");
                    }
                }
                let writable = matches!(a.writable, Writable::Write | Writable::ReadWrite);
                let t = if a.data_format == DataFormat::Scalar { a.data_type } else { a.data_type.array() };
                let input = form::input(a.data_type, a.data_format, (a.min_value, a.max_value), &a.enum_labels);
                if writable && input != Input::ReadOnly {
                    let text = d.writes.entry(a.name.clone()).or_default();
                    if input_widget(ui, &input, text, &format!("write {}", a.name)) {
                        write =
                            Some((a.name.clone(), t, text.clone(), a.min_value, a.max_value, a.min_alarm, a.max_alarm));
                    }
                } else {
                    ui.label("");
                }
                if ui.small_button("Config").on_hover_text("configuration").clicked() {
                    d.config = Some(a.clone());
                }
                ui.end_row();
            }
        });
        if let Some((name, t, text, min, max, min_alarm, max_alarm)) = write {
            match form::parse(t, &text, min, max) {
                Ok(v) if v.outside(min_alarm, max_alarm) => d.confirm = Some((name, v)),
                Ok(v) => self.write(name, v),
                Err(e) => self.report(format!("{name}: {e}")),
            }
        }
    }

    fn write(&mut self, attr: String, value: Value) {
        self.device_call(format!("write {attr}"), move |p| async move {
            p.lock().await.write_attribute(&attr, value).await.map(|_| String::new())
        });
    }

    fn dialogs(&mut self, ctx: &egui::Context) {
        let View::Device(d) = &mut self.view else { return };
        let mut apply = None;
        if let Some(c) = &mut d.config {
            let mut open = true;
            egui::Window::new(format!("Configuration of {}", c.name)).open(&mut open).show(ctx, |ui| {
                egui::Grid::new("config").show(ui, |ui| {
                    for (label, text) in [("label", &mut c.label), ("unit", &mut c.unit), ("format", &mut c.format)] {
                        ui.label(label);
                        ui.text_edit_singleline(text).accessible_name(label);
                        ui.end_row();
                    }
                    for (label, v) in [
                        ("min_value", &mut c.min_value),
                        ("max_value", &mut c.max_value),
                        ("min_alarm", &mut c.min_alarm),
                        ("max_alarm", &mut c.max_alarm),
                        ("abs_change", &mut c.abs_change),
                        ("rel_change", &mut c.rel_change),
                    ] {
                        ui.label(label);
                        let mut text = v.map(|x| x.to_string()).unwrap_or_default();
                        if ui.text_edit_singleline(&mut text).accessible_name(label).changed() {
                            *v = text.trim().parse().ok();
                        }
                        ui.end_row();
                    }
                });
                if ui.button("Apply").clicked() {
                    apply = Some(c.clone());
                }
            });
            if !open || apply.is_some() {
                d.config = None;
            }
        }
        let mut confirmed = None;
        if let Some((attr, v)) = &d.confirm {
            egui::Window::new("Confirm").collapsible(false).show(ctx, |ui| {
                ui.label(format!("{attr}: {} is outside the alarm limits.", form::edit_text(v)));
                ui.horizontal(|ui| {
                    if ui.button("Write anyway").clicked() {
                        confirmed = Some(true);
                    }
                    if ui.button("Cancel").clicked() {
                        confirmed = Some(false);
                    }
                });
            });
        }
        if let Some(yes) = confirmed
            && let Some((attr, v)) = d.confirm.take()
            && yes
        {
            self.write(attr, v);
        }
        if let Some(c) = apply {
            let name = c.name.clone();
            self.device_call(format!("config {name}"), move |p| async move {
                p.lock().await.set_attribute_config(vec![c]).await.map(|_| String::new())
            });
            // the configuration is re-read with the interface
            if let View::Device(d) = &self.view {
                let (name, url) = (d.name.clone(), d.url.clone());
                self.open_device(&name, url);
            }
        }
    }

    fn commands_ui(&mut self, ui: &mut Ui) {
        let View::Device(d) = &mut self.view else { return };
        let mut run = None;
        egui::Grid::new("commands").striped(true).show(ui, |ui| {
            for c in &d.cmds {
                let input = form::input(c.in_type, DataFormat::Scalar, (None, None), &[]);
                let text = d.args.entry(c.name.clone()).or_default();
                let clicked = ui.button(&c.name).on_hover_text(format!("{}\n-> {}", c.in_desc, c.out_desc)).clicked();
                ui.label(format!("{:?} -> {:?}", c.in_type, c.out_type));
                let entered = input_widget(ui, &input, text, &format!("argument of {}", c.name));
                if clicked || entered {
                    run = Some((c.name.clone(), c.in_type, text.clone()));
                }
                match d.results.get(&format!("command {}", c.name)) {
                    Some(Ok(r)) => {
                        ui.label(r);
                    }
                    Some(Err(e)) => {
                        ui.colored_label(Color32::LIGHT_RED, e);
                    }
                    None => {
                        ui.label("");
                    }
                }
                ui.end_row();
            }
        });
        if let Some((name, t, text)) = run {
            match form::parse(t, &text, None, None) {
                Ok(arg) => self.device_call(format!("command {name}"), move |p| async move {
                    let out = p.lock().await.command_inout(&name, arg).await?;
                    Ok(form::display(&out, "", "", &[]))
                }),
                Err(e) => {
                    if let View::Device(d) = &mut self.view {
                        d.results.insert(format!("command {name}"), Err(e));
                    }
                }
            }
        }
    }

    fn pipes_ui(&mut self, ui: &mut Ui) {
        let View::Device(d) = &mut self.view else { return };
        let (mut read, mut write) = (None, None);
        for p in &d.pipes {
            ui.horizontal(|ui| {
                ui.strong(&p.name).on_hover_text(&p.description);
                if ui.button(format!("Read {}", p.name)).clicked() {
                    read = Some(p.name.clone());
                }
            });
            match d.pipe_values.get(&p.name) {
                Some(Ok(v)) => blob_ui(ui, &v.blob),
                Some(Err(e)) => {
                    ui.colored_label(Color32::LIGHT_RED, e);
                }
                None => {}
            }
            if p.writable {
                let text = d.pipe_text.entry(p.name.clone()).or_default();
                ui.add(egui::TextEdit::multiline(text).desired_rows(3).code_editor())
                    .accessible_name(format!("blob of {}", p.name));
                if ui.button(format!("Write {}", p.name)).clicked() {
                    write = Some((p.name.clone(), text.clone()));
                }
            }
            ui.separator();
        }
        let name = d.name.clone();
        if let Some(pipe) = read
            && let Some(proxy) = d.proxy.clone()
        {
            self.spawn(async move {
                let v = proxy.lock().await.read_pipe(&pipe).await.map_err(err);
                Box::new(move |b: &mut Browser| {
                    if let View::Device(d) = &mut b.view
                        && d.name == name
                    {
                        if let Ok(v) = &v {
                            d.pipe_text.insert(pipe.clone(), serde_json::to_string_pretty(&v.blob).unwrap_or_default());
                        }
                        d.pipe_values.insert(pipe, v);
                    }
                })
            });
        } else if let Some((pipe, text)) = write {
            match serde_json::from_str::<PipeBlob>(&text) {
                Ok(blob) => self.device_call(format!("pipe {pipe}"), move |p| async move {
                    p.lock().await.write_pipe(&pipe, blob).await.map(|_| String::new())
                }),
                Err(e) => self.report(format!("{pipe}: {e}")),
            }
        }
    }

    fn properties_ui(&mut self, ui: &mut Ui) {
        if self.tango_host.is_none() {
            ui.label("Properties need a Database.");
            return;
        }
        if ui.button("Reload properties").clicked() {
            self.load_properties();
        }
        let View::Device(d) = &mut self.view else { return };
        let Some(db) = self.tango_host.as_ref().map(|h| format!("tango://{h}/{DATABASE_DEVICE}")) else { return };
        let mut put = None;
        let mut delete = None;
        egui::Grid::new("properties").striped(true).show(ui, |ui| {
            let mut names: Vec<String> = d.props.keys().cloned().collect();
            names.extend(d.wizard.keys().filter(|n| !d.props.keys().any(|p| p.eq_ignore_ascii_case(n))).cloned());
            for name in names {
                let (desc, default) = d.wizard.get(&name).cloned().unwrap_or_default();
                ui.label(&name).on_hover_text(&desc);
                let set = d.props.contains_key(&name);
                let value = d.props.entry(name.clone()).or_default();
                ui.add(egui::TextEdit::multiline(value).desired_rows(1).hint_text(format!("default: {default}")))
                    .accessible_name(format!("property {name}"));
                if ui.button("Save").clicked() {
                    put = Some((name.clone(), value.clone()));
                }
                if set && ui.button("Delete").clicked() {
                    delete = Some(name.clone());
                }
                if !set {
                    d.props.remove(&name);
                }
                ui.end_row();
            }
            ui.text_edit_singleline(&mut d.new_prop.0).accessible_name("new property name");
            ui.text_edit_singleline(&mut d.new_prop.1).accessible_name("new property value");
            if ui.button("Add").clicked() && !d.new_prop.0.is_empty() {
                put = Some(std::mem::take(&mut d.new_prop));
            }
            ui.end_row();
        });
        let name = d.name.clone();
        let reload = |b: &mut Browser, _: &Value| b.load_properties();
        if let Some((prop, value)) = put {
            let values: Vec<String> = value.lines().map(String::from).collect();
            let arg = [vec![name, "1".into(), prop.clone(), values.len().to_string()], values].concat();
            self.remote(db, format!("property {prop}"), reload, "DbPutDeviceProperty", Value::StringArray(arg));
        } else if let Some(prop) = delete {
            let arg = vec![name, prop.clone()];
            self.remote(db, format!("property {prop}"), reload, "DbDeleteDeviceProperty", Value::StringArray(arg));
        }
    }

    fn admin_ui(&mut self, ui: &mut Ui) {
        let View::Device(d) = &mut self.view else { return };
        let mut action = None;
        ui.horizontal(|ui| {
            for (label, a) in [("Init", 0), ("Restart", 1), ("Black box", 2), ("Polling status", 3)] {
                if ui.button(label).clicked() {
                    action = Some(a);
                }
            }
        });
        ui.horizontal(|ui| {
            ui.text_edit_singleline(&mut d.poll.0).accessible_name("attribute to poll");
            ui.add(egui::TextEdit::singleline(&mut d.poll.1).desired_width(60.0))
                .accessible_name("polling period (ms)");
            if ui.button("Add polling").clicked() {
                action = Some(4);
            }
        });
        ui.horizontal(|ui| {
            egui::ComboBox::from_label("logging level")
                .selected_text(["OFF", "FATAL", "ERROR", "WARN", "INFO", "DEBUG"][d.log_level.clamp(0, 5) as usize])
                .show_ui(ui, |ui| {
                    for (i, l) in ["OFF", "FATAL", "ERROR", "WARN", "INFO", "DEBUG"].iter().enumerate() {
                        ui.selectable_value(&mut d.log_level, i as i32, *l);
                    }
                });
            if ui.button("Set logging level").clicked() {
                action = Some(5);
            }
        });
        for key in ["admin", "Init"] {
            match d.results.get(key) {
                Some(Ok(r)) => {
                    ui.label(r);
                }
                Some(Err(e)) => {
                    ui.colored_label(Color32::LIGHT_RED, e);
                }
                None => {}
            }
        }
        for line in &d.admin {
            ui.monospace(line);
        }
        let (name, level, poll) = (d.name.clone(), d.log_level, d.poll.clone());
        let Some(proxy) = d.proxy.clone() else { return };
        let url = d.url.clone();
        let Some(action) = action else { return };
        if action == 0 {
            return self.device_call("Init".into(), |p| async move {
                p.lock().await.command_inout("Init", Value::Void).await.map(|_| "done".into())
            });
        }
        self.spawn(async move {
            let result = async {
                let adm = proxy.lock().await.adm_name().await?;
                let mut admin = DeviceProxy::connect(&url.replace(&name, &adm)).await?;
                let lines = |v: Value| match v {
                    Value::StringArray(v) => v,
                    v => vec![form::display(&v, "", "", &[])],
                };
                Ok::<_, DevFailed>(match action {
                    1 => lines(admin.command_inout("DevRestart", Value::String(name.clone())).await?),
                    2 => {
                        let records = proxy.lock().await.black_box(20).await?;
                        records
                            .iter()
                            .map(|r| format!("{:.3} {} client {} {}", r.time, r.operation, r.client_id, r.host))
                            .collect()
                    }
                    3 => lines(admin.command_inout("DevPollStatus", Value::String(name.clone())).await?),
                    4 => {
                        let period = poll.1.trim().parse().unwrap_or(1000);
                        let arg = Value::LongStringArray(
                            vec![period],
                            vec![name.clone(), "attribute".into(), poll.0.clone()],
                        );
                        admin.command_inout("AddObjPolling", arg).await?;
                        lines(admin.command_inout("DevPollStatus", Value::String(name.clone())).await?)
                    }
                    _ => {
                        let arg = Value::LongStringArray(vec![level], vec![name.clone()]);
                        lines(admin.command_inout("SetLoggingLevel", arg).await?)
                    }
                })
            }
            .await;
            Box::new(move |b: &mut Browser| match result {
                Ok(lines) => {
                    if let View::Device(d) = &mut b.view {
                        d.admin = lines;
                        d.results.insert("admin".into(), Ok("done".into()));
                    }
                }
                Err(e) => {
                    let e = err(e);
                    b.report(format!("admin: {e}"));
                    if let View::Device(d) = &mut b.view {
                        d.results.insert("admin".into(), Err(e));
                    }
                }
            })
        });
    }

    fn host_ui(&mut self, ui: &mut Ui) {
        let View::Host(h) = &mut self.view else { return };
        ui.horizontal(|ui| {
            ui.heading(format!("Host {}", h.host));
            if ui.button("Register a server").clicked() {
                h.register = Some(Register { level: "1".into(), controlled: true, ..Default::default() });
            }
        });
        if let Some(e) = &h.error {
            ui.colored_label(Color32::LIGHT_RED, e);
        }
        let mut cmd: Option<(&'static str, Value)> = None;
        let mut open = None;
        let mut query = None;
        let mut levels: BTreeMap<i16, Vec<ServerLine>> = BTreeMap::new();
        for s in &h.servers {
            levels.entry(s.level).or_default().push(s.clone());
        }
        egui::ScrollArea::vertical().show(ui, |ui| {
            for (level, servers) in levels {
                ui.horizontal(|ui| {
                    ui.strong(if level == 0 {
                        "Level 0 (not started at startup)".to_string()
                    } else {
                        format!("Level {level}")
                    });
                    if ui.button(format!("Start level {level}")).clicked() {
                        cmd = Some(("DevStartAll", Value::Short(level)));
                    }
                    if ui.button(format!("Stop level {level}")).clicked() {
                        cmd = Some(("DevStopAll", Value::Short(level)));
                    }
                });
                for s in servers {
                    ui.horizontal(|ui| {
                        let (colour, state) = match (s.state.as_str(), s.controlled) {
                            ("ON", _) => (Color32::GREEN, "running"),
                            ("MOVING", _) => (Color32::LIGHT_BLUE, "starting"),
                            (_, true) => (Color32::RED, "stopped"),
                            (_, false) => (Color32::GRAY, "not controlled"),
                        };
                        dot(ui, colour);
                        ui.label(&s.name);
                        ui.label(state);
                        let arg = || Value::String(s.name.clone());
                        for (label, c) in [
                            ("Start", "DevStart"),
                            ("Stop", "DevStop"),
                            ("Kill", "HardKillServer"),
                            ("Log", "DevReadLog"),
                        ] {
                            if ui.button(label).clicked() {
                                cmd = Some((c, arg()));
                            }
                        }
                        if ui.button("Devices").clicked() {
                            query = Some(s.name.clone());
                        }
                    });
                    if let Some(devices) = h.devices.get(&s.name) {
                        ui.indent(&s.name, |ui| {
                            for d in devices {
                                // QueryDevice gives `Class::device`
                                let name = d.split_once("::").map_or(d.as_str(), |(_, n)| n);
                                if ui.link(d).clicked() {
                                    open = Some(name.to_string());
                                }
                            }
                        });
                    }
                }
                ui.separator();
            }
        });
        let mut close_log = false;
        if let Some((_, text)) = &h.log {
            let mut shown = true;
            egui::Window::new("Log").open(&mut shown).default_size([500.0, 300.0]).show(ui.ctx(), |ui| {
                egui::ScrollArea::vertical().show(ui, |ui| ui.monospace(text));
            });
            close_log = !shown;
        }
        if close_log {
            h.log = None;
        }
        let mut submit = None;
        if let Some(r) = &mut h.register {
            let mut shown = true;
            egui::Window::new("Register a server").open(&mut shown).show(ui.ctx(), |ui| {
                egui::Grid::new("register").show(ui, |ui| {
                    for (label, text) in [
                        ("server (Executable/instance)", &mut r.server),
                        ("class", &mut r.class),
                        ("devices (comma-separated)", &mut r.devices),
                        ("startup level", &mut r.level),
                    ] {
                        ui.label(label);
                        ui.text_edit_singleline(text).accessible_name(label);
                        ui.end_row();
                    }
                });
                ui.checkbox(&mut r.controlled, "controlled");
                if ui.button("Register").clicked() {
                    submit = Some(std::mem::take(r));
                }
            });
            if !shown || submit.is_some() {
                h.register = None;
            }
        }
        let host = h.host.clone();
        if let Some(c) = cmd {
            self.starter_command(c.0, c.1);
        }
        if let Some(name) = open
            && let Some(url) = self.db_url(&name)
        {
            return self.open_device(&name, url);
        }
        if let Some(server) = query
            && let Some(url) = self.db_url(&format!("dserver/{server}"))
        {
            let then = move |b: &mut Browser, v: &Value| {
                if let (View::Host(h), Value::StringArray(d)) = (&mut b.view, v) {
                    h.devices.insert(server, d.clone());
                }
            };
            self.remote(url, String::new(), then, "QueryDevice", Value::Void);
        }
        if let Some(r) = submit
            && let Some(db) = self.db_url(DATABASE_DEVICE)
        {
            let mut add = vec![r.server.clone()];
            for d in r.devices.split(',').map(str::trim).filter(|d| !d.is_empty()) {
                add.extend([d.to_string(), r.class.clone()]);
            }
            let info = vec![r.server, host, if r.controlled { "1" } else { "0" }.into(), r.level.trim().into()];
            let (db2, starter) = (db.clone(), self.db_url(&format!("tango/admin/{}", info[1].to_lowercase())));
            let then = move |b: &mut Browser, _: &Value| {
                let after = move |b: &mut Browser, _: &Value| {
                    if let Some(starter) = starter {
                        b.remote(starter, String::new(), |_, _| {}, "UpdateServersInfo", Value::Void);
                    }
                };
                b.remote(db2, String::new(), after, "DbPutServerInfo", Value::StringArray(info));
            };
            self.remote(db, String::new(), then, "DbAddServer", Value::StringArray(add));
        }
    }
}

/// A coloured status light.
fn dot(ui: &mut Ui, colour: Color32) -> egui::Response {
    let (rect, response) = ui.allocate_exact_size(egui::vec2(12.0, 12.0), egui::Sense::hover());
    ui.painter().circle_filled(rect.center(), 5.0, colour);
    response
}

/// Parses the Starter's `Servers` lines: `name\tstate\tcontrolled\tlevel\tnb_instances`.
fn servers(v: &Value) -> Vec<ServerLine> {
    let Value::StringArray(lines) = v else { return vec![] };
    lines
        .iter()
        .filter_map(|l| {
            let f: Vec<&str> = l.split('\t').collect();
            Some(ServerLine {
                name: f.first()?.to_string(),
                state: f.get(1)?.to_string(),
                controlled: *f.get(2)? == "1",
                level: f.get(3)?.parse().ok()?,
            })
        })
        .collect()
}

/// The input widget of `input` editing `text`; returns true when the user submits it (button or
/// Enter).
fn input_widget(ui: &mut Ui, input: &Input, text: &mut String, name: &str) -> bool {
    let mut submit = false;
    ui.horizontal(|ui| {
        match input {
            Input::None => {}
            Input::ReadOnly => {
                ui.label("-");
            }
            Input::Check => {
                let mut b = text == "true";
                if ui.checkbox(&mut b, "").accessible_name(name).changed() {
                    *text = b.to_string();
                }
            }
            Input::Integer { min, max } => {
                let mut x: f64 = text.parse().unwrap_or(0.0);
                ui.add(egui::DragValue::new(&mut x).range(*min..=*max).speed(1.0)).accessible_name(name);
                *text = (x.round() as i64).to_string();
            }
            Input::State | Input::Enum(_) => {
                let options: Vec<String> = match input {
                    Input::Enum(labels) => labels.clone(),
                    _ => [
                        "ON", "OFF", "CLOSE", "OPEN", "INSERT", "EXTRACT", "MOVING", "STANDBY", "FAULT", "INIT",
                        "RUNNING", "ALARM", "DISABLE", "UNKNOWN",
                    ]
                    .map(String::from)
                    .to_vec(),
                };
                let enum_index = |t: &str| t.parse::<usize>().ok().and_then(|i| options.get(i)).cloned();
                let shown =
                    if matches!(input, Input::Enum(_)) { enum_index(text).unwrap_or_default() } else { text.clone() };
                egui::ComboBox::from_id_salt(name).selected_text(shown).show_ui(ui, |ui| {
                    for (i, o) in options.iter().enumerate() {
                        let value = if matches!(input, Input::Enum(_)) { i.to_string() } else { o.clone() };
                        ui.selectable_value(text, value, o);
                    }
                });
            }
            Input::Float | Input::Text | Input::List | Input::Pair => {
                let hint = match input {
                    Input::List => "a, b, c",
                    Input::Pair => "1, 2 ; a, b",
                    _ => "",
                };
                let r =
                    ui.add(egui::TextEdit::singleline(text).hint_text(hint).desired_width(140.0)).accessible_name(name);
                submit = r.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter));
            }
        }
        if !matches!(input, Input::None | Input::ReadOnly) && ui.button("Set").on_hover_text(name).clicked() {
            submit = true;
        }
    });
    submit
}

fn blob_ui(ui: &mut Ui, blob: &PipeBlob) {
    egui::CollapsingHeader::new(&blob.name).default_open(true).show(ui, |ui| {
        for (key, value) in &blob.elements {
            match value {
                Value::PipeBlob(inner) => blob_ui(ui, inner),
                v => {
                    ui.label(format!("{key} = {}", form::display(v, "", "", &[])));
                }
            }
        }
    });
}

impl eframe::App for Browser {
    fn ui(&mut self, ui: &mut Ui, _frame: &mut eframe::Frame) {
        self.show(ui);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filter() {
        assert!(matches("tg", "sys/TG/1"), "substring, case-insensitive");
        assert!(matches("sys/*/1", "sys/tg/1"));
        assert!(matches("*tg*", "sys/tg/1"));
        assert!(matches("sys*", "sys/tg/1"));
        assert!(!matches("sys/*/2", "sys/tg/1"));
        assert!(!matches("tango*", "sys/tg/1"));
        assert!(matches("*", "anything"));
    }

    #[test]
    fn device_names() {
        assert_eq!(device_of("grpc://host:50051/sys/tg/1"), "sys/tg/1");
        assert_eq!(device_of("tango://host:10000/sys/tg/1#dbase=no"), "sys/tg/1");
    }

    #[test]
    fn server_lines() {
        let v = Value::StringArray(vec!["A/1\tON\t1\t2\t1".into(), "bad".into()]);
        let s = servers(&v);
        assert_eq!((s.len(), s[0].name.as_str(), s[0].controlled, s[0].level), (1, "A/1", true, 2));
    }
}

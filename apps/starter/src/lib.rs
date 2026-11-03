//! The Starter device `tango/admin/<host>` (SPEC §10): starts, stops and monitors the device
//! servers registered for its host in the Database.
//!
//! A server is *running* when its admin device answers a ping, *starting* from `DevStart` until
//! then (at most `ServerStartupTimeout`), *stopped* otherwise. A controlled server that stops is
//! reported (`HostState` ALARM), not restarted.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};
use trust::*;

/// The Starter server: `Starter/<host>` with the device `tango/admin/<host>`, on the Database
/// at `tango_host`, checking its servers every `period`.
pub fn server(tango_host: &str, host: &str, period: Duration) -> Server {
    Server::new("Starter", host).database(tango_host).device(&device_name(host), Starter::new(tango_host, host, period))
}

pub fn device_name(host: &str) -> String {
    format!("tango/admin/{}", host.to_lowercase())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Run {
    Stopped,
    Starting,
    Running,
}

impl Run {
    fn state(self) -> &'static str {
        match self {
            Run::Stopped => "FAULT",
            Run::Starting => "MOVING",
            Run::Running => "ON",
        }
    }
}

struct Srv {
    controlled: bool,
    level: i16,
    run: Run,
    /// The process, when started by this Starter.
    child: Option<Child>,
    started: Instant,
    /// Stopped on purpose (DevStop, HardKillServer): its end is not a crash.
    stopping: bool,
}

#[derive(Clone)]
struct Config {
    paths: Vec<PathBuf>,
    level_wait: Duration,
    startup_timeout: Duration,
    at_startup: bool,
    log_home: PathBuf,
    keep_logs: usize,
}

struct State {
    config: Config,
    servers: BTreeMap<String, Srv>,
    /// Re-read the Database at the next check.
    refresh: bool,
    /// `HostState` and `Servers` of the last events sent.
    published: Option<(i16, Vec<String>)>,
}

struct Inner {
    host: String,
    tango_host: String,
    period: Duration,
    state: Mutex<State>,
    sink: Mutex<Option<EventSink>>,
}

pub struct Starter {
    inner: Arc<Inner>,
    monitoring: bool,
}

fn fail(reason: &str, desc: impl Into<String>) -> DevFailed {
    DevFailed::new(reason, desc, "Starter")
}

impl Starter {
    pub fn new(tango_host: &str, host: &str, period: Duration) -> Self {
        let config = Config {
            paths: vec![],
            level_wait: Duration::from_secs(1),
            startup_timeout: Duration::from_secs(60),
            at_startup: true,
            log_home: std::env::temp_dir(),
            keep_logs: 3,
        };
        let state = State { config, servers: BTreeMap::new(), refresh: true, published: None };
        let inner = Inner {
            host: host.into(),
            tango_host: tango_host.into(),
            period,
            state: Mutex::new(state),
            sink: Mutex::new(None),
        };
        Self { inner: Arc::new(inner), monitoring: false }
    }
}

impl Inner {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Appends a line to the Starter's own log.
    fn note(&self, st: &State, message: &str) {
        let path = st.config.log_home.join(format!("Starter_{}.log", self.host));
        let file = std::fs::OpenOptions::new().create(true).append(true).open(path);
        let _ = file.and_then(|mut f| writeln!(f, "{:.3} {message}", now()));
    }

    /// The registered name of `name` (case-insensitive).
    fn find(st: &State, name: &str) -> Option<String> {
        st.servers.keys().find(|k| k.eq_ignore_ascii_case(name)).cloned()
    }

    /// DevStart: `<exec> <instance>` from `StartDsPath`, output to `LogFileHome/<exec>_<instance>.log`.
    fn start(&self, name: &str) -> DevResult<()> {
        let mut st = self.lock();
        let name = Self::find(&st, name).unwrap_or_else(|| name.to_string());
        let Some((exec, instance)) = name.split_once('/') else {
            return Err(fail("API_InvalidArgs", format!("{name}: expected <executable>/<instance>")));
        };
        if st.servers.get(&name).is_some_and(|s| s.run != Run::Stopped) {
            return Err(fail("API_ServerRunning", format!("{name} is already running")));
        }
        let exe = st.config.paths.iter().map(|p| p.join(exec)).find(|p| p.is_file());
        let exe = exe.ok_or_else(|| fail("API_ExecutableNotFound", format!("{exec} not found in StartDsPath")))?;
        let log = st.config.log_home.join(format!("{exec}_{instance}.log"));
        for i in (1..st.config.keep_logs).rev() {
            let _ =
                std::fs::rename(log.with_extension(format!("log.{i}")), log.with_extension(format!("log.{}", i + 1)));
        }
        if st.config.keep_logs > 0 {
            let _ = std::fs::rename(&log, log.with_extension("log.1"));
        }
        let out = File::create(&log)?;
        let child = Command::new(&exe)
            .arg(instance)
            .env("TANGO_HOST", &self.tango_host)
            .stdin(Stdio::null())
            .stdout(out.try_clone()?)
            .stderr(out)
            .spawn()?;
        self.note(&st, &format!("starting {name} (pid {})", child.id()));
        let srv = st.servers.entry(name).or_insert_with(|| Srv {
            controlled: false,
            level: 0,
            run: Run::Stopped,
            child: None,
            started: Instant::now(),
            stopping: false,
        });
        (srv.run, srv.child, srv.started, srv.stopping) = (Run::Starting, Some(child), Instant::now(), false);
        drop(st);
        self.publish();
        Ok(())
    }

    /// DevStop: `Kill` on the server's admin device.
    fn stop(&self, name: &str) -> DevResult<()> {
        let mut st = self.lock();
        let srv = Self::find(&st, name).and_then(|n| st.servers.get_mut(&n).map(|s| (n, s)));
        let Some((name, srv)) = srv.filter(|(_, s)| s.run == Run::Running) else {
            return Err(fail("API_ServerNotRunning", format!("{name} is not running")));
        };
        srv.stopping = true;
        self.note(&st, &format!("stopping {name}"));
        let url = format!("tango://{}/dserver/{name}", self.tango_host);
        tokio::spawn(async move {
            if let Ok(mut admin) = DeviceProxy::connect(&url).await {
                let _ = admin.command_inout("Kill", Value::Void).await;
            }
        });
        Ok(())
    }

    /// HardKillServer: SIGKILL to a process started by this Starter.
    fn kill(&self, name: &str) -> DevResult<()> {
        let mut st = self.lock();
        let srv = Self::find(&st, name).and_then(|n| st.servers.get_mut(&n).map(|s| (n, s)));
        let Some((name, srv)) = srv.filter(|(_, s)| s.child.is_some()) else {
            return Err(fail("API_ProcessNotFound", format!("{name} was not started by this Starter")));
        };
        srv.stopping = true;
        let mut child = srv.child.take().expect("checked");
        let _ = child.kill();
        let _ = child.wait(); // SIGKILL ends it at once: reap it here
        self.note(&st, &format!("killing {name}"));
        Ok(())
    }

    /// Starts the stopped controlled servers level by level (`only`: one level), each level once
    /// the previous one runs (or after `ServerStartupTimeout`), `InterStartupLevelWait` apart.
    async fn start_levels(self: Arc<Self>, only: Option<i16>) {
        let levels: BTreeSet<i16> = self
            .lock()
            .servers
            .values()
            .filter(|s| s.controlled && only.map_or(s.level > 0, |l| s.level == l))
            .map(|s| s.level)
            .collect();
        for level in levels {
            let (names, config) = {
                let st = self.lock();
                let names: Vec<String> = st
                    .servers
                    .iter()
                    .filter(|(_, s)| s.controlled && s.level == level && s.run == Run::Stopped)
                    .map(|(n, _)| n.clone())
                    .collect();
                (names, st.config.clone())
            };
            for name in &names {
                if let Err(e) = self.start(name) {
                    self.note(&self.lock(), &format!("cannot start {name}: {e}"));
                }
            }
            let deadline = Instant::now() + config.startup_timeout;
            while Instant::now() < deadline
                && names.iter().any(|n| self.lock().servers.get(n).is_some_and(|s| s.run == Run::Starting))
            {
                tokio::time::sleep(self.period).await;
            }
            tokio::time::sleep(config.level_wait).await;
        }
    }

    /// `HostState`: 1 MOVING if a server is starting, 2 ALARM if a controlled server is stopped.
    fn host_state(st: &State) -> i16 {
        if st.servers.values().any(|s| s.run == Run::Starting) {
            1
        } else if st.servers.values().any(|s| s.controlled && s.run == Run::Stopped) {
            2
        } else {
            0
        }
    }

    fn servers(st: &State) -> Vec<String> {
        let line = |(n, s): (&String, &Srv)| {
            let instances = u8::from(s.run == Run::Running);
            format!("{n}\t{}\t{}\t{}\t{instances}", s.run.state(), u8::from(s.controlled), s.level)
        };
        st.servers.iter().map(line).collect()
    }

    fn names(st: &State, run: impl Fn(Run) -> bool, controlled_only: bool) -> Vec<String> {
        let keep = |s: &Srv| run(s.run) && (s.controlled || !controlled_only);
        st.servers.iter().filter(|(_, s)| keep(s)).map(|(n, _)| n.clone()).collect()
    }

    /// CHANGE events of `HostState` and `Servers` when they changed.
    fn publish(&self) {
        let (state, servers) = {
            let mut st = self.lock();
            let now = (Self::host_state(&st), Self::servers(&st));
            if st.published.as_ref() == Some(&now) {
                return;
            }
            st.published = Some(now.clone());
            now
        };
        if let Some(sink) = self.sink.lock().unwrap_or_else(|p| p.into_inner()).as_ref() {
            let _ = sink.push_value("HostState", EventType::Change, Value::Short(state));
            let _ = sink.push_value("Servers", EventType::Change, Value::StringArray(servers));
        }
    }

    /// Re-reads the servers of the host: `[server, controlled, level]...`.
    async fn refresh(&self, db: &Database) {
        let info = db.command("DbGetHostServersInfo", Value::String(self.host.clone())).await;
        let Ok(Value::StringArray(info)) = info else { return };
        let mut st = self.lock();
        let rows: Vec<_> = info.chunks(3).filter_map(|r| r.try_into().ok()).collect::<Vec<&[String; 3]>>();
        st.servers.retain(|n, s| s.run != Run::Stopped || s.child.is_some() || rows.iter().any(|[r, ..]| r == n));
        for [name, mode, level] in rows {
            let srv = st.servers.entry(name.clone()).or_insert_with(|| Srv {
                controlled: false,
                level: 0,
                run: Run::Stopped,
                child: None,
                started: Instant::now(),
                stopping: false,
            });
            srv.controlled = mode != "0";
            srv.level = level.parse().unwrap_or(0);
        }
    }

    /// Whether the admin device of `name` answers a ping.
    async fn ping(&self, proxies: &mut HashMap<String, DeviceProxy>, name: &str) -> bool {
        let url = format!("tango://{}/dserver/{name}", self.tango_host);
        let ping = async {
            if !proxies.contains_key(name) {
                proxies.insert(name.to_string(), DeviceProxy::connect(&url).await?);
            }
            proxies.get_mut(name).expect("inserted").ping().await
        };
        let alive = matches!(tokio::time::timeout(Duration::from_millis(500), ping).await, Ok(Ok(_)));
        if !alive {
            proxies.remove(name);
        }
        alive
    }

    /// Updates the state of a server from a ping and its process.
    fn update(&self, name: &str, alive: bool) {
        let mut st = self.lock();
        let timeout = st.config.startup_timeout;
        let Some(srv) = st.servers.get_mut(name) else { return };
        let exit = srv.child.as_mut().and_then(|c| c.try_wait().ok().flatten());
        if exit.is_some() {
            srv.child = None;
        }
        // while its process lives, a server that does not answer keeps its state: it is busy,
        // or on its way out, and its exit status tells which
        let run = match (alive, &srv.child) {
            (true, _) => Run::Running,
            (false, None) => Run::Stopped,
            (false, Some(_)) if srv.run == Run::Starting && srv.started.elapsed() >= timeout => Run::Stopped,
            (false, Some(_)) => srv.run,
        };
        if run == srv.run {
            return;
        }
        let what = match (run, exit) {
            (Run::Stopped, Some(status)) if !srv.stopping => format!("died ({status})"),
            (Run::Stopped, _) => "stopped".to_string(),
            _ => "running".to_string(),
        };
        srv.run = run;
        if run == Run::Stopped {
            srv.stopping = false;
        }
        self.note(&st, &format!("{name} {what}"));
    }

    /// Checks every server each period; starts the servers at startup.
    async fn monitor(self: Arc<Self>) {
        let db = loop {
            match Database::connect(&self.tango_host).await {
                Ok(db) => break db,
                Err(_) => tokio::time::sleep(self.period).await,
            }
        };
        let mut proxies = HashMap::new();
        let mut first = true;
        loop {
            if std::mem::take(&mut self.lock().refresh) {
                self.refresh(&db).await;
            }
            let names: Vec<String> = self.lock().servers.keys().cloned().collect();
            for name in names {
                let alive = self.ping(&mut proxies, &name).await;
                self.update(&name, alive);
            }
            self.publish();
            if std::mem::take(&mut first) && self.lock().config.at_startup {
                tokio::spawn(self.clone().start_levels(None));
            }
            tokio::time::sleep(self.period).await;
        }
    }
}

impl Device for Starter {
    fn class_name(&self) -> String {
        "Starter".into()
    }
    fn description(&self) -> String {
        format!("Starts and monitors the device servers of {}", self.inner.host)
    }
    fn properties(&self) -> Vec<PropInfo> {
        let prop = |name: &str, data_type, default: &str, description: &str| PropInfo {
            name: name.into(),
            data_type,
            description: description.into(),
            default: Some(vec![default.into()]),
            mandatory: false,
        };
        let temp = std::env::temp_dir().display().to_string();
        vec![
            PropInfo {
                default: Some(vec![]),
                ..prop("StartDsPath", DataType::StringArray, "", "executable directories")
            },
            prop("InterStartupLevelWait", DataType::Long, "1", "seconds between two startup levels"),
            prop("ServerStartupTimeout", DataType::Long, "60", "seconds a server may take to start"),
            prop("StartServersAtStartup", DataType::Boolean, "true", "start the controlled servers at startup"),
            prop("LogFileHome", DataType::String, &temp, "directory of the log files"),
            prop("KeepLogFiles", DataType::Long, "3", "previous log files kept per server"),
        ]
    }
    fn attach(&mut self, events: EventSink) {
        *self.inner.sink.lock().unwrap_or_else(|p| p.into_inner()) = Some(events);
    }
    fn init(&mut self, props: &Properties) -> DevResult<()> {
        let long = |n: &str| match props.get(n) {
            Some(Value::Long(v)) => (*v).max(0) as u64,
            _ => 0,
        };
        let mut st = self.inner.lock();
        st.config = Config {
            paths: match props.get("StartDsPath") {
                Some(Value::StringArray(p)) => p.iter().map(PathBuf::from).collect(),
                _ => vec![],
            },
            level_wait: Duration::from_secs(long("InterStartupLevelWait")),
            startup_timeout: Duration::from_secs(long("ServerStartupTimeout")),
            at_startup: props.get("StartServersAtStartup") == Some(&Value::Boolean(true)),
            log_home: match props.get("LogFileHome") {
                Some(Value::String(p)) => p.into(),
                _ => std::env::temp_dir(),
            },
            keep_logs: long("KeepLogFiles") as usize,
        };
        st.refresh = true;
        drop(st);
        if !std::mem::replace(&mut self.monitoring, true) {
            let handle = tokio::runtime::Handle::try_current().map_err(|e| fail("API_InternalError", e.to_string()))?;
            handle.spawn(self.inner.clone().monitor());
        }
        Ok(())
    }
    fn state(&mut self) -> DevState {
        match Inner::host_state(&self.inner.lock()) {
            0 => DevState::On,
            1 => DevState::Moving,
            _ => DevState::Alarm,
        }
    }
    fn commands(&self) -> Vec<CmdInfo> {
        use DataType::{Boolean, Short, String as Str, StringArray as Sa, Void};
        let table = [
            ("DevStart", Str, Void),
            ("DevStop", Str, Void),
            ("HardKillServer", Str, Void),
            ("DevStartAll", Short, Void),
            ("DevStopAll", Short, Void),
            ("DevGetRunningServers", Boolean, Sa),
            ("DevGetStopServers", Boolean, Sa),
            ("DevReadLog", Str, Str),
            ("UpdateServersInfo", Void, Void),
        ];
        table
            .map(|(name, in_type, out_type)| CmdInfo { name: name.into(), in_type, out_type, ..Default::default() })
            .into()
    }
    fn attributes(&self) -> Vec<AttrInfo> {
        let list = |name: &str| AttrInfo {
            name: name.into(),
            data_type: DataType::String,
            data_format: DataFormat::Spectrum,
            max_dim_x: 1024,
            events_by_code: name == "Servers",
            ..Default::default()
        };
        let host_state = AttrInfo {
            name: "HostState".into(),
            data_type: DataType::Short,
            events_by_code: true,
            description: "0 ON, 1 MOVING (a server starting), 2 ALARM (a controlled server stopped)".into(),
            ..Default::default()
        };
        vec![host_state, list("Servers"), list("RunningServers"), list("StoppedServers")]
    }
    fn execute(&mut self, cmd: &str, arg: Value) -> DevResult<Value> {
        let inner = &self.inner;
        let void = |r: DevResult<()>| r.map(|_| Value::Void);
        match (cmd, arg) {
            ("DevStart", Value::String(s)) => void(inner.start(&s)),
            ("DevStop", Value::String(s)) => void(inner.stop(&s)),
            ("HardKillServer", Value::String(s)) => void(inner.kill(&s)),
            ("DevStartAll", Value::Short(level)) => {
                tokio::spawn(inner.clone().start_levels(Some(level)));
                Ok(Value::Void)
            }
            ("DevStopAll", Value::Short(level)) => {
                let names = Inner::names(&inner.lock(), |r| r == Run::Running, false);
                for name in names {
                    if inner.lock().servers.get(&name).is_some_and(|s| s.level == level) {
                        inner.stop(&name)?;
                    }
                }
                Ok(Value::Void)
            }
            ("DevGetRunningServers", Value::Boolean(c)) => {
                Ok(Value::StringArray(Inner::names(&inner.lock(), |r| r == Run::Running, c)))
            }
            ("DevGetStopServers", Value::Boolean(c)) => {
                Ok(Value::StringArray(Inner::names(&inner.lock(), |r| r == Run::Stopped, c)))
            }
            ("DevReadLog", Value::String(s)) => {
                let file = match s.split_once('/') {
                    Some((exec, instance)) => format!("{exec}_{instance}.log"),
                    None => format!("Starter_{}.log", inner.host),
                };
                let path = inner.lock().config.log_home.join(file);
                let text = std::fs::read_to_string(&path);
                text.map(Value::String).map_err(|e| fail("API_FileNotFound", format!("{}: {e}", path.display())))
            }
            ("UpdateServersInfo", _) => {
                inner.lock().refresh = true;
                Ok(Value::Void)
            }
            (cmd, _) => Err(fail("API_CommandNotFound", cmd)),
        }
    }
    fn read_attribute(&mut self, attr: &str) -> DevResult<Value> {
        let st = self.inner.lock();
        Ok(match attr {
            "HostState" => Value::Short(Inner::host_state(&st)),
            "Servers" => Value::StringArray(Inner::servers(&st)),
            "RunningServers" => Value::StringArray(Inner::names(&st, |r| r == Run::Running, false)),
            _ => Value::StringArray(Inner::names(&st, |r| r == Run::Stopped, false)),
        })
    }
}

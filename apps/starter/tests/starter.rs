//! The Starter device (SPEC §10) driving real `TestServer` processes registered in a test
//! Database for the host `testhost`.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use trust::{DeviceProxy, EventData, EventType, Value};

const HOST: &str = "testhost";
const STARTER: &str = "tango/admin/testhost";

fn sa(v: &[&str]) -> Value {
    Value::StringArray(v.iter().map(|s| s.to_string()).collect())
}

fn strings(v: Value) -> Vec<String> {
    match v {
        Value::StringArray(v) => v,
        other => panic!("expected StringArray, got {other:?}"),
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

fn temp_dir(what: &str) -> PathBuf {
    static N: AtomicU32 = AtomicU32::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    let p = std::env::temp_dir().join(format!("trust-starter-{}-{what}-{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

/// A Database on a free port; returns its TANGO_HOST.
async fn start_db() -> String {
    loop {
        let host = format!("127.0.0.1:{}", free_port());
        let file = temp_dir("db").join("tango.sqlite");
        let task = tokio::spawn(
            trust_databaseds::server(&file, &format!("auto://{host}")).unwrap().run([format!("auto://{host}")]),
        );
        while !task.is_finished() && tokio::net::TcpStream::connect(&host).await.is_err() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        if !task.is_finished() {
            return host;
        }
    }
}

struct Setup {
    db: DeviceProxy,
    starter: DeviceProxy,
    host: String,
    logs: PathBuf,
}

/// Registers `TestServer/<instance>` with one device `test/dev/<instance>` for `testhost`
/// (`controlled`, startup `level`), then starts the Starter.
async fn setup(servers: &[(&str, bool, i16)], at_startup: bool) -> Setup {
    let host = start_db().await;
    let mut db = DeviceProxy::connect(&format!("tango://{host}/sys/database/2#dbase=no")).await.unwrap();
    for (instance, controlled, level) in servers {
        register(&mut db, instance, *controlled, *level).await;
    }
    let bin = PathBuf::from(env!("CARGO_BIN_EXE_TestServer"));
    let logs = temp_dir("logs");
    let (path, logs_dir) = (bin.parent().unwrap().display().to_string(), logs.display().to_string());
    let at_startup = at_startup.to_string();
    let props = [
        STARTER,
        "5",
        "StartDsPath",
        "1",
        &path,
        "LogFileHome",
        "1",
        &logs_dir,
        "StartServersAtStartup",
        "1",
        &at_startup,
        "InterStartupLevelWait",
        "1",
        "0",
        "ServerStartupTimeout",
        "1",
        "20",
    ];
    db.command_inout("DbPutDeviceProperty", sa(&props)).await.unwrap();
    let server = trust_starter::server(&host, HOST, Duration::from_millis(100));
    tokio::spawn(server.run(["auto://127.0.0.1:0"]));
    let url = format!("tango://{host}/{STARTER}");
    for _ in 0..500 {
        if let Ok(starter) = DeviceProxy::connect(&url).await {
            return Setup { db, starter, host, logs };
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the Starter is not exported");
}

async fn register(db: &mut DeviceProxy, instance: &str, controlled: bool, level: i16) {
    let server = format!("TestServer/{instance}");
    let device = format!("test/dev/{instance}");
    db.command_inout("DbAddDevice", sa(&[&server, &device, "TestDevice"])).await.unwrap();
    let info = [server.as_str(), HOST, if controlled { "1" } else { "0" }, &level.to_string()];
    db.command_inout("DbPutServerInfo", sa(&info)).await.unwrap();
}

impl Setup {
    async fn cmd(&mut self, name: &str, arg: Value) -> Value {
        self.starter.command_inout(name, arg).await.unwrap_or_else(|e| panic!("{name}: {e}"))
    }

    async fn read(&mut self, attr: &str) -> Value {
        self.starter.read_attribute(attr).await.unwrap().value
    }

    /// Waits until `server` is (not) in `RunningServers`.
    async fn running(&mut self, server: &str, running: bool) {
        for _ in 0..400 {
            if strings(self.read("RunningServers").await).contains(&server.to_string()) == running {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("{server} running != {running}");
    }

    async fn host_state(&mut self, expected: i16) {
        for _ in 0..400 {
            if self.read("HostState").await == Value::Short(expected) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("HostState != {expected}: {:?}", self.read("Servers").await);
    }

    async fn starter_log(&mut self) -> String {
        match self.cmd("DevReadLog", Value::String("Starter".into())).await {
            Value::String(s) => s,
            other => panic!("{other:?}"),
        }
    }

    /// Kills whatever is still running.
    async fn cleanup(mut self) {
        for s in strings(self.read("RunningServers").await) {
            let _ = self.starter.command_inout("HardKillServer", Value::String(s)).await;
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn start_stop_and_logs() {
    let mut s = setup(&[("a", false, 0)], true).await;
    assert_eq!(strings(s.read("StoppedServers").await), ["TestServer/a"], "level 0 is not started");
    s.cmd("DevStart", Value::String("TestServer/a".into())).await;
    s.running("TestServer/a", true).await;
    assert_eq!(s.read("HostState").await, Value::Short(0));
    let servers = strings(s.read("Servers").await);
    assert_eq!(servers, ["TestServer/a\tON\t0\t0\t1"]);

    // the server is a real device server, registered and exported
    let mut dev = DeviceProxy::connect(&format!("tango://{}/test/dev/a", s.host)).await.unwrap();
    assert_eq!(dev.read_attribute("value").await.unwrap().value, Value::Long(42));
    let again = s.starter.command_inout("DevStart", Value::String("TestServer/a".into())).await;
    assert_eq!(again.unwrap_err().reason(), "API_ServerRunning");
    assert_eq!(strings(s.cmd("DevGetRunningServers", Value::Boolean(false)).await), ["TestServer/a"]);
    assert!(strings(s.cmd("DevGetRunningServers", Value::Boolean(true)).await).is_empty(), "not controlled");

    let Value::String(log) = s.cmd("DevReadLog", Value::String("TestServer/a".into())).await else { panic!() };
    assert!(log.contains("TestServer/a started"), "{log}");
    assert!(s.logs.join("TestServer_a.log").is_file());

    s.cmd("DevStop", Value::String("TestServer/a".into())).await;
    s.running("TestServer/a", false).await;
    assert_eq!(strings(s.cmd("DevGetStopServers", Value::Boolean(false)).await), ["TestServer/a"]);
    let log = s.starter_log().await;
    assert!(log.contains("starting TestServer/a") && log.contains("TestServer/a stopped"), "{log}");
    assert!(!log.contains("died"), "{log}");
    let stop = s.starter.command_inout("DevStop", Value::String("TestServer/a".into())).await;
    assert_eq!(stop.unwrap_err().reason(), "API_ServerNotRunning");
    let missing = s.starter.command_inout("DevStart", Value::String("NoSuchServer/x".into())).await;
    assert_eq!(missing.unwrap_err().reason(), "API_ExecutableNotFound");

    // servers registered later are seen after UpdateServersInfo
    register(&mut s.db, "late", false, 0).await;
    s.cmd("UpdateServersInfo", Value::Void).await;
    for _ in 0..100 {
        if strings(s.read("StoppedServers").await).contains(&"TestServer/late".to_string()) {
            return s.cleanup().await;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("UpdateServersInfo did not see TestServer/late");
}

#[tokio::test(flavor = "multi_thread")]
async fn crash_detection_and_hard_kill() {
    let mut s = setup(&[("b", true, 1)], false).await;
    s.host_state(2).await; // a controlled server is stopped: ALARM
    s.cmd("DevStart", Value::String("TestServer/b".into())).await;
    s.running("TestServer/b", true).await;
    s.host_state(0).await;

    let mut dev = DeviceProxy::connect(&format!("tango://{}/test/dev/b", s.host)).await.unwrap();
    let _ = dev.command_inout("Crash", Value::Void).await;
    s.running("TestServer/b", false).await;
    s.host_state(2).await;
    let log = s.starter_log().await;
    assert!(log.contains("TestServer/b died"), "{log}");
    assert_eq!(strings(s.cmd("DevGetStopServers", Value::Boolean(true)).await), ["TestServer/b"]);

    s.cmd("DevStart", Value::String("TestServer/b".into())).await;
    s.running("TestServer/b", true).await;
    s.cmd("HardKillServer", Value::String("TestServer/b".into())).await;
    s.running("TestServer/b", false).await;
    let log = s.starter_log().await;
    assert_eq!(log.matches("died").count(), 1, "a hard kill is not a crash: {log}");
    // the previous log file is kept
    assert!(s.logs.join("TestServer_b.log.1").is_file());
    s.cleanup().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn startup_levels() {
    let servers = [("l2", true, 2), ("l1", true, 1), ("l0", true, 0), ("free", false, 1)];
    let mut s = setup(&servers, true).await;
    s.running("TestServer/l1", true).await;
    s.running("TestServer/l2", true).await;
    let stopped = strings(s.read("StoppedServers").await);
    assert_eq!(stopped, ["TestServer/free", "TestServer/l0"], "level 0 and uncontrolled servers stay stopped");

    // level 2 starts once level 1 runs
    let log = s.starter_log().await;
    let at = |what: &str| log.find(what).unwrap_or_else(|| panic!("{what} not in {log}"));
    assert!(at("starting TestServer/l1") < at("TestServer/l1 running"));
    assert!(at("TestServer/l1 running") < at("starting TestServer/l2"), "{log}");

    s.cmd("DevStopAll", Value::Short(2)).await;
    s.running("TestServer/l2", false).await;
    assert!(strings(s.read("RunningServers").await).contains(&"TestServer/l1".to_string()));
    s.cmd("DevStartAll", Value::Short(2)).await;
    s.running("TestServer/l2", true).await;
    s.cmd("DevStopAll", Value::Short(1)).await;
    s.running("TestServer/l1", false).await;
    s.cleanup().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn change_events() {
    let mut s = setup(&[("e", false, 0)], false).await;
    let mut servers = s.starter.event_stream("Servers", EventType::Change).await.unwrap();
    let mut state = s.starter.event_stream("HostState", EventType::Change).await.unwrap();
    servers.recv().await.unwrap();
    state.recv().await.unwrap();
    s.cmd("DevStart", Value::String("TestServer/e".into())).await;
    let wait = async {
        loop {
            let e = servers.recv().await.unwrap();
            if let Ok(EventData::Attr(v)) = e.data
                && strings(v.value).iter().any(|l| l.starts_with("TestServer/e\tON\t"))
            {
                break;
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(20), wait).await.expect("a Servers event with TestServer/e ON");
    // MOVING while starting, then ON
    let e = tokio::time::timeout(Duration::from_secs(5), state.recv()).await.unwrap().unwrap();
    assert!(matches!(e.data, Ok(EventData::Attr(ref v)) if v.value == Value::Short(1)), "{e:?}");
    s.cleanup().await;
}

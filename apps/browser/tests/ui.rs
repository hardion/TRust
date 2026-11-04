//! UI tests (BROWSER.md §5): the browser run headless against a Database, a Demo device server
//! and a Starter, driven and checked through the accessibility tree.

use eframe::egui::accesskit::Role;
use eframe::egui::{Key, Modifiers};
use egui_kittest::Harness;
use egui_kittest::kittest::{NodeT, Queryable};
use std::path::PathBuf;
use std::time::Duration;
use trust::{DeviceProxy, Value};
use trust_browser::Browser;

fn sa(v: &[&str]) -> Value {
    Value::StringArray(v.iter().map(|s| s.to_string()).collect())
}

fn temp_dir(what: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!("trust-browser-{}-{what}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

/// A Database on a free port; returns its TANGO_HOST.
async fn start_db(what: &str) -> String {
    loop {
        let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
        let host = format!("127.0.0.1:{port}");
        let file = temp_dir(what).join("tango.sqlite");
        let server = trust_databaseds::server(&file, &format!("auto://{host}")).unwrap();
        let task = tokio::spawn(server.run([format!("auto://{host}")]));
        while !task.is_finished() && tokio::net::TcpStream::connect(&host).await.is_err() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        if !task.is_finished() {
            return host;
        }
    }
}

async fn wait_device(host: &str, device: &str) -> DeviceProxy {
    for _ in 0..500 {
        if let Ok(d) = DeviceProxy::connect(&format!("tango://{host}/{device}")).await {
            return d;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("{device} not exported");
}

fn harness(browser: Browser) -> Harness<'static, Browser> {
    Harness::builder().with_size([1400.0, 1000.0]).build_ui_state(|ui, b: &mut Browser| b.show(ui), browser)
}

/// Steps the UI until a node whose label contains `label` shows up.
fn wait(h: &mut Harness<'_, Browser>, label: &str) {
    for _ in 0..400 {
        h.step();
        if h.query_all_by_label_contains(label).next().is_some() {
            return;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    panic!("no {label:?} in the UI");
}

/// Clicks the widget named `label` through the accessibility tree (where a window may cover it).
fn click(h: &mut Harness<'_, Browser>, label: &str) {
    wait(h, label);
    h.get_by_label(label).click_accesskit();
    h.step();
}

/// Replaces the text of the input field named `name`.
fn type_in(h: &mut Harness<'_, Browser>, name: &str, text: &str) {
    wait(h, name);
    let field = || h.get_by_role_and_label(Role::TextInput, name);
    field().focus();
    h.step();
    h.key_press_modifiers(Modifiers::COMMAND, Key::A);
    h.step();
    h.get_by_role_and_label(Role::TextInput, name).type_text(text);
    h.step();
}

/// Replaces the text of the field named `name` and presses Enter.
fn enter(h: &mut Harness<'_, Browser>, name: &str, text: &str) {
    type_in(h, name, text);
    h.key_press(Key::Enter);
    h.step();
}

#[test]
fn browse_and_operate_a_device() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let host = rt.block_on(async {
        let host = start_db("device").await;
        let mut db = DeviceProxy::connect(&format!("tango://{host}/sys/database/2#dbase=no")).await.unwrap();
        db.command_inout("DbAddDevice", sa(&["DemoServer/ui", "lab/demo/m1", "Demo"])).await.unwrap();
        for label in ["first", "hello"] {
            db.command_inout("DbPutDeviceProperty", sa(&["lab/demo/m1", "1", "Label", "1", label])).await.unwrap();
        }
        db.command_inout("DbPutDeviceAlias", sa(&["lab/demo/m1", "mymotor"])).await.unwrap();
        let args = ["ui", &format!("-dbhost={host}"), "-endpoint=auto://127.0.0.1:0"].map(String::from).to_vec();
        tokio::spawn(trust_browser::demo::server(args).unwrap().run_args());
        wait_device(&host, "lab/demo/m1").await;
        host
    });
    let mut h = harness(Browser::new(Some(host)));

    // a device from its alias, then from the tree: domain, family, member
    click(&mut h, "Aliases");
    click(&mut h, "mymotor");
    wait(&mut h, "[Demo]");
    click(&mut h, "lab");
    click(&mut h, "demo");
    click(&mut h, "m1");
    wait(&mut h, "0.00 mm");
    wait(&mut h, "hello");
    wait(&mut h, "4x3 image");
    wait(&mut h, "frame heat map");

    // writes: a plain one, one past the alarm limit (confirmed), one past max_value (refused)
    enter(&mut h, "write position", "12.5");
    wait(&mut h, "12.50 mm");
    enter(&mut h, "write position", "60");
    click(&mut h, "Write anyway");
    wait(&mut h, "60.00 mm");
    enter(&mut h, "write position", "101");
    wait(&mut h, "101 is outside [-inf, 100]");

    // the trend of a numeric scalar, which keeps filling
    click(&mut h, "position");
    wait(&mut h, "Trend of position");
    let points = |h: &Harness<'_, Browser>| {
        let node = h.get_by_label_contains("points, seconds from now");
        let label = node.accesskit_node().label().or_else(|| node.value()).unwrap_or_default();
        label.split(' ').next().and_then(|n| n.parse::<usize>().ok()).unwrap_or(0)
    };
    let before = points(&h);
    for _ in 0..200 {
        h.step();
        if points(&h) > before {
            break;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    assert!(points(&h) > before, "the trend grows");
    click(&mut h, "position");
    for _ in 0..40 {
        h.step();
    }
    assert!(h.query_by_label_contains("Trend of position").is_none(), "a second click closes the trend");

    click(&mut h, "Commands");
    enter(&mut h, "argument of Twice", "21");
    wait(&mut h, "42");

    click(&mut h, "Pipes");
    click(&mut h, "Read settings");
    wait(&mut h, "gain = 2");

    click(&mut h, "Properties");
    click(&mut h, "Reload properties");
    for _ in 0..400 {
        h.step();
        if h.query_by_label("property Label").and_then(|n| n.value()).as_deref() == Some("hello") {
            break;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    assert_eq!(h.get_by_label("property Label").value().as_deref(), Some("hello"));
    click(&mut h, "History");
    wait(&mut h, "History of Label");
    wait(&mut h, "  first");

    click(&mut h, "Admin");
    click(&mut h, "Black box");
    wait(&mut h, "CommandExecution");
}

#[test]
fn starter_interface() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let logs = temp_dir("logs");
    let host = rt.block_on(async {
        let host = start_db("starter").await;
        let mut db = DeviceProxy::connect(&format!("tango://{host}/sys/database/2#dbase=no")).await.unwrap();
        db.command_inout("DbAddDevice", sa(&["DemoServer/s", "lab/demo/s1", "Demo"])).await.unwrap();
        db.command_inout("DbPutServerInfo", sa(&["DemoServer/s", "testhost", "1", "1"])).await.unwrap();
        let bin = PathBuf::from(env!("CARGO_BIN_EXE_DemoServer"));
        let (path, logs) = (bin.parent().unwrap().display().to_string(), logs.display().to_string());
        let props = [
            "tango/admin/testhost",
            "3",
            "StartDsPath",
            "1",
            &path,
            "LogFileHome",
            "1",
            &logs,
            "StartServersAtStartup",
            "1",
            "false",
        ];
        db.command_inout("DbPutDeviceProperty", sa(&props)).await.unwrap();
        tokio::spawn(trust_starter::server(&host, "testhost", Duration::from_millis(100)).run(["auto://127.0.0.1:0"]));
        wait_device(&host, "tango/admin/testhost").await;
        host
    });
    let mut h = harness(Browser::new(Some(host.clone())));

    click(&mut h, "testhost");
    wait(&mut h, "DemoServer/s");
    wait(&mut h, "stopped");
    click(&mut h, "Start");
    wait(&mut h, "running");
    click(&mut h, "Devices");
    wait(&mut h, "Demo::lab/demo/s1");
    click(&mut h, "Log");
    wait(&mut h, "DemoServer/s started");
    click(&mut h, "Stop");
    wait(&mut h, "stopped");

    // a new server, registered from the host panel, shows up through the Starter
    click(&mut h, "Register a server");
    for (field, text) in [
        ("server (Executable/instance)", "DemoServer/r"),
        ("class", "Demo"),
        ("devices (comma-separated)", "lab/demo/r1"),
    ] {
        type_in(&mut h, field, text);
    }
    click(&mut h, "Register");
    wait(&mut h, "DemoServer/r");

    // the device of a server opens from the host panel; stopped, it says so
    click(&mut h, "Demo::lab/demo/s1");
    wait(&mut h, "lab/demo/s1 is not exported");
    rt.block_on(async {
        let mut starter = DeviceProxy::connect(&format!("tango://{host}/tango/admin/testhost")).await.unwrap();
        let _ = starter.command_inout("HardKillServer", Value::String("DemoServer/s".into())).await;
    });
}

//! Database as a file (RFC-6, SPEC §7.4) and server command-line arguments.

mod common;

use common::DEV;
use std::time::Duration;
use trust::{DbFile, DeviceProxy, Server};

const FILE: &str = r#"
# TgTest server definition
TgTest/test/DEVICE/TgTest: sys/tg/1, \
                           sys/tg/2
sys/tg/1->host: "my host"
sys/tg/1->ports: 10, 20,\
                 30
sys/tg/1/double_scalar->label: Position
CLASS/TgTest->doc_url: "http://example.org/doc"
"#;

#[test]
fn parse_rfc6_syntax() {
    let f = DbFile::parse(FILE).unwrap();
    assert_eq!(
        f.devices,
        [
            ("TgTest/test".into(), "TgTest".into(), "sys/tg/1".into()),
            ("TgTest/test".into(), "TgTest".into(), "sys/tg/2".into())
        ]
    );
    assert_eq!(f.device_props[&("sys/tg/1".into(), "host".into())], ["my host"]);
    assert_eq!(f.device_props[&("sys/tg/1".into(), "ports".into())], ["10", "20", "30"]);
    assert_eq!(f.attr_props[&("sys/tg/1".into(), "double_scalar".into(), "label".into())], ["Position"]);
    assert_eq!(f.class_props[&("TgTest".into(), "doc_url".into())], ["http://example.org/doc"]);
    assert_eq!(DbFile::parse("garbage line").unwrap_err().reason(), "API_DatabaseFileError");
}

#[tokio::test(flavor = "multi_thread")]
async fn server_from_file_and_arguments() {
    let path = std::env::temp_dir().join(format!("trust-dbfile-{}.res", std::process::id()));
    std::fs::write(&path, FILE).unwrap();
    let port = common::free_port();
    let args = ["test".to_string(), format!("-file={}", path.display()), format!("-endpoint=tcp://127.0.0.1:{port}")];
    let server = Server::from_args("TgTest", args).unwrap().class("TgTest", || Box::new(common::TgTest::default()));
    tokio::spawn(server.run_args());
    while tokio::net::TcpStream::connect(("127.0.0.1", port)).await.is_err() {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    for dev in [DEV, "sys/tg/2"] {
        let mut d = DeviceProxy::connect(&format!("tango://127.0.0.1:{port}/{dev}#dbase=no")).await.unwrap();
        assert_eq!(d.name().await.unwrap(), dev);
    }
    let mut d = DeviceProxy::connect(&format!("tcp://127.0.0.1:{port}/{DEV}")).await.unwrap();
    assert_eq!(d.get_attribute_config(&["double_scalar"]).await.unwrap()[0].label, "Position");
    assert!(Server::from_args("TgTest", Vec::<String>::new()).is_err(), "instance name required");
}

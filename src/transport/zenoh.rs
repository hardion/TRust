//! Zenoh transport: each device is the queryable `tango/<device name in lower case>`; a request is
//! a query whose payload is the JSON request frame, the reply payload is the JSON reply frame.
//! Zenoh routes a query to the device wherever it runs: the key expression replaces the address.

use crate::client::bad_reply;
use crate::server::Handler;
use crate::types::{DevFailed, DevResult};
use std::time::Duration;
use zenoh::{Config, Session, query::QueryTarget};

const TIMEOUT: Duration = Duration::from_secs(3);

fn failed(e: impl std::fmt::Display) -> DevFailed {
    DevFailed::new("API_CommunicationFailed", e.to_string(), "zenoh")
}

pub(crate) fn key(device: &str) -> String {
    format!("tango/{}", device.to_lowercase())
}

/// Session configuration: a server listens on `locator`, a client connects to it. An explicit
/// locator disables multicast scouting; an empty one keeps Zenoh's discovery.
pub(crate) fn config(locator: &str, server: bool) -> DevResult<Config> {
    let mut config = Config::default();
    let mut set = |k: &str, v: String| config.insert_json5(k, &v).map_err(failed);
    if !server {
        set("mode", r#""client""#.into())?;
    }
    if !locator.is_empty() {
        set(if server { "listen/endpoints" } else { "connect/endpoints" }, format!(r#"["{locator}"]"#))?;
        set("scouting/multicast/enabled", "false".into())?;
    }
    Ok(config)
}

pub(crate) async fn open(config: Config) -> DevResult<Session> {
    zenoh::open(config).await.map_err(failed)
}

pub(crate) async fn serve(session: Session, handler: Handler) -> DevResult<()> {
    let mut tasks = tokio::task::JoinSet::new();
    for device in handler.devices() {
        let queryable = session.declare_queryable(key(&device)).await.map_err(failed)?;
        let handler = handler.clone();
        tasks.spawn(async move {
            while let Ok(query) = queryable.recv_async().await {
                let handler = handler.clone();
                tokio::spawn(async move {
                    let payload = query.payload().map(|p| p.to_bytes().into_owned()).unwrap_or_default();
                    let reply = tokio::task::spawn_blocking(move || handler.frame(&payload, "zenoh")).await;
                    if let Ok(reply) = reply {
                        let _ = query.reply(query.key_expr().clone(), reply).await;
                    }
                });
            }
        });
    }
    tasks.join_all().await;
    Ok(())
}

pub(crate) struct Link {
    session: Session,
    key: String,
}

impl Link {
    /// `locator` is a Zenoh endpoint such as `tcp/host:7447`; empty means multicast discovery.
    pub(crate) async fn connect(locator: &str, device: &str) -> DevResult<Self> {
        Ok(Self { session: open(config(locator, false)?).await?, key: key(device) })
    }

    pub(crate) async fn call(&mut self, frame: Vec<u8>) -> DevResult<Vec<u8>> {
        let replies = self
            .session
            .get(&self.key)
            .payload(frame)
            .target(QueryTarget::BestMatching)
            .timeout(TIMEOUT)
            .await
            .map_err(failed)?;
        let Ok(reply) = replies.recv_async().await else {
            let desc = format!("no device answers on {}", self.key);
            return Err(DevFailed::new("API_DeviceNotFound", desc, "zenoh"));
        };
        match reply.result() {
            Ok(sample) => Ok(sample.payload().to_bytes().into_owned()),
            Err(e) => Err(bad_reply(e.payload().to_bytes())),
        }
    }
}

//! Zenoh transport: each device is the queryable `tango/<device name in lower case>`; a request is
//! a query whose payload is the JSON request frame, the reply payload is the JSON reply frame.
//! Zenoh routes a query to the device wherever it runs: the key expression replaces the address.

use crate::client::bad_reply;
use crate::events::Event;
use crate::server::Handler;
use crate::types::{DevFailed, DevResult};
use std::time::Duration;
use zenoh::{Config, Session, query::QueryTarget};

const TIMEOUT: Duration = Duration::from_secs(3);

fn failed(e: impl std::fmt::Display) -> DevFailed {
    DevFailed::new("API_CommunicationFailed", e.to_string(), "zenoh")
}

/// A Zenoh object (session, subscriber) dropped on a plain thread: dropping one blocks, which
/// panics when it happens inside tokio (a task cancelled at runtime shutdown, a current-thread
/// runtime).
pub(crate) struct Detached<T: Send + 'static = Session>(Option<T>);

impl<T: Send + 'static> std::ops::Deref for Detached<T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.0.as_ref().expect("taken only on drop")
    }
}

impl<T: Send + 'static> Drop for Detached<T> {
    fn drop(&mut self) {
        if let Some(inner) = self.0.take() {
            std::thread::spawn(move || drop(inner));
        }
    }
}

/// Zenoh key of an event channel.
fn event_key(channel: &str) -> String {
    format!("tango/event/{channel}")
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

pub(crate) async fn open(config: Config) -> DevResult<Detached> {
    Ok(Detached(Some(zenoh::open(config).await.map_err(failed)?)))
}

pub(crate) async fn serve(session: Detached, handler: Handler) -> DevResult<()> {
    let session = std::sync::Arc::new(session);
    let (publisher, mut events) = (session.clone(), crate::transport::events(handler.clone(), |_| true));
    tokio::spawn(async move {
        while let Some(e) = events.recv().await {
            let json = serde_json::to_vec(&*e).expect("an event is always serialisable");
            if publisher.put(event_key(&e.channel), json).await.is_err() {
                break;
            }
        }
    });
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

type Subscriber = zenoh::pubsub::Subscriber<zenoh::handlers::FifoChannelHandler<zenoh::sample::Sample>>;

/// The events of some channels: one subscriber on `tango/event/**`, filtered here.
pub(crate) struct EventLink {
    subscriber: Detached<Subscriber>,
    channels: Vec<String>,
    _session: Detached,
}

impl EventLink {
    pub(crate) async fn connect(locator: &str, channels: Vec<String>) -> DevResult<Self> {
        let session = open(config(locator, false)?).await?;
        let subscriber = session.declare_subscriber(event_key("**")).await.map_err(failed)?;
        Ok(Self { subscriber: Detached(Some(subscriber)), channels, _session: session })
    }

    pub(crate) async fn next(&mut self) -> DevResult<Event> {
        loop {
            let sample = self.subscriber.recv_async().await.map_err(failed)?;
            let event: Event = serde_json::from_slice(&sample.payload().to_bytes())?;
            if self.channels.contains(&event.channel) {
                return Ok(event);
            }
        }
    }
}

pub(crate) struct Link {
    session: Detached,
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

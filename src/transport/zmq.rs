//! ZeroMQ transport: ROUTER server socket, REQ client socket, one JSON frame per message.
//!
//! The server accepts any REQ or DEALER peer. Requests run on the blocking pool so a slow device
//! does not hold back other clients; replies may then leave out of order for a DEALER peer, which
//! matches them with the request `id`.

use crate::client::bad_reply;
use crate::events::Event;
use crate::server::Handler;
use crate::types::{DevFailed, DevResult};
use tokio::sync::mpsc;
use zeromq::{PubSocket, ReqSocket, RouterSocket, Socket, SocketRecv, SocketSend, SubSocket, ZmqError, ZmqMessage};

impl From<ZmqError> for DevFailed {
    fn from(e: ZmqError) -> Self {
        DevFailed::new("API_CommunicationFailed", e.to_string(), "zmq")
    }
}

/// Binds `endpoint` (e.g. `tcp://0.0.0.0:5555`) and serves forever.
pub(crate) async fn serve(endpoint: &str, handler: Handler) -> DevResult<()> {
    let mut socket = RouterSocket::new();
    socket.bind(endpoint).await?;
    publish(endpoint, handler.clone()).await?;
    let (mut send, mut recv) = socket.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<ZmqMessage>();
    tokio::spawn(async move {
        while let Some(reply) = rx.recv().await {
            if send.send(reply).await.is_err() {
                break;
            }
        }
    });
    loop {
        // [identity, (empty delimiter), payload]: the reply reuses the envelope
        let mut frames = recv.recv().await?.into_vecdeque();
        let (Some(payload), Some(identity)) = (frames.pop_back(), frames.front().cloned()) else { continue };
        let (handler, tx) = (handler.clone(), tx.clone());
        tokio::task::spawn_blocking(move || {
            let peer: String = identity.iter().map(|b| format!("{b:02x}")).collect();
            frames.push_back(handler.frame(&payload, &format!("zmq:{peer}")).into());
            let _ = tx.send(ZmqMessage::try_from(frames).expect("non-empty envelope"));
        });
    }
}

/// Binds the PUB socket next to the ROUTER (port chosen by the system), advertises it in
/// SUBSCRIPTION_INFO and publishes every event as `[channel, json]`.
async fn publish(endpoint: &str, handler: Handler) -> DevResult<()> {
    let at = match endpoint.strip_prefix("tcp://").and_then(|a| a.rsplit_once(':')) {
        Some((host, _)) => format!("tcp://{host}:0"),
        None => format!("{endpoint}.pub"), // ipc
    };
    let mut socket = PubSocket::new();
    let bound = socket.bind(&at).await?.to_string().replace("0.0.0.0", &crate::db::hostname());
    *handler.hub().zmq_endpoint.lock().unwrap_or_else(|p| p.into_inner()) = bound;
    let mut events = crate::transport::events(handler, |_| true);
    tokio::spawn(async move {
        while let Some(e) = events.recv().await {
            let mut message = ZmqMessage::from(e.channel.clone());
            message.push_back(serde_json::to_vec(&*e).expect("an event is always serialisable").into());
            if socket.send(message).await.is_err() {
                break;
            }
        }
    });
    Ok(())
}

/// The events of `channels` on a SUB socket connected to the server's PUB socket.
pub(crate) struct EventLink(SubSocket);

impl EventLink {
    pub(crate) async fn connect(endpoint: &str, channels: &[String]) -> DevResult<Self> {
        if endpoint.is_empty() {
            return Err(DevFailed::new("API_EventNotSupported", "the server publishes no ZeroMQ events", "zmq"));
        }
        let mut socket = SubSocket::new();
        socket.connect(endpoint).await?;
        for c in channels {
            socket.subscribe(c).await?;
        }
        Ok(Self(socket))
    }

    pub(crate) async fn next(&mut self) -> DevResult<Event> {
        let frames = self.0.recv().await?.into_vec();
        Ok(serde_json::from_slice(frames.last().ok_or_else(|| bad_reply("empty message"))?)?)
    }
}

pub(crate) struct Link(ReqSocket);

impl Link {
    /// `endpoint` is a ZeroMQ endpoint, e.g. `tcp://host:5555`.
    pub(crate) async fn connect(endpoint: &str) -> DevResult<Self> {
        let mut socket = ReqSocket::new();
        socket.connect(endpoint).await?;
        Ok(Self(socket))
    }

    pub(crate) async fn call(&mut self, frame: Vec<u8>) -> DevResult<Vec<u8>> {
        self.0.send(frame.into()).await?;
        let reply = self.0.recv().await?.into_vec();
        Ok(reply.last().ok_or_else(|| bad_reply("empty message"))?.to_vec())
    }
}

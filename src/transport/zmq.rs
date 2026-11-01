//! ZeroMQ transport: ROUTER server socket, REQ client socket, one JSON frame per message.
//!
//! The server accepts any REQ or DEALER peer. Requests run on the blocking pool so a slow device
//! does not hold back other clients; replies may then leave out of order for a DEALER peer, which
//! matches them with the request `id`.

use crate::client::bad_reply;
use crate::server::Handler;
use crate::types::{DevFailed, DevResult};
use tokio::sync::mpsc;
use zeromq::{ReqSocket, RouterSocket, Socket, SocketRecv, SocketSend, ZmqError, ZmqMessage};

impl From<ZmqError> for DevFailed {
    fn from(e: ZmqError) -> Self {
        DevFailed::new("API_CommunicationFailed", e.to_string(), "zmq")
    }
}

/// Binds `endpoint` (e.g. `tcp://0.0.0.0:5555`) and serves forever.
pub(crate) async fn serve(endpoint: &str, handler: Handler) -> DevResult<()> {
    let mut socket = RouterSocket::new();
    socket.bind(endpoint).await?;
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

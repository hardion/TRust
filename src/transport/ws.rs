//! WebSocket: the JSON frames of the TCP transport, one per text message, and an event connection
//! when the first message is `{"subscribe": [...]}`.
//!
//! Server: `ws://host:port`, and any `tcp://` or `auto://` port, where a connection that starts
//! with an HTTP `GET` is a WebSocket handshake. Client: tokio-tungstenite natively, the browser's
//! `WebSocket` on wasm32 (the only transport of a web page).

use crate::client::bad_reply;
use crate::events::Event;
use crate::types::{DevFailed, DevResult};
use serde::Deserialize;

/// The first message of an event connection.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(target_arch = "wasm32", allow(dead_code))]
struct Subscribe {
    subscribe: Vec<String>,
}

fn subscribe(channels: &[String]) -> String {
    serde_json::json!({ "subscribe": channels }).to_string()
}

fn failed(e: impl std::fmt::Display) -> DevFailed {
    DevFailed::new("API_CommunicationFailed", e.to_string(), "WebSocket")
}

#[cfg(not(target_arch = "wasm32"))]
mod native {
    use super::*;
    use crate::server::Handler;
    use futures_util::{SinkExt, StreamExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

    pub(crate) async fn serve(listener: TcpListener, handler: Handler) -> DevResult<()> {
        loop {
            let (stream, _) = listener.accept().await?;
            tokio::spawn(serve_stream(stream, handler.clone()));
        }
    }

    /// Serves one connection whose first bytes are a WebSocket handshake.
    pub(crate) async fn serve_stream(stream: TcpStream, handler: Handler) {
        let peer = stream.peer_addr().map_or_else(|_| "ws".into(), |a| a.to_string());
        let _ = stream.set_nodelay(true);
        let Ok(ws) = tokio_tungstenite::accept_async(stream).await else { return };
        let (mut tx, mut rx) = ws.split();
        loop {
            let message = tokio::select! {
                m = rx.next() => match m {
                    Some(Ok(m)) => m,
                    _ => break,
                },
                _ = handler.stopped() => break,
            };
            let text = match message {
                Message::Text(t) => t.to_string(),
                Message::Binary(b) => String::from_utf8_lossy(&b).into_owned(),
                Message::Close(_) => break,
                _ => continue, // ping/pong are answered by tungstenite
            };
            if let Ok(Subscribe { subscribe }) = serde_json::from_str(&text) {
                let mut events = crate::transport::events(handler, move |e| subscribe.contains(&e.channel));
                while let Some(e) = events.recv().await {
                    let text = serde_json::to_string(&*e).expect("an event is always serialisable");
                    if tx.send(Message::text(text)).await.is_err() {
                        break;
                    }
                }
                return;
            }
            let reply = String::from_utf8(handler.frame(text.as_bytes(), &peer)).expect("a reply is JSON");
            if tx.send(Message::text(reply)).await.is_err() {
                break;
            }
        }
    }

    type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

    async fn open(addr: &str) -> DevResult<Ws> {
        let (ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}")).await.map_err(failed)?;
        if let MaybeTlsStream::Plain(s) = ws.get_ref() {
            let _ = s.set_nodelay(true);
        }
        Ok(ws)
    }

    async fn next_text(ws: &mut Ws) -> DevResult<String> {
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(t))) => return Ok(t.to_string()),
                Some(Ok(Message::Binary(b))) => return Ok(String::from_utf8_lossy(&b).into_owned()),
                Some(Ok(Message::Close(_))) | None => return Err(bad_reply("connection closed")),
                Some(Err(e)) => return Err(failed(e)),
                Some(Ok(_)) => {}
            }
        }
    }

    pub(crate) struct Link(Ws);

    impl Link {
        pub(crate) async fn connect(addr: &str) -> DevResult<Self> {
            Ok(Self(open(addr).await?))
        }

        pub(crate) async fn call(&mut self, frame: Vec<u8>) -> DevResult<Vec<u8>> {
            let text = String::from_utf8(frame).map_err(failed)?;
            self.0.send(Message::text(text)).await.map_err(failed)?;
            Ok(next_text(&mut self.0).await?.into_bytes())
        }
    }

    pub(crate) struct EventLink(Ws);

    impl EventLink {
        pub(crate) async fn connect(addr: &str, channels: &[String]) -> DevResult<Self> {
            let mut ws = open(addr).await?;
            ws.send(Message::text(subscribe(channels))).await.map_err(failed)?;
            Ok(Self(ws))
        }

        pub(crate) async fn next(&mut self) -> DevResult<Event> {
            Ok(serde_json::from_str(&next_text(&mut self.0).await?)?)
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) use native::*;

#[cfg(target_arch = "wasm32")]
mod browser {
    use super::*;
    use tokio::sync::mpsc;
    use wasm_bindgen::JsCast;
    use wasm_bindgen::prelude::{Closure, JsValue};
    use web_sys::{MessageEvent, WebSocket};

    enum Note {
        Open,
        Text(String),
        Closed,
    }

    type Callback = Closure<dyn FnMut(JsValue)>;

    /// A browser WebSocket whose callbacks feed a channel.
    struct Conn {
        ws: WebSocket,
        rx: mpsc::UnboundedReceiver<Note>,
        _callbacks: [Callback; 3],
    }

    impl Conn {
        async fn open(addr: &str) -> DevResult<Self> {
            let url = format!("ws://{addr}");
            let ws = WebSocket::new(&url).map_err(|e| failed(format!("{url}: {e:?}")))?;
            let (tx, rx) = mpsc::unbounded_channel();
            let on = |tx: mpsc::UnboundedSender<Note>, note: fn(JsValue) -> Note| -> Callback {
                Closure::new(move |e: JsValue| drop(tx.send(note(e))))
            };
            let text =
                |e: JsValue| Note::Text(e.unchecked_into::<MessageEvent>().data().as_string().unwrap_or_default());
            let callbacks = [on(tx.clone(), text), on(tx.clone(), |_| Note::Open), on(tx, |_| Note::Closed)];
            ws.set_onmessage(Some(callbacks[0].as_ref().unchecked_ref()));
            ws.set_onopen(Some(callbacks[1].as_ref().unchecked_ref()));
            ws.set_onerror(Some(callbacks[2].as_ref().unchecked_ref()));
            ws.set_onclose(Some(callbacks[2].as_ref().unchecked_ref()));
            let mut conn = Self { ws, rx, _callbacks: callbacks };
            match conn.rx.recv().await {
                Some(Note::Open) => Ok(conn),
                _ => Err(failed(format!("cannot open {url}"))),
            }
        }

        fn send(&self, text: &str) -> DevResult<()> {
            self.ws.send_with_str(text).map_err(|e| failed(format!("{e:?}")))
        }

        async fn recv(&mut self) -> DevResult<String> {
            loop {
                match self.rx.recv().await {
                    Some(Note::Text(t)) => return Ok(t),
                    Some(Note::Open) => {}
                    _ => return Err(bad_reply("connection closed")),
                }
            }
        }
    }

    impl Drop for Conn {
        fn drop(&mut self) {
            // the callbacks go with this value: detach them before closing
            self.ws.set_onmessage(None);
            self.ws.set_onopen(None);
            self.ws.set_onerror(None);
            self.ws.set_onclose(None);
            let _ = self.ws.close();
        }
    }

    pub(crate) struct Link(Conn);

    impl Link {
        pub(crate) async fn connect(addr: &str) -> DevResult<Self> {
            Ok(Self(Conn::open(addr).await?))
        }

        pub(crate) async fn call(&mut self, frame: Vec<u8>) -> DevResult<Vec<u8>> {
            self.0.send(&String::from_utf8(frame).map_err(failed)?)?;
            Ok(self.0.recv().await?.into_bytes())
        }
    }

    pub(crate) struct EventLink(Conn);

    impl EventLink {
        pub(crate) async fn connect(addr: &str, channels: &[String]) -> DevResult<Self> {
            let conn = Conn::open(addr).await?;
            conn.send(&subscribe(channels))?;
            Ok(Self(conn))
        }

        pub(crate) async fn next(&mut self) -> DevResult<Event> {
            Ok(serde_json::from_str(&self.0.recv().await?)?)
        }
    }
}

#[cfg(target_arch = "wasm32")]
pub(crate) use browser::*;

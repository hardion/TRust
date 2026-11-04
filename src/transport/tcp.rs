//! TCP transport: one JSON frame per line.

use crate::client::bad_reply;
use crate::events::Event;
use crate::server::Handler;
use crate::types::DevResult;
use serde::Deserialize;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};

pub(crate) async fn serve(listener: TcpListener, handler: Handler) -> DevResult<()> {
    loop {
        let (stream, _) = listener.accept().await?;
        tokio::spawn(serve_stream(stream, handler.clone()));
    }
}

/// The first line of an event connection.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Subscribe {
    subscribe: Vec<String>,
}

/// Serves one accepted connection until it closes or the server stops: requests, or the events
/// of the channels its first line subscribes to. With the `ws` feature, a connection starting
/// with an HTTP `GET` is served as a WebSocket.
pub(crate) async fn serve_stream(stream: TcpStream, handler: Handler) {
    // a WebSocket handshake (a web page cannot open a plain TCP connection)
    #[cfg(feature = "ws")]
    {
        let mut head = [0; 4];
        if matches!(stream.peek(&mut head).await, Ok(4)) && &head == b"GET " {
            return super::ws::serve_stream(stream, handler).await;
        }
    }
    let peer = stream.peer_addr().map_or_else(|_| "tcp".into(), |a| a.to_string());
    let _ = stream.set_nodelay(true);
    let (r, mut w) = stream.into_split();
    let mut lines = BufReader::new(r).lines();
    loop {
        let line = tokio::select! {
            line = lines.next_line() => match line {
                Ok(Some(line)) => line,
                _ => break,
            },
            _ = handler.stopped() => break,
        };
        if let Ok(Subscribe { subscribe }) = serde_json::from_str(&line) {
            let mut events = crate::transport::events(handler, move |e| subscribe.contains(&e.channel));
            while let Some(e) = events.recv().await {
                let mut line = serde_json::to_vec(&*e).expect("an event is always serialisable");
                line.push(b'\n');
                if w.write_all(&line).await.is_err() {
                    break;
                }
            }
            return;
        }
        let mut reply = handler.frame(line.as_bytes(), &peer);
        reply.push(b'\n');
        if w.write_all(&reply).await.is_err() {
            break;
        }
    }
}

/// The events of `channels`, one JSON line each, on a dedicated connection.
pub(crate) struct EventLink(Lines<BufReader<TcpStream>>);

impl EventLink {
    pub(crate) async fn connect(addr: &str, channels: &[String]) -> DevResult<Self> {
        let mut stream = TcpStream::connect(addr).await?;
        let mut line = serde_json::to_vec(&serde_json::json!({ "subscribe": channels }))?;
        line.push(b'\n');
        stream.write_all(&line).await?;
        Ok(Self(BufReader::new(stream).lines()))
    }

    pub(crate) async fn next(&mut self) -> DevResult<Event> {
        let line = self.0.next_line().await?.ok_or_else(|| bad_reply("event connection closed"))?;
        Ok(serde_json::from_str(&line)?)
    }
}

pub(crate) struct Link {
    reader: Lines<BufReader<OwnedReadHalf>>,
    writer: OwnedWriteHalf,
}

impl Link {
    pub(crate) async fn connect(addr: &str) -> DevResult<Self> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        let (reader, writer) = stream.into_split();
        Ok(Self { reader: BufReader::new(reader).lines(), writer })
    }

    pub(crate) async fn call(&mut self, mut frame: Vec<u8>) -> DevResult<Vec<u8>> {
        frame.push(b'\n');
        self.writer.write_all(&frame).await?;
        let line = self.reader.next_line().await?.ok_or_else(|| bad_reply("connection closed"))?;
        Ok(line.into_bytes())
    }
}

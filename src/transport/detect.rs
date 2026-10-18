//! Wire detection.
//!
//! Server, `auto://host:port`: one TCP port for every enabled wire. All the clients speak first, so
//! the first bytes of a connection, peeked without consuming them, name the protocol:
//!
//! | first bytes          | wire                           | handled by                     |
//! |----------------------|--------------------------------|--------------------------------|
//! | whitespace then `{`  | JSON Lines                     | the TCP transport, in place    |
//! | `0xFF`               | ZMTP greeting (ZeroMQ)         | spliced to an internal ROUTER  |
//! | `PRI * HTTP/2.0`     | HTTP/2 preface (gRPC)          | handed to the tonic server     |
//! | `len len 0x?1`       | Zenoh INIT after a u16 length  | spliced to an internal session |
//! | anything else        | unknown                        | closed                         |
//!
//! Client, `auto://host:port`: ZeroMQ and HTTP/2 servers greet first, so a short read on a
//! probe connection tells them apart; a silent server is taken as JSON Lines (or as a detecting
//! server, which accepts it).

use super::tcp;
use crate::server::Handler;
use crate::types::{DevFailed, DevResult};
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};

const PROBE: Duration = Duration::from_millis(100);

/// The wire of a connection, from its first bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Wire {
    Json,
    Zmtp,
    Http2,
    Zenoh,
    Unknown,
}

pub fn sniff(head: &[u8]) -> Wire {
    let json = head.iter().find(|b| !b" \t\r\n".contains(b));
    match head {
        [0xFF, ..] => Wire::Zmtp,
        [b'P', ..] if b"PRI * HTTP/2.0".starts_with(&head[..head.len().min(14)]) => Wire::Http2,
        // u16 little-endian frame length, then the INIT transport message (id 0x01)
        [_, _, header, ..] if header & 0x1F == 0x01 && json != Some(&b'{') => Wire::Zenoh,
        _ if json == Some(&b'{') => Wire::Json,
        _ => Wire::Unknown,
    }
}

/// A backend reached by copying bytes to a loopback endpoint it serves.
#[cfg(any(feature = "zmq", feature = "zenoh"))]
async fn splice(mut stream: TcpStream, port: u16) {
    if let Ok(mut backend) = TcpStream::connect(("127.0.0.1", port)).await {
        // small request-reply frames: Nagle would hold them back behind delayed ACKs
        let _ = (stream.set_nodelay(true), backend.set_nodelay(true));
        let _ = tokio::io::copy_bidirectional(&mut stream, &mut backend).await;
    }
}

#[cfg(any(feature = "zmq", feature = "zenoh"))]
fn loopback_port() -> DevResult<u16> {
    Ok(std::net::TcpListener::bind("127.0.0.1:0")?.local_addr()?.port())
}

/// The internal backends of a detecting server.
#[derive(Clone)]
struct Backends {
    handler: Handler,
    #[cfg(feature = "grpc")]
    grpc: tokio::sync::mpsc::Sender<std::io::Result<TcpStream>>,
    #[cfg(feature = "zmq")]
    zmq: u16,
    #[cfg(feature = "zenoh")]
    zenoh: u16,
}

impl Backends {
    async fn start(handler: Handler) -> DevResult<Self> {
        #[cfg(feature = "grpc")]
        let grpc = {
            let (tx, rx) = tokio::sync::mpsc::channel(16);
            let incoming = tokio_stream::wrappers::ReceiverStream::new(rx);
            tokio::spawn(super::grpc::serve_incoming(incoming, handler.clone()));
            tx
        };
        #[cfg(feature = "zmq")]
        let zmq = {
            let (port, handler) = (loopback_port()?, handler.clone());
            tokio::spawn(async move { super::zmq::serve(&format!("tcp://127.0.0.1:{port}"), handler).await });
            port
        };
        #[cfg(feature = "zenoh")]
        let zenoh = {
            let port = loopback_port()?;
            let config = super::zenoh::config(&format!("tcp/127.0.0.1:{port}"), true)?;
            tokio::spawn(super::zenoh::serve(super::zenoh::open(config).await?, handler.clone()));
            port
        };
        Ok(Self {
            handler,
            #[cfg(feature = "grpc")]
            grpc,
            #[cfg(feature = "zmq")]
            zmq,
            #[cfg(feature = "zenoh")]
            zenoh,
        })
    }

    async fn dispatch(self, stream: TcpStream) {
        let mut head = [0u8; 14];
        let mut n = 0;
        for _ in 0..20 {
            n = stream.peek(&mut head).await.unwrap_or(0);
            if n >= 3 || n == 0 {
                break; // enough to decide, or closed
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        match sniff(&head[..n]) {
            Wire::Json => tcp::serve_stream(stream, self.handler).await,
            #[cfg(feature = "grpc")]
            Wire::Http2 => drop(self.grpc.send(Ok(stream)).await),
            #[cfg(feature = "zmq")]
            Wire::Zmtp => splice(stream, self.zmq).await,
            #[cfg(feature = "zenoh")]
            Wire::Zenoh => splice(stream, self.zenoh).await,
            #[allow(unreachable_patterns)]
            _ => {} // wire not compiled in: close
        }
    }
}

pub(crate) async fn serve(listener: TcpListener, handler: Handler) -> DevResult<()> {
    let backends = Backends::start(handler).await?;
    loop {
        let (stream, _) = listener.accept().await?;
        tokio::spawn(backends.clone().dispatch(stream));
    }
}

/// Finds the wire of the server at `addr` and returns its endpoint URL.
pub(crate) async fn probe(addr: &str) -> DevResult<String> {
    let mut stream = TcpStream::connect(addr).await?;
    let mut head = [0u8; 16];
    match tokio::time::timeout(PROBE, stream.read(&mut head)).await {
        Err(_) => Ok(format!("tcp://{addr}")), // silent: JSON Lines
        Ok(Ok(n)) if n > 0 && head[0] == 0xFF => Ok(format!("zmq+tcp://{addr}")), // ZMTP greeting
        Ok(Ok(n)) if n >= 9 && head[3] == 0x04 => Ok(format!("grpc://{addr}")), // HTTP/2 SETTINGS
        Ok(Ok(n)) => {
            let desc = format!("unknown wire at {addr}, greeting {:02x?}", &head[..n]);
            Err(DevFailed::new("API_UnsupportedTransport", desc, "probe"))
        }
        Ok(Err(e)) => Err(e.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sniff_first_bytes() {
        assert_eq!(sniff(br#"{"v":1}"#), Wire::Json);
        assert_eq!(sniff(b" \n{\"v\":1}"), Wire::Json);
        assert_eq!(sniff(b"\xff\0\0\0\0\0\0\0\x01\x7f"), Wire::Zmtp);
        assert_eq!(sniff(b"PRI * HTTP/2.0\r\n"), Wire::Http2);
        assert_eq!(sniff(b"PRI"), Wire::Http2, "short peek");
        // Zenoh InitSyn as sent by the Rust (25 bytes) and Python (32 bytes = ' ') clients
        assert_eq!(sniff(b"\x19\x00\xc1\x09\xf2"), Wire::Zenoh);
        assert_eq!(sniff(b"\x20\x00\xc1\x09\xf2"), Wire::Zenoh, "length byte 0x20 is not JSON whitespace");
        assert_eq!(sniff(b"POST /"), Wire::Unknown);
        assert_eq!(sniff(b""), Wire::Unknown);
    }
}

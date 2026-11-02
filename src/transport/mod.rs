//! Transport abstraction: the device model only sees `Handler` (server) and `Link` (client).
//!
//! * **By compilation**: each transport is a Cargo feature (`tcp`, `zmq`, `grpc`, `zenoh`);
//!   a disabled one costs no code and its scheme is refused with `API_UnsupportedTransport`.
//! * **By URL**: the endpoint scheme selects the transport at run time.
//! * **By detection of the wire** (`detect` feature): `auto://host:port` on the server accepts
//!   every enabled TCP based wire on one port; on the client it probes the server's first bytes.

use crate::events::Event;
use crate::protocol::{Reply, Request};
use crate::server::Handler;
use crate::types::{DevFailed, DevResult};

#[cfg(not(any(feature = "tcp", feature = "zmq", feature = "grpc", feature = "zenoh")))]
compile_error!("enable at least one transport feature: tcp, zmq, grpc or zenoh");

#[cfg(feature = "detect")]
pub(crate) mod detect;
#[cfg(feature = "grpc")]
pub(crate) mod grpc;
#[cfg(feature = "tcp")]
pub(crate) mod tcp;
#[cfg(feature = "zenoh")]
pub(crate) mod zenoh;
#[cfg(feature = "zmq")]
pub(crate) mod zmq;

/// The transports, named by the scheme of an endpoint URL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scheme {
    /// `tcp://host:port` (or no scheme): JSON Lines.
    Tcp,
    /// `zmq+tcp://host:port`, `zmq+ipc://path`: ZeroMQ.
    Zmq,
    /// `grpc://host:port`: gRPC.
    Grpc,
    /// `zenoh://<locator>`, `zenoh://` for multicast discovery: Zenoh.
    Zenoh,
    /// `auto://host:port`: wire detection.
    Auto,
}

impl Scheme {
    /// Splits an endpoint URL into its transport and the transport's native endpoint.
    pub fn parse(endpoint: &str) -> DevResult<(Self, String)> {
        let (scheme, rest) = endpoint.split_once("://").unwrap_or(("tcp", endpoint));
        let parsed = match scheme {
            "tcp" => (Self::Tcp, rest.into()),
            "zmq+tcp" | "zmq+ipc" => (Self::Zmq, format!("{}://{rest}", &scheme[4..])),
            "grpc" => (Self::Grpc, rest.into()),
            "zenoh" => (Self::Zenoh, rest.into()),
            "auto" => (Self::Auto, rest.into()),
            _ => {
                return Err(DevFailed::new("API_UnsupportedTransport", format!("unknown scheme in {endpoint}"), "url"));
            }
        };
        parsed.0.check()?;
        Ok(parsed)
    }

    /// The Cargo feature providing this transport.
    pub fn feature(self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
            Self::Zmq => "zmq",
            Self::Grpc => "grpc",
            Self::Zenoh => "zenoh",
            Self::Auto => "detect",
        }
    }

    /// Fails if the transport was not compiled in.
    pub fn check(self) -> DevResult<()> {
        let enabled = match self {
            Self::Tcp => cfg!(feature = "tcp"),
            Self::Zmq => cfg!(feature = "zmq"),
            Self::Grpc => cfg!(feature = "grpc"),
            Self::Zenoh => cfg!(feature = "zenoh"),
            Self::Auto => cfg!(feature = "detect"),
        };
        if enabled {
            return Ok(());
        }
        let desc = format!("{self:?} transport not compiled in, enable the `{}` feature", self.feature());
        Err(DevFailed::new("API_UnsupportedTransport", desc, "transport"))
    }
}

/// An endpoint bound but not served yet.
pub(crate) enum Listener {
    /// TCP based wires bind first so that port 0 resolves to the real port before export.
    #[allow(dead_code)] // without tcp, grpc and detect
    Tcp(Scheme, tokio::net::TcpListener),
    /// ZeroMQ and Zenoh bind when served.
    #[allow(dead_code)] // without zmq and zenoh
    Later(Scheme, String),
}

/// Binds an endpoint URL; returns the URL with the real port and the listener.
pub(crate) async fn listen(endpoint: &str) -> DevResult<(String, Listener)> {
    let (scheme, native) = Scheme::parse(endpoint)?;
    Ok(match scheme {
        Scheme::Tcp | Scheme::Grpc | Scheme::Auto => {
            let listener = tokio::net::TcpListener::bind(&native).await?;
            let prefix = match scheme {
                Scheme::Tcp => "tcp",
                Scheme::Grpc => "grpc",
                _ => "auto",
            };
            (format!("{prefix}://{}", listener.local_addr()?), Listener::Tcp(scheme, listener))
        }
        Scheme::Zmq | Scheme::Zenoh => (endpoint.to_string(), Listener::Later(scheme, native)),
    })
}

/// Serves `handler` on a bound endpoint forever.
#[allow(unused_variables)]
pub(crate) async fn serve(listener: Listener, handler: Handler) -> DevResult<()> {
    match listener {
        #[cfg(feature = "tcp")]
        Listener::Tcp(Scheme::Tcp, l) => tcp::serve(l, handler).await,
        #[cfg(feature = "grpc")]
        Listener::Tcp(Scheme::Grpc, l) => grpc::serve(l, handler).await,
        #[cfg(feature = "detect")]
        Listener::Tcp(Scheme::Auto, l) => detect::serve(l, handler).await,
        #[cfg(feature = "zmq")]
        Listener::Later(Scheme::Zmq, native) => zmq::serve(&native, handler).await,
        #[cfg(feature = "zenoh")]
        Listener::Later(Scheme::Zenoh, native) => {
            zenoh::serve(zenoh::open(zenoh::config(&native, true)?).await?, handler).await
        }
        #[allow(unreachable_patterns)]
        _ => unreachable!("checked by Scheme::parse"),
    }
}

/// The server's events for which `keep` holds, until the server stops or the receiver is dropped.
#[allow(dead_code)] // without a transport
pub(crate) fn events(
    handler: Handler,
    keep: impl Fn(&Event) -> bool + Send + 'static,
) -> tokio::sync::mpsc::Receiver<std::sync::Arc<Event>> {
    let (tx, rx) = tokio::sync::mpsc::channel(crate::events::BUFFER);
    tokio::spawn(async move {
        let mut events = handler.events();
        loop {
            tokio::select! {
                e = events.recv() => match e {
                    Ok(e) if keep(&e) => if tx.send(e).await.is_err() { break },
                    Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {} // a gap the client sees
                    Err(_) => break,
                },
                _ = handler.stopped() => break,
            }
        }
    });
    rx
}

/// The concrete endpoint URL: `auto://` is probed into a transport scheme.
pub(crate) async fn resolve(endpoint: &str) -> DevResult<String> {
    let (scheme, native) = Scheme::parse(endpoint)?;
    #[cfg(feature = "detect")]
    if scheme == Scheme::Auto {
        return detect::probe(&native).await;
    }
    let _ = (scheme, native);
    Ok(endpoint.to_string())
}

/// The client side of an event subscription, on the transport of `url` (SPEC §9.2).
pub(crate) enum EventLink {
    #[cfg(feature = "tcp")]
    Tcp(tcp::EventLink),
    #[cfg(feature = "zmq")]
    Zmq(zmq::EventLink),
    #[cfg(feature = "grpc")]
    Grpc(grpc::EventLink),
    #[cfg(feature = "zenoh")]
    Zenoh(zenoh::EventLink),
}

impl EventLink {
    /// `url` is a resolved endpoint URL; `zmq_endpoint` the PUB endpoint from SUBSCRIPTION_INFO.
    #[allow(unused_variables)]
    pub(crate) async fn connect(url: &str, zmq_endpoint: &str, channels: Vec<String>) -> DevResult<Self> {
        let (scheme, native) = Scheme::parse(url)?;
        Ok(match scheme {
            #[cfg(feature = "tcp")]
            Scheme::Tcp => Self::Tcp(tcp::EventLink::connect(&native, &channels).await?),
            #[cfg(feature = "zmq")]
            Scheme::Zmq => Self::Zmq(zmq::EventLink::connect(zmq_endpoint, &channels).await?),
            #[cfg(feature = "grpc")]
            Scheme::Grpc => Self::Grpc(grpc::EventLink::connect(&native, channels).await?),
            #[cfg(feature = "zenoh")]
            Scheme::Zenoh => Self::Zenoh(zenoh::EventLink::connect(&native, channels).await?),
            #[allow(unreachable_patterns)]
            _ => unreachable!("resolved and checked by Scheme::parse"),
        })
    }

    /// The next event of the subscribed channels.
    pub(crate) async fn next(&mut self) -> DevResult<Event> {
        match self {
            #[cfg(feature = "tcp")]
            Self::Tcp(l) => l.next().await,
            #[cfg(feature = "zmq")]
            Self::Zmq(l) => l.next().await,
            #[cfg(feature = "grpc")]
            Self::Grpc(l) => l.next().await,
            #[cfg(feature = "zenoh")]
            Self::Zenoh(l) => l.next().await,
        }
    }
}

/// The client side of one connection, whatever the transport.
pub(crate) enum Link {
    #[cfg(feature = "tcp")]
    Tcp(tcp::Link),
    #[cfg(feature = "zmq")]
    Zmq(zmq::Link),
    #[cfg(feature = "grpc")]
    Grpc(grpc::Link),
    #[cfg(feature = "zenoh")]
    Zenoh(zenoh::Link),
}

impl Link {
    #[cfg_attr(not(feature = "zenoh"), allow(unused_variables))]
    pub(crate) async fn connect(endpoint: &str, device: &str) -> DevResult<Self> {
        let (scheme, native) = Scheme::parse(endpoint)?;
        #[cfg(feature = "detect")]
        let (scheme, native) =
            if scheme == Scheme::Auto { Scheme::parse(&detect::probe(&native).await?)? } else { (scheme, native) };
        Ok(match scheme {
            #[cfg(feature = "tcp")]
            Scheme::Tcp => Self::Tcp(tcp::Link::connect(&native).await?),
            #[cfg(feature = "zmq")]
            Scheme::Zmq => Self::Zmq(zmq::Link::connect(&native).await?),
            #[cfg(feature = "grpc")]
            Scheme::Grpc => Self::Grpc(grpc::Link::connect(&native).await?),
            #[cfg(feature = "zenoh")]
            Scheme::Zenoh => Self::Zenoh(zenoh::Link::connect(&native, device).await?),
            #[allow(unreachable_patterns)]
            _ => unreachable!("checked by Scheme::parse, a probe never answers auto"),
        })
    }

    #[allow(unreachable_code, unused_variables, irrefutable_let_patterns)] // gRPC as the only transport
    pub(crate) async fn call(&mut self, req: Request) -> DevResult<Reply> {
        // gRPC carries its own envelope, the byte transports carry the JSON frame
        #[cfg(feature = "grpc")]
        if let Self::Grpc(l) = self {
            return l.call(req).await;
        }
        let frame = serde_json::to_vec(&req)?;
        let bytes: Vec<u8> = match self {
            #[cfg(feature = "tcp")]
            Self::Tcp(l) => l.call(frame).await?,
            #[cfg(feature = "zmq")]
            Self::Zmq(l) => l.call(frame).await?,
            #[cfg(feature = "zenoh")]
            Self::Zenoh(l) => l.call(frame).await?,
            #[allow(unreachable_patterns)]
            _ => unreachable!("handled above"),
        };
        Ok(serde_json::from_slice(&bytes)?)
    }
}

#[cfg(test)]
mod tests {
    use super::Scheme;

    #[test]
    fn parse() {
        let p = |e| Scheme::parse(e).map_err(|e| e.reason().to_owned());
        assert_eq!(p("ftp://h:1"), Err("API_UnsupportedTransport".into()));
        if cfg!(feature = "tcp") {
            assert_eq!(p("127.0.0.1:1"), Ok((Scheme::Tcp, "127.0.0.1:1".into())));
            assert_eq!(p("tango://h:1"), Err("API_UnsupportedTransport".into()), "resolved by the client");
            assert_eq!(p("tcp://h:1"), Ok((Scheme::Tcp, "h:1".into())));
        } else {
            assert_eq!(p("tcp://h:1"), Err("API_UnsupportedTransport".into()));
        }
        if cfg!(feature = "zmq") {
            assert_eq!(p("zmq+tcp://h:1"), Ok((Scheme::Zmq, "tcp://h:1".into())));
            assert_eq!(p("zmq+ipc:///tmp/s"), Ok((Scheme::Zmq, "ipc:///tmp/s".into())));
        }
        if cfg!(feature = "zenoh") {
            assert_eq!(p("zenoh://tcp/h:1"), Ok((Scheme::Zenoh, "tcp/h:1".into())));
            assert_eq!(p("zenoh://"), Ok((Scheme::Zenoh, "".into())));
        }
    }
}

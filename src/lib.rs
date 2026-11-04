//! TRust: proof of concept of a Tango control system (RFC device model) whose
//! request-reply protocol runs over pluggable transports: TCP JSON Lines, ZeroMQ, gRPC, Zenoh,
//! selected by Cargo feature, by URL scheme or by detection of the wire. Also WebSocket, the
//! transport of the client in a web page: on wasm32 only the client side is compiled.
//! See `docs/SPEC.md`.

mod client;
mod db;
#[cfg(not(target_arch = "wasm32"))]
mod dbfile;
mod events;
mod props;
mod protocol;
pub mod rt;
#[cfg(not(target_arch = "wasm32"))]
mod server;
#[cfg(not(target_arch = "wasm32"))]
mod store;
mod transport;
mod types;

pub use client::DeviceProxy;
pub use db::{DATABASE_DEVICE, Database, hostname};
#[cfg(not(target_arch = "wasm32"))]
pub use dbfile::DbFile;
#[cfg(not(target_arch = "wasm32"))]
pub use events::EventSink;
pub use events::{Event, EventData, EventStream, EventType, LogLevel, Subscription};
pub use props::{PropInfo, Properties, parse_value, valid_property_name};
pub use protocol::*;
#[cfg(not(target_arch = "wasm32"))]
pub use server::{Device, Handler, Server, Started};
pub use transport::Scheme;
#[cfg(feature = "detect")]
pub use transport::detect::{Wire, sniff};
pub use types::*;

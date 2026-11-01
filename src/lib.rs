//! TRust: proof of concept of a Tango control system (RFC device model) whose
//! request-reply protocol runs over pluggable transports: TCP JSON Lines, ZeroMQ, gRPC, Zenoh,
//! selected by Cargo feature, by URL scheme or by detection of the wire.
//! See `docs/SPEC.md`.

mod client;
mod protocol;
mod server;
mod transport;
mod types;

pub use client::DeviceProxy;
pub use protocol::*;
pub use server::{Device, Handler, Server};
pub use transport::Scheme;
#[cfg(feature = "detect")]
pub use transport::detect::{Wire, sniff};
pub use types::*;

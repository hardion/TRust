//! TRust: proof of concept of a Tango control system (RFC device model) whose
//! request-reply protocol runs over Zenoh queries: each device is the queryable `tango/<device>`.
//! See `docs/SPEC.md`.

mod client;
mod protocol;
mod server;
mod types;
mod zenoh;

pub use client::DeviceProxy;
pub use protocol::*;
pub use server::{Device, Handler, Server};
pub use types::*;

//! TRust: proof of concept of a Tango control system (RFC device model) whose
//! request-reply protocol runs over ZeroMQ (ROUTER/REQ sockets) with one JSON frame per message.
//! See `docs/SPEC.md`.

mod client;
mod protocol;
mod server;
mod types;
mod zmq;

pub use client::DeviceProxy;
pub use protocol::*;
pub use server::{Device, Handler, Server};
pub use types::*;

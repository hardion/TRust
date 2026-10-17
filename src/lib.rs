//! TRust: proof of concept of a Tango control system (RFC device model) whose
//! request-reply protocol runs over gRPC (service `tango.v1.Device`, see `proto/tango.proto`).
//! See `docs/SPEC.md`.

mod client;
mod grpc;
mod protocol;
mod server;
mod types;

pub use client::DeviceProxy;
pub use protocol::*;
pub use server::{Device, Handler, Server};
pub use types::*;

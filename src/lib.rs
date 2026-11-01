//! TRust: proof of concept of a Tango control system (RFC device model) whose
//! request-reply protocol runs over a plain TCP socket with JSON Lines frames.
//! See `docs/SPEC.md`.

mod client;
mod protocol;
mod server;
mod types;

pub use client::DeviceProxy;
pub use protocol::*;
pub use server::{Device, Server};
pub use types::*;

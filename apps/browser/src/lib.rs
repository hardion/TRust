//! TRust Browser (docs/BROWSER.md): a device browser and Starter interface with forms inferred
//! from the device interface. Native (eframe) or in a web page (wasm32, over WebSocket).

// in a web page everything runs on one thread: shared state needs not be Send
#![cfg_attr(target_arch = "wasm32", allow(clippy::arc_with_non_send_sync))]

mod app;
#[cfg(not(target_arch = "wasm32"))]
pub mod demo;
pub mod form;

pub use app::{Browser, matches};

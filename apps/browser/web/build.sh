#!/bin/sh
# Builds the browser for a web page into apps/browser/web/pkg. Needs the wasm32-unknown-unknown
# target and wasm-bindgen-cli at the version of the wasm-bindgen crate in Cargo.lock:
#   rustup target add wasm32-unknown-unknown
#   cargo install wasm-bindgen-cli --version <version in Cargo.lock>
# Then serve apps/browser/web and open index.html?db=<TANGO_HOST>.
set -e
cd "$(dirname "$0")/../../.."
cargo build -p trust-browser --bin trust-browser --release --target wasm32-unknown-unknown
wasm-bindgen --target web --no-typescript --out-dir apps/browser/web/pkg \
    target/wasm32-unknown-unknown/release/trust-browser.wasm

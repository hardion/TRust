//! `DemoServer <instance> [-nodb] [-endpoint=<url>]`: `Demo` devices to try the browser on.

#[cfg(not(target_arch = "wasm32"))]
#[tokio::main]
async fn main() -> trust::DevResult<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    println!("DemoServer/{} started", args.first().map(String::as_str).unwrap_or_default());
    trust_browser::demo::server(args)?.run_args().await
}

/// A device server does not run in a web page.
#[cfg(target_arch = "wasm32")]
fn main() {}

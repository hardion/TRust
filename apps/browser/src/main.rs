//! `trust-browser [<TANGO_HOST>]`: the device browser, on the Database at the argument or
//! `$TANGO_HOST` (direct device URLs only without either).
//!
//! In a web page (wasm32): drawn in the `<canvas id="browser">` of the page, on the Database
//! given by `?db=host:port` (default: the page's host, port 10000), over WebSocket.

#[cfg(not(target_arch = "wasm32"))]
fn main() -> eframe::Result {
    let tango_host = std::env::args().nth(1).or_else(|| std::env::var("TANGO_HOST").ok());
    let title = format!("TRust Browser - {}", tango_host.as_deref().unwrap_or("no Database"));
    let options = eframe::NativeOptions {
        viewport: eframe::egui::ViewportBuilder::default().with_inner_size([1100.0, 700.0]),
        ..Default::default()
    };
    eframe::run_native(&title, options, Box::new(|_| Ok(Box::new(trust_browser::Browser::new(tango_host)))))
}

#[cfg(target_arch = "wasm32")]
fn main() {
    use eframe::wasm_bindgen::JsCast;
    let window = web_sys::window().expect("a window");
    let location = window.location();
    let query = web_sys::UrlSearchParams::new_with_str(&location.search().unwrap_or_default()).ok();
    let db = query.and_then(|q| q.get("db")).or_else(|| location.hostname().ok().map(|h| format!("{h}:10000")));
    let canvas = window
        .document()
        .and_then(|d| d.get_element_by_id("browser"))
        .and_then(|c| c.dyn_into::<web_sys::HtmlCanvasElement>().ok())
        .expect("a <canvas id=\"browser\"> in the page");
    wasm_bindgen_futures::spawn_local(async move {
        let app = Box::new(move |_: &eframe::CreationContext<'_>| -> Result<Box<dyn eframe::App>, _> {
            Ok(Box::new(trust_browser::Browser::new(db)))
        });
        if let Err(e) = eframe::WebRunner::new().start(canvas, eframe::WebOptions::default(), app).await {
            web_sys::console::error_1(&e);
        }
    });
}

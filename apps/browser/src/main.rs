//! `trust-browser [<TANGO_HOST>]`: the device browser, on the Database at the argument or
//! `$TANGO_HOST` (direct device URLs only without either).

fn main() -> eframe::Result {
    let tango_host = std::env::args().nth(1).or_else(|| std::env::var("TANGO_HOST").ok());
    let title = format!("TRust Browser — {}", tango_host.as_deref().unwrap_or("no Database"));
    let options = eframe::NativeOptions {
        viewport: eframe::egui::ViewportBuilder::default().with_inner_size([1100.0, 700.0]),
        ..Default::default()
    };
    eframe::run_native(&title, options, Box::new(|_| Ok(Box::new(trust_browser::Browser::new(tango_host)))))
}

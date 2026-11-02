//! `DataBaseds <instance> [-db=<sqlite file>] [-endpoint=<url>]`: the Database server, by default
//! on `tango.db` and `auto://0.0.0.0:10000` (every transport on the TANGO_HOST port).

fn arg(name: &str) -> Option<String> {
    std::env::args().find_map(|a| a.strip_prefix(&format!("-{name}=")).map(String::from))
}

#[tokio::main]
async fn main() -> trust::DevResult<()> {
    let db = arg("db").unwrap_or_else(|| "tango.db".into());
    let endpoint = arg("endpoint").unwrap_or_else(|| "auto://0.0.0.0:10000".into());
    let exported = endpoint.replace("0.0.0.0", &trust::hostname());
    println!("DataBaseds: {} on {endpoint} ({db})", trust_databaseds::DEVICE);
    trust_databaseds::server(std::path::Path::new(&db), &exported)?.run([endpoint]).await
}

//! `Starter [<host>] [-endpoint=<url>]`: the Starter of a host (default: this host), on the
//! Database at `$TANGO_HOST`.

#[tokio::main]
async fn main() -> trust::DevResult<()> {
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    if args.first().is_none_or(|a| a.starts_with('-')) {
        args.insert(0, trust::hostname());
    }
    let host = args[0].clone();
    let tango_host = std::env::var("TANGO_HOST")
        .map_err(|_| trust::DevFailed::new("API_TangoHostNotSet", "TANGO_HOST is not set", "Starter"))?;
    let starter = trust_starter::Starter::new(&tango_host, &host, std::time::Duration::from_secs(1));
    println!("Starter: {} on {tango_host}", trust_starter::device_name(&host));
    trust::Server::from_args("Starter", args)?
        .database(&tango_host)
        .device(&trust_starter::device_name(&host), starter)
        .run_args()
        .await
}

//! Round-trip latency of every transport, direct and through `auto://`, on localhost:
//! `cargo run --release --features full --example bench [round trips]`.

#[path = "../tests/common/mod.rs"]
mod common;

use std::time::{Duration, Instant};
use trust::{DeviceProxy, Value};

#[tokio::main]
async fn main() -> trust::DevResult<()> {
    let n: usize = std::env::args().nth(1).and_then(|a| a.parse().ok()).unwrap_or(5000);
    let [tcp, zmq, grpc, zenoh, auto] = [(); 5].map(|_| common::free_port());
    let server = [
        format!("tcp://127.0.0.1:{tcp}"),
        format!("zmq+tcp://127.0.0.1:{zmq}"),
        format!("grpc://127.0.0.1:{grpc}"),
        format!("zenoh://tcp/127.0.0.1:{zenoh}"),
        format!("auto://127.0.0.1:{auto}"),
    ];
    tokio::spawn(common::server().run(server));
    for port in [tcp, zmq, grpc, zenoh, auto] {
        while tokio::net::TcpStream::connect(("127.0.0.1", port)).await.is_err() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
    let clients = [
        ("TCP JSON Lines", format!("tcp://127.0.0.1:{tcp}")),
        ("ZeroMQ", format!("zmq+tcp://127.0.0.1:{zmq}")),
        ("gRPC", format!("grpc://127.0.0.1:{grpc}")),
        ("Zenoh", format!("zenoh://tcp/127.0.0.1:{zenoh}")),
        ("TCP JSON Lines via `auto://`", format!("tcp://127.0.0.1:{auto}")),
        ("ZeroMQ via `auto://`", format!("zmq+tcp://127.0.0.1:{auto}")),
        ("gRPC via `auto://`", format!("grpc://127.0.0.1:{auto}")),
        ("Zenoh via `auto://`", format!("zenoh://tcp/127.0.0.1:{auto}")),
    ];
    let us = |d: Duration| d.as_secs_f64() * 1e6;
    println!("| Transport ({n} round trips) | median | p99 | round trips/s |\n|---|---|---|---|");
    for (name, url) in clients {
        let mut dev = DeviceProxy::connect(&format!("{url}/{}", common::DEV)).await?;
        for i in 0..200 {
            dev.command_inout("DevDouble", Value::Double(i as f64)).await?;
        }
        let mut times = Vec::with_capacity(n);
        let start = Instant::now();
        for i in 0..n {
            let t = Instant::now();
            dev.command_inout("DevDouble", Value::Double(i as f64)).await?;
            times.push(t.elapsed());
        }
        let rate = n as f64 / start.elapsed().as_secs_f64();
        times.sort();
        println!("| {name} | {:.0} µs | {:.0} µs | {rate:.0} |", us(times[n / 2]), us(times[n * 99 / 100]));
    }
    Ok(())
}

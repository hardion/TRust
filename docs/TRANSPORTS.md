# Transports — implementations, comparison and abstraction

The device model of TRust (Tango RFC-2/3/4/7/9, the 21 requests of the
Request-Reply cheatsheet, `docs/SPEC.md` §2–§5) does not depend on the wire.
This document compares the request-reply transports that were built on it and
proposes the abstraction that lets one code base select them **at compile
time** and **at run time, by detection of the wire**.

## 1. Branches

| Branch | Transport | Client URL | Server endpoint |
|---|---|---|---|
| `claude/elegant-fermat-aowp8f` (PR #1) | TCP, JSON Lines | `host:port/sys/tg/1` | `TcpListener` |
| `claude/transport-core` | same, behind the transport seam (`Handler`, `Link`) | `host:port/sys/tg/1` | `TcpListener` |
| `claude/transport-zmq` | ZeroMQ ROUTER ⇄ REQ/DEALER | `zmq+tcp://host:port/sys/tg/1` | `tcp://0.0.0.0:5555` |
| `claude/transport-grpc` | gRPC `tango.v1.Device/Call` | `grpc://host:port/sys/tg/1` | `TcpListener` |
| `claude/transport-zenoh` | Zenoh queryable `tango/<device>` | `zenoh://tcp/host:7447/sys/tg/1`, `zenoh:///sys/tg/1` | `zenoh::Config` |
| `claude/transport-abstraction` | all of the above, Cargo features + wire detection | any of the above, `auto://host:port/…` | `run(["tcp://…", "grpc://…", "auto://…"])` |

Each transport branch is the core branch plus one module of 40–80 lines; the
validation suite (24 tests, one group per cheatsheet row) is unchanged and
passes on every branch. Each branch has its own `tests/wire.rs` driving the
protocol without `DeviceProxy`, and each wire was checked against an
independent implementation in another language:

| Transport | Interoperability checked with |
|---|---|
| TCP JSON Lines | `nc`, Python `socket` |
| ZeroMQ | libzmq 4.3.5 through pyzmq, `REQ` and `DEALER` sockets |
| gRPC | Python `grpcio`, stubs generated from `proto/tango.proto` |
| Zenoh | Python `eclipse-zenoh`, explicit locator and multicast discovery |

## 2. Comparison

| | TCP JSON Lines | ZeroMQ | gRPC | Zenoh |
|---|---|---|---|---|
| Wire | `\n` framed JSON | ZMTP 3 frames | HTTP/2 + protobuf envelope | Zenoh protocol |
| Rust deps | none | `zeromq` (pure Rust, no libzmq) | `tonic`, `prost`, `hyper` (+ vendored `protoc` at build) | `zenoh` (largest) |
| Transport module | 40 lines | 46 lines | 81 lines + 22 lines `.proto` | 75 lines |
| Locating a device | address | address | address | **by name**: key expression, routers, multicast scouting |
| Several requests on one connection | in order, pipelined | `DEALER`: any order, matched by `id` | HTTP/2 multiplexing | independent queries |
| Publish-subscribe (events, RFC-12) | — | yes, Tango 9's event transport | server streaming | native, plus storages (history, memorised values) |
| Web clients | — | — | grpc-web through a proxy | WebSocket link + TypeScript API |
| Security | to add (TLS) | CURVE | TLS / mTLS, standard auth | TLS, mTLS, access control |
| Ecosystem | `nc` | every language | grpcurl, load balancers, tracing | `zenohd`, ROS 2 (`rmw_zenoh`), MQTT/DDS bridges, zenoh-pico for MCUs |
| Runtime constraint | — | — | — | multi-threaded tokio runtime |

Latency, measured with `cargo run --release --features full --example bench`
(sequential `DevDouble` round trips on localhost, same machine for client and
server):

| Transport (5000 round trips) | median | p99 | round trips/s |
|---|---|---|---|
| TCP JSON Lines | 41 µs | 86 µs | 23 101 |
| ZeroMQ | 75 µs | 134 µs | 12 613 |
| gRPC | 170 µs | 341 µs | 5 203 |
| Zenoh | 218 µs | 434 µs | 4 294 |
| TCP JSON Lines via `auto://` | 40 µs | 72 µs | 23 896 |
| ZeroMQ via `auto://` | 91 µs | 179 µs | 10 154 |
| gRPC via `auto://` | 181 µs | 373 µs | 4 911 |
| Zenoh via `auto://` | 257 µs | 562 µs | 3 591 |

How to read these numbers: one sequential client measures latency, not
capacity. They come from a 4 vCPU cloud container, so compare the rows with
each other and treat the absolute values as indicative. The TCP transport
handles a request inline; the other three hand it to tokio's blocking pool
so that a slow device does not stall the socket, which costs a thread hop
of a few tens of µs. Detection adds nothing for TCP and gRPC, which adopt
the socket, and one loopback hop for ZeroMQ and Zenoh, which are spliced.
Before `TCP_NODELAY` was set on the splice, Nagle's algorithm combined with
delayed ACKs pushed the spliced Zenoh p99 to 44 ms; with it set, the
spliced path stays within 20 % of the direct one.

### Recommendation

* **Zenoh** is the most relevant choice for a next-generation Tango.
  Device names map onto its key space, so it also takes over the
  *locating* part of the Tango database. The same session gives
  request-reply, events and storages, and it already sits at the centre of
  robotics middleware (ROS 2). Its costs are dependency weight and the
  multi-threaded runtime.
* **gRPC** is the integration choice: standard tooling, TLS/auth, load
  balancing, and clients in every language. A fully typed `.proto` (one
  `oneof` per `Op` and `Value`) is the next step if schema-checked clients
  matter more than code size.
* **ZeroMQ** provides continuity with Tango 9, whose events already use it,
  and adds the fewest dependencies beyond plain TCP.
* **TCP JSON Lines** stays as the reference wire: zero dependencies, and
  it can be debugged with `nc`.

Because no single transport wins on every axis, the abstraction below keeps
all four: you pick by compilation, by URL, or by what the wire says.

## 3. The abstraction (`claude/transport-abstraction`)

```
 Device trait ─▶ Entry (per-device state, black box, polling)       src/server.rs
                    ▲
 Handler  ── frame(&[u8], peer) -> Vec<u8>    (JSON frame in/out)
          └─ call(Request, peer) -> Reply     (typed, for gRPC's own envelope)
                    ▲
 transport::serve(endpoint URL, Handler)      │  transport::Link::connect(endpoint URL)
   tcp · zmq · grpc · zenoh · detect          │    Link::call(Request) -> Reply
                    ▲                                  ▲
               Server::run([...])               DeviceProxy
```

The only contract a transport fulfils is "carry one request frame to
`Handler` and carry its reply back". On the server that is a `serve`
function; on the client it is a `Link` with `connect` and `call`. Per-device
serialisation, the black box, polling and Tango error semantics stay in the
model, so every transport inherits them, and the shared validation suite
proves it.

### 3.1 By compilation: Cargo features

```toml
[features]
default = ["tcp"]
tcp = []
zmq = ["dep:zeromq"]
grpc = ["dep:tonic", "dep:tonic-prost", "dep:prost", "dep:tokio-stream",
        "dep:tonic-prost-build", "dep:protoc-bin-vendored"]
zenoh = ["dep:zenoh"]
detect = ["tcp"]
full = ["tcp", "zmq", "grpc", "zenoh", "detect"]
```

* A disabled transport costs no code and no dependency: its module, its
  `Link` variant and its `serve` arm are `#[cfg]`-ed out, and `build.rs`
  only runs `protoc` when `grpc` is on.
* Using a scheme that is not compiled in fails with
  `API_UnsupportedTransport` and names the feature to enable. A server
  checks every endpoint before binding any of them.
* Building with no transport at all is a `compile_error!`.
* Every combination (`tcp`, `zmq`, `grpc`, `zenoh` alone, `detect` plus
  any of them, `full`) builds without warnings and passes its tests.

The client side uses enum dispatch (`enum Link { Tcp(..), Zmq(..), … }`), not
`dyn Trait`. It needs no boxing and no `async-trait`, and the compiler checks
that every compiled transport is handled.

### 3.2 By URL

| Scheme | Transport | Native endpoint |
|---|---|---|
| none, `tcp://`, `tango://` | TCP JSON Lines | `host:port` |
| `zmq+tcp://`, `zmq+ipc://` | ZeroMQ | `tcp://host:port`, `ipc://path` |
| `grpc://` | gRPC | `host:port` |
| `zenoh://<locator>` | Zenoh | `tcp/host:7447`; empty = multicast discovery |
| `auto://` | wire detection | `host:port` |

A client URL is `<endpoint>/domain/family/member`; the device name is always
the last three segments, so endpoints may contain `/`. A server can serve
the same devices on several endpoints at once:

```rust
server.run(["tcp://0.0.0.0:10000", "grpc://0.0.0.0:50051", "zenoh://"]).await?;
```

This is the migration path: old and new clients talk to the same device
state during a transition.

### 3.3 By detection of the wire

**Server, `auto://host:port`: one port for every compiled TCP-based wire.**
All four protocols are *client speaks first*, so the server peeks at the
first bytes of each connection (without consuming them) and dispatches:

| First bytes | Wire | Dispatch |
|---|---|---|
| optional whitespace, then `{` | JSON Lines | handled in place by the TCP transport |
| `0xFF` | ZMTP greeting | spliced to an internal loopback ROUTER |
| `PRI * HTTP/2.0` | HTTP/2 preface | the socket is handed to the tonic server (`serve_with_incoming`) |
| `len len 0x?1` | Zenoh INIT message after its `u16` length | spliced to an internal loopback Zenoh listener |
| anything else | unknown | closed |

Every signature is positive; nothing is matched as "whatever is left". An
earlier version did exactly that, and it failed: a 32-byte Zenoh InitSyn
starts with `0x20 0x00`, and `0x20` is an ASCII space, which was taken for
JSON. The Rust Zenoh client's 25-byte InitSyn (`0x19`) hid the bug. The
Python client exposed it, and both byte sequences are now unit tests.
The final check served `nc`, pyzmq (libzmq), grpcio and eclipse-zenoh
clients from one `auto://` port.

**Client, `auto://host:port`: probe.** ZeroMQ and HTTP/2 servers speak
first (a ZMTP greeting `ff…7f`, an HTTP/2 SETTINGS frame with type `0x04`
in byte 3), and a JSON Lines server stays silent. One probe connection with
a 100 ms read therefore identifies the server. A silent server is treated as
JSON Lines, which a detecting server also accepts.

The validation suite runs over 11 combinations: each transport directly,
each client wire against an `auto://` server, and `auto://` clients against
TCP, ZeroMQ and gRPC servers. That is 264 tests, plus one test where every
wire writes and reads the same device through one port.

Limits of detection:

* A Zenoh server cannot be probed (it waits for the client), so a Zenoh
  client needs `zenoh://`. On the server side, `auto://` does detect Zenoh.
* A probe costs one extra connection and up to 100 ms when the server is
  silent. An explicit scheme avoids both.
* For ZeroMQ and Zenoh, `auto://` adds one loopback hop. Neither crate can
  adopt an already-accepted socket, which tonic and the TCP transport can.
* **TLS** hides the first bytes. The extension is to detect the TLS
  ClientHello (`0x16`), terminate TLS, then dispatch on the negotiated
  **ALPN** protocol (`h2`, `zmtp`, `tango-json`, …). That is the standard
  way to share a TLS port.

### 3.4 Next steps

* **Codec as a second axis.** JSON is used by every transport today, but
  the same seam takes a binary codec (CBOR or MessagePack) with no other
  change. First-byte detection still works: a JSON frame starts with `{`, a
  CBOR map with `0xA0–0xBF`.
* **Out-of-tree transports.** If third parties must add transports without
  touching the crate, replace the `Link` enum with a registry of
  `Box<dyn Transport>` keyed by scheme:

  ```rust
  pub trait Transport: Send + Sync + 'static {
      fn schemes(&self) -> &'static [&'static str];
      fn serve(&self, endpoint: &str, handler: Handler) -> BoxFuture<'static, DevResult<()>>;
      fn connect(&self, endpoint: &str, device: &str) -> BoxFuture<'static, DevResult<Box<dyn Link>>>;
  }
  ```

  Each call then pays for one boxed future, and the enum's exhaustiveness
  check is lost. For transports kept in this crate, the enum is simpler.
* **Events.** ZeroMQ PUB/SUB, gRPC server streaming and Zenoh
  publishers fit behind the same split (`Handler` plus a subscription
  sink), so RFC-12 can follow the same plan.

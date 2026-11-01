//! The validation suite (tests/common/suite.rs) over every compiled transport: directly, through a
//! wire-detecting server (`auto://` on the server) and through client probing (`auto://` on the
//! client). `{port}` is replaced by a free port.

mod common;

macro_rules! over {
    ($($name:ident if $cfg:meta: $server:literal => $client:literal;)*) => {$(
        #[cfg($cfg)]
        mod $name {
            async fn start() -> String {
                crate::common::start($server, $client).await
            }
            include!("common/suite.rs");
        }
    )*};
}

over! {
    tcp if feature = "tcp": "tcp://127.0.0.1:{port}" => "tcp://127.0.0.1:{port}";
    zmq if feature = "zmq": "zmq+tcp://127.0.0.1:{port}" => "zmq+tcp://127.0.0.1:{port}";
    grpc if feature = "grpc": "grpc://127.0.0.1:{port}" => "grpc://127.0.0.1:{port}";
    zenoh if feature = "zenoh": "zenoh://tcp/127.0.0.1:{port}" => "zenoh://tcp/127.0.0.1:{port}";
    detected_tcp if feature = "detect": "auto://127.0.0.1:{port}" => "tcp://127.0.0.1:{port}";
    detected_zmq if all(feature = "detect", feature = "zmq"): "auto://127.0.0.1:{port}" => "zmq+tcp://127.0.0.1:{port}";
    detected_grpc if all(feature = "detect", feature = "grpc"): "auto://127.0.0.1:{port}" => "grpc://127.0.0.1:{port}";
    detected_zenoh if all(feature = "detect", feature = "zenoh"): "auto://127.0.0.1:{port}" => "zenoh://tcp/127.0.0.1:{port}";
    probed_tcp if feature = "detect": "tcp://127.0.0.1:{port}" => "auto://127.0.0.1:{port}";
    probed_zmq if all(feature = "detect", feature = "zmq"): "zmq+tcp://127.0.0.1:{port}" => "auto://127.0.0.1:{port}";
    probed_grpc if all(feature = "detect", feature = "grpc"): "grpc://127.0.0.1:{port}" => "auto://127.0.0.1:{port}";
}

//! The event suite (tests/common/events_suite.rs) over every compiled transport, directly and
//! through a wire-detecting server. `{port}` is replaced by a free port.

mod common;

macro_rules! over {
    ($($name:ident if $cfg:meta: $server:literal => $client:literal;)*) => {$(
        #[cfg($cfg)]
        mod $name {
            async fn start() -> String {
                crate::common::start($server, $client).await
            }
            include!("common/events_suite.rs");
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
    ws if feature = "ws": "ws://127.0.0.1:{port}" => "ws://127.0.0.1:{port}";
    tcp_as_ws if all(feature = "tcp", feature = "ws"): "tcp://127.0.0.1:{port}" => "ws://127.0.0.1:{port}";
    detected_ws if all(feature = "detect", feature = "ws"): "auto://127.0.0.1:{port}" => "ws://127.0.0.1:{port}";
}

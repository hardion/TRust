//! Wire-level tests: the `tango.v1.Device` gRPC service driven with the bare generated stub,
//! as a client in any other language would, without `DeviceProxy`.

mod common;

use common::{DEV, start};
use serde_json::{Value as Json, json};

mod pb {
    tonic::include_proto!("tango.v1");
}

type Client = pb::device_client::DeviceClient<tonic::transport::Channel>;

async fn client() -> Client {
    let url = start().await;
    Client::connect(url.replace("grpc://", "http://")).await.unwrap()
}

async fn call(client: &mut Client, v: u32, id: u64, op: &[u8]) -> (u64, Json) {
    let req = pb::Request { v, id, client: 42, source: pb::Source::CacheDev as i32, device: DEV.into(), op: op.into() };
    let reply = client.call(req).await.unwrap().into_inner();
    (reply.id, serde_json::from_slice(&reply.reply).unwrap())
}

#[tokio::test]
async fn generated_stub() {
    let mut c = client().await;
    assert_eq!(call(&mut c, 1, 7, br#""DeviceName""#).await, (7, json!({"Ok": {"String": DEV}})));
    let op = br#"{"CommandExecution": {"name": "DevDouble", "arg": {"Double": 2.5}}}"#;
    assert_eq!(call(&mut c, 1, 8, op).await, (8, json!({"Ok": {"Value": {"Double": 2.5}}})));
    let (_, r) = call(&mut c, 1, 9, br#"{"AttributesRead": {"names": ["State"]}}"#).await;
    assert_eq!(r["Ok"]["Attributes"][0]["Ok"]["value"], json!({"State": "ON"}));
}

#[tokio::test]
async fn protocol_errors() {
    let mut c = client().await;
    let (id, r) = call(&mut c, 1, 3, b"this is not json").await;
    assert_eq!((id, r["Err"][0]["reason"].as_str()), (3, Some("API_InvalidRequest")));
    let (_, r) = call(&mut c, 99, 4, br#""DevicePing""#).await;
    assert_eq!(r["Err"][0]["reason"], "API_UnsupportedProtocol");
    assert_eq!(call(&mut c, 1, 5, br#""DevicePing""#).await, (5, json!({"Ok": "Void"})));
}

#[tokio::test]
async fn calls_are_multiplexed_on_one_channel() {
    let c = client().await;
    let calls = (0..50u64).map(|i| {
        let mut c = c.clone();
        async move {
            call(
                &mut c,
                1,
                i,
                format!(r#"{{"CommandExecution": {{"name": "DevDouble", "arg": {{"Double": {i}}}}}}}"#).as_bytes(),
            )
            .await
        }
    });
    for (i, (id, r)) in futures_join(calls).await.into_iter().enumerate() {
        assert_eq!(id, i as u64);
        assert_eq!(r["Ok"]["Value"]["Double"], json!(i as f64));
    }
}

#[tokio::test]
async fn black_box_records_grpc_peer() {
    let url = start().await;
    let mut dev = trust::DeviceProxy::connect(&format!("{url}/{DEV}")).await.unwrap();
    assert!(dev.black_box(1).await.unwrap()[0].host.starts_with("127.0.0.1:"));
}

async fn futures_join<F: Future + Send + 'static>(fs: impl Iterator<Item = F>) -> Vec<F::Output>
where
    F::Output: Send,
{
    let handles: Vec<_> = fs.map(tokio::spawn).collect();
    let mut out = vec![];
    for h in handles {
        out.push(h.await.unwrap());
    }
    out
}

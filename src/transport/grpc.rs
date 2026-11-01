//! gRPC transport: the unary service `tango.v1.Device/Call` (proto/tango.proto).
//! The envelope is a protobuf message, the operation and its result stay JSON (SPEC §3).

use crate::protocol::{Op, Reply, Request};
use crate::server::Handler;
use crate::types::{DevFailed, DevResult, Source};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Response, Status, transport::Channel};

mod pb {
    tonic::include_proto!("tango.v1");
}

impl From<tonic::transport::Error> for DevFailed {
    fn from(e: tonic::transport::Error) -> Self {
        DevFailed::new("API_CommunicationFailed", e.to_string(), "grpc")
    }
}

impl From<Status> for DevFailed {
    fn from(s: Status) -> Self {
        DevFailed::new("API_CommunicationFailed", s.message(), format!("grpc {:?}", s.code()))
    }
}

impl From<pb::Source> for Source {
    fn from(s: pb::Source) -> Self {
        match s {
            pb::Source::Dev => Source::Dev,
            pb::Source::Cache => Source::Cache,
            pb::Source::CacheDev => Source::CacheDev,
        }
    }
}

impl From<Source> for pb::Source {
    fn from(s: Source) -> Self {
        match s {
            Source::Dev => pb::Source::Dev,
            Source::Cache => pb::Source::Cache,
            Source::CacheDev => pb::Source::CacheDev,
        }
    }
}

struct Service(Handler);

#[tonic::async_trait]
impl pb::device_server::Device for Service {
    async fn call(&self, request: tonic::Request<pb::Request>) -> Result<Response<pb::Reply>, Status> {
        let peer = request.remote_addr().map_or_else(|| "grpc".into(), |a| a.to_string());
        let (r, handler) = (request.into_inner(), self.0.clone());
        let id = r.id;
        let reply = tokio::task::spawn_blocking(move || match serde_json::from_slice::<Op>(&r.op) {
            Ok(op) => {
                let source = r.source().into();
                handler.call(Request { v: r.v, id, client: r.client, source, device: r.device, op }, &peer).reply
            }
            Err(e) => Err(e.into()),
        })
        .await
        .map_err(|e| Status::internal(e.to_string()))?;
        let reply = serde_json::to_vec(&reply).map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(pb::Reply { id, reply }))
    }
}

pub(crate) async fn serve(listener: TcpListener, handler: Handler) -> DevResult<()> {
    serve_incoming(TcpListenerStream::new(listener), handler).await
}

/// Serves connections accepted elsewhere (wire detection).
pub(crate) async fn serve_incoming<S>(incoming: S, handler: Handler) -> DevResult<()>
where
    S: tokio_stream::Stream<Item = std::io::Result<tokio::net::TcpStream>> + Send + 'static,
{
    let service = pb::device_server::DeviceServer::new(Service(handler));
    tonic::transport::Server::builder().add_service(service).serve_with_incoming(incoming).await?;
    Ok(())
}

pub(crate) struct Link(pb::device_client::DeviceClient<Channel>);

impl Link {
    /// `endpoint` is `host:port`.
    pub(crate) async fn connect(endpoint: &str) -> DevResult<Self> {
        Ok(Self(pb::device_client::DeviceClient::connect(format!("http://{endpoint}")).await?))
    }

    pub(crate) async fn call(&mut self, req: Request) -> DevResult<Reply> {
        let op = serde_json::to_vec(&req.op)?;
        let source = pb::Source::from(req.source) as i32;
        let msg = pb::Request { v: req.v, id: req.id, client: req.client, source, device: req.device, op };
        let reply = self.0.call(msg).await?.into_inner();
        Ok(Reply { id: reply.id, reply: serde_json::from_slice(&reply.reply)? })
    }
}

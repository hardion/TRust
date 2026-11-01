//! TCP transport: one JSON frame per line.

use crate::client::bad_reply;
use crate::server::Handler;
use crate::types::DevResult;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};

pub(crate) async fn serve(listener: TcpListener, handler: Handler) -> DevResult<()> {
    loop {
        let (stream, peer) = listener.accept().await?;
        let handler = handler.clone();
        tokio::spawn(async move {
            let (r, mut w) = stream.into_split();
            let mut lines = BufReader::new(r).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let mut reply = handler.frame(line.as_bytes(), &peer.to_string());
                reply.push(b'\n');
                if w.write_all(&reply).await.is_err() {
                    break;
                }
            }
        });
    }
}

pub(crate) struct Link {
    reader: Lines<BufReader<OwnedReadHalf>>,
    writer: OwnedWriteHalf,
}

impl Link {
    pub(crate) async fn connect(addr: &str) -> DevResult<Self> {
        let (reader, writer) = TcpStream::connect(addr).await?.into_split();
        Ok(Self { reader: BufReader::new(reader).lines(), writer })
    }

    pub(crate) async fn call(&mut self, mut frame: Vec<u8>) -> DevResult<Vec<u8>> {
        frame.push(b'\n');
        self.writer.write_all(&frame).await?;
        let line = self.reader.next_line().await?.ok_or_else(|| bad_reply("connection closed"))?;
        Ok(line.into_bytes())
    }
}

use std::net::SocketAddr;

use async_trait::async_trait;
use bytes::Bytes;
use log::error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio_stream::StreamExt;
use tokio_util::codec::{FramedRead, FramedWrite};

use redis_codec_core::resp_decoder::RespPktDecoder;

use crate::tiny_client::TinyClient;

pub struct SessionAttr {
    pub db: u64,
}

pub struct ClientConn {
    pub id: u64,
    pub r: FramedRead<OwnedReadHalf, RespPktDecoder>,
    pub w: OwnedWriteHalf,
    pub session_attr: SessionAttr,
}

impl ClientConn {}

pub struct ClientConnManager {
    addr: SocketAddr,
}

impl ClientConnManager {
    pub fn new(addr: SocketAddr) -> Self {
        Self { addr }
    }
}

pub type Pool = bb8::Pool<ClientConnManager>;
// pub type Conn<'a> = bb8::PooledConnection<'a, ClientConnManager>;
pub type Conn = ClientConn;

#[async_trait]
impl bb8::ManageConnection for ClientConnManager {
    type Connection = ClientConn;
    type Error = anyhow::Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let mut res = TcpStream::connect(self.addr).await?;
        let (r, mut w) = res.into_split();
        let mut r = FramedRead::new(r, RespPktDecoder::new());

        let mut conn = ClientConn { id: 0, r, w, session_attr: SessionAttr { db: 0 } };
        let ok = TinyClient::query(&mut conn, b"*2\r\n$4\r\nAUTH\r\n$8\r\nfoobared\r\n").await?;
        if !ok {
            return Err(anyhow::anyhow!("auth failed"));
        }
        Ok(conn)
    }


    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        let ok = TinyClient::query(conn, b"*1\r\n$4\r\nPING\r\n").await?;
        error!("checking valid, ok:{}", ok);

        if ok {
            Ok(())
        } else {
            Err(anyhow::anyhow!("invalid connection"))
        }
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        false
    }
}
use std::net::SocketAddr;

use async_trait::async_trait;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio_util::codec::FramedRead;

use redis_codec_core::resp_decoder::RespPktDecoder;

use crate::tiny_client::TinyClient;

pub struct SessionAttr {
    pub db: u64,
    pub password: Option<String>,
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
    password: Option<String>,
}

impl ClientConnManager {
    pub fn new(addr: SocketAddr) -> Self {
        Self { addr, password: None }
    }
}

pub type Pool = bb8::Pool<ClientConnManager>;
pub type Conn = ClientConn;

#[async_trait]
impl bb8::ManageConnection for ClientConnManager {
    type Connection = ClientConn;
    type Error = anyhow::Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let mut res = TcpStream::connect(self.addr).await?;
        let (r, mut w) = res.into_split();
        let mut r = FramedRead::new(r, RespPktDecoder::new());

        let mut conn = ClientConn { id: 0, r, w, session_attr: SessionAttr { db: 0, password: self.password.clone() } };

        if let Some(ref pass) = self.password {
            let cmd = format!("*2\r\n$4\r\nAUTH\r\n${}\r\n{}\r\n", pass.len(), pass);
            let ok = TinyClient::query(&mut conn, cmd.as_bytes()).await?;
            if !ok {
                return Err(anyhow::anyhow!("auth failed"));
            }
        }
        Ok(conn)
    }


    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        // only cares about if the connection is still alive, not the actual result(eg noauth)
        let query_result = TinyClient::query(conn, b"*1\r\n$4\r\nPING\r\n").await;
        if query_result.is_err() {
            return Err(anyhow::anyhow!("ping failed"));
        }
        Ok(())
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        false
    }
}
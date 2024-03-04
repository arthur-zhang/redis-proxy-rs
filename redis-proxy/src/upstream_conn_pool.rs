use std::fmt::{Debug, Display, Formatter};
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;

use anyhow::{anyhow, bail};
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::SinkExt;
use log::{error, info};
use poolx::{Connection, ConnectOptions, Error, PoolConnection};
use poolx::url::Url;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, FramedRead};

use redis_codec_core::req_decoder::ReqPktDecoder;
use redis_codec_core::resp_decoder::RespPktDecoder;
use redis_proxy_common::cmd::CmdType;
use redis_proxy_common::ReqFrameData;

use crate::proxy::Session;
use crate::tiny_client::TinyClient;

pub type Pool = poolx::Pool<RedisConnection>;

#[derive(Debug, Default)]
pub struct SessionAttr {
    pub db: u64,
    pub password: Option<String>,
}


#[derive(Debug)]
pub struct RedisConnectionOption {
    counter: AtomicU64,
    addr: SocketAddr,
    password: Option<String>,
}

impl Clone for RedisConnectionOption {
    fn clone(&self) -> Self {
        Self {
            counter: Default::default(),
            addr: self.addr,
            password: self.password.clone(),
        }
    }
}

pub struct RedisConnection {
    pub id: u64,
    pub is_authed: bool,
    pub r: FramedRead<OwnedReadHalf, RespPktDecoder>,
    pub w: OwnedWriteHalf,
    pub session_attr: SessionAttr,
}

impl RedisConnection {
    async fn rebuild_session(&mut self, db: u64) -> anyhow::Result<()> {
        let db_index = format!("{}", db);
        let cmd = format!("*2\r\n$6\r\nselect\r\n${}\r\n{}\r\n", db_index.len(), db_index);
        let ok = TinyClient::query(self, cmd.as_bytes()).await?;
        self.session_attr.db = db;
        if !ok {
            return Err(anyhow!("rebuild session failed"));
        }
        Ok(())
    }

    // get password and db from session, auth connection if needed
    pub async fn init_from_session(&mut self, session: &mut Session) -> anyhow::Result<bool> {
        self.session_attr.password = session.downstream_session.password.clone();
        if self.session_attr.db != session.downstream_session.db {
            info!("rebuild session from {} to {}", self.session_attr.db,  session.downstream_session.db);
            self.rebuild_session(session.downstream_session.db).await?;
        }
        if session.cmd_type() != CmdType::AUTH {
            return self.auth_connection_if_needed(session).await;
        }
        Ok(false)
    }
    async fn auth_connection(&mut self, pass: &str) -> anyhow::Result<bool> {
        let cmd = format!("*2\r\n$4\r\nAUTH\r\n${}\r\n{}\r\n", pass.len(), pass);
        let (query_ok, resp_data) = TinyClient::query_with_resp(self, cmd.as_bytes()).await?;
        if !query_ok || resp_data.len() == 0 {
            return Ok(false);
        }
        let mut data = bytes::BytesMut::with_capacity(resp_data.first().unwrap().len());
        for d in resp_data {
            data.extend_from_slice(&d);
        }
        return Ok(data.as_ref() == b"+OK\r\n");
    }
    async fn auth_connection_if_needed(
        &mut self,
        session: &mut Session,
    ) -> anyhow::Result<bool> {
        match (self.is_authed, session.downstream_session.is_authed) {
            (true, true) | (false, false) => {}
            (false, true) => {
                // auth connection
                let authed = self.auth_connection(session.downstream_session.password.as_ref().unwrap()).await?;
                if authed {
                    self.is_authed = true;
                } else {
                    bail!("auth failed");
                }
            }
            (true, false) => {
                // connection is auth, but ctx is not auth, should return no auth
                if let Some(ref header_frame) = session.downstream_session.header_frame {
                    if header_frame.is_done {
                        session.downstream_session.underlying_stream.send(Bytes::from_static(b"-NOAUTH Authentication required nimei.\r\n")).await?;
                    }
                }
                return Ok(true);
            }
        }
        return Ok(false);
    }
}

impl Display for RedisConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RedisConnection({})", self.id)
    }
}

impl Debug for RedisConnection {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

impl Connection for RedisConnection {
    type Options = RedisConnectionOption;

    fn close(self) -> BoxFuture<'static, Result<(), Error>> {
        Box::pin(async move {
            Ok(())
        })
    }

    fn close_hard(self) -> BoxFuture<'static, Result<(), Error>> {
        Box::pin(async move {
            Ok(())
        })
    }

    fn ping(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            Ok(())
        })
    }
}

impl FromStr for RedisConnectionOption {
    type Err = poolx::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let url = s.parse::<Url>().map_err(|e| poolx::Error::Configuration(Box::new(e)))?;
        Self::from_url(&url)
    }
}

impl ConnectOptions for RedisConnectionOption {
    type Connection = RedisConnection;

    fn from_url(url: &Url) -> Result<Self, Error> {
        // todo
        let host = url.host_str().unwrap();
        let port = url.port().unwrap_or(6379);
        let password = url.password().map(|s| s.to_string());
        let addr = format!("{}:{}", host, port).parse::<SocketAddr>().map_err(|e| poolx::Error::Configuration(Box::new(e)))?;
        Ok(Self { counter: AtomicU64::new(0), addr, password })
    }

    fn connect(&self) -> BoxFuture<'_, Result<Self::Connection, Error>> where Self::Connection: Sized {
        Box::pin({
            let addr = self.addr;
            async move {
                error!("connect to redis: {:?}", addr);
                let (r, w) = tokio::net::TcpStream::connect(addr).await.map_err(|e| poolx::Error::Io(std::io::Error::from(e)))?.into_split();

                let mut conn = RedisConnection {
                    id: self.counter.fetch_add(1, Relaxed),
                    is_authed: false,
                    r: FramedRead::new(r, RespPktDecoder::new()),
                    w,
                    session_attr: SessionAttr::default(),
                };

                if let Some(ref pass) = self.password {
                    let cmd = format!("*2\r\n$4\r\nAUTH\r\n${}\r\n{}\r\n", pass.len(), pass);
                    // todo
                    let ok = TinyClient::query(&mut conn, cmd.as_bytes()).await;
                    error!("auth .............. {:?}", ok);

                    return match ok {
                        Ok(true) => {
                            conn.is_authed = true;
                            Ok(conn)
                        }
                        _ => {
                            Err(poolx::Error::ResponseError)
                        }
                    };
                }
                Ok(conn)
            }
        })
    }
}



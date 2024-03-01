use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;

use futures::future::BoxFuture;
use log::error;
use poolx::{Connection, ConnectOptions, Error};
use poolx::url::Url;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio_util::codec::FramedRead;

use redis_codec_core::resp_decoder::RespPktDecoder;

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
                    }
                }
                Ok(conn)
            }
        })
    }
}



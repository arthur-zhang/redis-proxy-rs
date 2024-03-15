use std::fmt::{Debug, Display, Formatter};
use std::io::IoSlice;
use std::str::FromStr;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;

use anyhow::{anyhow, bail};
use bytes::{Bytes, BytesMut};
use futures::future::BoxFuture;
use futures::SinkExt;
use log::{debug, info};
use poolx::{Connection, ConnectOptions, Error};
use poolx::url::quirks::password;
use poolx::url::Url;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio_stream::StreamExt;
use tokio_util::codec::FramedRead;

use redis_codec_core::resp_decoder::RespPktDecoder;
use redis_proxy_common::cmd::CmdType;

use crate::proxy::Session;

pub type Pool = poolx::Pool<RedisConnection>;

#[derive(Debug, Default)]
pub struct SessionAttr {
    pub db: u64,
    pub password: Option<Vec<u8>>,
}


#[derive(Debug)]
pub struct RedisConnectionOption {
    counter: AtomicU64,
    addr: String,
    password: Option<String>,
}

impl Clone for RedisConnectionOption {
    fn clone(&self) -> Self {
        Self {
            counter: Default::default(),
            addr: self.addr.clone(),
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

#[derive(PartialOrd, PartialEq)]
pub enum AuthStatus {
    Authed,
    AuthFailed,
}

impl RedisConnection {
    async fn select_db(&mut self, db: u64) -> anyhow::Result<()> {
        let db_index = format!("{}", db);
        let cmd = format!("*2\r\n$6\r\nselect\r\n${}\r\n{}\r\n", db_index.len(), db_index);
        let (ok, _) = self.query_with_resp(cmd.as_bytes()).await?;
        if !ok {
            return Err(anyhow!("rebuild session failed"));
        }
        self.session_attr.db = db;
        Ok(())
    }

    // get password and db from session, auth connection if needed
    pub async fn init_from_session(&mut self, cmd_type: CmdType, session_authed: bool,
                                   password: &Option<Vec<u8>>, session_db: u64) -> anyhow::Result<AuthStatus> {
        if cmd_type != CmdType::AUTH {
            let auth_status = self.auth_connection_if_needed_v2(session_authed, password).await?;
            if matches!(auth_status, AuthStatus::AuthFailed) {
                return Ok(AuthStatus::AuthFailed);
            }
            self.is_authed = true;
        }
        self.session_attr.password = password.clone();
        if self.session_attr.db != session_db {
            info!("rebuild session from {} to {}", self.session_attr.db,  session_db);
            self.select_db(session_db).await?;
        }
        Ok(AuthStatus::Authed)
    }
    async fn auth_connection(&mut self, pass: &[u8]) -> anyhow::Result<bool> {
        let pass_len = pass.len().to_string();
        let mut cmd = BytesMut::with_capacity(b"*2\r\n$4\r\nAUTH\r\n$".len() + pass_len.len() + 2 + pass.len() + 2);
        cmd.extend_from_slice(b"*2\r\n$4\r\nAUTH\r\n$");
        cmd.extend_from_slice(pass_len.as_bytes());
        cmd.extend_from_slice(b"\r\n");
        cmd.extend_from_slice(pass);
        cmd.extend_from_slice(b"\r\n");
        let (query_ok, resp_data) = self.query_with_resp(cmd.as_ref()).await?;

        if !query_ok || resp_data.len() == 0 {
            return Ok(false);
        }

        return Ok(resp_data.as_ref() == b"+OK\r\n");
    }


    async fn auth_connection_if_needed_v2(
        &mut self,
        // session: &mut Session,
        session_authed: bool,
        password: &Option<Vec<u8>>,
    ) -> anyhow::Result<AuthStatus> {
        debug!("auth connection if needed: conn authed: {}, session authed: {}", self.is_authed, session_authed);

        return match (self.is_authed, session_authed) {
            (true, true) | (false, false) => {
                Ok(AuthStatus::Authed)
            }
            (false, true) => {
                // auth connection
                let password = password.as_ref().cloned().unwrap();
                let authed = self.auth_connection(&password).await?;
                if authed {
                    self.is_authed = true;
                    Ok(AuthStatus::Authed)
                } else {
                    Ok(AuthStatus::AuthFailed)
                }
            }
            (true, false) => {
                // connection is auth, but ctx is not auth, should return no auth
                Ok(AuthStatus::AuthFailed)
            }
        };
    }

    async fn auth_connection_if_needed(
        &mut self,
        session: &mut Session,
    ) -> anyhow::Result<bool> {
        debug!("auth connection if needed: conn authed: {}, session authed: {}", self.is_authed, session.is_authed);

        match (self.is_authed, session.is_authed) {
            (true, true) | (false, false) => {}
            (false, true) => {
                // auth connection
                let authed = self.auth_connection(session.password.as_ref().unwrap()).await?;
                if authed {
                    self.is_authed = true;
                } else {
                    bail!("auth failed");
                }
            }
            (true, false) => {
                // connection is auth, but ctx is not auth, should return no auth
                session.underlying_stream.send(Bytes::from_static(b"-NOAUTH Authentication required.\r\n")).await?;
                return Ok(true);
            }
        }
        return Ok(false);
    }

    pub async fn query_with_resp_vec<'a>(&mut self, vec: Vec<Bytes>) -> anyhow::Result<(bool, Vec<Bytes>)> {
        let ios = vec.iter().map(|bytes| IoSlice::new(&bytes)).collect::<Vec<_>>();
        self.w.write_vectored(&ios).await?;

        let mut result = vec![];
        while let Some(it) = self.r.next().await {
            match it {
                Ok(it) => {
                    let is_done = it.is_done;
                    result.push(it.data);
                    // bytes.extend_from_slice(&it.data);
                    if is_done {
                        return Ok((it.res_is_ok, result));
                    }
                }
                Err(e) => {
                    bail!("read error {:?}", e);
                }
            }
        }
        bail!("read eof");
    }
    pub async fn query_with_resp(&mut self, data: &[u8]) -> anyhow::Result<(bool, Bytes)> {
        let mut bytes = bytes::BytesMut::new();
        self.w.write_all(data.as_ref()).await?;
        while let Some(it) = self.r.next().await {
            match it {
                Ok(it) => {
                    bytes.extend_from_slice(&it.data);
                    if it.is_done {
                        return Ok((it.res_is_ok, bytes.freeze()));
                    }
                }
                Err(e) => {
                    bail!("read error {:?}", e);
                }
            }
        }
        bail!("read eof");
    }
}

impl Display for RedisConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RedisConnection[{}]: is_authed:{}, attrs: {:?}", self.id, self.is_authed, self.session_attr)
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
        // todo fix unwrap
        let host = url.host_str().unwrap();
        let port = url.port().unwrap_or(6379);
        let password = url.password().map(|s| s.to_string());
        let addr = format!("{}:{}", host, port);
        Ok(Self { counter: AtomicU64::new(0), addr, password })
    }

    fn connect(&self) -> BoxFuture<'_, Result<Self::Connection, Error>> where Self::Connection: Sized {
        Box::pin({
            let addr = self.addr.clone();
            async move {
                let conn = tokio::net::TcpStream::connect(addr).await.map_err(|e| poolx::Error::Io(std::io::Error::from(e)))?;
                conn.set_nodelay(true).unwrap();
                let (r, w) = conn.into_split();

                let mut conn = RedisConnection {
                    id: self.counter.fetch_add(1, Relaxed),
                    is_authed: false,
                    r: FramedRead::new(r, RespPktDecoder::new()),
                    w,
                    session_attr: SessionAttr::default(),
                };

                if let Some(ref pass) = self.password {
                    let cmd = format!("*2\r\n$4\r\nAUTH\r\n${}\r\n{}\r\n", pass.len(), pass);

                    return match conn.query_with_resp(cmd.as_bytes()).await {
                        Ok((true, _)) => {
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

#[cfg(test)]
mod tests {
    use tokio::net::TcpStream;

    use super::*;

    #[tokio::test]
    async fn test_query() -> anyhow::Result<()> {
        let inner = TcpStream::connect("127.0.0.1:6379").await?;
        let (r, w) = inner.into_split();

        let mut conn = RedisConnection {
            id: 0,
            is_authed: false,
            r: FramedRead::new(r, RespPktDecoder::new()),
            w,
            session_attr: SessionAttr::default(),
        };

        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
        let resp = conn.query_with_resp("*1\r\n$4\r\nPING\r\n".as_bytes()).await;
        println!("resp: {:?}", resp);
        Ok(())
    }

    #[test]
    fn test_parse_url() {
        let url = "redis://redis-mdc-sync1-6638-master.paas.abc.com:9999";
        let url = url.parse::<Url>().unwrap();
        println!("url: {:?}", url);
    }
}


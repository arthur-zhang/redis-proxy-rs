use std::fmt::{Debug, Display, Formatter};
use std::io::IoSlice;
use std::str::FromStr;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;

use anyhow::{anyhow, bail};
use bytes::{Bytes, BytesMut};
use futures::future::BoxFuture;
use log::{debug, info};
use poolx::{Connection, ConnectOptions, Error};
use poolx::url::Url;
use smol_str::SmolStr;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio_stream::StreamExt;
use tokio_util::codec::FramedRead;

use redis_codec_core::resp_decoder::{ResFramedData, RespPktDecoder};
use redis_proxy_common::command::utils::CMD_TYPE_AUTH;
use redis_proxy_common::ReqPkt;

use crate::prometheus::{CONN_UPSTREAM, METRICS};

pub type Pool = poolx::Pool<RedisConnection>;

#[derive(Debug, Default)]
pub struct SessionAttr {
    pub db: u64,
    pub username: Option<Vec<u8>>,
    pub password: Option<Vec<u8>>,
}


#[derive(Debug)]
pub struct RedisConnectionOption {
    counter: AtomicU64,
    addr: String,
    username: Option<String>,
    password: Option<String>,
}

impl Clone for RedisConnectionOption {
    fn clone(&self) -> Self {
        Self {
            counter: Default::default(),
            addr: self.addr.clone(),
            username: self.username.clone(),
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
    pub async fn init_from_session(&mut self, cmd_type: &SmolStr, session_authed: bool, username: &Option<Vec<u8>>,
                                   password: &Option<Vec<u8>>, session_db: u64) -> anyhow::Result<AuthStatus> {
        if !CMD_TYPE_AUTH.eq(cmd_type) {
            let auth_status = self.auth_connection_if_needed(session_authed, username, password).await?;
            if matches!(auth_status, AuthStatus::AuthFailed) {
                return Ok(AuthStatus::AuthFailed);
            }
        }
        self.session_attr.password = password.clone();
        self.session_attr.username = username.clone();
        if self.session_attr.db != session_db {
            info!("rebuild session from {} to {}", self.session_attr.db,  session_db);
            self.select_db(session_db).await?;
        }
        Ok(AuthStatus::Authed)
    }
    async fn auth_connection(&mut self, username: &Option<Vec<u8>>, password: &Option<Vec<u8>>) -> anyhow::Result<bool> {
        if username.is_some() {
            return self.auth_connection_with_username(username, password).await
        }
        
        let password = password.as_ref().unwrap();
        let pass_len = password.len().to_string();
        
        let mut cmd = BytesMut::with_capacity(b"*2\r\n$4\r\nAUTH\r\n$".len() + pass_len.len() + 2 + password.len() + 2);
        cmd.extend_from_slice(b"*2\r\n$4\r\nAUTH\r\n$");
        cmd.extend_from_slice(pass_len.as_bytes());
        cmd.extend_from_slice(b"\r\n");
        cmd.extend_from_slice(password);
        cmd.extend_from_slice(b"\r\n");
        let (query_ok, resp_data) = self.query_with_resp(cmd.as_ref()).await?;

        if !query_ok || resp_data.len() == 0 {
            return Ok(false);
        }

        return Ok(resp_data.as_ref() == b"+OK\r\n");
    }

    async fn auth_connection_with_username(&mut self, username: &Option<Vec<u8>>, password: &Option<Vec<u8>>) -> anyhow::Result<bool> {
        let password = password.as_ref().unwrap();
        let pass_len = password.len().to_string();
        
        let username = username.as_ref().unwrap();
        let username_len = username.len().to_string();

        let mut cmd = BytesMut::with_capacity(
            b"*3\r\n$4\r\nAUTH\r\n$".len() 
                + username_len.len() + 2 + username.len() + 2
                + 1 + pass_len.len() + 2 + password.len() + 2);
        cmd.extend_from_slice(b"*3\r\n$4\r\nAUTH\r\n$");
        cmd.extend_from_slice(username_len.as_bytes());
        cmd.extend_from_slice(b"\r\n");
        cmd.extend_from_slice(username);
        cmd.extend_from_slice(b"\r\n");
        cmd.extend_from_slice(b"$");
        cmd.extend_from_slice(pass_len.as_bytes());
        cmd.extend_from_slice(b"\r\n");
        cmd.extend_from_slice(password);
        cmd.extend_from_slice(b"\r\n");
        let (query_ok, resp_data) = self.query_with_resp(cmd.as_ref()).await?;

        if !query_ok || resp_data.len() == 0 {
            return Ok(false);
        }

        return Ok(resp_data.as_ref() == b"+OK\r\n");
    }

    async fn auth_connection_if_needed(
        &mut self,
        session_authed: bool,
        username: &Option<Vec<u8>>,
        password: &Option<Vec<u8>>,
    ) -> anyhow::Result<AuthStatus> {
        debug!("auth connection if needed: conn authed: {}, session authed: {}", self.is_authed, session_authed);
        return match (self.is_authed, session_authed) {
            (true, true) | (false, false) => {
                Ok(AuthStatus::Authed)
            }
            (false, true) => {
                // auth connection
                let authed = self.auth_connection(username, password).await?;
                if authed {
                    self.is_authed = true;
                    Ok(AuthStatus::Authed)
                } else {
                    Ok(AuthStatus::AuthFailed)
                }
            }
            (true, false) => {
                // connection is auth, but session is not auth, should return no auth
                Ok(AuthStatus::AuthFailed)
            }
        };
    }

    pub async fn send_bytes_vectored(&mut self, pkt: &ReqPkt) -> anyhow::Result<()> {
        let bytes_vec = &pkt.bulk_args;
        let mut iov: Vec<IoSlice> = Vec::with_capacity(bytes_vec.len());

        iov.push(IoSlice::new(b"*"));
        let cmd_len = bytes_vec.len().to_string();
        iov.push(IoSlice::new(cmd_len.as_bytes()));
        iov.push(IoSlice::new(b"\r\n"));
        let tmp_len_arr = bytes_vec.iter().map(|it| SmolStr::from(it.len().to_string())).collect::<Vec<SmolStr>>();
        for (i, bytes) in bytes_vec.iter().enumerate() {
            iov.push(IoSlice::new(b"$"));
            iov.push(IoSlice::new(tmp_len_arr[i].as_bytes()));
            iov.push(IoSlice::new(b"\r\n"));
            iov.push(IoSlice::new(bytes.as_ref()));
            iov.push(IoSlice::new(b"\r\n"));
        }

        write_all_vectored(&mut self.w, &mut iov).await?;

        Ok(())
    }

    pub async fn send_bytes_vectored_and_wait_resp(&mut self, pkt: &ReqPkt) -> anyhow::Result<(bool, Vec<ResFramedData>, usize)> {
        self.send_bytes_vectored(pkt).await?;
        let mut result: Vec<ResFramedData> = Vec::with_capacity(1);
        let mut total_size = 0;

        while let Some(it) = self.r.next().await {
            match it {
                Ok(it) => {
                    let is_done = it.is_done;
                    let res_is_ok = it.res_is_ok;
                    total_size += it.data.len();
                    result.push(it);
                    if is_done {
                        return Ok((res_is_ok, result, total_size));
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
        self.w.write_all(data).await?;
        let mut bytes: Vec<Bytes> = Vec::with_capacity(1);
        while let Some(it) = self.r.next().await {
            match it {
                Ok(it) => {
                    let is_done = it.is_done;
                    bytes.push(it.data);
                    if is_done {
                        return if bytes.len() == 1 {
                            let data = bytes.pop().unwrap();
                            Ok((it.res_is_ok, data))
                        } else {
                            Ok((it.res_is_ok, Bytes::from(bytes.concat())))
                        };
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

/// A helper function that performs a vector write to completion, since
/// the `tokio` one is not guaranteed to write all the data.
async fn write_all_vectored<'a, W: AsyncWrite + Unpin>(
    w: &'a mut W,
    mut slices: &'a mut [IoSlice<'a>],
) -> tokio::io::Result<()> {
    let mut n: usize = slices.iter().map(|s| s.len()).sum();

    loop {
        let mut did_write = w.write_vectored(slices).await?;

        if did_write == n {
            // Done, yay
            break Ok(());
        }

        n -= did_write;

        // Not done, need to advance the slices
        while did_write >= slices[0].len() {
            // First skip entire slices
            did_write -= slices[0].len();
            slices = &mut slices[1..];
        }

        // Skip a partial buffer
        slices[0].advance(did_write);
    }
}

impl Display for RedisConnection {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
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
        METRICS.connections.with_label_values(&[CONN_UPSTREAM]).dec();
        Box::pin(async move {
            Ok(())
        })
    }

    fn close_hard(self) -> BoxFuture<'static, Result<(), Error>> {
        METRICS.connections.with_label_values(&[CONN_UPSTREAM]).dec();
        Box::pin(async move {
            Ok(())
        })
    }

    fn ping(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            self.query_with_resp(b"*1\r\n$4\r\nPING\r\n").await?;
            return Ok(());
        })
    }
}

impl FromStr for RedisConnectionOption {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let url = s.parse::<Url>().map_err(|e| anyhow!("parse url error: {:?}", e))?;
        Self::from_url(&url)
    }
}

impl ConnectOptions for RedisConnectionOption {
    type Connection = RedisConnection;

    fn from_url(url: &Url) -> Result<Self, Error> {
        // todo fix unwrap
        let host = url.host_str().unwrap();
        let port = url.port().unwrap_or(6379);
        let username = url.username();
        let username = if username.is_empty() { None } else { Some(username.to_string()) };
        let password = url.password().map(|s| s.to_string());
        let addr = format!("{}:{}", host, port);
        Ok(Self { counter: AtomicU64::new(0), addr, password, username })
    }

    fn connect(&self) -> BoxFuture<'_, Result<Self::Connection, Error>> where Self::Connection: Sized {
        Box::pin({
            let addr = self.addr.clone();
            async move {
                let conn = tokio::net::TcpStream::connect(addr).await.map_err(|e| anyhow!("connect error :{:?}", e))?;
                conn.set_nodelay(true).unwrap();
                METRICS.connections.with_label_values(&[CONN_UPSTREAM]).inc();
                let (r, w) = conn.into_split();

                let mut conn = RedisConnection {
                    id: self.counter.fetch_add(1, Relaxed),
                    is_authed: false,
                    r: FramedRead::new(r, RespPktDecoder::new()),
                    w,
                    session_attr: SessionAttr::default(),
                };

                if let Some(ref pass) = self.password {
                    let cmd = if let Some(ref user) = self.username {
                        format!("*3\r\n$4\r\nAUTH\r\n${}\r\n{}\r\n${}\r\n{}\r\n", user.len(), user, pass.len(), pass)
                    } else {
                        format!("*2\r\n$4\r\nAUTH\r\n${}\r\n{}\r\n", pass.len(), pass)
                    };

                    return match conn.query_with_resp(cmd.as_bytes()).await {
                        Ok((true, _)) => {
                            conn.is_authed = true;
                            Ok(conn)
                        }
                        _ => {
                            Err(Error::ResponseError)
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


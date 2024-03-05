use std::fmt::format;
use std::net::SocketAddr;
use std::os::macos::raw::stat;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{anyhow, bail};
use bytes::Bytes;
use futures::SinkExt;
use log::{debug, error, info};
use poolx::{PoolConnection, PoolOptions};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{mpsc, oneshot};
use tokio::sync::mpsc::{Receiver, Sender, unbounded_channel};
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, FramedRead};

use redis_codec_core::req_decoder::ReqPktDecoder;
use redis_codec_core::resp_decoder::{ResFramedData, RespPktDecoder};
use redis_proxy_common::cmd::CmdType;
use redis_proxy_common::ReqFrameData;

use crate::config::{Blacklist, Config, Mirror, TConfig};
use crate::proxy::{Proxy, RedisProxy, RedisSession, Session};
use crate::tiny_client::TinyClient;
use crate::upstream_conn_pool::{Pool, RedisConnection, RedisConnectionOption};

pub struct ProxyServer<P> {
    config: TConfig,
    // filter_chain: TFilterChain,
    proxy: P,
}

impl<P> ProxyServer<P> where P: Proxy + Send + Sync + 'static, <P as Proxy>::CTX: Send + Sync {
    pub fn new(config: Arc<Config>, proxy: P) -> anyhow::Result<Self> {
        Ok(ProxyServer { config, proxy })
    }

    pub async fn start(self) -> anyhow::Result<()> {
        let listener = TcpListener::bind(&self.config.server.address).await.map_err(|e| {
            error!("bind error: {:?}", e);
            e
        })?;

        let conn_option = self.config.upstream.address.parse::<RedisConnectionOption>().unwrap();
        let pool: poolx::Pool<RedisConnection> = PoolOptions::new()
            .idle_timeout(std::time::Duration::from_secs(3))
            .min_connections(3)
            .max_connections(50000)
            .connect_lazy_with(conn_option);

        let app_logic = Arc::new(RedisProxy { inner: self.proxy, upstream_pool: pool.clone() });
        loop {
            tokio::spawn({
                let (c2p_conn, _) = listener.accept().await?;
                // one connection per task
                let pool = pool.clone();

                let framed = Framed::new(c2p_conn, ReqPktDecoder::new());
                let mut session = Session { downstream_session: RedisSession { underlying_stream: framed, header_frame: None, password: None, db: 0, is_authed: false } };
                let app_logic = app_logic.clone();
                async move {
                    loop {
                        if let Some(s) = app_logic.handle_new_request(session, pool.clone()).await? {
                            session = s;
                        } else {
                            break;
                        }
                    }
                    info!("session done");
                    Ok::<_, anyhow::Error>(())
                }
            });
        };
    }
}


pub const TASK_BUFFER_SIZE: usize = 4;


#[derive(Debug)]
pub enum ProxyChanData {
    ReqFrameData(ReqFrameData),
    ResFrameData(ResFramedData),
}


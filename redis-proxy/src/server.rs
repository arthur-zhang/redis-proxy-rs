use std::sync::Arc;

use log::{debug, error, info};
use poolx::PoolOptions;
use tokio::net::TcpListener;
use tokio_util::codec::Framed;

use redis_codec_core::req_decoder::ReqPktDecoder;
use redis_codec_core::resp_decoder::ResFramedData;
use redis_proxy_common::ReqFrameData;

use crate::config::{Config, TConfig};
use crate::prometheus::METRICS;
use crate::proxy::{Proxy, RedisProxy, Session};
use crate::upstream_conn_pool::{RedisConnection, RedisConnectionOption};

pub struct ProxyServer<P> {
    name: &'static str,
    config: TConfig,
    proxy: P,
}

impl<P> ProxyServer<P> where P: Proxy + Send + Sync + 'static, <P as Proxy>::CTX: Send + Sync {
    pub fn new(config: Arc<Config>, proxy: P) -> anyhow::Result<Self> {
        Ok(ProxyServer { name: "redis_proxy_rs", config, proxy })
    }

    pub async fn start(self) -> anyhow::Result<()> {
        let listener = TcpListener::bind(&self.config.server.address).await.map_err(|e| {
            error!("bind error: {:?}", e);
            e
        })?;

        let conn_option = self.config.upstream.address.parse::<RedisConnectionOption>().unwrap();
        let pool: poolx::Pool<RedisConnection> = PoolOptions::new()
            .idle_timeout(std::time::Duration::from_secs(30))
            .min_connections(100)
            .max_connections(50000)
            .connect_lazy_with(conn_option);

        let app_logic = Arc::new(RedisProxy { inner: self.proxy, upstream_pool: pool.clone() });
        loop {
            // one connection per task
            let (c2p_conn, peer_addr) = listener.accept().await?;
            c2p_conn.set_nodelay(true).unwrap();
            debug!("session start: {:?}", peer_addr);

            let framed = Framed::new(c2p_conn, ReqPktDecoder::new());
            let mut session = Session::new(framed);
            let app_logic = app_logic.clone();
            let pool = pool.clone();
            tokio::spawn(async move {
                METRICS.connections.with_label_values(&[self.name]).inc();
                loop {
                    match app_logic.handle_new_request(session, pool.clone()).await {
                        Some(s) => {
                            session = s
                        }
                        None => {
                            break;
                        }
                    }
                }
                METRICS.connections.with_label_values(&[self.name]).dec();
                debug!("session done: {:?}", peer_addr);
                Ok::<_, anyhow::Error>(())
            }
            );
        };
    }
}


pub const TASK_BUFFER_SIZE: usize = 4;


#[derive(Debug)]
pub enum ProxyChanData {
    None,
    ReqFrameData(ReqFrameData),
    ResFrameData(ResFramedData),
}


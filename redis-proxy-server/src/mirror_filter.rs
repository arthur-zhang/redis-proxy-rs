use std::sync::Arc;
use anyhow::bail;
use async_trait::async_trait;
use poolx::PoolOptions;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_stream::StreamExt;
use redis_proxy::config::EtcdConfig;

use redis_proxy::proxy::{Proxy, Session};
use redis_proxy::router::RouterManager;
use redis_proxy::upstream_conn_pool::{Pool, RedisConnection, RedisConnectionOption};
use redis_proxy_common::ReqFrameData;

use crate::filter_trait::{FilterContext, Value};

pub struct Mirror {
    router_manager: Arc<RouterManager>,
    pool: Pool,
}

const DATA_TX: &'static str = "mirror_filter_data_tx";
const SHOULD_MIRROR: &'static str = "mirror_filter_should_mirror";

impl Mirror {
    pub async fn new(mirror: &str, etcd_config: EtcdConfig) -> anyhow::Result<Self> {
        let router_manager = RouterManager::new(etcd_config, String::from("mirror")).await?;
        let conn_option = mirror.parse::<RedisConnectionOption>().unwrap();
        let pool: poolx::Pool<RedisConnection> = PoolOptions::new()
            .idle_timeout(std::time::Duration::from_secs(3))
            .min_connections(3)
            .max_connections(50000)
            .connect_lazy_with(conn_option);

        Ok(Self { router_manager, pool })
    }
    fn should_mirror(&self, req_frame_data: &ReqFrameData) -> bool {
        let args = req_frame_data.args();
        if let Some(key) = args {
            for key in key {
                return self.router_manager.get_router().get(key, req_frame_data.cmd_type).is_some();
            }
        }
        return false;
    }
}

#[async_trait]
impl Proxy for Mirror {
    type CTX = FilterContext;

    async fn proxy_upstream_filter(&self, session: &mut Session, ctx: &mut Self::CTX) -> anyhow::Result<bool> {
        let data = session.header_frame.as_ref().unwrap();
        let should_mirror = if data.cmd_type.is_connection_command() {
            true
        } else if data.cmd_type.is_read_cmd() {
            false
        } else {
            self.should_mirror(&data)
        };
        if !should_mirror {
            return Ok(false);
        }
        let mut conn = self.pool.acquire().await?;
        conn.init_from_session(session).await?;

        let (tx, mut rx): (Sender<bytes::Bytes>, Receiver<bytes::Bytes>) = tokio::sync::mpsc::channel(100);
        ctx.attrs.insert(DATA_TX.to_string(), Value::ChanSender(tx));

        ctx.set_attr(SHOULD_MIRROR, Value::Bool(should_mirror));
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    res = conn.r.next() => {
                        match res {
                            Some(Ok(it)) => {
                                // continue
                                if it.is_done {
                                    return Ok::<_, anyhow::Error>(());
                                }
                             }
                            Some(Err(e)) => {
                                bail!("read error: {:?}", e)
                            }
                            None => {
                                return Ok::<_, anyhow::Error>(());
                            }
                        }
                    }
                    req = rx.recv() => {
                        match req {
                            Some(data) => {
                                conn.w.write_all(&data).await?;
                            }
                            None => {
                                return Ok::<_, anyhow::Error>(());
                            }
                        }
                    }
                }
            }
        });
        Ok(false)
    }
    async fn upstream_request_filter(&self, _session: &mut Session, upstream_request: &mut ReqFrameData, ctx: &mut Self::CTX) -> anyhow::Result<()> {
        let should_mirror = ctx.get_attr_as_bool(SHOULD_MIRROR).unwrap_or(false);
        if !should_mirror {
            return Ok(());
        }
        let tx = ctx.get_attr_as_sender(DATA_TX).unwrap();
        tx.send(upstream_request.raw_bytes.clone()).await?;
        Ok(())
    }
}
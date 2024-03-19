use std::sync::Arc;

use log::{debug};
use tokio::net::TcpListener;

use redis_codec_core::resp_decoder::ResFramedData;
use redis_proxy_common::ReqFrameData;

use crate::config::{Config, TConfig};
use crate::double_writer::DoubleWriter;
use crate::etcd_client::EtcdClient;
use crate::prometheus::{CONN_DOWNSTREAM, METRICS};
use crate::proxy::{Proxy, RedisProxy};
use crate::session::Session;
use crate::upstream_conn_pool::{RedisConnection, RedisConnectionOption};

pub struct ProxyServer<P> {
    config: TConfig,
    etcd_client: Option<EtcdClient>,
    proxy: P,
}

impl<P> ProxyServer<P> where P: Proxy + Send + Sync + 'static, <P as Proxy>::CTX: Send + Sync {
    pub fn new(config: Arc<Config>, proxy: P, etcd_client: Option<EtcdClient>) -> anyhow::Result<Self> {
        Ok(ProxyServer { config, etcd_client, proxy })
    }

    pub async fn start(self) -> anyhow::Result<()> {
        let listener = TcpListener::bind(&self.config.server.address).await?;

        let conn_option = self.config.upstream.address.parse::<RedisConnectionOption>().unwrap();
        let pool: poolx::Pool<RedisConnection> = self.config.upstream.conn_pool_conf
            .new_pool_opt()
            .connect_lazy_with(conn_option);

        let double_writer = DoubleWriter::new(
            self.config.splitter.unwrap_or(':'), 
            String::from("double_write"), 
            &self.config.double_write, 
            self.etcd_client.clone()).await?;
        
        let app_logic = Arc::new(RedisProxy { 
            inner: self.proxy, 
            upstream_pool: pool.clone(),
            double_writer 
        });
        loop {
            // one connection per task
            let (c2p_conn, peer_addr) = listener.accept().await?;
            c2p_conn.set_nodelay(true).unwrap();
            METRICS.connections.with_label_values(&[CONN_DOWNSTREAM]).inc();
            debug!("session start: {:?}", peer_addr);

            let mut session = Session::new(c2p_conn);
            let app_logic = app_logic.clone();
            tokio::spawn(async move {
                loop {
                    match app_logic.handle_new_request(session).await {
                        Some(s) => {
                            session = s
                        }
                        None => {
                            break;
                        }
                    }
                }
                METRICS.connections.with_label_values(&[CONN_DOWNSTREAM]).dec();
                debug!("session done: {:?}", peer_addr);
                Ok::<_, anyhow::Error>(())
            }
            );
        };
    }
}


#[derive(Debug)]
pub enum ProxyChanData {
    None,
    ReqFrameData(ReqFrameData),
    ResFrameData(ResFramedData),
}


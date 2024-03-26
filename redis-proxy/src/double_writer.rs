use std::sync::Arc;

use poolx::{Error, PoolConnection};

use redis_proxy_common::ReqPkt;

use crate::config::GenericUpstream;
use crate::etcd_client::EtcdClient;
use crate::router::{create_router, Router};
use crate::upstream_conn_pool::{Pool, RedisConnection, RedisConnectionOption};

pub struct DoubleWriter {
    router: Arc<dyn Router>,
    pool: Pool,
}

impl DoubleWriter {
    pub async fn new(
        splitter: char,
        router_name: String,
        upstream_conf: &GenericUpstream,
        etcd_client: Option<EtcdClient>,
    ) -> anyhow::Result<Self> {
        let router = create_router(
            splitter,
            router_name,
            upstream_conf.config_center,
            upstream_conf.local_routes.clone(),
            etcd_client).await?;
        let conn_option = upstream_conf.address.parse::<RedisConnectionOption>().unwrap();
        let pool: poolx::Pool<RedisConnection> = upstream_conf.conn_pool_conf.new_pool_opt()
            .connect_lazy_with(conn_option);

        Ok(Self { router, pool })
    }
    pub fn should_double_write(&self, req_frame_data: &ReqPkt) -> bool {
        let keys = req_frame_data.keys();
        if let Some(key) = keys {
            for key in key {
                return self.router.match_route(key, req_frame_data.cmd_type);
            }
        }
        return false;
    }

    pub async fn acquire_conn(&self) -> Result<PoolConnection<RedisConnection>, Error> {
        self.pool.acquire().await
    }
}
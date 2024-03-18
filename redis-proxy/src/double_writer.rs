use std::sync::Arc;

use poolx::{Error, PoolConnection};

use redis_proxy_common::ReqFrameData;

use crate::config::{EtcdConfig, GenericUpstream};
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
        etcd_config: Option<EtcdConfig>
    ) -> anyhow::Result<Self> {
        let router = create_router(
            splitter,
            router_name,
            upstream_conf.config_center,
            upstream_conf.local_routes.clone(),
            etcd_config).await?;
        let conn_option = upstream_conf.address.parse::<RedisConnectionOption>().unwrap();
        let pool: poolx::Pool<RedisConnection> = upstream_conf.conn_pool_conf.new_pool_opt().connect_lazy_with(conn_option);

        Ok(Self { router, pool })
    }
    pub fn should_double_write(&self, req_frame_data: &ReqFrameData) -> bool {
        let args = req_frame_data.args();
        if let Some(key) = args {
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
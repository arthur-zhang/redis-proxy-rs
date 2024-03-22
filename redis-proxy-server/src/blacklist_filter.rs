use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;

use redis_proxy::config::Blacklist;
use redis_proxy::etcd_client::EtcdClient;
use redis_proxy::filter_trait::{Filter, FilterContext};
use redis_proxy::router::{create_router, Router};
use redis_proxy::session::Session;
use redis_proxy_common::ReqPkt;

pub struct BlackListFilter {
    router: Arc<dyn Router>,
}

impl BlackListFilter {
    pub async fn new(splitter: char, blacklist_conf: &Blacklist, etcd_client: Option<EtcdClient>) -> anyhow::Result<Self> {
        let router = create_router(
            splitter,
            String::from("blacklist"),
            blacklist_conf.config_center,
            blacklist_conf.local_routes.clone(),
            etcd_client).await?;

        Ok(BlackListFilter { router })
    }
}


#[async_trait]
impl Filter for BlackListFilter {

    async fn on_request(&self, session: &mut Session, req_pkt: &ReqPkt, _ctx: &mut FilterContext) -> anyhow::Result<bool> {
        let keys = req_pkt.keys();
        if let Some(keys) = keys {
            for key in keys {
                if self.router.match_route(key, &req_pkt.cmd_type) {
                    session.send_resp_to_downstream(Bytes::from_static(b"-ERR black list\r\n")).await?;
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }
}
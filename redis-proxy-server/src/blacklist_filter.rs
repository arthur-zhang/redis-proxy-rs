use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;

use redis_proxy::config::Blacklist;
use redis_proxy::etcd_client::EtcdClient;
use redis_proxy::proxy::Proxy;
use redis_proxy::router::{create_router, Router};
use redis_proxy::session::Session;

use crate::filter_trait::FilterContext;

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
impl Proxy for BlackListFilter {
    type CTX = FilterContext;

    async fn request_filter(&self, session: &mut Session, _ctx: &mut Self::CTX) -> anyhow::Result<bool> {
        let req_frame = session.header_frame.as_ref().unwrap();
        let args = req_frame.args();
        if let Some(args) = args {
            for key in args {
                if self.router.match_route(key, req_frame.cmd_type) {
                    session.send_resp_to_downstream(Bytes::from_static(b"-ERR black list\r\n")).await?;
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }
}
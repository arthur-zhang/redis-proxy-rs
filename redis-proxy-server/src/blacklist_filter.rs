use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::SinkExt;

use redis_proxy::config::{Blacklist, EtcdConfig};
use redis_proxy::proxy::{Proxy, Session};
use redis_proxy::router::{create_router, Router};

use crate::filter_trait::FilterContext;

pub struct BlackListFilter {
    router: Arc<dyn Router>,
}

impl BlackListFilter {
    pub async fn new(splitter: char, blacklist_conf: &Blacklist, etcd_config: Option<EtcdConfig>) -> anyhow::Result<Self> {
        let router = create_router(
            splitter,
            String::from("blacklist"),
            blacklist_conf.config_center,
            blacklist_conf.local_routes.clone(),
            etcd_config).await?;
        
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
                    session.underlying_stream.send(Bytes::from_static(b"-ERR black list\r\n")).await?;
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }
}
use std::sync::Arc;
use async_trait::async_trait;
use bytes::Bytes;
use futures::SinkExt;
use redis_proxy::config::EtcdConfig;

use redis_proxy::proxy::{Proxy, Session};
use redis_proxy::router::RouterManager;

use crate::filter_trait::FilterContext;

pub struct BlackListFilter {
    router_manager: Arc<RouterManager>,
}

impl BlackListFilter {
    pub async fn new(etcd_config: EtcdConfig) -> anyhow::Result<Self> {
        let router_manager = RouterManager::new(etcd_config, String::from("blacklist")).await?;
        Ok(BlackListFilter { router_manager })
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
                if self.router_manager.get_router().get(key, req_frame.cmd_type).is_some() {
                    session.underlying_stream.send(Bytes::from_static(b"-ERR black list\r\n")).await?;
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }
}
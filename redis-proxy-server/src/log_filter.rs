use std::time::Instant;

use anyhow::Error;
use async_trait::async_trait;
use log::info;

use redis_proxy::proxy::{Proxy, Session};
use redis_proxy_common::ReqFrameData;

use crate::filter_trait::FilterContext;

pub struct LogFilter {}

#[async_trait]
impl Proxy for LogFilter {
    type CTX = FilterContext;

    async fn request_done(&self, session: &mut Session, _e: Option<&Error>, _ctx: &mut Self::CTX) where Self::CTX: Send + Sync {
        let start = session.downstream_session.req_start;
        let time_cost = start.elapsed();
        let cmd = session.cmd_type();
        let resp_is_ok = session.downstream_session.resp_is_ok;
        info!("cmd: {:?}, time_cost: {:?}, resp_is_ok: {}, req_size: {}, res_size:{}",
            cmd, time_cost, resp_is_ok, session.downstream_session.req_size,
        session.downstream_session.res_size);
    }
}
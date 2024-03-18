use anyhow::Error;
use async_trait::async_trait;
use log::{error, info};

use redis_proxy::proxy::Proxy;
use redis_proxy::session::Session;

use crate::filter_trait::FilterContext;

pub struct LogFilter {}

#[async_trait]
impl Proxy for LogFilter {
    type CTX = FilterContext;

    async fn request_done(&self, session: &mut Session, e: Option<&Error>, _ctx: &mut Self::CTX) where Self::CTX: Send + Sync {
        let start = session.req_start;
        let time_cost = start.elapsed();
        let cmd = session.cmd_type();
        let resp_is_ok = session.res_is_ok;
        if let Some(e) = e {
            error!("cmd: {:?}, time_cost: {:?}, resp_is_ok: {}, req_size: {}, res_size:{}, err: {:?}",
                cmd, time_cost, resp_is_ok, session.req_size,
                session.res_size, e);
            return;
        }
        info!("cmd: {:?}, time_cost: {:?}, resp_is_ok: {}, req_size: {}, res_size:{}, err: {:?}",
            cmd, time_cost, resp_is_ok, session.req_size,
            session.res_size, e);
    }
}
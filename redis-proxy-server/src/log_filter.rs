use anyhow::Error;
use async_trait::async_trait;
use log::{error, info};
use redis_proxy::filter_trait::{Filter, FilterContext};

use redis_proxy::session::Session;
use redis_proxy::SmolStr;


pub struct LogFilter {}

#[async_trait]
impl Filter for LogFilter {

    async fn on_request_done(&self, session: &mut Session, cmd_type: &SmolStr, e: Option<&anyhow::Error>, ctx: &mut FilterContext) {
        let time_cost = (&session.req_start).elapsed();
        let resp_is_ok = session.res_is_ok && e.is_none();
        if let Some(e) = e {
            error!("cmd: {:?}, time_cost: {:?}, upstream_time:{:?}, pool_acquire:{:?}, resp_is_ok: {}, req_size: {}, res_size:{}, err: {:?}",
                cmd_type, time_cost, session.upstream_elapsed, session.pool_acquire_elapsed, resp_is_ok, session.req_size,
                session.res_size, e);
            return;
        }
        info!("cmd: {:?}, time_cost: {:?}, upstream_time:{:?}, pool_acquire:{:?}, resp_is_ok: {}, req_size: {}, res_size:{}, err: {:?}",
            cmd_type, time_cost, session.upstream_elapsed, session.pool_acquire_elapsed, resp_is_ok, session.req_size,
            session.res_size, e);
    }
}
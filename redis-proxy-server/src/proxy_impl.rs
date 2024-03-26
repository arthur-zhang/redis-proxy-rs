use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;

use redis_command_gen::CmdType;
use redis_proxy::config::Config;
use redis_proxy::filter_trait::{Filter, FilterContext, Value};
use redis_proxy::prometheus::{METRICS, RESP_FAILED, RESP_SUCCESS, TRAFFIC_TYPE_EGRESS, TRAFFIC_TYPE_INGRESS};
use redis_proxy::session::Session;
use redis_proxy_common::ReqPkt;

use crate::filter_trait::{REQ_SIZE, RES_IS_OK, RES_SIZE, START_INSTANT};

pub struct FilterImpl {
    pub filters: Vec<Box<dyn Filter + Send + Sync>>,
    pub conf: Arc<Config>,
}


#[async_trait]
impl Filter for FilterImpl {
    async fn on_request(&self, session: &mut Session, req_pkt: &ReqPkt, ctx: &mut FilterContext) -> anyhow::Result<bool> {
        ctx.set_attr(START_INSTANT, Value::Instant(Instant::now()));
        ctx.remote_attr(RES_IS_OK);
        ctx.set_attr(REQ_SIZE, Value::U64(0));
        ctx.set_attr(RES_SIZE, Value::U64(0));

        for filter in &self.filters {
            let response_sent = filter.on_request(session, req_pkt, ctx).await?;
            if response_sent {
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn on_request_done(&self, session: &mut Session, cmd_type: CmdType, e: Option<&anyhow::Error>, ctx: &mut FilterContext) {
        let resp_ok_label = if session.res_is_ok { RESP_SUCCESS } else { RESP_FAILED };
        METRICS.request_latency.with_label_values(&[cmd_type.as_ref(), resp_ok_label]).observe(session.req_start.elapsed().as_secs_f64());
        METRICS.bandwidth.with_label_values(&[cmd_type.as_ref(), TRAFFIC_TYPE_INGRESS]).inc_by(session.req_size as u64);
        METRICS.bandwidth.with_label_values(&[cmd_type.as_ref(), TRAFFIC_TYPE_EGRESS]).inc_by(session.res_size as u64);
        for filter in &self.filters {
            filter.on_request_done(session, cmd_type, e, ctx).await;
        }
    }
}
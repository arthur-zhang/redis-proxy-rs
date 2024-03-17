use std::sync::Arc;
use std::time::Instant;

use anyhow::Error;
use async_trait::async_trait;

use redis_proxy::config::Config;
use redis_proxy::prometheus::{METRICS, RESP_FAILED, RESP_SUCCESS, TRAFFIC_TYPE_EGRESS, TRAFFIC_TYPE_INGRESS};
use redis_proxy::proxy::{Proxy, Session};
use redis_proxy_common::ReqFrameData;

use crate::filter_trait::{FilterContext, REQ_SIZE, RES_IS_OK, RES_SIZE, START_INSTANT, Value};

pub struct RedisProxyImpl {
    pub filters: Vec<Box<dyn Proxy<CTX=FilterContext> + Send + Sync>>,
    pub conf: Arc<Config>,
}


#[async_trait]
impl Proxy for RedisProxyImpl {
    type CTX = FilterContext;

    fn new_ctx(&self) -> Self::CTX {
        FilterContext {
            attrs: Default::default(),
        }
    }
    async fn on_session_create(&self) -> anyhow::Result<()> {
        for filter in &self.filters {
            filter.on_session_create().await?;
        }
        Ok(())
    }

    async fn request_filter(&self, session: &mut Session, ctx: &mut Self::CTX) -> anyhow::Result<bool> {

        ctx.set_attr(START_INSTANT, Value::Instant(Instant::now()));
        ctx.remote_attr(RES_IS_OK);
        ctx.set_attr(REQ_SIZE, Value::U64(0));
        ctx.set_attr(RES_SIZE, Value::U64(0));

        for filter in &self.filters {
            let response_sent = filter.request_filter(session, ctx).await?;
            if response_sent {
                return Ok(true);
            }
        }
        Ok(false)
    }
    async fn proxy_upstream_filter(&self, session: &mut Session, ctx: &mut Self::CTX) -> anyhow::Result<bool> {
        for filter in &self.filters {
            let res = filter.proxy_upstream_filter(session, ctx).await?;
            if res {
                return Ok(true);
            }
        }
        Ok(false)
    }
    async fn upstream_request_filter(&self, _session: &mut Session, _upstream_request: &ReqFrameData, _ctx: &mut Self::CTX) -> anyhow::Result<()> {
        for filter in &self.filters {
            filter.upstream_request_filter(_session, _upstream_request, _ctx).await?;
        }
        Ok(())
    }
    async fn request_done(&self, session: &mut Session, e: Option<&Error>, ctx: &mut Self::CTX) where Self::CTX: Send + Sync {
        let cmd = &session.cmd_type().to_string();
        let resp_ok_label = if session.res_is_ok { RESP_SUCCESS } else { RESP_FAILED };
        METRICS.request_latency.with_label_values(&[cmd, resp_ok_label]).observe(session.req_start.elapsed().as_secs_f64());
        METRICS.bandwidth.with_label_values(&[cmd, TRAFFIC_TYPE_INGRESS]).inc_by(session.req_size as u64);
        METRICS.bandwidth.with_label_values(&[cmd, TRAFFIC_TYPE_EGRESS]).inc_by(session.res_size as u64);
        for filter in &self.filters {
            filter.request_done(session, e, ctx).await;
        }
    }
}
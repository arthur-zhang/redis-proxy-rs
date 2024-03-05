use std::sync::Arc;
use std::time::Instant;

use anyhow::Error;
use async_trait::async_trait;

use redis_proxy::config::Config;
use redis_proxy::proxy::{Proxy, Session};
use redis_proxy_common::cmd::CmdType;
use redis_proxy_common::ReqFrameData;

use crate::filter_trait::{FilterContext, REQ_SIZE, RES_IS_OK, RES_SIZE, START_INSTANT, Value};

pub struct MyProxy {
    pub filters: Vec<Box<dyn Proxy<CTX=FilterContext> + Send + Sync>>,
    pub conf: Arc<Config>,
}


#[async_trait]
impl Proxy for MyProxy {
    type CTX = FilterContext;

    fn new_ctx(&self) -> Self::CTX {
        FilterContext {
            db: 0,
            is_authed: false,
            cmd_type: CmdType::UNKNOWN,
            password: None,
            attrs: Default::default(),
        }
    }
    async fn on_session_create(&self) -> anyhow::Result<()> {
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
    async fn proxy_upstream_filter(&self, session: &mut Session, ctx: &mut Self::CTX) -> anyhow::Result<()> {
        for filter in &self.filters {
            filter.proxy_upstream_filter(session, ctx).await?;
        }
        Ok(())
    }
    async fn upstream_request_filter(&self, session: &mut Session, upstream_request: &mut ReqFrameData, ctx: &mut Self::CTX) -> anyhow::Result<()> {
        for filter in &self.filters {
            filter.upstream_request_filter(session, upstream_request, ctx).await?;
        }
        Ok(())
    }
    async fn request_done(&self, session: &mut Session, e: Option<&Error>, ctx: &mut Self::CTX) where Self::CTX: Send + Sync {
        for filter in &self.filters {
            filter.request_done(session, e, ctx).await;
        }
    }
}
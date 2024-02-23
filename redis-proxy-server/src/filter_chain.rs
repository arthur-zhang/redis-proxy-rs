use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;

use redis_proxy_common::DecodedFrame;
use redis_proxy_filter::traits::{CMD_TYPE_KEY, ContextValue, Filter, FilterStatus, REQ_SIZE, RES_IS_ERROR, RES_SIZE, START_INSTANT, TFilterContext};

pub type TFilterChain = Arc<FilterChain>;

pub struct FilterChain {
    filters: Vec<Box<dyn Filter>>,
}

impl FilterChain {
    pub fn new(filters: Vec<Box<dyn Filter>>) -> Self {
        FilterChain {
            filters,
        }
    }
}

#[async_trait]
impl Filter for FilterChain {
    async fn on_new_connection(&self, context: &mut TFilterContext) -> anyhow::Result<()> {
        for filter in self.filters.iter() {
            filter.on_new_connection(context).await?;
        }
        Ok(())
    }

    async fn pre_handle(&self, context: &mut TFilterContext) -> anyhow::Result<()> {
        if let Ok(mut context) = context.lock() {

            context.set_attr(START_INSTANT, ContextValue::Instant(Instant::now()));
            context.remote_attr(CMD_TYPE_KEY);
            context.remote_attr(RES_IS_ERROR);
            context.set_attr(REQ_SIZE, ContextValue::U64(0));
            context.set_attr(RES_SIZE, ContextValue::U64(0));
        }

        for filter in self.filters.iter() {
            filter.pre_handle(context).await?;
        }
        Ok(())
    }

    async fn on_req_data(&self, context: &mut TFilterContext, data: &DecodedFrame) -> anyhow::Result<FilterStatus> {
        context.lock().unwrap().get_attr_mut_as_u64(REQ_SIZE).map(|it| *it += data.raw_bytes.len() as u64);

        for filter in self.filters.iter() {
            let status = filter.on_req_data(context, data).await?;
            if status != FilterStatus::Continue {
                return Ok(status);
            }
        }
        Ok(FilterStatus::Continue)
    }

    async fn post_handle(&self, context: &mut TFilterContext) -> anyhow::Result<()> {
        for filter in self.filters.iter() {
            filter.post_handle(context).await?;
        }
        Ok(())
    }
}

use std::sync::Arc;

use async_trait::async_trait;

use redis_proxy_common::DecodedFrame;
use redis_proxy_filter::traits::{CMD_TYPE_KEY, Filter, FilterStatus, RES_IS_ERROR, TFilterContext};

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
            context.remote_attr(CMD_TYPE_KEY);
            context.remote_attr(RES_IS_ERROR);
        }

        for filter in self.filters.iter() {
            filter.pre_handle(context).await?;
        }
        Ok(())
    }

    async fn on_data(&self, context: &mut TFilterContext, data: &DecodedFrame) -> anyhow::Result<FilterStatus> {
        for filter in self.filters.iter() {
            let status = filter.on_data(context, data).await?;
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

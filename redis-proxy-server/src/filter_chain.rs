use std::sync::Arc;

use redis_proxy_common::cmd::CmdType;
use redis_proxy_common::DecodedFrame;
use redis_proxy_filter::traits::{Filter, FilterStatus, TFilterContext};

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


#[async_trait::async_trait]
impl Filter for FilterChain {
    async fn on_new_connection(&self, context: &mut TFilterContext) -> anyhow::Result<()> {
        for filter in self.filters.iter() {
            filter.on_new_connection(context).await?;
        }
        Ok(())
    }

    async fn pre_handle(&self, context: &mut TFilterContext) -> anyhow::Result<()> {
        for filter in self.filters.iter() {
            filter.pre_handle(context).await?;
        }
        Ok(())
    }

    async fn post_handle(&self, context: &mut TFilterContext, resp_error: bool) -> anyhow::Result<()> {
        for filter in self.filters.iter() {
            filter.post_handle(context, resp_error).await?;
        }
        Ok(())
    }

    async fn on_data(&self, data: &DecodedFrame, context: &mut TFilterContext) -> anyhow::Result<FilterStatus> {
        for filter in self.filters.iter() {
            let status = filter.on_data(data, context).await?;
            if status != FilterStatus::Continue {
                return Ok(status);
            }
        }
        Ok(FilterStatus::Continue)
    }
}

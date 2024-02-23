use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use redis_codec_core::resp_decoder::ResFramedData;

use redis_proxy_common::ReqFrameData;
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

impl Filter for FilterChain {
    fn on_new_connection(&self, context: &mut TFilterContext) -> anyhow::Result<()> {
        for filter in self.filters.iter() {
            filter.on_new_connection(context)?;
        }
        Ok(())
    }

    fn pre_handle(&self, context: &mut TFilterContext) -> anyhow::Result<()> {
        if let Ok(mut context) = context.lock() {
            context.set_attr(START_INSTANT, ContextValue::Instant(Instant::now()));
            context.remote_attr(CMD_TYPE_KEY);
            context.remote_attr(RES_IS_ERROR);
            context.set_attr(REQ_SIZE, ContextValue::U64(0));
            context.set_attr(RES_SIZE, ContextValue::U64(0));
        }

        for filter in self.filters.iter() {
            filter.pre_handle(context)?;
        }
        Ok(())
    }

    fn on_req_data(&self, context: &mut TFilterContext, data: &ReqFrameData) -> anyhow::Result<FilterStatus> {
        context.lock().unwrap().get_attr_mut_as_u64(REQ_SIZE).map(|it| *it += data.raw_bytes.len() as u64);

        for filter in self.filters.iter() {
            let status = filter.on_req_data(context, data)?;
            if status != FilterStatus::Continue {
                return Ok(status);
            }
        }
        Ok(FilterStatus::Continue)
    }

    fn on_res_data(&self, context: &mut TFilterContext, data: &ResFramedData) -> anyhow::Result<()> {
        context.lock().unwrap().get_attr_mut_as_u64(RES_SIZE).map(|it| *it += data.data.len() as u64);

        for filter in self.filters.iter() {
            filter.on_res_data(context, data)?;
        }
        Ok(())
    }

    fn post_handle(&self, context: &mut TFilterContext) -> anyhow::Result<()> {
        for filter in self.filters.iter() {
            filter.post_handle(context)?;
        }
        Ok(())
    }
}

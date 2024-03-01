use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use log::info;

use redis_codec_core::resp_decoder::ResFramedData;
use redis_proxy_common::ReqFrameData;

use crate::traits::{Filter, FilterContext, FilterStatus, REQ_SIZE, RES_IS_ERROR, RES_SIZE, START_INSTANT, Value};

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
    fn on_session_create(&self, context: &mut FilterContext) -> anyhow::Result<()> {
        info!("filter chain on_session_create");
        for filter in self.filters.iter() {
            filter.on_session_create(context)?;
        }
        Ok(())
    }

    fn pre_handle(&self, context: &mut FilterContext) -> anyhow::Result<()> {
        context.set_attr(START_INSTANT, Value::Instant(Instant::now()));
        context.remote_attr(RES_IS_ERROR);
        context.set_attr(REQ_SIZE, Value::U64(0));
        context.set_attr(RES_SIZE, Value::U64(0));

        for filter in self.filters.iter() {
            filter.pre_handle(context)?;
        }
        Ok(())
    }

    async fn on_req_data(&self, context: &mut FilterContext, data: &ReqFrameData) -> anyhow::Result<FilterStatus> {
        context.get_attr_mut_as_u64(REQ_SIZE).map(|it| *it += data.raw_bytes.len() as u64);

        for filter in self.filters.iter() {
            let s = filter.on_req_data(context, data).await?;
            if s != FilterStatus::Continue {
                return Ok(s);
            }
        }

        Ok(FilterStatus::Continue)
    }

    async fn on_res_data(&self, context: &mut FilterContext, data: &ResFramedData) -> anyhow::Result<()> {
        context.get_attr_mut_as_u64(RES_SIZE).map(|it| *it += data.data.len() as u64);

        for filter in self.filters.iter() {
            filter.on_res_data(context, data).await?;
        }
        Ok(())
    }

    fn post_handle(&self, context: &mut FilterContext) -> anyhow::Result<()> {
        for filter in self.filters.iter() {
            filter.post_handle(context)?;
        }
        Ok(())
    }
    fn on_session_close(&self, context: &mut FilterContext) -> anyhow::Result<()> {
        info!("filter chain on_session_close");
        for filter in self.filters.iter() {
            filter.on_session_close(context)?;
        }
        Ok(())
    }
}

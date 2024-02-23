use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;

use redis_codec_core::resp_decoder::ResFramedData;
use redis_proxy_common::ReqFrameData;
use redis_proxy_filter::traits::{CMD_TYPE_KEY, Filter, FilterStatus, REQ_SIZE, RES_IS_ERROR, RES_SIZE, START_INSTANT, TFilterContext, Value};

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
    fn on_new_connection(&self, context: &mut TFilterContext) -> anyhow::Result<()> {
        for filter in self.filters.iter() {
            filter.on_new_connection(context)?;
        }
        Ok(())
    }

    fn pre_handle(&self, context: &mut TFilterContext) -> anyhow::Result<()> {
        if let Ok(mut context) = context.lock() {
            context.set_attr(START_INSTANT, Value::Instant(Instant::now()));
            context.remote_attr(CMD_TYPE_KEY);
            context.remote_attr(RES_IS_ERROR);
            context.set_attr(REQ_SIZE, Value::U64(0));
            context.set_attr(RES_SIZE, Value::U64(0));
        }

        for filter in self.filters.iter() {
            filter.pre_handle(context)?;
        }
        Ok(())
    }

    async fn on_req_data(&self, context: &mut TFilterContext, data: &ReqFrameData) -> anyhow::Result<FilterStatus> {
        context.lock().unwrap().get_attr_mut_as_u64(REQ_SIZE).map(|it| *it += data.raw_bytes.len() as u64);

        let mut status = FilterStatus::Continue;
        for filter in self.filters.iter() {
            let s = filter.on_req_data(context, data).await?;
            if s != FilterStatus::Continue {
                status = s;
                break;
            }
        }

        if status == FilterStatus::Continue {
            context.lock().unwrap().p2b_w.try_write(&data.raw_bytes)?;
            return Ok(status);
        }

        if data.is_done {
            self.on_res_data(context, &ResFramedData {
                data: Bytes::from_static(b"-ERR blocked\r\n"),
                is_done: true,
                is_error: true,
            }).await?;
        }

        Ok(status)
    }

    async fn on_res_data(&self, context: &mut TFilterContext, data: &ResFramedData) -> anyhow::Result<()> {
        context.lock().unwrap().get_attr_mut_as_u64(RES_SIZE).map(|it| *it += data.data.len() as u64);

        for filter in self.filters.iter() {
            filter.on_res_data(context, data).await?;
        }
        context.lock().unwrap().p2c_w.try_write(&data.data)?;
        Ok(())
    }

    fn post_handle(&self, context: &mut TFilterContext) -> anyhow::Result<()> {
        for filter in self.filters.iter() {
            filter.post_handle(context)?;
        }
        Ok(())
    }
}

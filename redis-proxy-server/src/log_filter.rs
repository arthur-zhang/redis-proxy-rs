use std::time::Instant;

use anyhow::bail;
use async_trait::async_trait;
use log::{error, info, log};

use redis_codec_core::resp_decoder::ResFramedData;
use redis_proxy_common::cmd::CmdType;
use redis_proxy_common::ReqFrameData;
use redis_proxy_filter::traits::{ContextValue, Filter, FilterContext, FilterStatus, REQ_SIZE, RES_SIZE, START_INSTANT, TFilterContext};

pub struct LogFilter {}

impl LogFilter {
    pub fn new() -> Self {
        LogFilter {}
    }
}

impl Filter for LogFilter {
    fn on_new_connection(&self, _context: &mut TFilterContext) -> anyhow::Result<()> {
        Ok(())
    }

    fn pre_handle(&self, _context: &mut TFilterContext) -> anyhow::Result<()> {
        Ok(())
    }

    fn on_req_data(&self, _context: &mut TFilterContext, _data: &ReqFrameData) -> anyhow::Result<FilterStatus> {
        Ok(FilterStatus::Continue)
    }

    fn on_res_data(&self, context: &mut TFilterContext, data: &ResFramedData) -> anyhow::Result<()> {
        Ok(())
    }

    fn post_handle(&self, context: &mut TFilterContext) -> anyhow::Result<()> {
        let context = context.lock().unwrap();

        let cmd_type = context.get_attr_as_cmd_type();
        let start = context.get_attr(START_INSTANT).ok_or(anyhow::anyhow!("start_instant not found"))?;
        let req_size = context.get_attr_as_u64(REQ_SIZE).ok_or(anyhow::anyhow!("req_size not found"))?;
        let res_size = context.get_attr_as_u64(RES_SIZE).ok_or(anyhow::anyhow!("res_size not found"))?;

        let res_is_error = context.get_attr_res_is_error();
        if let ContextValue::Instant(start) = start {
            let elapsed = start.elapsed();
            error!("[{:?}] elapsed:{:?}, req_size:{}, res_size:{}, res_is_error:{}",
                cmd_type, elapsed, req_size, res_size, res_is_error);
            return Ok(());
        }

        bail!("start_instant is not an Instant");
    }
}

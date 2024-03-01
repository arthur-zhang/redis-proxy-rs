use std::time::Instant;

use anyhow::bail;
use log::{error, info, log};

use redis_codec_core::resp_decoder::ResFramedData;
use redis_proxy_common::cmd::CmdType;
use redis_proxy_common::ReqFrameData;
use crate::traits::{Value, Filter, FilterContext, FilterStatus, REQ_SIZE, RES_SIZE, START_INSTANT, TFilterContext};

pub struct LogFilter {}

impl LogFilter {
    pub fn new() -> Self {
        LogFilter {}
    }
}

impl Filter for LogFilter {
    fn post_handle(&self, context: &mut FilterContext) -> anyhow::Result<()> {

        let start = context.get_attr(START_INSTANT).ok_or(anyhow::anyhow!("start_instant not found"))?;
        let req_size = context.get_attr_as_u64(REQ_SIZE).ok_or(anyhow::anyhow!("req_size not found"))?;
        let res_size = context.get_attr_as_u64(RES_SIZE).ok_or(anyhow::anyhow!("res_size not found"))?;

        let res_is_error = context.get_attr_res_is_error();
        if let Value::Instant(start) = start {
            let elapsed = start.elapsed();
            error!("[{:?}] elapsed:{:?}, req_size:{}, res_size:{}, res_is_error:{}",
                context.cmd_type, elapsed, req_size, res_size, res_is_error);
            return Ok(());
        }

        bail!("start_instant is not an Instant");
    }
}

use std::time::Instant;

use anyhow::bail;
use log::{error, info, log};

use redis_proxy_common::cmd::CmdType;
use redis_proxy_common::DecodedFrame;
use redis_proxy_filter::traits::{ContextValue, Filter, FilterContext, FilterStatus, TFilterContext};

pub struct LogFilter {}

impl LogFilter {
    pub fn new() -> Self {
        LogFilter {}
    }
}

#[async_trait::async_trait]
impl Filter for LogFilter {
    async fn on_new_connection(&self, context: &mut TFilterContext) -> anyhow::Result<()> {
        Ok(())
    }

    async fn pre_handle(&self, context: &mut TFilterContext) -> anyhow::Result<()> {
        context.lock().unwrap().set_attr("start_instant", ContextValue::Instant(Instant::now()));
        Ok(())
    }

    async fn post_handle(&self, context: &mut TFilterContext, resp_error: bool) -> anyhow::Result<()> {
        let context = context.lock().unwrap();

        let cmd_type = context.get_attr_as_cmd_type();
        let start = context.get_attr("start_instant").ok_or(anyhow::anyhow!("start_instant not found"))?;

        if let ContextValue::Instant(start) = start {
            let elapsed = start.elapsed();
            error!("[{:?}] elapsed: {:?}, response is_error: {}", cmd_type, elapsed, resp_error);
            return Ok(());
        }

        bail!("start_instant is not an Instant");
    }

    async fn on_data(&self, data: &DecodedFrame, context: &mut TFilterContext) -> anyhow::Result<FilterStatus> {
        // let cmd = data.cmd_type.as_ref().unwrap();
        // info!("{:?}, key info: {:?}, eager {}, {}", cmd, cmd.redis_key_info(),  data.is_eager, data.is_done);
        //
        // if data.is_eager {
        //     if let Some(ref it) = data.eager_read_list {
        //         for range in it {
        //             info!("\tpart: {:?}", std::str::from_utf8(&data.raw_bytes[range.start..range.end]).unwrap_or(""));
        //         }
        //     }
        // }
        Ok(FilterStatus::Continue)
    }
}

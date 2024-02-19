use log::{info, log};

use redis_proxy_common::DecodedFrame;
use redis_proxy_filter::traits::{Filter, FilterStatus};

pub struct LogFilter {}

impl LogFilter {
    pub fn new() -> Self {
        LogFilter {}
    }
}

#[async_trait::async_trait]
impl Filter for LogFilter {
    async fn init(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn on_data(&mut self, data: &DecodedFrame) -> anyhow::Result<FilterStatus> {
        info!("{:?}, eager {}, {}", data.cmd_type, data.is_eager, data.is_done);
        Ok(FilterStatus::Continue)
    }
}

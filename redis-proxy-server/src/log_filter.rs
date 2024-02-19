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
        let cmd = data.cmd_type.as_ref().unwrap();
        info!("{:?}, key info: {:?}, eager {}, {}", cmd, cmd.redis_key_info(),  data.is_eager, data.is_done);

        if data.is_eager {
            if let Some(ref it) = data.eager_read_list {
                for range in it {
                    info!("\tpart: {:?}", std::str::from_utf8(&data.raw_bytes[range.start..range.end]).unwrap_or(""));
                }
            }
        }
        Ok(FilterStatus::Continue)
    }
}

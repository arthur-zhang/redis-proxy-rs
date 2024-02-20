use std::time::Instant;

use log::{error, info, log};

use redis_proxy_common::DecodedFrame;
use redis_proxy_filter::traits::{Filter, FilterStatus};

pub struct LogFilter {
    start: Instant,
}

impl LogFilter {
    pub fn new() -> Self {
        LogFilter { start: Instant::now() }
    }
}

#[async_trait::async_trait]
impl Filter for LogFilter {
    async fn init(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn pre_handle(&mut self) -> anyhow::Result<()> {
        self.start = Instant::now();
        Ok(())
    }

    async fn post_handle(&mut self) -> anyhow::Result<()> {
        let elapsed = self.start.elapsed();
        error!("elapsed: {:?}", elapsed);
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

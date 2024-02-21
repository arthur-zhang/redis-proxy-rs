use redis_proxy_common::DecodedFrame;
use redis_proxy_filter::traits::{Filter, FilterContext, FilterStatus};

pub struct BlackListFilter {
    blocked: bool,
    blacklist: Vec<String>,
}

impl BlackListFilter {
    pub fn new(blacklist: Vec<String>) -> Self {
        BlackListFilter { blocked: false, blacklist }
    }
}

#[async_trait::async_trait]
impl Filter for BlackListFilter {
    async fn init(&mut self, context: &mut FilterContext) -> anyhow::Result<()> {
        Ok(())
    }

    async fn pre_handle(&mut self, context: &mut FilterContext) -> anyhow::Result<()> {
        self.blocked = false;
        Ok(())
    }

    async fn post_handle(&mut self, context: &mut FilterContext) -> anyhow::Result<()> {
        Ok(())
    }

    async fn on_data(&mut self, data: &DecodedFrame, context: &mut FilterContext) -> anyhow::Result<FilterStatus> {
        // if self.blocked && !data.is_done {
        //     return Ok(FilterStatus::StopIteration);
        // }
        if self.blocked {
            return Ok(FilterStatus::Block);
        }


        let DecodedFrame { frame_start, cmd_type, eager_read_list, raw_bytes, is_eager, is_done } = &data;
        if *frame_start && *is_eager {
            let key = eager_read_list.as_ref().and_then(|it| it.first().map(|it| &raw_bytes[it.start..it.end]));
            if let Some(key) = key {
                if self.blacklist.contains(&std::str::from_utf8(key).unwrap().to_string()) {
                    self.blocked = true;
                    // self.tx.send(Bytes::from_static(b"-ERR blocked\r\n")).await.unwrap();
                    return Ok(FilterStatus::Block);
                }
            }
        }
        // if data.is_done {
        //     self.blocked = false;
        // }
        Ok(FilterStatus::Continue)
    }
}
use bytes::Bytes;
use tokio::sync::mpsc::Sender;
use redis_proxy_common::DecodedFrame;
use redis_proxy_filter::traits::{Filter, FilterStatus};

pub struct BlackListFilter {
    blocked: bool,
    black_list: Vec<String>,
    tx: Sender<Bytes>,
}

impl BlackListFilter {
    pub fn new(black_list: Vec<String>, tx: Sender<Bytes>) -> Self {
        BlackListFilter { blocked: false, black_list, tx }
    }
}

#[async_trait::async_trait]
impl Filter for BlackListFilter {
    async fn init(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn pre_handle(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn post_handle(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn on_data(&mut self, data: &DecodedFrame) -> anyhow::Result<FilterStatus> {
        if self.blocked && !data.is_done {
            return Ok(FilterStatus::StopIteration);
        }

        let DecodedFrame { frame_start, cmd_type, eager_read_list, raw_bytes, is_eager, is_done } = &data;
        if *is_eager {
            let key = eager_read_list.as_ref().and_then(|it| it.first().map(|it| &raw_bytes[it.start..it.end]));
            if let Some(key) = key {
                if self.black_list.contains(&std::str::from_utf8(key).unwrap().to_string()) {
                    self.blocked = true;
                    if *is_done {
                        self.blocked = false;
                    }
                    self.tx.send(Bytes::from_static(b"-ERR blocked\r\n")).await.unwrap();
                    return Ok(FilterStatus::StopIteration);
                }
            }
        }
        if data.is_done {
            self.blocked = false;
        }
        Ok(FilterStatus::Continue)
    }
}
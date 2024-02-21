use redis_proxy_common::DecodedFrame;
use redis_proxy_filter::traits::{ContextValue, Filter, FilterContext, FilterStatus};

pub struct BlackListFilter {
    blacklist: Vec<String>,
}

impl BlackListFilter {
    pub fn new(blacklist: Vec<String>) -> Self {
        BlackListFilter { blacklist }
    }
}

const BLOCKED: &'static str = "blacklist_blocked";

#[async_trait::async_trait]
impl Filter for BlackListFilter {
    async fn on_new_connection(&self, context: &mut FilterContext) -> anyhow::Result<()> {
        Ok(())
    }

    async fn pre_handle(&self, context: &mut FilterContext) -> anyhow::Result<()> {
        context.remote_attr(BLOCKED);
        Ok(())
    }

    async fn post_handle(&self, context: &mut FilterContext) -> anyhow::Result<()> {
        Ok(())
    }

    async fn on_data(&self, data: &DecodedFrame, context: &mut FilterContext) -> anyhow::Result<FilterStatus> {
        let blocked = context.get_attr_as_bool(BLOCKED).unwrap_or(false);
        if blocked {
            return Ok(FilterStatus::Block);
        }

        let DecodedFrame { is_first_frame: frame_start, cmd_type, eager_read_list, raw_bytes, is_eager, is_done } = &data;
        if *frame_start && *is_eager {
            let key = eager_read_list.as_ref().and_then(|it| it.first().map(|it| &raw_bytes[it.start..it.end]));
            if let Some(key) = key {
                if self.blacklist.contains(&std::str::from_utf8(key).unwrap().to_string()) {
                    context.set_attr(BLOCKED, ContextValue::Bool(true));
                    context.is_error = true;
                    return Ok(FilterStatus::Block);
                }
            }
        }
        Ok(FilterStatus::Continue)
    }
}
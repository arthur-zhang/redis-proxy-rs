use async_trait::async_trait;

use redis_proxy_common::ReqFrameData;
use crate::traits::{Filter, FilterContext, FilterStatus, TFilterContext, Value};

use crate::path_trie::PathTrie;

pub struct BlackListFilter {
    trie: PathTrie,
}

impl BlackListFilter {
    pub fn new(blacklist: Vec<String>, split_regex: &str) -> anyhow::Result<Self> {
        let trie = PathTrie::new(&blacklist, split_regex)?;
        Ok(BlackListFilter { trie })
    }
}

pub const BLOCKED: &'static str = "blacklist_blocked";

#[async_trait]
impl Filter for BlackListFilter {
    fn pre_handle(&self, context: &mut FilterContext) -> anyhow::Result<()> {
        context.remote_attr(BLOCKED);
        Ok(())
    }

    async fn on_req_data(&self, context: &mut FilterContext, data: &ReqFrameData) -> anyhow::Result<FilterStatus> {
        let blocked = context.get_attr_as_bool(BLOCKED).unwrap_or(false);
        if blocked {
            return Ok(FilterStatus::Block);
        }

        let ReqFrameData { is_first_frame: frame_start, cmd_type, eager_read_list, raw_bytes, is_eager, is_done } = &data;
        if *frame_start && *is_eager {
            let key = eager_read_list.as_ref().and_then(|it| it.first().map(|it| &raw_bytes[it.start..it.end]));
            if let Some(key) = key {
                if self.trie.exists_path(key) {
                    context.set_attr(BLOCKED, Value::Bool(true));
                    return Ok(FilterStatus::Block);
                }
            }
        }
        Ok(FilterStatus::Continue)
    }
}
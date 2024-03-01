use async_trait::async_trait;

use redis_proxy_common::ReqFrameData;

use crate::path_trie::PathTrie;
use crate::traits::{Filter, FilterContext, FilterStatus, Value};

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

        if data.is_head_frame {
            let args = data.args();
            if let Some(key) = args {
                for key in key {
                    if self.trie.exists_path(key) {
                        context.set_attr(BLOCKED, Value::Bool(true));
                        return Ok(FilterStatus::Block);
                    }
                }
            }
        }
        Ok(FilterStatus::Continue)
    }
}
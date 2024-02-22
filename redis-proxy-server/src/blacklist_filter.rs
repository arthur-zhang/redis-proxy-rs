use async_trait::async_trait;
use redis_proxy_common::DecodedFrame;
use redis_proxy_filter::traits::{ContextValue, Filter, FilterStatus, TFilterContext};

use crate::path_trie::PathTrie;

pub struct BlackListFilter {
    // blacklist: Vec<String>,
    trie: PathTrie,
}

impl BlackListFilter {
    pub fn new(blacklist: Vec<String>, split_regex: &str) -> Self {
        let trie = PathTrie::new(&blacklist, split_regex).unwrap();
        BlackListFilter { trie }
    }
}

const BLOCKED: &'static str = "blacklist_blocked";

#[async_trait]
impl Filter for BlackListFilter {
    async fn on_new_connection(&self, _context: &mut TFilterContext) -> anyhow::Result<()> {
        Ok(())
    }

    async fn pre_handle(&self, context: &mut TFilterContext) -> anyhow::Result<()> {
        context.lock().unwrap().remote_attr(BLOCKED);
        Ok(())
    }

    async fn on_data(&self, context: &mut TFilterContext, data: &DecodedFrame) -> anyhow::Result<FilterStatus> {
        let blocked = context.lock().unwrap().get_attr_as_bool(BLOCKED).unwrap_or(false);
        if blocked {
            return Ok(FilterStatus::Block);
        }

        let DecodedFrame { is_first_frame: frame_start, cmd_type, eager_read_list, raw_bytes, is_eager, is_done } = &data;
        if *frame_start && *is_eager {
            let key = eager_read_list.as_ref().and_then(|it| it.first().map(|it| &raw_bytes[it.start..it.end]));
            if let Some(key) = key {
                if self.trie.exists_path(key) {
                    context.lock().unwrap().set_attr(BLOCKED, ContextValue::Bool(true));
                    return Ok(FilterStatus::Block);
                }
            }
        }
        Ok(FilterStatus::Continue)
    }

    async fn post_handle(&self, _context: &mut TFilterContext) -> anyhow::Result<()> {
        Ok(())
    }
}
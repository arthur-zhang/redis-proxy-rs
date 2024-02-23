use redis_codec_core::resp_decoder::ResFramedData;
use redis_proxy_common::ReqFrameData;
use redis_proxy_filter::traits::{Value, Filter, FilterStatus, TFilterContext};

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

const BLOCKED: &'static str = "blacklist_blocked";

impl Filter for BlackListFilter {
    fn pre_handle(&self, context: &mut TFilterContext) -> anyhow::Result<()> {
        context.lock().unwrap().remote_attr(BLOCKED);
        Ok(())
    }

    fn on_req_data(&self, context: &mut TFilterContext, data: &ReqFrameData) -> anyhow::Result<FilterStatus> {
        let blocked = context.lock().unwrap().get_attr_as_bool(BLOCKED).unwrap_or(false);
        if blocked {
            return Ok(FilterStatus::Block);
        }

        let ReqFrameData { is_first_frame: frame_start, cmd_type, eager_read_list, raw_bytes, is_eager, is_done } = &data;
        if *frame_start && *is_eager {
            let key = eager_read_list.as_ref().and_then(|it| it.first().map(|it| &raw_bytes[it.start..it.end]));
            if let Some(key) = key {
                if self.trie.exists_path(key) {
                    context.lock().unwrap().set_attr(BLOCKED, Value::Bool(true));
                    return Ok(FilterStatus::Block);
                }
            }
        }
        Ok(FilterStatus::Continue)
    }
}
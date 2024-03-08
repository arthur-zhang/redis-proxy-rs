use async_trait::async_trait;
use bytes::Bytes;
use futures::SinkExt;

use redis_proxy::proxy::{Proxy, Session};

use crate::filter_trait::FilterContext;
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


#[async_trait]
impl Proxy for BlackListFilter {
    type CTX = FilterContext;

    async fn request_filter(&self, session: &mut Session, ctx: &mut Self::CTX) -> anyhow::Result<bool> {
        let req_frame = session.header_frame.as_ref().unwrap();
        let args = req_frame.args();
        if let Some(args) = args {
            for key in args {
                if self.trie.exists_path(key) {
                    session.underlying_stream.send(Bytes::from_static(b"-ERR black list\r\n")).await?;
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }
}
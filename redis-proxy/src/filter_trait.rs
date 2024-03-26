use std::collections::HashMap;

use async_trait::async_trait;

use redis_command_gen::CmdType;
use redis_proxy_common::ReqPkt;

use crate::session::Session;

pub enum Value {
    U64(u64),
    Instant(std::time::Instant),
}

pub struct FilterContext {
    pub attrs: HashMap<String, Value>,
}

impl FilterContext {
    pub fn new() -> Self {
        FilterContext {
            attrs: HashMap::new(),
        }
    }
    pub fn set_attr(&mut self, key: &str, value: Value) {
        self.attrs.insert(key.to_string(), value);
    }
    pub fn remote_attr(&mut self, key: &str) {
        self.attrs.remove(key);
    }
}

#[allow(unused_variables)]
#[async_trait]
pub trait Filter {
    async fn on_request(&self, session: &mut Session, req_pkt: &ReqPkt, ctx: &mut FilterContext) -> anyhow::Result<bool> {
        Ok(false)
    }

    async fn on_request_done(&self, session: &mut Session, cmd_type: CmdType, e: Option<&anyhow::Error>, ctx: &mut FilterContext) {}
}

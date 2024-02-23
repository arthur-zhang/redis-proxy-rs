use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio::sync::mpsc::Sender;

use redis_proxy_common::cmd::CmdType;
use redis_proxy_common::DecodedFrame;

pub enum ContextValue {
    String(String),
    U64(u64),
    Bool(bool),
    Instant(std::time::Instant),
    ChanSender(Sender<bytes::Bytes>),
    CmdType(redis_proxy_common::cmd::CmdType),
}

pub const CMD_TYPE_KEY: &'static str = "cmd_type";
pub const RES_IS_ERROR: &'static str = "res_is_error";
pub const START_INSTANT: &'static str = "log_start_instant";

pub const REQ_SIZE: &'static str = "req_size";
pub const RES_SIZE: &'static str = "res_size";

pub type TFilterContext = Arc<Mutex<FilterContext>>;


// per session filter context
pub struct FilterContext {
    attrs: HashMap<String, ContextValue>,
}

impl FilterContext {
    pub fn new() -> Self {
        FilterContext { attrs: HashMap::new() }
    }
    pub fn set_attr(&mut self, key: &str, value: ContextValue) {
        self.attrs.insert(key.to_string(), value);
    }
    pub fn remote_attr(&mut self, key: &str) {
        self.attrs.remove(key);
    }
    pub fn get_attr(&self, key: &str) -> Option<&ContextValue> {
        self.attrs.get(key)
    }
    pub fn get_attr_mut(&mut self, key: &str) -> Option<&mut ContextValue> {
        self.attrs.get_mut(key)
    }
    pub fn get_attr_as_u64(&self, key: &str) -> Option<u64> {
        self.attrs.get(key).and_then(|it| {
            if let ContextValue::U64(u) = it {
                Some(*u)
            } else {
                None
            }
        })
    }
    pub fn get_attr_mut_as_u64(&mut self, key: &str) -> Option<&mut u64> {
        self.attrs.get_mut(key).and_then(|it| {
            if let ContextValue::U64(u) = it {
                Some(u)
            } else {
                None
            }
        })
    }

    pub fn get_attr_as_bool(&self, key: &str) -> Option<bool> {
        self.attrs.get(key).and_then(|it| {
            if let ContextValue::Bool(b) = it {
                Some(*b)
            } else {
                None
            }
        })
    }

    pub fn get_attr_as_sender(&self, key: &str) -> Option<&Sender<bytes::Bytes>> {
        self.attrs.get(key).and_then(|it| {
            if let ContextValue::ChanSender(tx) = it {
                Some(tx)
            } else {
                None
            }
        })
    }
    pub fn set_attr_cmd_type(&mut self, cmd_type: CmdType) {
        self.set_attr(CMD_TYPE_KEY, ContextValue::CmdType(cmd_type));
    }
    pub fn get_attr_as_cmd_type(&self) -> CmdType {
        return match self.attrs.get(CMD_TYPE_KEY) {
            Some(ContextValue::CmdType(cmd_type)) => {
                cmd_type.clone()
            }
            _ => {
                CmdType::UNKNOWN
            }
        };
    }
    pub fn set_attr_res_is_error(&mut self, is_error: bool) {
        self.set_attr(RES_IS_ERROR, ContextValue::Bool(is_error));
    }
    pub fn get_attr_res_is_error(&self) -> bool {
        return match self.attrs.get(RES_IS_ERROR) {
            Some(ContextValue::Bool(is_error)) => {
                *is_error
            }
            _ => {
                false
            }
        };
    }
}

// stateless filter, mutable data is stored in FilterContext
#[async_trait]
pub trait Filter: Send + Sync {
    async fn on_new_connection(&self, context: &mut TFilterContext) -> anyhow::Result<()>;
    async fn pre_handle(&self, context: &mut TFilterContext) -> anyhow::Result<()>;
    async fn on_req_data(&self, context: &mut TFilterContext, data: &DecodedFrame) -> anyhow::Result<FilterStatus>;
    async fn post_handle(&self, context: &mut TFilterContext) -> anyhow::Result<()>;
}

#[derive(Debug, Eq, PartialEq)]
pub enum FilterStatus {
    Continue,
    StopIteration,
    Block,
}

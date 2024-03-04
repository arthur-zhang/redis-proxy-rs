use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio::sync::mpsc::Sender;

use redis_codec_core::resp_decoder::ResFramedData;
use redis_proxy_common::cmd::CmdType;
use redis_proxy_common::ReqFrameData;

pub enum Value {
    String(String),
    U64(u64),
    Bool(bool),
    Instant(std::time::Instant),
    ChanSender(Sender<bytes::Bytes>),
}

impl Value {
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None
        }
    }
    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Value::U64(u) => Some(*u),
            _ => None
        }
    }
    pub fn as_u64_mut(&mut self) -> Option<&mut u64> {
        match self {
            Value::U64(u) => Some(u),
            _ => None
        }
    }
    pub fn as_instant(&self) -> Option<&std::time::Instant> {
        match self {
            Value::Instant(i) => Some(i),
            _ => None
        }
    }
    pub fn as_sender(&self) -> Option<&Sender<bytes::Bytes>> {
        match self {
            Value::ChanSender(tx) => Some(tx),
            _ => None
        }
    }
    pub fn as_string(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s),
            _ => None
        }
    }
}


pub const RES_IS_ERROR: &'static str = "res_is_error";
pub const START_INSTANT: &'static str = "log_start_instant";

pub const REQ_SIZE: &'static str = "req_size";
pub const RES_SIZE: &'static str = "res_size";

pub type TFilterContext = Arc<Mutex<FilterContext>>;

// per session filter context
pub struct FilterContext {
    pub db: u64,
    pub is_authed: bool,
    pub cmd_type: CmdType,
    pub password: Option<String>,
    pub attrs: HashMap<String, Value>,
}

impl FilterContext {
    pub fn set_attr(&mut self, key: &str, value: Value) {
        self.attrs.insert(key.to_string(), value);
    }
    pub fn remote_attr(&mut self, key: &str) {
        self.attrs.remove(key);
    }
    pub fn get_attr(&self, key: &str) -> Option<&Value> {
        self.attrs.get(key)
    }
    pub fn get_attr_mut(&mut self, key: &str) -> Option<&mut Value> {
        self.attrs.get_mut(key)
    }
    pub fn get_attr_as_u64(&self, key: &str) -> Option<u64> {
        self.attrs.get(key).and_then(|it| {
            it.as_u64()
        })
    }
    pub fn get_attr_mut_as_u64(&mut self, key: &str) -> Option<&mut u64> {
        self.attrs.get_mut(key).and_then(|it| {
            it.as_u64_mut()
        })
    }

    pub fn get_attr_as_bool(&self, key: &str) -> Option<bool> {
        self.attrs.get(key).and_then(|it| {
            it.as_bool()
        })
    }

    pub fn get_attr_as_sender(&self, key: &str) -> Option<&Sender<bytes::Bytes>> {
        self.attrs.get(key).and_then(|it| {
            it.as_sender()
        })
    }
    pub fn set_attr_res_is_error(&mut self, is_error: bool) {
        self.set_attr(RES_IS_ERROR, Value::Bool(is_error));
    }
    pub fn get_attr_res_is_error(&self) -> bool {
        self.attrs.get(RES_IS_ERROR)
            .and_then(|it| it.as_bool())
            .unwrap_or(false)
    }
}

// stateless + nonblocking filter, mutable data is stored in FilterContext
#[async_trait]
pub trait Filter: Send + Sync {
    fn on_session_create(&self) -> anyhow::Result<()> { Ok(()) }
    fn pre_handle(&self, context: &mut FilterContext) -> anyhow::Result<()> { Ok(()) }
    async fn on_req_data(&self, context: &mut FilterContext, data: &ReqFrameData) -> anyhow::Result<FilterStatus> { Ok(FilterStatus::Continue) }
    async fn on_res_data(&self, context: &mut FilterContext, data: &ResFramedData) -> anyhow::Result<()> { Ok(()) }
    fn post_handle(&self, context: &mut FilterContext) -> anyhow::Result<()> { Ok(()) }
    fn on_session_close(&self) -> anyhow::Result<()> { Ok(()) }
}

#[derive(Debug, Eq, PartialEq)]
pub enum FilterStatus {
    Continue,
    Block,
}

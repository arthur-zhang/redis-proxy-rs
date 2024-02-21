use async_trait::async_trait;
use tokio::sync::mpsc::Sender;

use redis_proxy_common::cmd::CmdType;
use redis_proxy_common::DecodedFrame;

pub enum ContextValue {
    String(String),
    Int(i64),
    Bool(bool),
    Instant(std::time::Instant),
    ChanSender(Sender<bytes::Bytes>),
}

// per session filter context
pub struct FilterContext {
    pub cmd_type: CmdType,
    attrs: std::collections::HashMap<String, ContextValue>,
    pub is_error: bool,
}

impl FilterContext {
    pub fn new() -> Self {
        FilterContext { cmd_type: CmdType::UNKNOWN, attrs: std::collections::HashMap::new(), is_error: false }
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
}

// stateless filter, mutable data is stored in FilterContext
#[async_trait]
pub trait Filter: Send + Sync {
    async fn on_new_connection(&self, context: &mut FilterContext) -> anyhow::Result<()>;
    async fn pre_handle(&self, context: &mut FilterContext) -> anyhow::Result<()>;
    async fn post_handle(&self, context: &mut FilterContext) -> anyhow::Result<()>;
    async fn on_data(&self, data: &DecodedFrame, context: &mut FilterContext) -> anyhow::Result<FilterStatus>;
}

#[derive(Debug, Eq, PartialEq)]
pub enum FilterStatus {
    Continue,
    StopIteration,
    Block,
}

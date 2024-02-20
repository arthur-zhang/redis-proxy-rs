use redis_proxy_common::DecodedFrame;

pub enum ContextValue {
    String(String),
    Int(i64),
    Bool(bool),
    Instant(std::time::Instant),
}

pub struct FilterContext {
    attrs: std::collections::HashMap<String, ContextValue>,
}

impl FilterContext {
    pub fn new() -> Self {
        FilterContext { attrs: std::collections::HashMap::new() }
    }
    pub fn set_attr(&mut self, key: &str, value: ContextValue) {
        self.attrs.insert(key.to_string(), value);
    }
    pub fn get_attr(&self, key: &str) -> Option<&ContextValue> {
        self.attrs.get(key)
    }
    pub fn clear(&mut self) {
        self.attrs.clear();
    }
}

#[async_trait::async_trait]
pub trait Filter: Send + Sync {
    async fn init(&mut self, context: &mut FilterContext) -> anyhow::Result<()>;
    async fn pre_handle(&mut self, context: &mut FilterContext) -> anyhow::Result<()>;
    async fn post_handle(&mut self, context: &mut FilterContext) -> anyhow::Result<()>;
    async fn on_data(&mut self, data: &DecodedFrame, context: &mut FilterContext) -> anyhow::Result<FilterStatus>;
}

#[derive(Debug, Eq, PartialEq)]
pub enum FilterStatus {
    Continue,
    StopIteration,
}

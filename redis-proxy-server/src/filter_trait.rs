use std::collections::HashMap;

pub enum Value {
    U64(u64),
    Instant(std::time::Instant),
}

pub const RES_IS_OK: &'static str = "res_is_error";
pub const START_INSTANT: &'static str = "log_start_instant";

pub const REQ_SIZE: &'static str = "req_size";
pub const RES_SIZE: &'static str = "res_size";

// per session filter context
pub struct FilterContext {
    pub attrs: HashMap<String, Value>,
}

impl FilterContext {
    pub fn set_attr(&mut self, key: &str, value: Value) {
        self.attrs.insert(key.to_string(), value);
    }
    pub fn remote_attr(&mut self, key: &str) {
        self.attrs.remove(key);
    }
}
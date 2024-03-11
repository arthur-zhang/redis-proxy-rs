use std::collections::HashMap;

use tokio::sync::mpsc::Sender;

pub enum Value {
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

    pub fn as_sender(&self) -> Option<&Sender<bytes::Bytes>> {
        match self {
            Value::ChanSender(tx) => Some(tx),
            _ => None
        }
    }
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
}
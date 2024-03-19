use serde::{Deserialize, Serialize};

pub mod holder;
pub mod utils;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedisCmdDescribeEntity {
    pub summary: String,
    pub container: Option<String>,
    pub group: String,
    pub since: String,
    pub arity: i32,
    pub function: Option<String>,
    pub command_flags: Option<Vec<String>>,
    pub key_specs: Option<Vec<KeySpecs>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeySpecs {
    pub flags: Vec<String>,
    pub begin_search: BeginSearch,
    pub find_keys: FindKeys,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BeginSearch {
    pub index: Option<Index>,
    pub keyword: Option<Keyword>
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Index {
    pub pos: i32
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Keyword {
    pub keyword: String,
    pub startfrom: i32
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FindKeys {
    pub range: Option<Range>,
    pub keynum: Option<KeyNum>
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Range {
    pub lastkey: i32,
    pub step: i32,
    pub limit: i32
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyNum {
    pub keynumidx: i32,
    pub firstkey: i32,
    pub step: i32
}
use serde::{Deserialize, Serialize};

pub mod holder;
pub mod utils;

#[repr(u32)]
#[derive(Clone)]
pub enum CommandFlags {
    Admin = 1_u32 << 0,
    AllowBusy = 1_u32 << 1,
    Asking = 1_u32 << 2,
    Blocking = 1_u32 << 3,
    DenyOOM = 1_u32 << 4,
    Fast = 1_u32 << 5,
    Loading = 1_u32 << 6,
    MayReplicate = 1_u32 << 7,
    Noscript = 1_u32 << 8,
    NoAsyncLoading = 1_u32 << 9,
    NoAuth = 1_u32 << 10,
    NoMandatoryKeys = 1_u32 << 11,
    NoMulti = 1_u32 << 12,
    OnlySentinel = 1_u32 << 13,
    Protected = 1_u32 << 14,
    PubSub = 1_u32 << 15,
    Readonly = 1_u32 << 16,
    Sentinel = 1_u32 << 17,
    SkipMonitor = 1_u32 << 18,
    SkipSlowLog = 1_u32 << 19,
    Stale = 1_u32 << 20,
    TouchesArbitraryKeys = 1_u32 << 21,
    Write = 1_u32 << 22,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Group {
    Bitmap,
    Cluster,
    Connection,
    Generic,
    Geo,
    Hash,
    Hyperloglog,
    List,
    PubSub,
    Scripting,
    Sentinel,
    Server,
    Set,
    SortedSet,
    Stream,
    String,
    Transactions,
}

#[derive(Debug)]
pub struct Command {
    pub name: String,
    pub container: Option<String>,
    pub group: Group,
    pub arity: i32,
    pub function: Option<String>,
    pub command_flags: u32,
    pub key_specs: Option<Vec<KeySpecs>>,
}

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
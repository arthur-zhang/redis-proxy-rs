use bitflags::bitflags;
use serde::{Deserialize, Serialize};

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct CommandFlags : u32 {
        const Admin = 1_u32 << 0;
        const AllowBusy = 1_u32 << 1;
        const Asking = 1_u32 << 2;
        const Blocking = 1_u32 << 3;
        const DenyOOM = 1_u32 << 4;
        const Fast = 1_u32 << 5;
        const Loading = 1_u32 << 6;
        const MayReplicate = 1_u32 << 7;
        const Noscript = 1_u32 << 8;
        const NoAsyncLoading = 1_u32 << 9;
        const NoAuth = 1_u32 << 10;
        const NoMandatoryKeys = 1_u32 << 11;
        const NoMulti = 1_u32 << 12;
        const OnlySentinel = 1_u32 << 13;
        const Protected = 1_u32 << 14;
        const PubSub = 1_u32 << 15;
        const Readonly = 1_u32 << 16;
        const Sentinel = 1_u32 << 17;
        const SkipMonitor = 1_u32 << 18;
        const SkipSlowLog = 1_u32 << 19;
        const Stale = 1_u32 << 20;
        const TouchesArbitraryKeys = 1_u32 << 21;
        const Write = 1_u32 << 22;
    }
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
    pub command_flags: CommandFlags,
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
    pub pos: usize,
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
    pub step: usize,
    pub limit: usize
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyNum {
    pub keynumidx: usize,
    pub firstkey: usize,
    pub step: usize
}
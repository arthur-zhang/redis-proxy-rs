use std::fs;
use std::path::Path;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

pub type TConfig = Arc<Config>;
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Config {
    pub debug: Option<bool>,
    pub server: Server,
    pub upstream: Upstream,
    pub filter_chain: FilterChain,
}


#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Server {
    pub address: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Upstream {
    pub address: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FilterChain {
    pub blacklist: Option<Blacklist>,
    pub mirror: Option<Mirror>,
    pub log: Option<Log>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Blacklist {
    pub block_patterns: Vec<String>,
    pub split_regex: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Mirror {
    pub address: String,
    pub mirror_patterns: Vec<String>,
    pub split_regex: String,
    pub queue_size: usize,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Log {}

impl Config {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let conf_str = fs::read_to_string(path)?;
        let conf: Config = toml::from_str(&conf_str)?;
        Ok(conf)
    }
}
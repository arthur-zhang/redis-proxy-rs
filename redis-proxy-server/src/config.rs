use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Config {
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
    pub filters: Vec<String>,
    pub blacklist: Option<Blacklist>,
    pub mirror: Option<Mirror>,
    pub log: Option<Log>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Blacklist {
    pub block_patterns: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Mirror {
    pub address: String,
    pub pattern: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Log {}

impl Config {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let conf_str = fs::read_to_string(path)?;
        let conf: Config = toml::from_str(&conf_str)?;
        Ok(conf)
    }
}
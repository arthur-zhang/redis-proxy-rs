use std::fs;
use std::path::Path;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

pub type TConfig = Arc<Config>;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub debug: Option<bool>,
    pub server: Server,
    pub upstream: Upstream,
    pub etcd_config: EtcdConfig,
    pub filter_chain: FilterChain,
    pub prometheus: Option<Prometheus>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct EtcdConfig {
    pub(crate) endpoints: Vec<String>,
    pub(crate) username: String,
    pub(crate) password: String,
    pub(crate) prefix: String,
    pub(crate) interval: u64,
    pub(crate) timeout: u64,
    pub(crate) key_splitter: char
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
pub struct Blacklist {}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Mirror { pub address: String}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Log {}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Prometheus {
    pub address: String,
    pub export_uri: String,
}

impl Config {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let conf_str = fs::read_to_string(path)?;
        let conf: Config = toml::from_str(&conf_str)?;
        Ok(conf)
    }
}
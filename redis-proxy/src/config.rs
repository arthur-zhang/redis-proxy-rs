use std::fs;
use std::path::Path;
use std::sync::Arc;

use serde::Deserialize;

use crate::upstream_conn_pool::RedisConnection;

pub type TConfig = Arc<Config>;

#[derive(Debug, Deserialize, Clone, Copy)]
pub enum ConfigCenter {
    Etcd,
    Local,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub debug: Option<bool>,
    pub splitter: Option<char>,
    pub server: Server,
    pub upstream: Upstream,
    pub etcd_config: Option<EtcdConfig>,
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
}

#[derive(Debug, Deserialize, Clone)]
pub struct ConnPoolConf {
    pub(crate) idle_timeout: u64,
    pub(crate) min_connections: u32,
    pub(crate) max_connections: u32,
    pub(crate) test_before_acquire: bool,
    pub(crate) max_lifetime: u64,
    pub(crate) acquire_timeout: u64,
}

impl ConnPoolConf {
    pub fn new_pool_opt(&self) -> poolx::PoolOptions<RedisConnection> { 
        poolx::PoolOptions::new()
            .idle_timeout(std::time::Duration::from_secs(self.idle_timeout))
            .min_connections(self.min_connections)
            .max_connections(self.max_connections)
            .test_before_acquire(self.test_before_acquire)
            .max_lifetime(std::time::Duration::from_secs(self.max_lifetime))
            .acquire_timeout(std::time::Duration::from_secs(self.acquire_timeout))
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct Server {
    pub address: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Upstream {
    pub address: String,
    pub conn_pool_conf: ConnPoolConf,
}

#[derive(Debug, Deserialize, Clone)]
pub struct FilterChain {
    pub blacklist: Option<Blacklist>,
    pub mirror: Option<Mirror>,
    pub log: Option<Log>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct LocalRoute {
    pub commands: Vec<String>,
    pub keys: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Blacklist {
    pub config_center: ConfigCenter,
    pub local_routes: Option<Vec<LocalRoute>>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Mirror {
    pub address: String,
    pub config_center: ConfigCenter,
    pub local_routes: Option<Vec<LocalRoute>>,
    pub conn_pool_conf: ConnPoolConf,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Log {}

#[derive(Debug, Deserialize, Clone)]
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
use std::env::args;
use std::path::Path;
use std::sync::Arc;

use anyhow::anyhow;
use log::{debug, error, info};
use poolx::PoolOptions;

use redis_proxy::config;
use redis_proxy::config::{Blacklist, Config};
use redis_proxy::prometheus::PrometheusServer;
use redis_proxy::proxy::Proxy;
use redis_proxy::server::ProxyServer;
use redis_proxy::upstream_conn_pool::{RedisConnection, RedisConnectionOption};

use crate::blacklist_filter::BlackListFilter;
use crate::filter_trait::FilterContext;
use crate::log_filter::LogFilter;
use crate::mirror_filter::Mirror;
use crate::proxy_impl::RedisProxyImpl;

mod path_trie;
mod proxy_impl;
mod tools;
mod filter_trait;
mod mirror_filter;
mod log_filter;
mod blacklist_filter;


#[tokio::main]
async fn main() -> anyhow::Result<()> {
    std::env::set_var("RUST_LOG", "error");
    env_logger::init();

    PrometheusServer::start("127.0.0.1:9095", "/metrics");
    // let conf_path = args().nth(1).ok_or(anyhow::anyhow!("config file path is required"))?;
    let conf_path = "../config.toml".to_string();

    let conf = get_conf(conf_path.as_ref()).map_err(|e| { anyhow::anyhow!("load config error: {:?}", e) })?;
    debug!("{:?}", conf);
    let conf = Arc::new(conf);
    info!("Starting server...");

    let filters = load_filters(&conf);

    let proxy = RedisProxyImpl { filters, conf: conf.clone() };
    let server = ProxyServer::new(conf, proxy)?;
    let _ = server.start().await;
    info!("Server quit.");
    Ok(())
}

fn load_filters(conf: &Arc<Config>) -> Vec<Box<dyn Proxy<CTX=FilterContext> + Send + Sync>> {
    let mut filters: Vec<Box<dyn Proxy<CTX=FilterContext> + Send + Sync>> = vec![];

    if let Some(ref blacklist_filter) = conf.filter_chain.blacklist {
        let blacklist_filter = BlackListFilter::new(blacklist_filter.block_patterns.clone(), &blacklist_filter.split_regex).unwrap();
        filters.push(Box::new(blacklist_filter));
    }

    if let Some(ref mirror) = conf.filter_chain.mirror {
        // let mirror_filter = Mirror::new(&mirror.address, &mirror.mirror_patterns, &mirror.split_regex, mirror.queue_size).unwrap();
        // filters.push(Box::new(mirror_filter));
    }
    if let Some(ref log) = conf.filter_chain.log {
        let log_filter = LogFilter {};
        filters.push(Box::new(log_filter));
    }
    filters
}

fn get_conf(path: &Path) -> anyhow::Result<config::Config> {
    let conf = config::Config::load(path)?;
    Ok(conf)
}

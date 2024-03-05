use std::env::args;
use std::path::Path;
use std::sync::Arc;

use anyhow::anyhow;
use log::{debug, error, info};
use poolx::PoolOptions;

use redis_proxy::config;
use redis_proxy::config::Blacklist;
use redis_proxy::proxy::Proxy;
use redis_proxy::server::ProxyServer;
use redis_proxy::upstream_conn_pool::{RedisConnection, RedisConnectionOption};

use crate::blacklist_filter::BlackListFilter;
use crate::filter_trait::FilterContext;
use crate::log_filter::LogFilter;
use crate::mirror_filter::Mirror;
use crate::proxy_impl::MyProxy;

mod path_trie;
mod proxy_impl;
mod tools;
mod filter_trait;
mod mirror_filter;
mod log_filter;
mod blacklist_filter;


#[tokio::main]
async fn main() -> anyhow::Result<()> {
    std::env::set_var("RUST_LOG", "info");
    env_logger::init();

    let conf_path = args().nth(1).ok_or(anyhow::anyhow!("config file path is required"))?;
    let conf = get_conf(conf_path.as_ref()).map_err(|e| { anyhow::anyhow!("load config error: {:?}", e) })?;
    debug!("{:?}", conf);
    let conf = Arc::new(conf);
    info!("Starting server...");


    let log_filter = LogFilter {};
    let mut filters: Vec<Box<dyn Proxy<CTX=FilterContext> + Send + Sync>> = vec![];

    if let Some(ref blacklist_filter) = conf.filter_chain.blacklist {
        let blacklist_filter = BlackListFilter::new(blacklist_filter.block_patterns.clone(), &blacklist_filter.split_regex).unwrap();
        filters.push(Box::new(blacklist_filter));
    }

    if let Some(ref mirror) = conf.filter_chain.mirror {
        let mirror_filter = Mirror::new(&mirror.address, &mirror.mirror_patterns, &mirror.split_regex, mirror.queue_size).unwrap();
        filters.push(Box::new(mirror_filter));
    }
    if let Some(ref log) = conf.filter_chain.log {
        let log_filter = LogFilter {};
        filters.push(Box::new(log_filter));
    }

    let proxy = MyProxy { filters, conf: conf.clone() };
    let server = ProxyServer::new(conf, proxy)?;
    let _ = server.start().await;
    info!("Server quit.");
    Ok(())
}

fn get_conf(path: &Path) -> anyhow::Result<config::Config> {
    let conf = config::Config::load(path)?;
    Ok(conf)
}

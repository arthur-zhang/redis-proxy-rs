use std::env::args;
use std::path::Path;
use std::sync::Arc;

use anyhow::anyhow;
use log::{debug, error, info};
use poolx::PoolOptions;

use redis_proxy::config;
use redis_proxy::server::ProxyServer;
use redis_proxy::upstream_conn_pool::{RedisConnection, RedisConnectionOption};

use crate::mirror::Mirror;
use crate::proxy_impl::MyProxy;

mod path_trie;
mod proxy_impl;
mod tools;
mod filter_trait;
mod mirror;


#[tokio::main]
async fn main() -> anyhow::Result<()> {
    std::env::set_var("RUST_LOG", "info");
    env_logger::init();

    let conf_path = args().nth(1).ok_or(anyhow::anyhow!("config file path is required"))?;
    let conf = get_conf(conf_path.as_ref()).map_err(|e| { anyhow::anyhow!("load config error: {:?}", e) })?;
    debug!("{:?}", conf);
    let conf = Arc::new(conf);
    info!("Starting server...");


    let conn_option = conf.filter_chain.mirror.as_ref().unwrap().address.parse::<RedisConnectionOption>().unwrap();
    let pool: poolx::Pool<RedisConnection> = PoolOptions::new()
        .idle_timeout(std::time::Duration::from_secs(3))
        .min_connections(3)
        .max_connections(50000)
        .connect_lazy_with(conn_option);

    let mirror = Mirror::new(pool);
    let proxy = MyProxy { filters: vec![Box::new(mirror)], conf: conf.clone() };
    let server = ProxyServer::new(conf, proxy)?;
    let _ = server.start().await;
    info!("Server quit.");
    Ok(())
}

fn get_conf(path: &Path) -> anyhow::Result<config::Config> {
    let conf = config::Config::load(path)?;
    Ok(conf)
}

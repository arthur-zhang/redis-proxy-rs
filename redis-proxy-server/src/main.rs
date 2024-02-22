use std::env::args;
use std::path::Path;

use log::{debug, error, info};

use server::ProxyServer;

mod server;

mod mirror_filter;
mod path_trie;
mod log_filter;
mod blacklist_filter;
mod time_filter;
mod config;
mod filter_chain;


#[tokio::main]
async fn main() -> anyhow::Result<()> {
    std::env::set_var("RUST_LOG", "debug");
    env_logger::init();

    let conf_path = args().nth(1).ok_or(Err(anyhow::anyhow!("config file path is required")))?;
    let conf = get_conf(conf_path.as_ref()).map_err(|e| { error!("load config error: {:?}", e); })?;
    debug!("{:?}", conf);

    info!("Starting server...");
    let server = ProxyServer::new(conf)?;
    let _ = server.start().await;
    info!("Server quit.");
}

fn get_conf(path: &Path) -> anyhow::Result<config::Config> {
    let conf = config::Config::load(path)?;
    Ok(conf)
}

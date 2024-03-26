use std::env::args;
use std::path::Path;
use std::sync::Arc;

use log::info;
#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

use redis_proxy::config::Config;
use redis_proxy::etcd_client::EtcdClient;
use redis_proxy::filter_trait::Filter;
use redis_proxy::prometheus::PrometheusServer;
use redis_proxy::server::ProxyServer;

use crate::blacklist_filter::BlackListFilter;
use crate::log_filter::LogFilter;
use crate::proxy_impl::FilterImpl;

mod proxy_impl;
mod filter_trait;
mod log_filter;
mod blacklist_filter;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let conf_path = args().nth(1).ok_or(anyhow::anyhow!("config file path is required"))?;
    let conf = get_conf(conf_path.as_ref()).map_err(|e| { anyhow::anyhow!("load config error: {:?}", e) })?;
    info!("loaded config: {:?}", conf);
    start_prometheus_server(&conf);

    if conf.debug.unwrap_or(false) {
        console_subscriber::init();
    }
    info!("starting redis proxy server...");

    let mut etcd_client = None;
    if let Some(etcd_config) = &conf.etcd_config {
        etcd_client = Some(EtcdClient::new(etcd_config.clone()).await?);
        info!("etcd client connected.");
    }

    let conf = Arc::new(conf);
    let filters = load_filters(&conf, etcd_client.clone()).await?;

    let filters = FilterImpl { filters, conf: conf.clone() };
    let server = ProxyServer::new(conf, filters, etcd_client)?;
    let _ = server.start().await;
    info!("redis proxy server quit.");
    Ok(())
}

fn start_prometheus_server(conf: &Config) {
    if let Some(ref prometheus) = conf.prometheus {
        PrometheusServer::start(&prometheus.address, &prometheus.export_uri);
        info!("start prometheus server at: {}{}", prometheus.address, prometheus.export_uri);
    }
}

async fn load_filters(conf: &Arc<Config>, etcd_client: Option<EtcdClient>) -> anyhow::Result<Vec<Box<dyn Filter + Send + Sync>>> {
    let mut filters: Vec<Box<dyn Filter + Send + Sync>> = vec![];
    let splitter = conf.splitter.unwrap_or(':');

    if let Some(blacklist) = &conf.filter_chain.blacklist {
        let blacklist_filter = BlackListFilter::new(splitter, blacklist, etcd_client.clone()).await?;
        filters.push(Box::new(blacklist_filter));
    }

    if let Some(_) = conf.filter_chain.log {
        let log_filter = LogFilter {};
        filters.push(Box::new(log_filter));
    }
    Ok(filters)
}

fn get_conf(path: &Path) -> anyhow::Result<Config> {
    let conf = Config::load(path)?;
    Ok(conf)
}

#[cfg(test)]
mod tests {
    use redis_command_gen::COMMAND_ATTRIBUTES;

    #[test]
    fn test_a() {
        for (k, v) in COMMAND_ATTRIBUTES.iter() {
            if let Some(key) = &v.key_specs {
                println!("{}: {:?}, key:{:?}", k, v.arity, key);
            }
        }
    }
}

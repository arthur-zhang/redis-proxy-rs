use log::info;

use server::ProxyServer;

mod server;

mod mirror_filter;
mod path_trie;
mod log_filter;

#[tokio::main]
async fn main() {
    std::env::set_var("RUST_LOG", "info");
    env_logger::init();
    info!("Starting server...");
    let server = ProxyServer::new();
    let _ = server.start().await;
    info!("Server quit.");
}

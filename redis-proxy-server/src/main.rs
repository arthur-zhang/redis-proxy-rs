use log::info;

mod server;

mod mirror;
mod path_trie;

#[tokio::main]
async fn main() {
    std::env::set_var("RUST_LOG", "info");
    env_logger::init();
    info!("Starting server...");
    let server = server::ProxyServer::new();
    server.start().await;
}

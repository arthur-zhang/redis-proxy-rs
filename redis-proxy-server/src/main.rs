mod server;
mod path_trie;

#[tokio::main]
async fn main() {
    env_logger::init();

    println!("Hello, world!");
    let server = server::ProxyServer::new();
    server.start().await;
}

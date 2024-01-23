use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio_stream::StreamExt;
use tokio_util::codec::{BytesCodec, FramedRead};

use redis_proxy_codec::codec::{MyClientCodec, ParseError};
use redis_proxy_codec::command::Command;

pub struct ProxyServer {}

pub struct UpstreamSession {
    c2p_read_half: OwnedReadHalf,
    p2b_write_half: OwnedWriteHalf,
}

pub struct DownstreamSession {
    p2c_write_half: OwnedWriteHalf,
    b2p_read_half: OwnedReadHalf,
}

impl ProxyServer {
    pub fn new() -> Self {
        ProxyServer {}
    }
    pub async fn start(&self) -> anyhow::Result<()> {
        let addr = "127.0.0.1:16379";
        let remote = "127.0.0.1:6379";

        let listener = TcpListener::bind(addr).await?;
        loop {
            let (mut sock, _) = listener.accept().await?;

            let remote = remote.clone();

            let mut upstream_conn = TcpStream::connect(remote).await?;

            let (p2b_r, mut p2b_w) = upstream_conn.into_split();

            let (c2p_r, mut c2p_w) = sock.into_split();

            tokio::spawn(async move {
                let mut reader = FramedRead::new(c2p_r, MyClientCodec::new());
                while let Some(it) = reader.next().await {
                    match it {
                        Ok(resp) => {
                            let command = Command::new(resp);
                            println!("command: {:?}", command);
                            println!("command name: {:?}", command.get_command_name());
                            let key = command.get_key().map(|it|std::str::from_utf8(it));
                            if let Some(Ok(key)) = key {
                                println!("key: {:?}", key);
                                // let parts = key.split(":").collect::<Vec<_>>();
                            }
                            p2b_w.write_all(command.into_packet().data.as_ref()).await.unwrap();
                        }
                        Err(ParseError::NotEnoughData) => {
                            continue;
                        }
                        _ => {
                            break;
                        }
                    }
                }
            });


            tokio::spawn(async move {
                let mut reader = FramedRead::new(p2b_r, BytesCodec::new());
                loop {
                    while let Some(it) = reader.next().await {
                        if let Ok(mut it) = it {
                            c2p_w.write_buf(&mut it).await.unwrap();
                        }
                    }
                }
            });
        }
    }
}
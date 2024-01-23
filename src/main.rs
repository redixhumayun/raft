use log::{error, info};
use std::env;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    //  initialize the logger
    env::set_var("RUST_LOG", "info");
    env_logger::init();

    //  Create a TCP listener
    let listener = TcpListener::bind("127.0.0.1:8080").await.unwrap();
    info!("Server running on port 8080");

    //  listen for messages
    loop {
        match listener.accept().await {
            Ok((mut socket, addr)) => {
                info!("New connection from: {}", addr);

                tokio::spawn(async move {
                    let mut buf = [0; 1024];

                    loop {
                        //  read the data
                        let n = match socket.read(&mut buf).await {
                            Ok(n) if n == 0 => return,
                            Ok(n) => n,
                            Err(err) => {
                                error!("Failed to read from socket; err={:?}", err);
                                return;
                            }
                        };
                        //  write the data
                        if let Err(e) = socket.write_all(&buf[0..n]).await {
                            error!("Failed to write back to socket; err={:?}", e);
                            return;
                        }
                    }
                });
            }
            Err(err) => error!("Failed to accept connection: {}", err),
        }
    }
}

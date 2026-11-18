use std::{
    io::{self, ErrorKind},
    time::Duration,
};
use untitled::net::transport::Transport;

const BIND_ADDR: &str = "127.0.0.1:6767";

#[tokio::main]
async fn main() -> io::Result<()> {
    let mut transport = Transport::bind(BIND_ADDR).await?;

    println!("Listening on {BIND_ADDR}");

    loop {
        tokio::select! {
            result = transport.recv() => {
                let datagram = match result {
                    Ok(datagram) => datagram,
                    Err(err) if err.kind() == ErrorKind::InvalidData => {
                        eprintln!("Dropping invalid datagram: {err}");
                        continue;
                    }
                    Err(err) => {
                        eprintln!("Failed to receive datagram: {err}");
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        continue;
                    }
                };

                if let Err(err) = transport.send(&datagram.payload, datagram.sender).await {
                    eprintln!("Failed to send datagram to {}: {err}", datagram.sender);
                }
            }
            _ = tokio::signal::ctrl_c() => {
                println!("Shutting down...");
                break;
            }
        }
    }

    Ok(())
}

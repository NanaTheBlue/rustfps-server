use std::{
    collections::HashSet,
    io::{self, ErrorKind},
    net::SocketAddr,
};

use tokio::net::{ToSocketAddrs, UdpSocket};

pub const MAX_DATAGRAM_SIZE: usize = 1024;
pub const MAX_CLIENTS: usize = 64;

#[derive(Debug)]
pub struct Datagram {
    pub payload: Vec<u8>,
    pub sender: SocketAddr,
}

pub struct Transport {
    socket: UdpSocket,
    clients: HashSet<SocketAddr>,
    recv_buf: [u8; MAX_DATAGRAM_SIZE + 1],
}

impl Transport {
    pub async fn bind(addr: impl ToSocketAddrs) -> io::Result<Self> {
        let socket = UdpSocket::bind(addr).await?;

        Ok(Self {
            socket,
            clients: HashSet::new(),
            recv_buf: [0u8; MAX_DATAGRAM_SIZE + 1],
        })
    }

    pub async fn send(&self, payload: &[u8], recipient: SocketAddr) -> io::Result<usize> {
        if payload.len() > MAX_DATAGRAM_SIZE {
            return Err(datagram_too_large(ErrorKind::InvalidInput));
        }

        self.socket.send_to(payload, recipient).await
    }

    pub async fn broadcast(&self, payload: &[u8]) -> io::Result<Vec<(SocketAddr, io::Error)>> {
        if payload.len() > MAX_DATAGRAM_SIZE {
            return Err(datagram_too_large(ErrorKind::InvalidInput));
        }

        let mut failures = Vec::new();
        for &client in &self.clients {
            if let Err(err) = self.socket.send_to(payload, client).await {
                failures.push((client, err));
            }
        }

        Ok(failures)
    }

    pub async fn recv(&mut self) -> io::Result<Datagram> {
        loop {
            match self.socket.recv_from(&mut self.recv_buf).await {
                Ok((size, sender)) => {
                    if size > MAX_DATAGRAM_SIZE {
                        return Err(datagram_too_large(ErrorKind::InvalidData));
                    }

                    return Ok(Datagram {
                        payload: self.recv_buf[..size].to_vec(),
                        sender,
                    });
                }
                Err(err) if err.kind() == ErrorKind::ConnectionReset => continue,
                #[cfg(windows)]
                Err(err) if err.raw_os_error() == Some(10040) => {
                    return Err(datagram_too_large(ErrorKind::InvalidData));
                }
                Err(err) => return Err(err),
            }
        }
    }

    pub fn add_client(&mut self, client: SocketAddr) -> bool {
        if self.clients.len() >= MAX_CLIENTS {
            return false;
        }

        self.clients.insert(client)
    }

    pub fn remove_client(&mut self, client: &SocketAddr) -> bool {
        self.clients.remove(client)
    }

    pub fn clients(&self) -> &HashSet<SocketAddr> {
        &self.clients
    }
}

fn datagram_too_large(kind: ErrorKind) -> io::Error {
    io::Error::new(kind, format!("datagram exceeds {MAX_DATAGRAM_SIZE} bytes"))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::timeout;

    use super::*;

    const TEST_TIMEOUT: Duration = Duration::from_secs(1);

    async fn recv_with_timeout(transport: &mut Transport) -> io::Result<Datagram> {
        timeout(TEST_TIMEOUT, transport.recv())
            .await
            .expect("timed out waiting for datagram")
    }

    #[tokio::test]
    async fn test_send_and_receive() -> io::Result<()> {
        let server_addr: SocketAddr = "127.0.0.1:16767".parse().unwrap();
        let client_addr: SocketAddr = "127.0.0.1:16768".parse().unwrap();

        let mut server = Transport::bind(server_addr).await?;
        let mut client = Transport::bind(client_addr).await?;

        let message = b"hello from client";
        let sent = client.send(message, server_addr).await?;
        assert_eq!(sent, message.len());

        let datagram = recv_with_timeout(&mut server).await?;
        assert_eq!(datagram.payload, message);
        assert_eq!(datagram.sender, client_addr);
        assert!(server.clients().is_empty());

        let reply = b"ack from server";
        server.send(reply, datagram.sender).await?;

        let response = recv_with_timeout(&mut client).await?;
        assert_eq!(response.payload, reply);
        assert_eq!(response.sender, server_addr);

        Ok(())
    }

    #[tokio::test]
    async fn test_broadcast() -> io::Result<()> {
        let server_addr: SocketAddr = "127.0.0.1:16770".parse().unwrap();
        let client1_addr: SocketAddr = "127.0.0.1:16771".parse().unwrap();
        let client2_addr: SocketAddr = "127.0.0.1:16772".parse().unwrap();

        let mut server = Transport::bind(server_addr).await?;
        let mut client1 = Transport::bind(client1_addr).await?;
        let mut client2 = Transport::bind(client2_addr).await?;

        assert!(server.add_client(client1_addr));
        assert!(server.add_client(client2_addr));

        let failures = server.broadcast(b"broadcast message").await?;
        assert!(failures.is_empty());

        let msg1 = recv_with_timeout(&mut client1).await?;
        let msg2 = recv_with_timeout(&mut client2).await?;
        assert_eq!(msg1.payload, b"broadcast message");
        assert_eq!(msg2.payload, b"broadcast message");

        Ok(())
    }

    #[tokio::test]
    async fn test_oversized_datagram_rejected() -> io::Result<()> {
        let server_addr: SocketAddr = "127.0.0.1:16773".parse().unwrap();
        let raw_client_addr: SocketAddr = "127.0.0.1:16774".parse().unwrap();

        let mut server = Transport::bind(server_addr).await?;
        let raw_client = UdpSocket::bind(raw_client_addr).await?;

        let oversized = vec![0xAB; MAX_DATAGRAM_SIZE + 2];

        let send_err = server.send(&oversized, raw_client_addr).await.unwrap_err();
        assert_eq!(send_err.kind(), ErrorKind::InvalidInput);

        raw_client.send_to(&oversized, server_addr).await?;
        let recv_err = recv_with_timeout(&mut server).await.unwrap_err();
        assert_eq!(recv_err.kind(), ErrorKind::InvalidData);

        Ok(())
    }
}

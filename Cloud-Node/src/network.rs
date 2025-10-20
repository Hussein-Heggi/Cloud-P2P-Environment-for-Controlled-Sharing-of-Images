use crate::message::Message;
use anyhow::{Context, Result};
use log::{debug, error, info, warn};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

/// Manages TCP connections between nodes
#[derive(Clone)]
pub struct NetworkLayer {
    node_id: u32,
    listen_addr: String,
}

impl NetworkLayer {
    pub fn new(node_id: u32, listen_addr: String) -> Self {
        Self {
            node_id,
            listen_addr,
        }
    }

    /// Start listening for incoming connections
    pub async fn start_listener(
        &self,
        tx: mpsc::UnboundedSender<(u32, Message)>,
    ) -> Result<()> {
        let listener = TcpListener::bind(&self.listen_addr)
            .await
            .context(format!("Failed to bind to {}", self.listen_addr))?;

        info!(
            "Node {} listening on {}",
            self.node_id, self.listen_addr
        );

        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    debug!("New connection from {}", addr);
                    let tx = tx.clone();
                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_connection(stream, tx).await {
                            error!("Connection error: {}", e);
                        }
                    });
                }
                Err(e) => {
                    error!("Failed to accept connection: {}", e);
                }
            }
        }
    }

    /// Handle an incoming connection
    async fn handle_connection(
        mut stream: TcpStream,
        tx: mpsc::UnboundedSender<(u32, Message)>,
    ) -> Result<()> {
        loop {
            // Read length prefix (4 bytes)
            let len = match stream.read_u32().await {
                Ok(len) => len as usize,
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    debug!("Connection closed by peer");
                    break;
                }
                Err(e) => return Err(e.into()),
            };

            // Read message data
            let mut buffer = vec![0u8; len];
            stream
                .read_exact(&mut buffer)
                .await
                .context("Failed to read message")?;

            // Deserialize message
            let message: Message = serde_json::from_slice(&buffer)
                .context("Failed to deserialize message")?;

            let sender_id = message.sender_id();
            debug!("Received {:?} from node {}", message, sender_id);

            // Send to node's message handler
            if tx.send((sender_id, message)).is_err() {
                warn!("Failed to send message to handler - channel closed");
                break;
            }
        }

        Ok(())
    }

    /// Connect to a remote node
    pub async fn connect_to_peer(
        &self,
        peer_addr: &str,
    ) -> Result<PeerConnection> {
        let mut stream = TcpStream::connect(peer_addr)
            .await
            .context(format!("Failed to connect to {}", peer_addr))?;

        // Send initial hello
        let hello = Message::Hello {
            from_node: self.node_id,
        };
        Self::send_message(&mut stream, &hello).await?;

        info!("Connected to peer at {}", peer_addr);

        Ok(PeerConnection {
            stream: Arc::new(tokio::sync::Mutex::new(stream)),
        })
    }

    /// Send a message over a stream
    async fn send_message(stream: &mut TcpStream, message: &Message) -> Result<()> {
        let bytes = message.to_bytes()?;
        stream
            .write_all(&bytes)
            .await
            .context("Failed to send message")?;
        Ok(())
    }
}

/// Represents a connection to a peer node
#[derive(Clone)]
pub struct PeerConnection {
    stream: Arc<tokio::sync::Mutex<TcpStream>>,
}

impl PeerConnection {
    /// Send a message to this peer
    pub async fn send(&self, message: &Message) -> Result<()> {
        let mut stream = self.stream.lock().await;
        let bytes = message.to_bytes()?;
        stream
            .write_all(&bytes)
            .await
            .context("Failed to send to peer")?;
        debug!("Sent {:?} to peer", message);
        Ok(())
    }
}
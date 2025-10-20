use crate::message::Message;
use anyhow::{Context, Result};
use log::{debug, error, info, warn};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, RwLock};

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
        peers: Arc<RwLock<HashMap<u32, PeerConnection>>>,
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
                    let peers = peers.clone();
                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_incoming_connection(stream, tx, peers).await {
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
    async fn handle_incoming_connection(
        stream: TcpStream,
        tx: mpsc::UnboundedSender<(u32, Message)>,
        peers: Arc<RwLock<HashMap<u32, PeerConnection>>>,
    ) -> Result<()> {
        // Wrap the stream immediately
        let peer_conn = PeerConnection {
            stream: Arc::new(tokio::sync::Mutex::new(stream)),
        };
        
        // Clone for reading
        let read_conn = peer_conn.clone();
        
        // Read first message (should be Hello)
        let first_msg = read_conn.receive_one().await?;
        
        if let Message::Hello { from_node } = first_msg {
            info!("Received Hello from node {} on incoming connection", from_node);
            
            // Store the connection if we don't already have one
            let mut peers_write = peers.write().await;
            if !peers_write.contains_key(&from_node) {
                peers_write.insert(from_node, peer_conn.clone());
                info!("Stored incoming connection from node {}", from_node);
            }
            drop(peers_write);
            
            // Forward Hello message
            tx.send((from_node, first_msg))?;
            
            // Continue reading messages
            Self::read_loop(from_node, read_conn, tx).await?;
        } else {
            warn!("First message was not Hello, closing connection");
        }
        
        Ok(())
    }

    /// Continuous read loop for a connection
    async fn read_loop(
        peer_id: u32,
        conn: PeerConnection,
        tx: mpsc::UnboundedSender<(u32, Message)>,
    ) -> Result<()> {
        loop {
            match conn.receive_one().await {
                Ok(message) => {
                    debug!("Received {:?} from node {}", message, peer_id);
                    if tx.send((peer_id, message)).is_err() {
                        warn!("Failed to send message to handler - channel closed");
                        break;
                    }
                }
                Err(e) => {
                    if e.to_string().contains("UnexpectedEof") {
                        debug!("Connection closed by node {}", peer_id);
                    } else {
                        error!("Error reading from node {}: {}", peer_id, e);
                    }
                    break;
                }
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
    
    /// Receive one message from this peer
    async fn receive_one(&self) -> Result<Message> {
        let mut stream = self.stream.lock().await;
        
        // Read length prefix (4 bytes)
        let len = stream.read_u32().await
            .context("Failed to read message length")? as usize;
        
        // Read message data
        let mut buffer = vec![0u8; len];
        stream.read_exact(&mut buffer).await
            .context("Failed to read message")?;
        
        // Deserialize message
        let message: Message = serde_json::from_slice(&buffer)
            .context("Failed to deserialize message")?;
        
        Ok(message)
    }
}
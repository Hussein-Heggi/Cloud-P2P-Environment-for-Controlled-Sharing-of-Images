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
    listen_addr: String,
}

impl NetworkLayer {
    pub fn new(listen_addr: String) -> Self {
        Self { listen_addr }
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

        info!("📡 Listening on {}", self.listen_addr);

        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    debug!("New connection from {}", addr);
                    let tx = tx.clone();
                    let peers = peers.clone();
                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_connection(stream, tx, peers).await {
                            error!("Connection error from {}: {}", addr, e);
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
        stream: TcpStream,
        tx: mpsc::UnboundedSender<(u32, Message)>,
        peers: Arc<RwLock<HashMap<u32, PeerConnection>>>,
    ) -> Result<()> {
        let peer_conn = PeerConnection::new(stream);
        let read_conn = peer_conn.clone();
        
        // Read first message to identify the node
        let first_msg = read_conn.receive_one().await?;
        
        // Extract node ID from first message
        let node_id = match &first_msg {
            Message::WhoIsLeader { node_id, .. } => *node_id,
            Message::Heartbeat { node_id } => *node_id,
            Message::Coordinator { leader_id, .. } => *leader_id,
            Message::Takeover { from_id } => *from_id,
        };
        
        info!("🔌 Connection identified: Node {}", node_id);
        
        // Store connection
        peers.write().await.insert(node_id, peer_conn.clone());
        
        // Forward first message
        tx.send((node_id, first_msg))?;
        
        // Continue reading messages
        Self::read_loop(node_id, read_conn, tx).await?;
        
        Ok(())
    }

    /// Continuous read loop for a connection
    async fn read_loop(
        node_id: u32,
        conn: PeerConnection,
        tx: mpsc::UnboundedSender<(u32, Message)>,
    ) -> Result<()> {
        loop {
            match conn.receive_one().await {
                Ok(message) => {
                    if tx.send((node_id, message)).is_err() {
                        warn!("Channel closed for Node {}", node_id);
                        break;
                    }
                }
                Err(e) => {
                    if e.to_string().contains("UnexpectedEof") {
                        debug!("Connection closed: Node {}", node_id);
                    } else {
                        error!("Read error from Node {}: {}", node_id, e);
                    }
                    break;
                }
            }
        }
        Ok(())
    }

    /// Connect to a remote node
    pub async fn connect_to_peer(&self, peer_addr: &str) -> Result<PeerConnection> {
        let stream = TcpStream::connect(peer_addr)
            .await
            .context(format!("Failed to connect to {}", peer_addr))?;

        info!("🔗 Connected to {}", peer_addr);
        Ok(PeerConnection::new(stream))
    }
}

/// Represents a connection to a peer node
#[derive(Clone)]
pub struct PeerConnection {
    stream: Arc<tokio::sync::Mutex<TcpStream>>,
}

impl PeerConnection {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream: Arc::new(tokio::sync::Mutex::new(stream)),
        }
    }

    /// Send a message to this peer
    pub async fn send(&self, message: &Message) -> Result<()> {
        let mut stream = self.stream.lock().await;
        let bytes = message.to_bytes()?;
        stream.write_all(&bytes).await?;
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
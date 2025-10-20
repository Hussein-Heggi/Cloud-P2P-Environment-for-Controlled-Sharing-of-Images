use crate::message::Message;
use crate::network::{NetworkLayer, PeerConnection};
use anyhow::{Context, Result};
use log::{error, info, warn};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, RwLock};
use tokio::time::{interval, Duration};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub id: u32,
    pub address: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub nodes: Vec<NodeInfo>,
}

impl Config {
    pub fn from_file(path: &str) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .context(format!("Failed to read config file: {}", path))?;
        serde_json::from_str(&content).context("Failed to parse config")
    }
}

pub struct Node {
    id: u32,
    network: NetworkLayer,
    peers: Arc<RwLock<HashMap<u32, PeerConnection>>>,
    config: Config,
    message_rx: mpsc::UnboundedReceiver<(u32, Message)>,
    message_tx: mpsc::UnboundedSender<(u32, Message)>,
}

impl Node {
    pub fn new(node_id: u32, config: Config) -> Result<Self> {
        let node_info = config
            .nodes
            .iter()
            .find(|n| n.id == node_id)
            .context(format!("Node ID {} not found in config", node_id))?;

        let (message_tx, message_rx) = mpsc::unbounded_channel();

        Ok(Self {
            id: node_id,
            network: NetworkLayer::new(node_id, node_info.address.clone()),
            peers: Arc::new(RwLock::new(HashMap::new())),
            config,
            message_rx,
            message_tx,
        })
    }

    /// Start the node
    pub async fn run(mut self) -> Result<()> {
        info!("Starting node {}", self.id);

        // Start listener in background
        let network = self.network.clone();
        let tx = self.message_tx.clone();
        tokio::spawn(async move {
            if let Err(e) = network.start_listener(tx).await {
                error!("Listener error: {}", e);
            }
        });

        // Wait a bit for all nodes to start listening
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Connect to all other peers
        self.connect_to_peers().await?;

        // Start heartbeat task
        let peers = self.peers.clone();
        let node_id = self.id;
        tokio::spawn(async move {
            Self::heartbeat_task(node_id, peers).await;
        });

        // Handle incoming messages
        self.message_loop().await;

        Ok(())
    }

    /// Connect to all peer nodes
    async fn connect_to_peers(&self) -> Result<()> {
        for node_info in &self.config.nodes {
            if node_info.id == self.id {
                continue; // Don't connect to self
            }

            info!("Attempting to connect to node {} at {}", node_info.id, node_info.address);

            match self.network.connect_to_peer(&node_info.address).await {
                Ok(conn) => {
                    self.peers.write().await.insert(node_info.id, conn);
                    info!("Successfully connected to node {}", node_info.id);
                }
                Err(e) => {
                    warn!("Failed to connect to node {}: {}", node_info.id, e);
                    warn!("Will retry in heartbeat task");
                }
            }
        }

        Ok(())
    }

    /// Main message processing loop
    async fn message_loop(&mut self) {
        while let Some((sender_id, message)) = self.message_rx.recv().await {
            if let Err(e) = self.handle_message(sender_id, message).await {
                error!("Error handling message: {}", e);
            }
        }
    }

    /// Handle received messages
    async fn handle_message(&self, _sender_id: u32, message: Message) -> Result<()> {
        match message {
            Message::Hello { from_node } => {
                info!("Received Hello from node {}", from_node);
                // Note: The connection is already established by the listener
                // We might want to send HelloAck back
            }
            Message::HelloAck { from_node } => {
                info!("Received HelloAck from node {}", from_node);
            }
            Message::Heartbeat { from_node, timestamp } => {
                info!("Received Heartbeat from node {} (ts: {})", from_node, timestamp);
            }
            Message::Ping { from_node } => {
                info!("Received Ping from node {}", from_node);
                // Send pong back
                if let Some(peer) = self.peers.read().await.get(&from_node) {
                    let pong = Message::Pong { from_node: self.id };
                    peer.send(&pong).await?;
                }
            }
            Message::Pong { from_node } => {
                info!("Received Pong from node {}", from_node);
            }
        }
        Ok(())
    }

    /// Periodic heartbeat to all peers
    async fn heartbeat_task(node_id: u32, peers: Arc<RwLock<HashMap<u32, PeerConnection>>>) {
        let mut ticker = interval(Duration::from_secs(5));

        loop {
            ticker.tick().await;

            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();

            let heartbeat = Message::Heartbeat {
                from_node: node_id,
                timestamp,
            };

            let peers_lock = peers.read().await;
            for (peer_id, peer) in peers_lock.iter() {
                if let Err(e) = peer.send(&heartbeat).await {
                    error!("Failed to send heartbeat to node {}: {}", peer_id, e);
                }
            }
        }
    }

    /// Broadcast a message to all peers
    #[allow(dead_code)]
    pub async fn broadcast(&self, message: &Message) -> Result<()> {
        let peers = self.peers.read().await;
        for (peer_id, peer) in peers.iter() {
            if let Err(e) = peer.send(message).await {
                error!("Failed to send to node {}: {}", peer_id, e);
            }
        }
        Ok(())
    }
}
use crate::message::Message;
use crate::network::{NetworkLayer, PeerConnection};
use anyhow::{Context, Result};
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, RwLock};
use tokio::time::{interval, timeout, Duration};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(2);
const COORDINATOR_INTERVAL: Duration = Duration::from_secs(2);
const FAILURE_TIMEOUT: Duration = Duration::from_secs(6); // 3x heartbeat
const TAKEOVER_TIMEOUT: Duration = Duration::from_secs(8); // Wait for successor

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub id: u32,
    pub address: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub nodes: Vec<NodeInfo>,
}

impl Config {
    pub fn from_file(path: &str) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        serde_json::from_str(&content).context("Failed to parse config")
    }
}

pub struct Node {
    // Identity
    my_id: u32,
    my_address: String,
    all_nodes: Vec<NodeInfo>,
    
    // Leadership state
    current_leader: Arc<RwLock<Option<u32>>>,
    current_successor: Arc<RwLock<Option<u32>>>,
    am_i_leader: Arc<RwLock<bool>>,
    
    // Alive nodes tracking (for leader)
    alive_nodes: Arc<RwLock<HashSet<u32>>>,
    last_heartbeat: Arc<RwLock<HashMap<u32, Instant>>>,
    
    // Network
    peers: Arc<RwLock<HashMap<u32, PeerConnection>>>,
    network: NetworkLayer,
    message_rx: mpsc::UnboundedReceiver<(u32, Message)>,
    message_tx: mpsc::UnboundedSender<(u32, Message)>,
}

impl Node {
    pub fn new(my_id: u32, config: Config) -> Result<Self> {
        let (message_tx, message_rx) = mpsc::unbounded_channel();
        
        let my_node_info = config.nodes.iter()
            .find(|n| n.id == my_id)
            .context(format!("Node ID {} not found in config", my_id))?;

        Ok(Self {
            my_id,
            my_address: my_node_info.address.clone(),
            all_nodes: config.nodes.clone(),
            network: NetworkLayer::new(my_node_info.address.clone()),
            
            current_leader: Arc::new(RwLock::new(None)),
            current_successor: Arc::new(RwLock::new(None)),
            am_i_leader: Arc::new(RwLock::new(false)),
            
            alive_nodes: Arc::new(RwLock::new(HashSet::new())),
            last_heartbeat: Arc::new(RwLock::new(HashMap::new())),
            
            peers: Arc::new(RwLock::new(HashMap::new())),
            message_rx,
            message_tx,
        })
    }

    pub async fn run(mut self) -> Result<()> {
        info!("╔═══════════════════════════════════════════════════════════╗");
        info!("║ Modified Bully Algorithm - Node Starting                 ║");
        info!("╠═══════════════════════════════════════════════════════════╣");
        info!("║ Node ID: {}                                               ║", self.my_id);
        info!("║ Address: {}                                   ║", self.my_address);
        info!("╚═══════════════════════════════════════════════════════════╝");

        // Start listener
        let network = self.network.clone();
        let tx = self.message_tx.clone();
        let peers = self.peers.clone();
        tokio::spawn(async move {
            if let Err(e) = network.start_listener(tx, peers).await {
                error!("Listener error: {}", e);
            }
        });

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Discover network
        self.discover_network().await?;

        // Start background tasks
        self.spawn_background_tasks();

        // Handle messages
        self.message_loop().await;

        Ok(())
    }

    /// Discover the network and current leader
    async fn discover_network(&mut self) -> Result<()> {
        info!("🔍 Discovering network...");

        let discovery_msg = Message::WhoIsLeader {
            node_id: self.my_id,
            from_address: self.my_address.clone(),
        };

        // Try to connect to all other nodes
        let mut connected = false;
        for node in &self.all_nodes {
            if node.id == self.my_id {
                continue;
            }

            match self.network.connect_to_peer(&node.address).await {
                Ok(conn) => {
                    if let Err(e) = conn.send(&discovery_msg).await {
                        warn!("Failed to send discovery to {}: {}", node.address, e);
                    } else {
                        self.peers.write().await.insert(node.id, conn.clone());
                        connected = true;
                        
                        // Start read loop for outgoing connection
                        let node_id = node.id;
                        let tx = self.message_tx.clone();
                        tokio::spawn(async move {
                            if let Err(e) = Self::read_from_peer(node_id, conn, tx).await {
                                debug!("Read loop ended for node {}: {}", node_id, e);
                            }
                        });
                    }
                }
                Err(e) => {
                    debug!("Could not connect to node {}: {}", node.id, e);
                }
            }
        }

        if !connected {
            info!("📍 No other nodes found - I am the leader!");
            *self.am_i_leader.write().await = true;
            *self.current_leader.write().await = Some(self.my_id);
            self.alive_nodes.write().await.insert(self.my_id);
        } else {
            // Wait for coordinator message
            info!("⏳ Waiting for leader announcement...");
            
            match timeout(Duration::from_secs(5), self.wait_for_coordinator()).await {
                Ok(_) => {
                    let leader = self.current_leader.read().await;
                    let successor = self.current_successor.read().await;
                    info!("✅ Network discovered: Leader={:?}, Successor={:?}", leader, successor);
                }
                Err(_) => {
                    warn!("⚠️  No coordinator received - starting election");
                    self.become_leader().await;
                }
            }
        }

        Ok(())
    }

    async fn wait_for_coordinator(&mut self) -> Result<()> {
        while let Some((_, msg)) = self.message_rx.recv().await {
            if matches!(msg, Message::Coordinator { .. }) {
                self.handle_message(msg).await;
                return Ok(());
            }
        }
        Ok(())
    }

    /// Helper to read from a peer connection and forward messages to channel
    async fn read_from_peer(
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
                        debug!("Read error from Node {}: {}", node_id, e);
                    }
                    break;
                }
            }
        }
        Ok(())
    }

    fn spawn_background_tasks(&self) {
        // Heartbeat sender (if not leader)
        let my_id = self.my_id;
        let peers = self.peers.clone();
        let am_i_leader = self.am_i_leader.clone();
        let current_leader = self.current_leader.clone();
        tokio::spawn(async move {
            Self::heartbeat_sender_task(my_id, peers, am_i_leader, current_leader).await;
        });

        // Coordinator broadcaster (if leader)
        let my_id = self.my_id;
        let peers = self.peers.clone();
        let am_i_leader = self.am_i_leader.clone();
        let current_successor = self.current_successor.clone();
        tokio::spawn(async move {
            Self::coordinator_broadcaster_task(my_id, peers, am_i_leader, current_successor).await;
        });

        // Leader updates successor based on heartbeats
        let my_id = self.my_id;
        let am_i_leader = self.am_i_leader.clone();
        let alive_nodes = self.alive_nodes.clone();
        let current_successor = self.current_successor.clone();
        tokio::spawn(async move {
            Self::successor_updater_task(my_id, am_i_leader, alive_nodes, current_successor).await;
        });

        // Failure detector
        let my_id = self.my_id;
        let current_leader = self.current_leader.clone();
        let current_successor = self.current_successor.clone();
        let last_heartbeat = self.last_heartbeat.clone();
        let peers = self.peers.clone();
        let am_i_leader = self.am_i_leader.clone();
        let alive_nodes = self.alive_nodes.clone();
        tokio::spawn(async move {
            Self::failure_detector_task(
                my_id,
                current_leader,
                current_successor,
                last_heartbeat,
                peers,
                am_i_leader,
                alive_nodes,
            )
            .await;
        });
    }

    /// Background task: Send heartbeats to leader (if not leader)
    async fn heartbeat_sender_task(
        my_id: u32,
        peers: Arc<RwLock<HashMap<u32, PeerConnection>>>,
        am_i_leader: Arc<RwLock<bool>>,
        current_leader: Arc<RwLock<Option<u32>>>,
    ) {
        let mut ticker = interval(HEARTBEAT_INTERVAL);

        loop {
            ticker.tick().await;

            if *am_i_leader.read().await {
                continue; // Leaders don't send heartbeats
            }

            let leader = *current_leader.read().await;
            if let Some(leader_id) = leader {
                let heartbeat = Message::Heartbeat { node_id: my_id };
                
                let peers_lock = peers.read().await;
                if let Some(leader_conn) = peers_lock.get(&leader_id) {
                    if let Err(e) = leader_conn.send(&heartbeat).await {
                        debug!("Failed to send heartbeat to leader {}: {}", leader_id, e);
                    }
                }
            }
        }
    }

    /// Background task: Broadcast coordinator messages (if leader)
    async fn coordinator_broadcaster_task(
        my_id: u32,
        peers: Arc<RwLock<HashMap<u32, PeerConnection>>>,
        am_i_leader: Arc<RwLock<bool>>,
        current_successor: Arc<RwLock<Option<u32>>>,
    ) {
        let mut ticker = interval(COORDINATOR_INTERVAL);

        loop {
            ticker.tick().await;

            if !*am_i_leader.read().await {
                continue; // Only leaders broadcast
            }

            let successor = *current_successor.read().await;
            let coordinator = Message::Coordinator {
                leader_id: my_id,
                successor_id: successor,
            };

            let peers_lock = peers.read().await;
            for peer in peers_lock.values() {
                let _ = peer.send(&coordinator).await;
            }
        }
    }

    /// Background task: Leader updates successor based on alive nodes
    async fn successor_updater_task(
        my_id: u32,
        am_i_leader: Arc<RwLock<bool>>,
        alive_nodes: Arc<RwLock<HashSet<u32>>>,
        current_successor: Arc<RwLock<Option<u32>>>,
    ) {
        let mut ticker = interval(Duration::from_secs(1));

        loop {
            ticker.tick().await;

            if !*am_i_leader.read().await {
                continue;
            }

            // Calculate successor = max(alive_nodes - self)
            let alive = alive_nodes.read().await;
            let new_successor = alive
                .iter()
                .filter(|&&id| id != my_id)
                .max()
                .copied();

            let mut successor = current_successor.write().await;
            if *successor != new_successor {
                info!("📋 Successor updated: {:?} → {:?}", *successor, new_successor);
                *successor = new_successor;
            }
        }
    }

    /// Background task: Detect leader failures
    async fn failure_detector_task(
        my_id: u32,
        current_leader: Arc<RwLock<Option<u32>>>,
        current_successor: Arc<RwLock<Option<u32>>>,
        last_heartbeat: Arc<RwLock<HashMap<u32, Instant>>>,
        peers: Arc<RwLock<HashMap<u32, PeerConnection>>>,
        am_i_leader: Arc<RwLock<bool>>,
        alive_nodes: Arc<RwLock<HashSet<u32>>>,
    ) {
        let mut ticker = interval(Duration::from_secs(1));

        loop {
            ticker.tick().await;

            if *am_i_leader.read().await {
                continue; // Leaders don't check for failures
            }

            let leader_id = match *current_leader.read().await {
                Some(id) => id,
                None => continue,
            };

            // Check if leader has timed out
            let heartbeats = last_heartbeat.read().await;
            let leader_timeout = heartbeats
                .get(&leader_id)
                .map(|t| t.elapsed() > FAILURE_TIMEOUT)
                .unwrap_or(false);

            if !leader_timeout {
                continue;
            }

            // Leader failed!
            warn!("⚠️  LEADER FAILURE DETECTED: Node {} timeout", leader_id);

            let successor_id = *current_successor.read().await;

            if successor_id == Some(my_id) {
                // I am the successor - take over immediately
                info!("👑 I am successor - TAKING OVER as leader!");
                
                *am_i_leader.write().await = true;
                *current_leader.write().await = Some(my_id);
                
                // Reset alive nodes (I'm alive, at least)
                let mut alive = alive_nodes.write().await;
                alive.clear();
                alive.insert(my_id);
                
                // Broadcast takeover
                let coordinator = Message::Coordinator {
                    leader_id: my_id,
                    successor_id: None, // Will be updated as heartbeats arrive
                };
                
                let peers_lock = peers.read().await;
                for peer in peers_lock.values() {
                    let _ = peer.send(&coordinator).await;
                }
                
                info!("✅ Successfully became leader (Node {})", my_id);
                
            } else if let Some(succ_id) = successor_id {
                // I'm not the successor - notify successor and wait
                info!("📨 Notifying successor (Node {}) to take over", succ_id);
                
                let takeover = Message::Takeover { from_id: my_id };
                
                let peers_lock = peers.read().await;
                if let Some(succ_conn) = peers_lock.get(&succ_id) {
                    let _ = succ_conn.send(&takeover).await;
                }
                
                // Wait for new coordinator message
                tokio::time::sleep(TAKEOVER_TIMEOUT).await;
                
                // Check if we got a new leader
                let heartbeats = last_heartbeat.read().await;
                let successor_alive = heartbeats
                    .get(&succ_id)
                    .map(|t| t.elapsed() < FAILURE_TIMEOUT)
                    .unwrap_or(false);
                
                if !successor_alive {
                    // Successor also failed - I'm the only one left
                    warn!("⚠️  Successor also failed - I'm taking over!");
                    
                    *am_i_leader.write().await = true;
                    *current_leader.write().await = Some(my_id);
                    
                    let mut alive = alive_nodes.write().await;
                    alive.clear();
                    alive.insert(my_id);
                    
                    let coordinator = Message::Coordinator {
                        leader_id: my_id,
                        successor_id: None,
                    };
                    
                    for peer in peers_lock.values() {
                        let _ = peer.send(&coordinator).await;
                    }
                    
                    info!("✅ Successfully became leader (last node standing)");
                }
                
            } else {
                // No successor known - become leader
                warn!("⚠️  No successor known - becoming leader");
                
                *am_i_leader.write().await = true;
                *current_leader.write().await = Some(my_id);
                
                let mut alive = alive_nodes.write().await;
                alive.clear();
                alive.insert(my_id);
            }

            // Reset failure detection
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }

    async fn message_loop(&mut self) {
        while let Some((from_id, message)) = self.message_rx.recv().await {
            self.handle_message_from(from_id, message).await;
        }
    }

    async fn handle_message_from(&mut self, from_id: u32, message: Message) {
        // Update last heartbeat time for any message
        self.last_heartbeat.write().await.insert(from_id, Instant::now());
        
        self.handle_message(message).await;
    }

    async fn handle_message(&mut self, message: Message) {
        match message {
            Message::WhoIsLeader { node_id, from_address } => {
                info!("📩 Received WhoIsLeader from Node {}", node_id);
                
                // Connect back if not already connected
                if !self.peers.read().await.contains_key(&node_id) {
                    if let Ok(conn) = self.network.connect_to_peer(&from_address).await {
                        self.peers.write().await.insert(node_id, conn);
                    }
                }
                
                // All nodes respond with their known leader info (not just the leader)
                let am_leader = *self.am_i_leader.read().await;
                let known_leader = *self.current_leader.read().await;
                let known_successor = *self.current_successor.read().await;
                
                // Send coordinator info we know about
                if let Some(leader_id) = known_leader {
                    let coordinator = Message::Coordinator {
                        leader_id,
                        successor_id: known_successor,
                    };
                    
                    if let Some(conn) = self.peers.read().await.get(&node_id) {
                        let _ = conn.send(&coordinator).await;
                        info!("📤 Sent coordinator info to Node {}: Leader={}, Successor={:?}", 
                              node_id, leader_id, known_successor);
                    }
                }
                
                // If I'm the leader, also add this node to alive set
                if am_leader {
                    self.alive_nodes.write().await.insert(node_id);
                }
            }

            Message::Coordinator { leader_id, successor_id } => {
                let old_leader = *self.current_leader.read().await;
                
                if old_leader != Some(leader_id) {
                    info!("👑 Leader is Node {}, Successor: {:?}", leader_id, successor_id);
                }
                
                *self.current_leader.write().await = Some(leader_id);
                *self.current_successor.write().await = successor_id;
                *self.am_i_leader.write().await = leader_id == self.my_id;
            }

            Message::Heartbeat { node_id } => {
                debug!("💓 Heartbeat from Node {}", node_id);
                
                // Leader tracks alive nodes
                if *self.am_i_leader.read().await {
                    self.alive_nodes.write().await.insert(node_id);
                }
            }

            Message::Takeover { from_id } => {
                info!("📨 Received Takeover notification from Node {}", from_id);
                
                // Verify leader is actually down
                let leader_id = match *self.current_leader.read().await {
                    Some(id) => id,
                    None => return,
                };
                
                let leader_down = {
                    let heartbeats = self.last_heartbeat.read().await;
                    heartbeats
                        .get(&leader_id)
                        .map(|t| t.elapsed() > FAILURE_TIMEOUT)
                        .unwrap_or(true)
                };
                
                if leader_down && *self.current_successor.read().await == Some(self.my_id) {
                    info!("✅ Confirmed leader down - taking over as requested");
                    self.become_leader().await;
                }
            }
        }
    }

    async fn become_leader(&mut self) {
        info!("👑 Becoming leader (Node {})", self.my_id);
        
        *self.am_i_leader.write().await = true;
        *self.current_leader.write().await = Some(self.my_id);
        
        let mut alive = self.alive_nodes.write().await;
        alive.clear();
        alive.insert(self.my_id);
        
        // Announce leadership
        let coordinator = Message::Coordinator {
            leader_id: self.my_id,
            successor_id: None,
        };
        
        let peers_lock = self.peers.read().await;
        for peer in peers_lock.values() {
            let _ = peer.send(&coordinator).await;
        }
    }
}
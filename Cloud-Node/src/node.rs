use crate::message::Message;
use crate::network::{NetworkLayer, PeerConnection};
use anyhow::{Context, Result};
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH, Instant}; // ← ADDED: Instant for timing
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, RwLock};
use tokio::time::{interval, Duration};

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
    
    // ============ NEW: Election state fields ============
    /// Who is currently the leader? None if no leader elected yet
    current_leader: Arc<RwLock<Option<u32>>>,
    
    /// Highest PID we know about (updated from leader's heartbeats)
    max_known_pid: Arc<RwLock<u32>>,
    
    /// All PIDs the leader has interacted with
    known_pids: Arc<RwLock<Vec<u32>>>,
    
    /// When did we last hear from the leader?
    last_leader_heartbeat: Arc<RwLock<Instant>>,
    
    /// Are we currently in an election?
    in_election: Arc<RwLock<bool>>,
    // ====================================================
}

impl Node {
    pub fn new(node_id: u32, config: Config) -> Result<Self> {
        let node_info = config
            .nodes
            .iter()
            .find(|n| n.id == node_id)
            .context(format!("Node ID {} not found in config", node_id))?;

        let (message_tx, message_rx) = mpsc::unbounded_channel();

        // ============ NEW: Collect PIDs and find max ============
        let all_pids: Vec<u32> = config.nodes.iter().map(|n| n.id).collect();
        let max_pid = all_pids.iter().copied().max().unwrap_or(node_id);
        // ========================================================

        Ok(Self {
            id: node_id,
            network: NetworkLayer::new(node_id, node_info.address.clone()),
            peers: Arc::new(RwLock::new(HashMap::new())),
            config,
            message_rx,
            message_tx,
            
            // ============ NEW: Initialize election state ============
            current_leader: Arc::new(RwLock::new(None)),
            max_known_pid: Arc::new(RwLock::new(max_pid)),
            known_pids: Arc::new(RwLock::new(all_pids)),
            last_leader_heartbeat: Arc::new(RwLock::new(Instant::now())),
            in_election: Arc::new(RwLock::new(false)),
            // ========================================================
        })
    }

    /// Start the node
    pub async fn run(mut self) -> Result<()> {
        info!("Starting node {}", self.id);

        // Start listener in background
        let network = self.network.clone();
        let tx = self.message_tx.clone();
        let peers = self.peers.clone();
        tokio::spawn(async move {
            if let Err(e) = network.start_listener(tx, peers).await {
                error!("Listener error: {}", e);
            }
        });

        // Wait a bit for all nodes to start listening
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Connect to all other peers (only higher IDs to avoid duplicates)
        self.connect_to_peers().await?;

        // ============ CHANGED: Pass election state to heartbeat ============
        // Start heartbeat task
        let peers = self.peers.clone();
        let node_id = self.id;
        let config = self.config.clone();
        let current_leader = self.current_leader.clone();
        let max_known_pid = self.max_known_pid.clone();
        let known_pids = self.known_pids.clone();
        tokio::spawn(async move {
            Self::heartbeat_task(node_id, peers, config, current_leader, max_known_pid, known_pids).await;
        });
        // ===================================================================

        // ============ NEW: Start election monitor task ============
        // This task watches for leader failures and triggers elections
        let node_id = self.id;
        let peers = self.peers.clone();
        let current_leader = self.current_leader.clone();
        let max_known_pid = self.max_known_pid.clone();
        let known_pids = self.known_pids.clone();
        let last_leader_heartbeat = self.last_leader_heartbeat.clone();
        let in_election = self.in_election.clone();
        tokio::spawn(async move {
            Self::election_monitor_task(
                node_id,
                peers,
                current_leader,
                max_known_pid,
                known_pids,
                last_leader_heartbeat,
                in_election,
            ).await;
        });
        // ==========================================================

        // Handle incoming messages
        self.message_loop().await;

        Ok(())
    }

    /// Connect to all peer nodes with higher IDs (avoids duplicate connections)
    async fn connect_to_peers(&self) -> Result<()> {
        for node_info in &self.config.nodes {
            if node_info.id <= self.id {
                continue;
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
                // ============ NEW: Update known PIDs ============
                let mut known = self.known_pids.write().await;
                if !known.contains(&from_node) {
                    known.push(from_node);
                }
                // Update max if necessary
                if let Some(&max) = known.iter().max() {
                    *self.max_known_pid.write().await = max;
                }
                // ================================================
            }
            Message::HelloAck { from_node } => {
                info!("Received HelloAck from node {}", from_node);
            }
            // ============ CHANGED: Handle new heartbeat fields ============
            Message::Heartbeat { from_node, timestamp, is_leader, max_known_pid: broadcasted_max } => {
                debug!("Received Heartbeat from node {} (ts: {}, leader: {}, max_pid: {})", 
                       from_node, timestamp, is_leader, broadcasted_max);
                
                // Update our max_known_pid from leader's broadcast
                let mut our_max = self.max_known_pid.write().await;
                if broadcasted_max > *our_max {
                    info!("Updating max_known_pid from {} to {} (via heartbeat from node {})", 
                          *our_max, broadcasted_max, from_node);
                    *our_max = broadcasted_max;
                }
                drop(our_max);
                
                // If from leader, update last heartbeat time
                let current_leader = *self.current_leader.read().await;
                if is_leader || current_leader == Some(from_node) {
                    *self.last_leader_heartbeat.write().await = Instant::now();
                }
            }
            // ==============================================================
            Message::Ping { from_node } => {
                info!("Received Ping from node {}", from_node);
                if let Some(peer) = self.peers.read().await.get(&from_node) {
                    let pong = Message::Pong { from_node: self.id };
                    peer.send(&pong).await?;
                }
            }
            Message::Pong { from_node } => {
                info!("Received Pong from node {}", from_node);
            }
            // ============ NEW: Handle election messages ============
            Message::Election { from_node } => {
                info!("Received Election message from node {}", from_node);
                
                // Send OK response (I'm alive)
                if let Some(peer) = self.peers.read().await.get(&from_node) {
                    let ok_msg = Message::OK { from_node: self.id };
                    let _ = peer.send(&ok_msg).await;
                }
                
                // If I have higher ID and not in election, start my own
                if !*self.in_election.read().await && self.id > from_node {
                    info!("Node {} starting own election (higher than {})", self.id, from_node);
                    *self.in_election.write().await = true;
                    
                    let node_id = self.id;
                    let peers = self.peers.clone();
                    let current_leader = self.current_leader.clone();
                    let max_known_pid = self.max_known_pid.clone();
                    let known_pids = self.known_pids.clone();
                    let in_election = self.in_election.clone();
                    
                    tokio::spawn(async move {
                        Self::start_election(node_id, peers, current_leader, max_known_pid, known_pids, in_election).await;
                    });
                }
            }
            Message::OK { from_node } => {
                info!("Received OK from node {} (they're alive)", from_node);
            }
            Message::Coordinator { from_node } => {
                info!("🎉 Node {} announced as COORDINATOR!", from_node);
                
                // Accept the new leader
                *self.current_leader.write().await = Some(from_node);
                *self.last_leader_heartbeat.write().await = Instant::now();
                *self.in_election.write().await = false;
            }
            Message::BecomeLeader { from_node } => {
                info!("Received BecomeLeader request from node {}", from_node);
                
                // Start election since we're being asked to be leader
                if !*self.in_election.read().await {
                    info!("Accepting leadership request, starting election");
                    *self.in_election.write().await = true;
                    
                    let node_id = self.id;
                    let peers = self.peers.clone();
                    let current_leader = self.current_leader.clone();
                    let max_known_pid = self.max_known_pid.clone();
                    let known_pids = self.known_pids.clone();
                    let in_election = self.in_election.clone();
                    
                    tokio::spawn(async move {
                        Self::start_election(node_id, peers, current_leader, max_known_pid, known_pids, in_election).await;
                    });
                }
            }
            // =======================================================
        }
        Ok(())
    }

    /// Periodic heartbeat to all peers and retry failed connections
    async fn heartbeat_task(
        node_id: u32, 
        peers: Arc<RwLock<HashMap<u32, PeerConnection>>>, 
        config: Config,
        current_leader: Arc<RwLock<Option<u32>>>,
        max_known_pid: Arc<RwLock<u32>>,
        known_pids: Arc<RwLock<Vec<u32>>>,
    ) {
        let mut ticker = interval(Duration::from_secs(5));

        loop {
            ticker.tick().await;

            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();

            // ============ NEW: Add election info to heartbeat ============
            // Check if we're the leader
            let leader = *current_leader.read().await;
            let is_leader = leader == Some(node_id);
            
            // If we're leader, update our known PIDs
            if is_leader {
                let mut known = known_pids.write().await;
                let peers_lock = peers.read().await;
                for &peer_id in peers_lock.keys() {
                    if !known.contains(&peer_id) {
                        known.push(peer_id);
                    }
                }
                if !known.contains(&node_id) {
                    known.push(node_id);
                }
                drop(peers_lock);
                
                // Update max
                if let Some(&max) = known.iter().max() {
                    *max_known_pid.write().await = max;
                }
            }
            
            let max_pid = *max_known_pid.read().await;

            let heartbeat = Message::Heartbeat {
                from_node: node_id,
                timestamp,
                is_leader,
                max_known_pid: max_pid,
            };
            // ============================================================

            // Send heartbeats to connected peers
            let peers_lock = peers.read().await;
            for (peer_id, peer) in peers_lock.iter() {
                if let Err(e) = peer.send(&heartbeat).await {
                    error!("Failed to send heartbeat to node {}: {}", peer_id, e);
                }
            }
            drop(peers_lock);

            // Retry connections to missing peers
            for node_info in &config.nodes {
                if node_info.id <= node_id {
                    continue;
                }

                let has_connection = peers.read().await.contains_key(&node_info.id);
                
                if !has_connection {
                    debug!("Retrying connection to node {} at {}", node_info.id, node_info.address);
                    
                    match TcpStream::connect(&node_info.address).await {
                        Ok(mut stream) => {
                            let hello = Message::Hello { from_node: node_id };
                            if let Ok(bytes) = hello.to_bytes() {
                                if stream.write_all(&bytes).await.is_ok() {
                                    let conn = PeerConnection::new(stream);
                                    peers.write().await.insert(node_info.id, conn);
                                    info!("Successfully reconnected to node {}", node_info.id);
                                }
                            }
                        }
                        Err(_) => {
                            debug!("Node {} still not reachable, will retry in 5s", node_info.id);
                        }
                    }
                }
            }
        }
    }

    // ============ NEW: Election monitor task ============
    /// Watches for leader failures and triggers elections
    async fn election_monitor_task(
        node_id: u32,
        peers: Arc<RwLock<HashMap<u32, PeerConnection>>>,
        current_leader: Arc<RwLock<Option<u32>>>,
        max_known_pid: Arc<RwLock<u32>>,
        known_pids: Arc<RwLock<Vec<u32>>>,
        last_leader_heartbeat: Arc<RwLock<Instant>>,
        in_election: Arc<RwLock<bool>>,
    ) {
        const LEADER_TIMEOUT: Duration = Duration::from_secs(15); // 3x heartbeat
        let mut ticker = interval(Duration::from_secs(2));

        loop {
            ticker.tick().await;

            let leader = *current_leader.read().await;
            let last_hb = *last_leader_heartbeat.read().await;
            let already_in_election = *in_election.read().await;

            // Detect leader failure
            if leader.is_some() && last_hb.elapsed() > LEADER_TIMEOUT && !already_in_election {
                let leader_id = leader.unwrap();
                warn!("Leader {} timeout! Starting election...", leader_id);
                
                *in_election.write().await = true;
                Self::start_election(node_id, peers.clone(), current_leader.clone(), 
                                   max_known_pid.clone(), known_pids.clone(), in_election.clone()).await;
            }
            
            // Start initial election if no leader
            if leader.is_none() && !already_in_election {
                info!("No leader detected, starting election...");
                *in_election.write().await = true;
                Self::start_election(node_id, peers.clone(), current_leader.clone(), 
                                   max_known_pid.clone(), known_pids.clone(), in_election.clone()).await;
            }
        }
    }

    /// IMPROVED BULLY ALGORITHM - Start an election
    async fn start_election(
        node_id: u32,
        peers: Arc<RwLock<HashMap<u32, PeerConnection>>>,
        current_leader: Arc<RwLock<Option<u32>>>,
        max_known_pid: Arc<RwLock<u32>>,
        known_pids: Arc<RwLock<Vec<u32>>>,
        in_election: Arc<RwLock<bool>>,
    ) {
        info!("Node {} starting election...", node_id);

        let max_pid = *max_known_pid.read().await;

        // ========== IMPROVED BULLY: Check hint first ==========
        if node_id < max_pid {
            info!("My PID ({}) < max_known_pid ({}), asking {} to become leader", 
                  node_id, max_pid, max_pid);
            
            // Send BecomeLeader to max_known_pid
            let peers_lock = peers.read().await;
            if let Some(peer) = peers_lock.get(&max_pid) {
                let msg = Message::BecomeLeader { from_node: node_id };
                if let Err(e) = peer.send(&msg).await {
                    warn!("Failed to contact node {}: {}", max_pid, e);
                } else {
                    info!("Sent BecomeLeader to node {}", max_pid);
                }
            } else {
                warn!("Max PID node {} not connected", max_pid);
            }
            drop(peers_lock);
            
            // Wait for response
            tokio::time::sleep(Duration::from_secs(3)).await;
            
            if current_leader.read().await.is_some() {
                info!("Node {} became leader, election complete", max_pid);
                *in_election.write().await = false;
                return;
            }
            
            warn!("Max PID node {} didn't respond, falling back to Bully", max_pid);
        }

        // ========== If my_PID >= max OR fallback ==========
        if node_id >= max_pid {
            info!("My PID ({}) >= max_known_pid ({}), becoming leader", node_id, max_pid);
            Self::become_leader(node_id, peers, current_leader, known_pids, in_election).await;
            return;
        }

        // ========== Standard Bully fallback ==========
        info!("Falling back to standard Bully");
        let mut higher_nodes = vec![];
        let peers_lock = peers.read().await;
        for &peer_id in peers_lock.keys() {
            if peer_id > node_id {
                higher_nodes.push(peer_id);
            }
        }
        drop(peers_lock);

        if higher_nodes.is_empty() {
            info!("Node {} is highest, becoming leader", node_id);
            Self::become_leader(node_id, peers, current_leader, known_pids, in_election).await;
            return;
        }

        // Send Election to higher nodes
        info!("Contacting {} higher nodes: {:?}", higher_nodes.len(), higher_nodes);
        let election_msg = Message::Election { from_node: node_id };

        let peers_lock = peers.read().await;
        for &higher_id in &higher_nodes {
            if let Some(peer) = peers_lock.get(&higher_id) {
                let _ = peer.send(&election_msg).await;
            }
        }
        drop(peers_lock);

        // Wait for them to take over
        tokio::time::sleep(Duration::from_secs(3)).await;
        
        if current_leader.read().await.is_some() {
            info!("Higher node became leader");
            *in_election.write().await = false;
            return;
        }

        // No one responded, we win
        info!("No responses, becoming leader");
        Self::become_leader(node_id, peers, current_leader, known_pids, in_election).await;
    }

    /// Become the leader and announce
    async fn become_leader(
        node_id: u32,
        peers: Arc<RwLock<HashMap<u32, PeerConnection>>>,
        current_leader: Arc<RwLock<Option<u32>>>,
        known_pids: Arc<RwLock<Vec<u32>>>,
        in_election: Arc<RwLock<bool>>,
    ) {
        info!("🎉 Node {} is now the LEADER!", node_id);
        
        *current_leader.write().await = Some(node_id);
        *in_election.write().await = false;
        
        // Initialize known_pids with connected peers
        let mut known = known_pids.write().await;
        known.clear();
        known.push(node_id);
        
        let peers_lock = peers.read().await;
        for &peer_id in peers_lock.keys() {
            known.push(peer_id);
        }
        drop(peers_lock);

        // Announce to all
        let coordinator_msg = Message::Coordinator { from_node: node_id };
        let peers_lock = peers.read().await;
        for (peer_id, peer) in peers_lock.iter() {
            if let Err(e) = peer.send(&coordinator_msg).await {
                error!("Failed to announce to node {}: {}", peer_id, e);
            }
        }
    }
    // ====================================================

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
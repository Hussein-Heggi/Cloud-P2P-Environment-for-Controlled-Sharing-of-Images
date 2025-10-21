use crate::message::Message;
use crate::network::{NetworkLayer, PeerConnection};
use anyhow::{Context, Result};
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, RwLock};
use tokio::time::{interval, Duration};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub id: u32,        // Physical ID
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
    // ============ PHYSICAL IDENTITY (Static, never changes) ============
    my_physical_id: u32,            // My physical ID from config (IMMUTABLE)
    my_address: String,             // My network address
    all_nodes: Vec<NodeInfo>,       // All known nodes with physical IDs
    
    // ============ LOGICAL IDENTITY (Dynamic, changes during elections) ============
    my_logical_pid: Arc<RwLock<u32>>,    // My current logical PID (0, 1, or 2)
    is_leader: Arc<RwLock<bool>>,        // Am I the leader? (logical PID == 2)
    
    // ============ MAPPING TABLES (Physical ↔ Logical) ============
    physical_to_logical: Arc<RwLock<HashMap<u32, u32>>>,  // Physical ID → Logical PID
    logical_to_physical: Arc<RwLock<HashMap<u32, u32>>>,  // Logical PID → Physical ID
    physical_to_address: Arc<RwLock<HashMap<u32, String>>>, // Physical ID → Address
    
    // ============ NETWORK STATE ============
    peers: Arc<RwLock<HashMap<u32, PeerConnection>>>,  // Physical ID → Connection
    last_heartbeat: Arc<RwLock<HashMap<u32, Instant>>>, // Physical ID → Last HB time
    
    // ============ COMMUNICATION ============
    network: NetworkLayer,
    message_rx: mpsc::UnboundedReceiver<(u32, Message)>,  // Physical ID, Message
    message_tx: mpsc::UnboundedSender<(u32, Message)>,
}

impl Node {
    pub fn new(my_physical_id: u32, config: Config) -> Result<Self> {
        let (message_tx, message_rx) = mpsc::unbounded_channel();
        
        // Find our address
        let my_node_info = config.nodes.iter()
            .find(|n| n.id == my_physical_id)
            .context(format!("Physical ID {} not found in config", my_physical_id))?;

        Ok(Self {
            my_physical_id,
            my_address: my_node_info.address.clone(),
            all_nodes: config.nodes.clone(),
            network: NetworkLayer::new(my_node_info.address.clone()),
            
            my_logical_pid: Arc::new(RwLock::new(2)), // Start assuming leader
            is_leader: Arc::new(RwLock::new(false)),
            
            physical_to_logical: Arc::new(RwLock::new(HashMap::new())),
            logical_to_physical: Arc::new(RwLock::new(HashMap::new())),
            physical_to_address: Arc::new(RwLock::new(HashMap::new())),
            
            peers: Arc::new(RwLock::new(HashMap::new())),
            last_heartbeat: Arc::new(RwLock::new(HashMap::new())),
            
            message_rx,
            message_tx,
        })
    }

    /// Start the node
    pub async fn run(mut self) -> Result<()> {
        info!("╔═══════════════════════════════════════════════════════════╗");
        info!("║ Starting Node                                            ║");
        info!("║ Physical ID: {}                                           ║", self.my_physical_id);
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

        // Join the network (discover our logical PID)
        self.join_network().await?;

        // Display mapping
        self.display_id_mapping().await;

        // Start heartbeat (every 5ms)
        let my_physical_id = self.my_physical_id;
        let peers = self.peers.clone();
        let my_logical_pid = self.my_logical_pid.clone();
        let is_leader = self.is_leader.clone();
        let logical_to_physical = self.logical_to_physical.clone();
        tokio::spawn(async move {
            Self::heartbeat_task(
                my_physical_id,
                peers,
                my_logical_pid,
                is_leader,
                logical_to_physical,
            ).await;
        });

        // Start failure detector
        let my_physical_id = self.my_physical_id;
        let my_logical_pid = self.my_logical_pid.clone();
        let is_leader = self.is_leader.clone();
        let last_heartbeat = self.last_heartbeat.clone();
        let peers = self.peers.clone();
        let physical_to_logical = self.physical_to_logical.clone();
        let logical_to_physical = self.logical_to_physical.clone();
        tokio::spawn(async move {
            Self::failure_detector_task(
                my_physical_id,
                my_logical_pid,
                is_leader,
                last_heartbeat,
                peers,
                physical_to_logical,
                logical_to_physical,
            ).await;
        });

        // Handle messages
        self.message_loop().await;

        Ok(())
    }

    /// Join the network and discover our logical PID
    async fn join_network(&mut self) -> Result<()> {
        info!("Sending JoinRequest to discover network...");

        let join_msg = Message::JoinRequest {
            physical_id: self.my_physical_id,
            from_address: self.my_address.clone(),
        };

        // Initialize our own mapping
        self.physical_to_address.write().await.insert(
            self.my_physical_id, 
            self.my_address.clone()
        );

        // Try to connect to all other nodes
        let mut connected_count = 0;
        for node in &self.all_nodes {
            if node.id == self.my_physical_id {
                continue; // Skip self
            }

            match self.network.connect_to_peer(&node.address).await {
                Ok(conn) => {
                    if let Err(e) = conn.send(&join_msg).await {
                        warn!("Failed to send join request to {}: {}", node.address, e);
                        continue;
                    }

                    self.peers.write().await.insert(node.id, conn);
                    self.physical_to_address.write().await.insert(node.id, node.address.clone());
                    info!("Connected to Physical ID {} at {}", node.id, node.address);
                    connected_count += 1;
                }
                Err(e) => {
                    debug!("Could not connect to Physical ID {} at {}: {}", node.id, node.address, e);
                }
            }
        }

        // Wait for responses
        tokio::time::sleep(Duration::from_millis(500)).await;

        if connected_count == 0 {
            // We're alone - we're the leader with logical PID 2
            info!("═══════════════════════════════════════════════════════");
            info!("No peers found - I am the LEADER!");
            info!("Physical ID: {} → Logical PID: 2 (LEADER)", self.my_physical_id);
            info!("═══════════════════════════════════════════════════════");
            
            *self.my_logical_pid.write().await = 2;
            *self.is_leader.write().await = true;
            
            // Update mappings
            self.physical_to_logical.write().await.insert(self.my_physical_id, 2);
            self.logical_to_physical.write().await.insert(2, self.my_physical_id);
        }

        Ok(())
    }

    /// Display current ID mapping
    async fn display_id_mapping(&self) {
        info!("═══════════════════════════════════════════════════════");
        info!("Current ID Mapping:");
        info!("───────────────────────────────────────────────────────");
        
        let p2l = self.physical_to_logical.read().await;
        let l2p = self.logical_to_physical.read().await;
        
        // Display in logical order (2, 1, 0)
        for logical_pid in [2u32, 1, 0] {
            if let Some(&physical_id) = l2p.get(&logical_pid) {
                let role = match logical_pid {
                    2 => "LEADER",
                    1 => "NEXT LEADER",
                    0 => "FOLLOWER",
                    _ => "UNKNOWN",
                };
                let me = if physical_id == self.my_physical_id { " (ME)" } else { "" };
                info!("Logical PID {} ({:12}) ← Physical ID {}{}", logical_pid, role, physical_id, me);
            }
        }
        info!("═══════════════════════════════════════════════════════");
    }

    /// Message processing loop
    async fn message_loop(&mut self) {
        while let Some((from_physical_id, message)) = self.message_rx.recv().await {
            if let Err(e) = self.handle_message(from_physical_id, message).await {
                error!("Error handling message: {}", e);
            }
        }
    }

    /// Handle incoming messages
    async fn handle_message(&mut self, from_physical_id: u32, message: Message) -> Result<()> {
        match message {
            Message::JoinRequest { physical_id, from_address } => {
                info!("Received JoinRequest from Physical ID {}", physical_id);

                // Store their address
                self.physical_to_address.write().await.insert(physical_id, from_address.clone());

                // Respond with our current state
                let my_logical_pid = *self.my_logical_pid.read().await;
                let my_is_leader = *self.is_leader.read().await;
                
                // Find next leader's physical ID (logical PID 1)
                let next_leader_physical = self.logical_to_physical.read().await.get(&1).copied();

                let response = Message::JoinResponse {
                    physical_id: self.my_physical_id,
                    logical_pid: my_logical_pid,
                    is_leader: my_is_leader,
                    next_leader_physical_id: next_leader_physical,
                };

                // Connect back if not already connected
                if !self.peers.read().await.contains_key(&physical_id) {
                    match self.network.connect_to_peer(&from_address).await {
                        Ok(conn) => {
                            conn.send(&response).await?;
                            self.peers.write().await.insert(physical_id, conn);
                            info!("Connected to Physical ID {} at {}", physical_id, from_address);
                        }
                        Err(e) => {
                            warn!("Failed to connect to Physical ID {}: {}", physical_id, e);
                        }
                    }
                } else {
                    if let Some(peer) = self.peers.read().await.get(&physical_id) {
                        peer.send(&response).await?;
                    }
                }
            }

            Message::JoinResponse { physical_id, logical_pid, is_leader, next_leader_physical_id } => {
                info!("Received JoinResponse from Physical ID {} → Logical PID {}{}", 
                      physical_id, logical_pid, if is_leader { " (LEADER)" } else { "" });

                // Update mappings
                self.physical_to_logical.write().await.insert(physical_id, logical_pid);
                self.logical_to_physical.write().await.insert(logical_pid, physical_id);
                self.last_heartbeat.write().await.insert(physical_id, Instant::now());

                // Determine our logical PID based on what's taken
                let p2l = self.physical_to_logical.read().await.clone();
                let taken_logical_pids: Vec<u32> = p2l.values().copied().collect();
                
                // Find available logical PID (0, 1, or 2)
                let mut my_new_pid = None;
                for pid in [0u32, 1, 2] {
                    if !taken_logical_pids.contains(&pid) {
                        my_new_pid = Some(pid);
                        break;
                    }
                }

                if let Some(pid) = my_new_pid {
                    info!("═══════════════════════════════════════════════════════");
                    info!("Assigned Logical PID: {}", pid);
                    info!("Physical ID {} → Logical PID {}", self.my_physical_id, pid);
                    info!("═══════════════════════════════════════════════════════");
                    
                    *self.my_logical_pid.write().await = pid;
                    *self.is_leader.write().await = (pid == 2);
                    
                    // Update mappings
                    self.physical_to_logical.write().await.insert(self.my_physical_id, pid);
                    self.logical_to_physical.write().await.insert(pid, self.my_physical_id);
                }
            }

            Message::Heartbeat { physical_id, logical_pid, next_leader_physical_id } => {
                debug!("Heartbeat: Physical ID {} (Logical PID {})", physical_id, logical_pid);
                
                // Update last seen
                self.last_heartbeat.write().await.insert(physical_id, Instant::now());
                
                // Update mappings
                self.physical_to_logical.write().await.insert(physical_id, logical_pid);
                self.logical_to_physical.write().await.insert(logical_pid, physical_id);
            }

            Message::LeadershipTakeover { physical_id, new_logical_pid, old_logical_pid } => {
                info!("═══════════════════════════════════════════════════════");
                info!("LEADERSHIP TAKEOVER!");
                info!("Physical ID {} changed: Logical PID {} → {}", physical_id, old_logical_pid, new_logical_pid);
                info!("═══════════════════════════════════════════════════════");
                
                // Update mappings
                self.physical_to_logical.write().await.insert(physical_id, new_logical_pid);
                self.logical_to_physical.write().await.remove(&old_logical_pid);
                self.logical_to_physical.write().await.insert(new_logical_pid, physical_id);
                
                self.display_id_mapping().await;
            }

            Message::ShiftIDs { from_physical_id, from_logical_pid } => {
                info!("Received ShiftIDs from Physical ID {} (Logical PID {})", from_physical_id, from_logical_pid);
                
                let my_current_logical_pid = *self.my_logical_pid.read().await;
                
                // Increment our logical PID (0→1, 1→2, 2 stays 2)
                let new_logical_pid = if my_current_logical_pid < 2 { 
                    my_current_logical_pid + 1 
                } else { 
                    my_current_logical_pid 
                };
                
                if new_logical_pid != my_current_logical_pid {
                    info!("═══════════════════════════════════════════════════════");
                    info!("SHIFTING LOGICAL PID!");
                    info!("Physical ID {}: Logical PID {} → {}", self.my_physical_id, my_current_logical_pid, new_logical_pid);
                    info!("═══════════════════════════════════════════════════════");
                    
                    *self.my_logical_pid.write().await = new_logical_pid;
                    *self.is_leader.write().await = (new_logical_pid == 2);
                    
                    // Update mappings
                    self.physical_to_logical.write().await.remove(&self.my_physical_id);
                    self.physical_to_logical.write().await.insert(self.my_physical_id, new_logical_pid);
                    
                    self.logical_to_physical.write().await.remove(&my_current_logical_pid);
                    self.logical_to_physical.write().await.insert(new_logical_pid, self.my_physical_id);
                    
                    self.display_id_mapping().await;
                }
            }
        }

        Ok(())
    }

    /// Heartbeat task - sends heartbeat every 5 SECONDS
    async fn heartbeat_task(
        my_physical_id: u32,
        peers: Arc<RwLock<HashMap<u32, PeerConnection>>>,
        my_logical_pid: Arc<RwLock<u32>>,
        is_leader: Arc<RwLock<bool>>,
        logical_to_physical: Arc<RwLock<HashMap<u32, u32>>>,
    ) {
        // ============ CHANGED: 5 SECONDS instead of 5ms ============
        let mut ticker = interval(Duration::from_secs(5));
        // ============================================================

        loop {
            ticker.tick().await;

            let logical_pid = *my_logical_pid.read().await;
            let leader = *is_leader.read().await;
            
            // If we're leader, include next leader's physical ID
            let next_leader_physical = if leader {
                logical_to_physical.read().await.get(&1).copied()
            } else {
                None
            };

            let heartbeat = Message::Heartbeat {
                physical_id: my_physical_id,
                logical_pid,
                next_leader_physical_id: next_leader_physical,
            };

            let peers_lock = peers.read().await;
            for peer in peers_lock.values() {
                let _ = peer.send(&heartbeat).await;
            }
        }
    }

    /// Failure detector - checks for leader failure
    /// Only triggers election when:
    /// 1. No leader exists (initial startup)
    /// 2. Current leader fails (timeout)
    async fn failure_detector_task(
        my_physical_id: u32,
        my_logical_pid: Arc<RwLock<u32>>,
        is_leader: Arc<RwLock<bool>>,
        last_heartbeat: Arc<RwLock<HashMap<u32, Instant>>>,
        peers: Arc<RwLock<HashMap<u32, PeerConnection>>>,
        physical_to_logical: Arc<RwLock<HashMap<u32, u32>>>,
        logical_to_physical: Arc<RwLock<HashMap<u32, u32>>>,
    ) {
        // ============ CHANGED: 15 SECONDS timeout (3x heartbeat) ============
        const LEADER_TIMEOUT: Duration = Duration::from_secs(15);
        // ====================================================================
        
        let mut ticker = interval(Duration::from_secs(2)); // Check every 2 seconds
        let mut election_in_progress = false;

        loop {
            ticker.tick().await;

            let my_logical = *my_logical_pid.read().await;
            let my_is_leader = *is_leader.read().await;

            // ============ NEW: Once we have a leader, only check for failure ============
            // Don't trigger elections if we already have a stable leader
            // =============================================================================

            // Find physical ID of current leader (logical PID 2)
            let leader_physical_id = logical_to_physical.read().await.get(&2).copied();

            // Case 1: No leader exists at all - initial election needed
            if leader_physical_id.is_none() && !election_in_progress {
                info!("═══════════════════════════════════════════════════════");
                info!("No leader in network - waiting for initial election...");
                info!("═══════════════════════════════════════════════════════");
                continue; // Leader will be assigned during join process
            }

            if let Some(leader_phys_id) = leader_physical_id {
                // Case 2: We ARE the leader - nothing to check
                if leader_phys_id == my_physical_id && my_is_leader {
                    election_in_progress = false; // Reset flag
                    continue;
                }

                // Case 3: Someone else is leader - monitor their heartbeat
                let heartbeats = last_heartbeat.read().await;
                if let Some(last_hb) = heartbeats.get(&leader_phys_id) {
                    if last_hb.elapsed() > LEADER_TIMEOUT && !election_in_progress {
                        // ============ LEADER FAILURE DETECTED ============
                        election_in_progress = true; // Prevent multiple elections
                        
                        warn!("═══════════════════════════════════════════════════════");
                        warn!("⚠️  LEADER FAILURE DETECTED!");
                        warn!("Physical ID {} (Logical PID 2) has timed out!", leader_phys_id);
                        warn!("Last heartbeat: {:.1}s ago", last_hb.elapsed().as_secs_f32());
                        warn!("═══════════════════════════════════════════════════════");
                        
                        // Am I logical PID 1? Then I take over
                        if my_logical == 1 {
                            info!("🎯 I am Logical PID 1 - TAKING OVER AS LEADER!");
                            info!("This is the ONLY re-election (leader failure)");
                            
                            // Shift to logical PID 2
                            *my_logical_pid.write().await = 2;
                            *is_leader.write().await = true;
                            
                            // Update mappings
                            physical_to_logical.write().await.insert(my_physical_id, 2);
                            logical_to_physical.write().await.remove(&1);
                            logical_to_physical.write().await.remove(&2);
                            logical_to_physical.write().await.insert(2, my_physical_id);
                            
                            // Announce takeover
                            let takeover = Message::LeadershipTakeover {
                                physical_id: my_physical_id,
                                new_logical_pid: 2,
                                old_logical_pid: 1,
                            };
                            
                            let peers_lock = peers.read().await;
                            for peer in peers_lock.values() {
                                let _ = peer.send(&takeover).await;
                            }
                            
                            // Tell others to shift
                            let shift = Message::ShiftIDs {
                                from_physical_id: my_physical_id,
                                from_logical_pid: 2,
                            };
                            
                            for peer in peers_lock.values() {
                                let _ = peer.send(&shift).await;
                            }
                            
                            info!("═══════════════════════════════════════════════════════");
                            info!("✅ Physical ID {} is now LEADER (Logical PID 2)", my_physical_id);
                            info!("System stabilized - no more elections until next failure");
                            info!("═══════════════════════════════════════════════════════");
                            
                            election_in_progress = false;
                            
                        } else if my_logical == 0 {
                            // Check if PID 1 also failed
                            let pid1_physical = logical_to_physical.read().await.get(&1).copied();
                            
                            if let Some(pid1_phys) = pid1_physical {
                                if let Some(pid1_last_hb) = heartbeats.get(&pid1_phys) {
                                    if pid1_last_hb.elapsed() > LEADER_TIMEOUT {
                                        warn!("⚠️  BOTH Leader AND Logical PID 1 have failed!");
                                        warn!("Physical ID {} (Logical PID 0) taking over!", my_physical_id);
                                        
                                        *my_logical_pid.write().await = 2;
                                        *is_leader.write().await = true;
                                        
                                        physical_to_logical.write().await.insert(my_physical_id, 2);
                                        logical_to_physical.write().await.clear();
                                        logical_to_physical.write().await.insert(2, my_physical_id);
                                        
                                        let takeover = Message::LeadershipTakeover {
                                            physical_id: my_physical_id,
                                            new_logical_pid: 2,
                                            old_logical_pid: 0,
                                        };
                                        
                                        let peers_lock = peers.read().await;
                                        for peer in peers_lock.values() {
                                            let _ = peer.send(&takeover).await;
                                        }
                                        
                                        info!("✅ Physical ID {} is now LEADER", my_physical_id);
                                        election_in_progress = false;
                                    }
                                }
                            }
                        } else {
                            // We're not next in line, wait for proper successor
                            info!("Waiting for Logical PID 1 to take over...");
                            tokio::time::sleep(Duration::from_secs(5)).await;
                            election_in_progress = false;
                        }
                    }
                } else {
                    // No heartbeat record yet for leader - might be starting up
                    debug!("No heartbeat record yet for leader Physical ID {}", leader_phys_id);
                }
            }
        }
    }
}
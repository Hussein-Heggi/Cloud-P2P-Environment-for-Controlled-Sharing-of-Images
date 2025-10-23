use clap::Parser;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::net::UdpSocket;
use tokio::sync::RwLock;
use tokio::time::{sleep, interval};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
enum Message {
    Discovery {
        sender_id: u32,
        timestamp: u64,
    },
    LeaderAnnounce {
        leader_id: u32,
        timestamp: u64,
    },
    Election {
        sender_id: u32,
        timestamp: u64,
    },
    ElectionOk {
        sender_id: u32,
        timestamp: u64,
    },
    Coordinator {
        leader_id: u32,
        timestamp: u64,
    },
    Heartbeat {
        leader_id: u32,
        successor_id: Option<u32>,  // Second-highest active node
        timestamp: u64,
    },
    HeartbeatAck {
        sender_id: u32,
        timestamp: u64,
    },
}

#[derive(Debug, Clone, PartialEq)]
enum NodeState {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, Deserialize)]
struct NodeConfig {
    id: u32,
    address: String,
}

#[derive(Debug, Clone, Deserialize)]
struct Config {
    nodes: Vec<NodeConfig>,
}

struct Node {
    id: u32,
    address: SocketAddr,
    all_nodes: HashMap<u32, SocketAddr>,
    state: Arc<RwLock<NodeState>>,
    current_leader: Arc<RwLock<Option<u32>>>,
    successor_hint: Arc<RwLock<Option<u32>>>,  // Known successor from leader
    active_nodes: Arc<RwLock<HashMap<u32, SystemTime>>>,  // Track last seen time for each node
    last_heartbeat: Arc<RwLock<SystemTime>>,
    election_in_progress: Arc<RwLock<bool>>,
    socket: Arc<UdpSocket>,
}

impl Node {
    async fn new(id: u32, config: &Config) -> Result<Self, Box<dyn std::error::Error>> {
        let node_config = config
            .nodes
            .iter()
            .find(|n| n.id == id)
            .ok_or("Node ID not found in config")?;

        let address: SocketAddr = node_config.address.parse()?;
        let socket = UdpSocket::bind(address).await?;
        
        let mut all_nodes = HashMap::new();
        for node in &config.nodes {
            all_nodes.insert(node.id, node.address.parse()?);
        }

        println!("Node {} starting at {}", id, address);

        Ok(Self {
            id,
            address,
            all_nodes,
            state: Arc::new(RwLock::new(NodeState::Follower)),
            current_leader: Arc::new(RwLock::new(None)),
            successor_hint: Arc::new(RwLock::new(None)),
            active_nodes: Arc::new(RwLock::new(HashMap::new())),
            last_heartbeat: Arc::new(RwLock::new(SystemTime::now())),
            election_in_progress: Arc::new(RwLock::new(false)),
            socket: Arc::new(socket),
        })
    }

    async fn start(self: Arc<Self>) {
        // Start message listener
        let node_clone = Arc::clone(&self);
        tokio::spawn(async move {
            node_clone.listen().await;
        });

        // Give listener time to start
        sleep(Duration::from_millis(500)).await;

        // Discover cluster
        self.discover_cluster().await;

        // Start heartbeat monitor
        let node_clone = Arc::clone(&self);
        tokio::spawn(async move {
            node_clone.monitor_leader().await;
        });

        // Start heartbeat sender
        let node_clone = Arc::clone(&self);
        tokio::spawn(async move {
            node_clone.send_heartbeats().await;
        });

        // Start status reporter
        let node_clone = Arc::clone(&self);
        tokio::spawn(async move {
            node_clone.report_status().await;
        });

        println!("Node {} started successfully", self.id);
    }

    fn calculate_successor(
        &self,
        active_nodes: &HashMap<u32, SystemTime>,
        exclude_id: u32, // the id to exclude (the current leader)
    ) -> Option<u32> {
        // Gather candidates from active nodes and ensure self is included
        let mut ids: Vec<u32> = active_nodes.keys().copied().collect();
        ids.push(self.id);
    
        // De-dup and sort descending
        ids.sort_unstable_by(|a, b| b.cmp(a));
        ids.dedup();
    
        // Exclude the leader (or any exclude_id) and return the highest remaining
        ids.into_iter().find(|&id| id != exclude_id)
    }
    

    async fn discover_cluster(&self) {
        println!("Node {}: Starting cluster discovery...", self.id);

        let discovery_msg = Message::Discovery {
            sender_id: self.id,
            timestamp: current_timestamp(),
        };

        // Send to all other nodes
        for (node_id, addr) in &self.all_nodes {
            if *node_id != self.id {
                self.send_message(addr, &discovery_msg).await;
            }
        }

        // Wait for responses
        sleep(Duration::from_secs(2)).await;

        let leader = *self.current_leader.read().await;
        if leader.is_none() {
            println!("Node {}: No leader found, starting election...", self.id);
            self.start_election().await;
        } else {
            println!("Node {}: Discovered leader is Node {}", self.id, leader.unwrap());
        }
    }

    async fn start_election(&self) {
        let mut election_in_progress = self.election_in_progress.write().await;
        if *election_in_progress {
            return;
        }
        *election_in_progress = true;
        drop(election_in_progress);

        println!("Node {}: Starting election...", self.id);

        // Check if we have a successor hint
        let successor_hint = *self.successor_hint.read().await;
        
        // IMPROVED BULLY: Check if we ARE the successor
        if let Some(successor_id) = successor_hint {
            if successor_id == self.id {
                println!("Node {}: I am the successor! Becoming leader directly.", self.id);
                self.become_leader().await;
                *self.election_in_progress.write().await = false;
                return;
            } else if successor_id > self.id {
                // We know about a higher successor, defer to it first
                println!("Node {}: Deferring to known successor Node {}", self.id, successor_id);
                
                let election_msg = Message::Election {
                    sender_id: self.id,
                    timestamp: current_timestamp(),
                };
                
                if let Some(successor_addr) = self.all_nodes.get(&successor_id) {
                    self.send_message(successor_addr, &election_msg).await;
                }
                
                // Wait briefly for successor to respond
                sleep(Duration::from_millis(800)).await;
                
                // Check if we got a response
                let state = self.state.read().await;
                if *state == NodeState::Leader {
                    drop(state);
                    *self.election_in_progress.write().await = false;
                    return;
                }
                drop(state);
                
                // Successor didn't respond, fall back to normal election
                println!("Node {}: Successor didn't respond, falling back to normal election", self.id);
            }
        }

        // Normal Bully Algorithm election
        let election_msg = Message::Election {
            sender_id: self.id,
            timestamp: current_timestamp(),
        };

        let higher_nodes: Vec<_> = self
            .all_nodes
            .iter()
            .filter(|(id, _)| **id > self.id)
            .collect();

        if higher_nodes.is_empty() {
            // No higher nodes, become leader
            self.become_leader().await;
            *self.election_in_progress.write().await = false;
            return;
        }

        // Contact all higher nodes
        for (_, addr) in &higher_nodes {
            self.send_message(addr, &election_msg).await;
        }

        // Wait for OK responses
        sleep(Duration::from_millis(1500)).await;

        // Check if we should become leader
        let state = self.state.read().await;
        if *state != NodeState::Leader {
            println!("Node {}: No response from higher nodes", self.id);
            drop(state);
            self.become_leader().await;
        }

        *self.election_in_progress.write().await = false;
    }

    async fn become_leader(&self) {
        println!("Node {}: Becoming leader!", self.id);
    
        *self.state.write().await = NodeState::Leader;
        *self.current_leader.write().await = Some(self.id);
        *self.last_heartbeat.write().await = SystemTime::now();
    
        // NEW: successor_hint is meaningless for a leader—clear it
        *self.successor_hint.write().await = None;
    
        // (Optional) clear any stale active_nodes, start fresh
        self.active_nodes.write().await.clear();
    
        // announce...
        let coordinator_msg = Message::Coordinator {
            leader_id: self.id,
            timestamp: current_timestamp(),
        };
        for (node_id, addr) in &self.all_nodes {
            if *node_id != self.id {
                self.send_message(addr, &coordinator_msg).await;
            }
        }
    }
    
    async fn send_heartbeats(&self) {
        let mut interval = interval(Duration::from_secs(2));
        
        loop {
            interval.tick().await;
            
            let state = self.state.read().await;
            if *state == NodeState::Leader {
                drop(state);
                
                // Calculate successor from active nodes
                let active_nodes = self.active_nodes.read().await;
                // exclude self (the leader) to get the next-highest active node
                let successor_id = self.calculate_successor(&active_nodes, self.current_leader.read().await.unwrap());
                drop(active_nodes);

                if let Some(succ_id) = successor_id {
                    println!("Node {}: Current successor is Node {}", self.id, succ_id);
                }

                
                let heartbeat_msg = Message::Heartbeat {
                    leader_id: self.id,
                    successor_id,
                    timestamp: current_timestamp(),
                };

                for (node_id, addr) in &self.all_nodes {
                    if *node_id != self.id {
                        self.send_message(addr, &heartbeat_msg).await;
                    }
                }
            }
        }
    }

    async fn monitor_leader(&self) {
        let mut interval = interval(Duration::from_secs(1));
        
        loop {
            interval.tick().await;
            
            let state = self.state.read().await;
            if *state != NodeState::Leader {
                drop(state);
                
                let last_hb = self.last_heartbeat.read().await;
                let elapsed = SystemTime::now()
                    .duration_since(*last_hb)
                    .unwrap_or(Duration::from_secs(0));
                
                drop(last_hb);
                
                if elapsed > Duration::from_secs(5) {
                    let election_in_progress = *self.election_in_progress.read().await;
                    if !election_in_progress {
                        println!("Node {}: Leader timeout detected!", self.id);
                        *self.current_leader.write().await = None;
                        self.start_election().await;
                    }
                }
            }
        }
    }

    async fn listen(&self) {
        let mut buf = [0u8; 4096];
        
        loop {
            match self.socket.recv_from(&mut buf).await {
                Ok((len, addr)) => {
                    if let Ok(message) = serde_json::from_slice::<Message>(&buf[..len]) {
                        self.handle_message(message, addr).await;
                    }
                }
                Err(e) => {
                    eprintln!("Node {}: Error receiving: {}", self.id, e);
                }
            }
        }
    }

    async fn handle_message(&self, message: Message, _addr: SocketAddr) {
        match message {
            Message::Discovery { sender_id, .. } => {
                // Track that this node is active
                let mut active_nodes = self.active_nodes.write().await;
                active_nodes.insert(sender_id, SystemTime::now());
                drop(active_nodes);
                
                let state = self.state.read().await;
                if *state == NodeState::Leader {
                    drop(state);
                    
                    let response = Message::LeaderAnnounce {
                        leader_id: self.id,
                        timestamp: current_timestamp(),
                    };
                    
                    if let Some(sender_addr) = self.all_nodes.get(&sender_id) {
                        self.send_message(sender_addr, &response).await;
                    }
                }
            }
            
            Message::LeaderAnnounce { leader_id, .. } => {
                let current = *self.current_leader.read().await;
                if current.is_none() || leader_id > current.unwrap() {
                    println!("Node {}: Accepting Node {} as leader", self.id, leader_id);
                    *self.current_leader.write().await = Some(leader_id);
                    *self.state.write().await = NodeState::Follower;
                    *self.last_heartbeat.write().await = SystemTime::now();
                }
            }
            
            Message::Election { sender_id, .. } => {
                // Track that this node is active
                let mut active_nodes = self.active_nodes.write().await;
                active_nodes.insert(sender_id, SystemTime::now());
                drop(active_nodes);
                
                if sender_id < self.id {
                    // We have higher ID, send OK and start our own election
                    let ok_msg = Message::ElectionOk {
                        sender_id: self.id,
                        timestamp: current_timestamp(),
                    };
                    
                    if let Some(sender_addr) = self.all_nodes.get(&sender_id) {
                        self.send_message(sender_addr, &ok_msg).await;
                    }
                    
                    // Start our own election
                    let election_in_progress = *self.election_in_progress.read().await;
                    if !election_in_progress {
                        // Call directly instead of spawning - we're already in async context
                        self.start_election().await;
                    }
                }
            }
            
            Message::ElectionOk { sender_id, .. } => {
                println!("Node {}: Higher node {} responded to election", self.id, sender_id);
                *self.state.write().await = NodeState::Follower;
            }
            
            Message::Coordinator { leader_id, .. } => {
                println!("Node {}: New coordinator is Node {}", self.id, leader_id);
                *self.current_leader.write().await = Some(leader_id);
                *self.state.write().await = NodeState::Follower;
                *self.last_heartbeat.write().await = SystemTime::now();
            }
            
            Message::Heartbeat { leader_id, successor_id, .. } => {
                let current = *self.current_leader.read().await;
                if current == Some(leader_id) {
                    *self.last_heartbeat.write().await = SystemTime::now();
                    
                    // Store successor hint
                    *self.successor_hint.write().await = successor_id;
                    
                    // Send acknowledgment back to leader
                    let ack_msg = Message::HeartbeatAck {
                        sender_id: self.id,
                        timestamp: current_timestamp(),
                    };
                    
                    if let Some(leader_addr) = self.all_nodes.get(&leader_id) {
                        self.send_message(leader_addr, &ack_msg).await;
                    }
                }
            }
            
            Message::HeartbeatAck { sender_id, .. } => {
                // Leader receives acks to track active nodes
                let state = self.state.read().await;
                if *state == NodeState::Leader {
                    drop(state);
                    let mut active_nodes = self.active_nodes.write().await;
                    active_nodes.insert(sender_id, SystemTime::now());
                }
            }
        }
    }

    async fn send_message(&self, addr: &SocketAddr, message: &Message) {
        if let Ok(data) = serde_json::to_vec(message) {
            let _ = self.socket.send_to(&data, addr).await;
        }
    }

    async fn report_status(&self) {
        let mut interval = interval(Duration::from_secs(5));
        loop {
            interval.tick().await;
    
            let state = self.state.read().await.clone();
            let leader = *self.current_leader.read().await;
            let last_hb = self.last_heartbeat.read().await;
            let elapsed = SystemTime::now()
                .duration_since(*last_hb)
                .unwrap_or(Duration::from_secs(0))
                .as_secs_f64();
            drop(last_hb);
    
            if state == NodeState::Leader {
                // Leader: compute successor from current acks
                let active_nodes = self.active_nodes.read().await;
                let computed_succ = self.calculate_successor(&active_nodes, self.id);
                let active_count = active_nodes.len()+1; // acks from others only
                drop(active_nodes);
    
                println!(
                    "Node {} Status: State={:?}, Leader={:?}, Successor(computed)={:?}, Active nodes={}, Time since heartbeat={:.1}s",
                    self.id, state, leader, computed_succ, active_count, elapsed
                );
            } else {
                // Follower: show the hint learned from leader heartbeats
                let successor_hint = *self.successor_hint.read().await;
                println!(
                    "Node {} Status: State={:?}, Leader={:?}, Successor(hint)={:?}, Time since heartbeat={:.1}s",
                    self.id, state, leader, successor_hint, elapsed
                );
            }
        }
    }
}    

fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Node ID (0, 1, or 2)
    #[arg(short, long)]
    id: u32,
    
    /// Config file path (optional, will use default if not provided)
    #[arg(short, long)]
    config: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    
    // Default config
    let config_json = r#"{
        "nodes": [
            {"id": 0, "address": "127.0.0.1:8080"},
            {"id": 1, "address": "127.0.0.1:8081"},
            {"id": 2, "address": "127.0.0.1:8083"}
        ]
    }"#;
    
    let config: Config = if let Some(config_path) = args.config {
        let config_str = tokio::fs::read_to_string(config_path).await?;
        serde_json::from_str(&config_str)?
    } else {
        serde_json::from_str(config_json)?
    };
    
    let node = Arc::new(Node::new(args.id, &config).await?);
    node.start().await;
    
    // Keep running
    tokio::signal::ctrl_c().await?;
    println!("\nShutting down node {}...", args.id);
    
    Ok(())
}
// let config_json = r#"{
//     "nodes": [
//         {"id": 0, "address": "10.40.61.79:8080"},
//         {"id": 1, "address": "10.40.58.169:8081"},
//         {"id": 2, "address": "10.40.50.93:8083"}
//     ]
// }"#;
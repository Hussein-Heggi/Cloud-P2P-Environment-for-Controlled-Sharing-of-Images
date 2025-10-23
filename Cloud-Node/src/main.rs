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

        // Announce to all nodes
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
                
                let heartbeat_msg = Message::Heartbeat {
                    leader_id: self.id,
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
            
            Message::Heartbeat { leader_id, .. } => {
                let current = *self.current_leader.read().await;
                if current == Some(leader_id) {
                    *self.last_heartbeat.write().await = SystemTime::now();
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
            
            let state = self.state.read().await;
            let leader = *self.current_leader.read().await;
            let last_hb = self.last_heartbeat.read().await;
            let elapsed = SystemTime::now()
                .duration_since(*last_hb)
                .unwrap_or(Duration::from_secs(0));
            
            println!(
                "Node {} Status: State={:?}, Leader={:?}, Time since heartbeat={:.1}s",
                self.id,
                *state,
                leader,
                elapsed.as_secs_f64()
            );
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


//"nodes": [
//     {"id": 0, "address": "10.40.61.79:8080"},
//     {"id": 1, "address": "10.40.58.169:8081"},
//     {"id": 2, "address": "10.40.50.93:8083"}
// ]
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, SystemTime},
};
use serde::{Deserialize, Serialize};
use tokio::{
    net::UdpSocket,
    sync::{RwLock, watch},
    time::{sleep, interval},
};
use tracing::{info, warn};

use crate::{state::SharedState, config::Config};

// === Failure-detection tuning ===
const LEADER_TIMEOUT_SECS: u64 = 5;  // follower waits this long for leader heartbeat
const PEER_EXPIRY_SECS: u64 = 6;     // leader prunes peers that haven't ACKed in this long

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
enum Message {
    Discovery { sender_id: u32, timestamp: u64 },
    LeaderAnnounce { leader_id: u32, timestamp: u64 },
    Election { sender_id: u32, timestamp: u64 },
    ElectionOk { sender_id: u32, timestamp: u64 },
    Coordinator { leader_id: u32, timestamp: u64 },
    Heartbeat { leader_id: u32, successor_id: Option<u32>, timestamp: u64 },
    HeartbeatAck { sender_id: u32, timestamp: u64 },
}

#[derive(Debug, Clone, PartialEq)]
enum NodeState { Follower, Candidate, Leader }

struct Node {
    id: u32,
    address: SocketAddr,
    all_nodes: HashMap<u32, SocketAddr>,
    state: Arc<RwLock<NodeState>>,
    current_leader: Arc<RwLock<Option<u32>>>,
    successor_hint: Arc<RwLock<Option<u32>>>,
    active_nodes: Arc<RwLock<HashMap<u32, SystemTime>>>,
    last_heartbeat: Arc<RwLock<SystemTime>>,
    election_in_progress: Arc<RwLock<bool>>,
    socket: Arc<UdpSocket>,

    // Integration hooks:
    leader_tx: watch::Sender<u32>,
    shared: SharedState,
}

impl Node {
    async fn new(cfg: &Config, leader_tx: watch::Sender<u32>, shared: SharedState) -> anyhow::Result<Self> {
        let id = cfg.node_id;
        let address: SocketAddr = cfg.election_bind_addr();
        let socket = UdpSocket::bind(address).await?;

        // Build election peer list (ip: port+100)
        let mut all_nodes = HashMap::new();
        for (idx, addr) in Config::election_peer_addrs().into_iter().enumerate() {
            let node_id = (idx as u32) + 1;
            all_nodes.insert(node_id, addr);
        }

        info!("Election node {} starting at {}", id, address);

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
            leader_tx,
            shared,
        })
    }

    async fn start(self: Arc<Self>) {
        // listener
        let node_clone = self.clone();
        tokio::spawn(async move { node_clone.listen().await; });

        sleep(Duration::from_millis(300)).await;

        // discovery
        self.discover_cluster().await;

        // monitors
        let node_clone = self.clone();
        tokio::spawn(async move { node_clone.monitor_leader().await; });

        let node_clone = self.clone();
        tokio::spawn(async move { node_clone.send_heartbeats().await; });

        let node_clone = self.clone();
        tokio::spawn(async move { node_clone.report_status().await; });

        info!("Election: node {} started", self.id);
    }

    fn calculate_successor(&self, active_nodes: &HashMap<u32, SystemTime>, exclude_id: u32) -> Option<u32> {
        let mut ids: Vec<u32> = active_nodes.keys().copied().collect();
        ids.push(self.id);
        ids.sort_unstable_by(|a, b| b.cmp(a));
        ids.dedup();
        ids.into_iter().find(|&id| id != exclude_id)
    }

    async fn discover_cluster(&self) {
        let discovery_msg = Message::Discovery { sender_id: self.id, timestamp: now_ts() };
        for (node_id, addr) in &self.all_nodes {
            if *node_id != self.id {
                self.send_message(addr, &discovery_msg).await;
            }
        }
        sleep(Duration::from_secs(2)).await;

        if self.current_leader.read().await.is_none() {
            self.start_election().await;
        }
    }

    async fn start_election(&self) {
        {
            let mut in_prog = self.election_in_progress.write().await;
            if *in_prog { return; }
            *in_prog = true;
        }

        // Successor hint path
        if let Some(succ) = *self.successor_hint.read().await {
            if succ == self.id {
                self.become_leader().await;
                *self.election_in_progress.write().await = false;
                return;
            } else if succ > self.id {
                // defer to higher successor
                if let Some(addr) = self.all_nodes.get(&succ) {
                    let election_msg = Message::Election { sender_id: self.id, timestamp: now_ts() };
                    self.send_message(addr, &election_msg).await;
                }
                sleep(Duration::from_millis(800)).await;
                if *self.state.read().await == NodeState::Leader {
                    *self.election_in_progress.write().await = false;
                    return;
                }
            }
        }

        // Bully
        let election_msg = Message::Election { sender_id: self.id, timestamp: now_ts() };
        let higher: Vec<_> = self.all_nodes.iter().filter(|(id, _)| **id > self.id).collect();

        if higher.is_empty() {
            self.become_leader().await;
            *self.election_in_progress.write().await = false;
            return;
        }

        for (_, addr) in &higher { self.send_message(addr, &election_msg).await; }
        sleep(Duration::from_millis(1500)).await;

        if *self.state.read().await != NodeState::Leader {
            self.become_leader().await;
        }

        *self.election_in_progress.write().await = false;
    }

    async fn become_leader(&self) {
        *self.state.write().await = NodeState::Leader;
        *self.current_leader.write().await = Some(self.id);
        *self.last_heartbeat.write().await = SystemTime::now();
        *self.successor_hint.write().await = None;
        self.active_nodes.write().await.clear();

        // Announce to peers
        let msg = Message::Coordinator { leader_id: self.id, timestamp: now_ts() };
        for (node_id, addr) in &self.all_nodes {
            if *node_id != self.id { self.send_message(addr, &msg).await; }
        }

        // Integration: update shared & notify
        {
            let mut s = self.shared.write().await;
            s.leader_id = self.id;
            s.is_leader = true;
        }
        let _ = self.leader_tx.send(self.id);
        info!("Election: Node {} became leader", self.id);
    }

    async fn send_heartbeats(&self) {
        let mut tick = interval(Duration::from_secs(2));
        loop {
            tick.tick().await;
            if *self.state.read().await == NodeState::Leader {
                // prune stale peers
                {
                    let mut active = self.active_nodes.write().await;
                    let now = SystemTime::now();
                    active.retain(|peer_id, last_seen| {
                        now.duration_since(*last_seen).unwrap_or(Duration::ZERO) < Duration::from_secs(PEER_EXPIRY_SECS)
                    });
                }
                // compute successor
                let active_nodes = self.active_nodes.read().await;
                let exclude = self.current_leader.read().await.unwrap_or(self.id);
                let successor_id = self.calculate_successor(&active_nodes, exclude);
                drop(active_nodes);

                let heartbeat = Message::Heartbeat {
                    leader_id: self.id,
                    successor_id,
                    timestamp: now_ts(),
                };
                for (node_id, addr) in &self.all_nodes {
                    if *node_id != self.id {
                        self.send_message(addr, &heartbeat).await;
                    }
                }
            }
        }
    }

    async fn monitor_leader(&self) {
        let mut tick = interval(Duration::from_secs(1));
        loop {
            tick.tick().await;
            if *self.state.read().await != NodeState::Leader {
                let elapsed = SystemTime::now()
                    .duration_since(*self.last_heartbeat.read().await)
                    .unwrap_or(Duration::ZERO);
                if elapsed > Duration::from_secs(LEADER_TIMEOUT_SECS) {
                    *self.current_leader.write().await = None;
                    self.start_election().await;
                }
            }
        }
    }

    async fn listen(&self) {
        let mut buf = [0u8; 4096];
        loop {
            match self.socket.recv_from(&mut buf).await {
                Ok((len, addr)) => {
                    if let Ok(msg) = serde_json::from_slice::<Message>(&buf[..len]) {
                        self.handle_message(msg, addr).await;
                    }
                }
                Err(e) => { warn!("Election recv error: {e}"); }
            }
        }
    }

    async fn handle_message(&self, message: Message, _addr: SocketAddr) {
        match message {
            Message::Discovery { sender_id, .. } => {
                self.active_nodes.write().await.insert(sender_id, SystemTime::now());
                if *self.state.read().await == NodeState::Leader {
                    if let Some(sender_addr) = self.all_nodes.get(&sender_id) {
                        let response = Message::LeaderAnnounce { leader_id: self.id, timestamp: now_ts() };
                        self.send_message(sender_addr, &response).await;
                    }
                }
            }
            Message::LeaderAnnounce { leader_id, .. } => {
                let mut st = self.state.write().await;
                *st = NodeState::Follower;
                *self.current_leader.write().await = Some(leader_id);
                *self.last_heartbeat.write().await = SystemTime::now();

                // Integration: update shared & notify
                {
                    let mut s = self.shared.write().await;
                    s.leader_id = leader_id;
                    s.is_leader = s.node_id == leader_id;
                }
                let _ = self.leader_tx.send(leader_id);
            }
            Message::Election { sender_id, .. } => {
                self.active_nodes.write().await.insert(sender_id, SystemTime::now());
                if sender_id < self.id {
                    if let Some(sender_addr) = self.all_nodes.get(&sender_id) {
                        let ok = Message::ElectionOk { sender_id: self.id, timestamp: now_ts() };
                        self.send_message(sender_addr, &ok).await;
                    }
                    if !*self.election_in_progress.read().await {
                        self.start_election().await;
                    }
                }
            }
            Message::ElectionOk { .. } => {
                *self.state.write().await = NodeState::Follower;
            }
            Message::Coordinator { leader_id, .. } => {
                *self.current_leader.write().await = Some(leader_id);
                *self.state.write().await = NodeState::Follower;
                *self.last_heartbeat.write().await = SystemTime::now();

                // Integration: update shared & notify
                {
                    let mut s = self.shared.write().await;
                    s.leader_id = leader_id;
                    s.is_leader = s.node_id == leader_id;
                }
                let _ = self.leader_tx.send(leader_id);
            }
            Message::Heartbeat { leader_id, successor_id, .. } => {
                if *self.current_leader.read().await == Some(leader_id) || *self.state.read().await != NodeState::Leader {
                    *self.current_leader.write().await = Some(leader_id);
                    *self.last_heartbeat.write().await = SystemTime::now();
                    *self.successor_hint.write().await = successor_id;

                    // ack
                    if let Some(leader_addr) = self.all_nodes.get(&leader_id) {
                        let ack = Message::HeartbeatAck { sender_id: self.id, timestamp: now_ts() };
                        self.send_message(leader_addr, &ack).await;
                    }
                }
            }
            Message::HeartbeatAck { sender_id, .. } => {
                if *self.state.read().await == NodeState::Leader {
                    self.active_nodes.write().await.insert(sender_id, SystemTime::now());
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
        let mut every = interval(Duration::from_secs(5));
        loop {
            every.tick().await;
            let state = self.state.read().await.clone();
            let leader = *self.current_leader.read().await;
            let elapsed = SystemTime::now()
                .duration_since(*self.last_heartbeat.read().await)
                .unwrap_or(Duration::ZERO)
                .as_secs_f64();
            if state == NodeState::Leader {
                let active_nodes = self.active_nodes.read().await;
                let computed_succ = self.calculate_successor(&active_nodes, self.id);
                let active_count = active_nodes.len() + 1;
                let live: Vec<u32> = active_nodes.keys().copied().collect();
                drop(active_nodes);
                info!("Node {}: Leader, leader={leader:?}, successor={computed_succ:?}, active={}, live={:?}, Δhb={:.1}s",
                      self.id, active_count, live, elapsed);
            } else {
                let successor_hint = *self.successor_hint.read().await;
                info!("Node {}: Follower, leader={leader:?}, successor_hint={successor_hint:?}, Δhb={:.1}s",
                      self.id, elapsed);
            }
        }
    }
}

#[inline]
fn now_ts() -> u64 {
    SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs()
}

// === Public API used by main.rs ===

pub async fn run_election_loop(state: SharedState, cfg: Config, leader_tx: watch::Sender<u32>) {
    let node = Arc::new(Node::new(&cfg, leader_tx, state).await.expect("election init"));
    node.clone().start().await;
    futures::future::pending::<()>().await;
}

pub async fn handle_leader_changes(_state: SharedState, mut _rx: watch::Receiver<u32>) {
    // The election module already writes SharedState + broadcasts.
    // You can add extra hooks here if you need to react elsewhere.
    while _rx.changed().await.is_ok() {}
}

pub async fn reconcile_state_after_revive() {
    // Optional: on simulated failure recovery you could re-run discovery.
}

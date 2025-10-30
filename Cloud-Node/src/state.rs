use std::{collections::HashMap, net::IpAddr, sync::Arc};
use tokio::sync::RwLock;

#[derive(Clone, Debug)]
pub struct Row {
    pub req_id: u32,
    pub sender_id: u32,
    pub assigned: Option<u32>,
    pub completed: bool,
    pub lease_deadline_ms: Option<u128>,
    pub version: u64,
}

#[derive(Default, Clone, Debug)]
pub struct HistoryState {
    pub rows: HashMap<u32, Row>,
}

#[derive(Clone, Debug)]
pub struct ServerState {
    pub node_id: u32,
    pub leader_id: u32,
    pub is_leader: bool,
    pub ignoring: bool,
    pub history: HistoryState,
    pub live_peers: Vec<u32>,

    // current executor (from ASSIGN)
    pub executor_ip: Option<IpAddr>,              // IP-only; client port is fixed globally
    pub executor_lease_deadline_ms: Option<u128>, // unix_ms deadline
}

impl Default for ServerState {
    fn default() -> Self {
        Self {
            node_id: 1,
            leader_id: 0,
            is_leader: false,
            ignoring: false,
            history: HistoryState::default(),
            live_peers: vec![1],
            executor_ip: None,
            executor_lease_deadline_ms: None,
        }
    }
}

impl ServerState {
    pub fn new(node_id: u32) -> Self {
        Self {
            node_id,
            leader_id: 0,
            is_leader: false,
            ignoring: false,
            history: HistoryState::default(),
            live_peers: vec![node_id],
            executor_ip: None,
            executor_lease_deadline_ms: None,
        }
    }
}

pub type SharedState = Arc<RwLock<ServerState>>;

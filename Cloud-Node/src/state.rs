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
pub struct LoadInfo {
    pub server_id: u32,
    pub load_score: f32,
    pub timestamp_ms: u128,
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

    // ---- Load balancing state ----
    pub load_reports: HashMap<u32, LoadInfo>,     // server_id -> load info
    pub current_executor_id: Option<u32>,          // which server is currently assigned

    // ---- Metrics ----
    pub requests_received: u64, // count of accepted REQ_META (one per request)
    pub requests_served: u64,   // count of completed responses
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
            load_reports: HashMap::new(),
            current_executor_id: None,
            requests_received: 0,
            requests_served: 0,
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
            load_reports: HashMap::new(),
            current_executor_id: None,
            requests_received: 0,
            requests_served: 0,
        }
    }
}

pub type SharedState = Arc<RwLock<ServerState>>;
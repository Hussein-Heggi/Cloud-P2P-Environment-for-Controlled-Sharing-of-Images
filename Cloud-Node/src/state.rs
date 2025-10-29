use std::{collections::HashMap, sync::Arc};
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
    pub rr_next: usize,
}

#[derive(Debug, Clone)]
pub struct ServerState {
    pub node_id: u32,
    pub leader_id: u32,
    pub is_leader: bool,
    pub ignoring: bool,       // true => node is “DOWN” (silent)
    pub history: HistoryState,
    pub live_peers: Vec<u32>, // node_ids currently considered UP
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
        }
    }
}

pub type SharedState = Arc<RwLock<ServerState>>;

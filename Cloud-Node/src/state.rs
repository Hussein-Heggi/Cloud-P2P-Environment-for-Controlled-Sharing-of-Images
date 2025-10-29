use tokio::sync::RwLock;
use std::{collections::HashMap, sync::Arc};

#[derive(Clone, Debug)]
pub struct Row {
    pub req_id: u32,
    pub sender_id: u32,
    pub assigned: Option<u32>,
    pub completed: bool,
    pub lease_deadline_ms: Option<u128>,
    pub version: u64,
}

#[derive(Default)]
pub struct HistoryState {
    pub rows: HashMap<u32, Row>,
    pub rr_next: usize,
}

#[derive(Debug, Clone)]
pub struct ServerState {
    pub node_id: u32,
    pub leader_id: u32,
    pub is_leader: bool,
    pub ignoring: bool,
    pub history: HistoryState,
}

impl ServerState {
    pub fn new(node_id: u32) -> Self {
        Self {
            node_id,
            leader_id: 0,
            is_leader: false,
            ignoring: false,
            history: HistoryState::default(),
        }
    }
}

pub type SharedState = Arc<RwLock<ServerState>>;

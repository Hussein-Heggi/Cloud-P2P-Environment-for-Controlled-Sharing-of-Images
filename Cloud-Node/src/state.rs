use std::{collections::HashMap, net::IpAddr, sync::Arc};
use tokio::sync::RwLock;
use firestore::FirestoreDb;
use serde::{Deserialize, Serialize};

use crate::firebase::DosClient;

/// History table record for fault tolerance and deduplication
#[derive(Clone, Debug)]
pub struct HistoryRecord {
    pub req_id: u32,
    pub executor_node: IpAddr,
    pub path_to_output_image: Option<String>, // None = in-progress, Some = completed
    pub timestamp: u128, // Completion time in milliseconds (0 = in-progress)
}

#[derive(Clone, Debug)]
pub struct LoadInfo {
    pub server_id: u32,
    pub load_score: f32,
    pub timestamp_ms: u128,
}

/// Request types for pending requests
#[derive(Clone, Debug)]
pub enum RequestType {
    View,
    AdjustViews,
    Revoke,
}

/// Pending request tracking (repurposed history table for new protocol)
#[derive(Clone, Debug)]
pub struct PendingRequest {
    pub req_id: u32,
    pub executor_ip: IpAddr,
    pub req_type: RequestType,
    pub owner_name: String,
    pub viewer_name: String,
    pub image_name: String,
    pub initiated_at: u128,
}

/// Offline request stored when recipient is not online (NEW P2P architecture)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OfflineRequest {
    pub request_type: String,        // "VIEW", "ADJUST", "REVOKE"
    pub sender: String,
    pub recipient: String,
    pub image_name: String,
    pub data: Vec<u8>,               // Full message bytes (for forwarding)
    pub timestamp: u64,              // Unix timestamp in milliseconds
}

#[derive(Clone, Debug)]
pub struct ServerState {
    pub node_id: u32,
    pub leader_id: u32,
    pub is_leader: bool,
    pub ignoring: bool,
    pub live_peers: Vec<u32>,

    // current executor (from ASSIGN)
    pub executor_ip: Option<IpAddr>,              // IP-only; client port is fixed globally
    pub executor_lease_deadline_ms: Option<u128>, // unix_ms deadline

    // ---- Load balancing state ----
    pub load_reports: HashMap<u32, LoadInfo>,     // server_id -> load info
    pub current_executor_id: Option<u32>,          // which server is currently assigned

    // ---- History table for fault tolerance ----
    pub history: HashMap<u32, HistoryRecord>,     // req_id -> history record
                                                   // path_to_output_image: None = in-progress
                                                   // path_to_output_image: Some = completed

    // ---- Metrics ----
    pub requests_received: u64, // count of accepted REQ_META (one per request)
    pub requests_served: u64,   // count of completed responses

    // ---- Firebase Connection ----
    pub firestore_db: Option<FirestoreDb>, // Firebase connection (None if Firebase down)

    // ---- DOS-S Local Copy (synchronized from Firebase) ----
    pub dos_clients: HashMap<String, DosClient>,  // username -> client info
    pub dos_c_version: u32,                        // Incremented on DOS changes
    // REMOVED Phase 2: dos_access field - Access map now managed locally by clients only

    // ---- TCP Connection Registry ----
    pub client_connections: HashMap<String, Arc<tokio::sync::Mutex<tokio::net::TcpStream>>>, // username -> active TCP connection

    // ---- Owner image uploads (TCP stego pipeline) ----
    // owner -> image_name -> buffers
    pub owner_images: HashMap<String, HashMap<String, StoredImageAssets>>,

    // ---- Pending Requests (new protocol) ----
    pub pending_requests: HashMap<u32, PendingRequest>, // req_id -> pending request

    // ---- NEW P2P: Offline Requests Queue ----
    pub offline_requests: HashMap<String, Vec<OfflineRequest>>, // recipient_username -> requests
}

impl Default for ServerState {
    fn default() -> Self {
        Self {
            node_id: 1,
            leader_id: 0,
            is_leader: false,
            ignoring: false,
            live_peers: vec![1],
            executor_ip: None,
            executor_lease_deadline_ms: None,
            load_reports: HashMap::new(),
            current_executor_id: None,
            history: HashMap::new(),
            requests_received: 0,
            requests_served: 0,
            firestore_db: None,
            dos_clients: HashMap::new(),
            dos_c_version: 0,
            client_connections: HashMap::new(),
            owner_images: HashMap::new(),
            pending_requests: HashMap::new(),
            offline_requests: HashMap::new(),
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
            live_peers: vec![node_id],
            executor_ip: None,
            executor_lease_deadline_ms: None,
            load_reports: HashMap::new(),
            current_executor_id: None,
            history: HashMap::new(),
            requests_received: 0,
            requests_served: 0,
            firestore_db: None,
            dos_clients: HashMap::new(),
            dos_c_version: 0,
            client_connections: HashMap::new(),
            owner_images: HashMap::new(),
            pending_requests: HashMap::new(),
            offline_requests: HashMap::new(),
        }
    }
}

pub type SharedState = Arc<RwLock<ServerState>>;

#[derive(Clone, Debug, Default)]
pub struct StoredImageAssets {
    pub true_expected: usize,
    pub cover_expected: usize,
    pub meta_expected: usize,
    pub true_buf: Vec<u8>,
    pub cover_buf: Vec<u8>,
    pub meta_buf: Vec<u8>,
    // Track bytes actually received (not just buffer size)
    pub true_received: usize,
    pub cover_received: usize,
    pub meta_received: usize,
}

impl StoredImageAssets {
    pub fn ready(&self) -> bool {
        self.true_received == self.true_expected
            && self.cover_received == self.cover_expected
            && self.meta_received == self.meta_expected
    }
}

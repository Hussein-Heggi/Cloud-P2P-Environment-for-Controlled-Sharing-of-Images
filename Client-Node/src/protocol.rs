//! Protocol message type constants for client-server communication

// Phase 1: Initial handshake
pub const REQ: u8 = 10;
pub const ACCEPT: u8 = 11;

// Phase 2: Request details
pub const VIEW_REQUEST: u8 = 12;
pub const ADJUST_REQUEST: u8 = 13;
pub const REVOKE_REQUEST: u8 = 14;

// Phase 3: Owner notifications
pub const VIEW_NOTIFICATION: u8 = 15;
pub const ADJUST_NOTIFICATION: u8 = 16;

// Phase 4: Owner responses
pub const APPROVE_VIEW: u8 = 17;
pub const DENY_VIEW: u8 = 18;
pub const APPROVE_ADJUST: u8 = 19;
pub const DENY_ADJUST: u8 = 20;

// Phase 5: Viewer responses
pub const APPROVED: u8 = 21;
pub const REJECTED: u8 = 22;
pub const IMAGE_CHUNK: u8 = 23;
pub const ADJUSTED_VIEWS: u8 = 24;
pub const REVOKED: u8 = 25;

// Management messages
pub const DELETE_IMAGE: u8 = 26;
pub const JOIN: u8 = 27;
pub const JOIN_ACK: u8 = 28;
pub const DOS_UPDATE: u8 = 29;
pub const DOS_QUERY: u8 = 52;
pub const CLIENT_PING: u8 = 50;
pub const SERVER_PONG: u8 = 51;

// Sync messages
pub const SYNC_USAGE: u8 = 30;
pub const SYNC_ACK: u8 = 31;
pub const REQUEST_VIEW_PERMISSION: u8 = 32;
pub const VIEW_PERMISSION_GRANTED: u8 = 33;
pub const VIEW_PERMISSION_DENIED: u8 = 34;
pub const OWNER_IMAGE_META: u8 = 35;
pub const OWNER_IMAGE_CHUNK: u8 = 36;

// P2P Client-to-Client Messages (NEW for P2P architecture)
pub const PEER_VIEW_REQUEST: u8 = 60;     // Viewer → Owner direct
pub const PEER_VIEW_RESPONSE: u8 = 61;    // Owner → Viewer (approved)
pub const PEER_VIEW_REJECTED: u8 = 62;    // Owner → Viewer (denied)
pub const PEER_IMAGE_CHUNK: u8 = 63;      // Owner → Viewer (image data)
pub const PEER_ADJUST_REQUEST: u8 = 64;   // Viewer → Owner (adjust views request)
pub const PEER_REVOKE: u8 = 65;           // Owner → Viewer (revoke access)
pub const PEER_ACK: u8 = 66;              // Generic acknowledgment

// P2P Adjust and Revoke Messages
pub const PEER_ADJUST_APPROVED: u8 = 67;       // Owner → Viewer (adjust approved)
pub const PEER_ADJUST_REJECTED: u8 = 68;       // Owner → Viewer (adjust rejected)
pub const PEER_ADJUST_ORDER: u8 = 69;          // Owner → Viewer (force view count change)
pub const PEER_ADJUST_ORDER_ACK: u8 = 80;      // Viewer → Owner (adjustment confirmed)
pub const PEER_ADJUST_ORDER_NOT_FOUND: u8 = 81; // Viewer → Owner (image not found)
pub const PEER_REVOKE_ACK: u8 = 82;            // Viewer → Owner (revocation confirmed)

// Server Offline/Access Map Messages (NEW for P2P architecture)
pub const OFFLINE_REQUESTS_QUERY: u8 = 53;    // Client → Server on startup
pub const OFFLINE_REQUESTS_RESPONSE: u8 = 54; // Server → Client
pub const ACCESS_MAP_QUERY: u8 = 55;          // Client → Server on startup
pub const ACCESS_MAP_RESPONSE: u8 = 56;       // Server → Client
pub const PENDING_REQUEST: u8 = 57;           // Server → Client: Deliver offline request
pub const ADJUST_ORDER_REQUEST: u8 = 58;      // Owner → Server: Adjust viewer's views (offline fallback)

// Multi-Server Discovery Messages
pub const REQUEST_EXECUTOR: u8 = 70;          // Client → Server: Find executor
pub const EXECUTOR_ACK: u8 = 71;              // Server → Client: I am executor

// System Recovery and Failover (TCP-based)
pub const LIFE_CHECK: u8 = 72;                // Server → Client P2P TCP: "Are you alive?"
pub const LIFE_CHECK_ACK: u8 = 73;            // Client → Server TCP: "Yes, I'm alive"
pub const CLIENT_LEAVE: u8 = 74;              // Client → Server TCP: Graceful shutdown

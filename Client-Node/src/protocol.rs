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

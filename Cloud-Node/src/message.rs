use serde::{Deserialize, Serialize};

/// Message types for the dynamic ID system
/// NOTE: All messages use PHYSICAL_ID for addressing, LOGICAL_PID for roles
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    /// Join announcement - new node entering network
    /// Physical ID is static (from config), Logical PID is assigned dynamically
    JoinRequest { 
        physical_id: u32,       // My physical ID (from config, never changes)
        from_address: String,   // My address
    },
    
    /// Response to join request with current state
    JoinResponse { 
        physical_id: u32,       // Responder's physical ID
        logical_pid: u32,       // Responder's current logical PID (0, 1, or 2)
        is_leader: bool,        // Is responder the leader?
        next_leader_physical_id: Option<u32>, // Physical ID of node with logical PID 1
    },
    
    /// Simple heartbeat - includes both IDs for mapping
    Heartbeat { 
        physical_id: u32,                       // Who I am physically
        logical_pid: u32,                       // My current role (0, 1, or 2)
        next_leader_physical_id: Option<u32>,   // Physical ID of next leader (if I'm leader)
    },
    
    /// Leader failure detected - node with logical PID 1 announces takeover
    LeadershipTakeover {
        physical_id: u32,           // My physical ID (never changes)
        new_logical_pid: u32,       // Always 2 (I'm new leader)
        old_logical_pid: u32,       // What my logical PID was (should be 1)
    },
    
    /// Notify nodes to shift their logical PIDs after leader failure
    ShiftIDs {
        from_physical_id: u32,      // Physical ID of sender
        from_logical_pid: u32,      // Logical PID of sender (should be 2 = new leader)
    },
}

impl Message {
    /// Serialize message to JSON bytes with length prefix
    pub fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        let json = serde_json::to_string(self)?;
        let len = json.len() as u32;
        
        let mut bytes = Vec::with_capacity(4 + json.len());
        bytes.extend_from_slice(&len.to_be_bytes());
        bytes.extend_from_slice(json.as_bytes());
        
        Ok(bytes)
    }
}
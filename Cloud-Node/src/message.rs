use serde::{Deserialize, Serialize};

/// Message types for inter-node communication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    /// Initial handshake when connecting
    Hello { from_node: u32 },
    
    /// Response to hello
    HelloAck { from_node: u32 },
    
    // ============ CHANGED: Added election-related fields ============
    /// Periodic heartbeat to detect failures
    /// NOW INCLUDES: is_leader flag and max_known_pid hint
    Heartbeat { 
        from_node: u32, 
        timestamp: u64,
        is_leader: bool,          // NEW: Is this node the leader?
        max_known_pid: u32,       // NEW: Highest PID the leader knows about
    },
    // ================================================================
    
    /// Test message for verifying connectivity
    Ping { from_node: u32 },
    
    /// Response to ping
    Pong { from_node: u32 },
    
    // ============ NEW: Election message types ============
    /// Election message - asking if you're alive and can be leader
    Election { from_node: u32 },
    
    /// OK response - I'm alive and will handle the election
    OK { from_node: u32 },
    
    /// Coordinator announcement - declaring leadership
    Coordinator { from_node: u32 },
    
    /// Request to become leader (sent to max_known_pid when my PID is lower)
    BecomeLeader { from_node: u32 },
    // ====================================================
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
    
    /// Get the sender's node ID from any message
    #[allow(dead_code)]
    pub fn sender_id(&self) -> u32 {
        match self {
            Message::Hello { from_node } => *from_node,
            Message::HelloAck { from_node } => *from_node,
            Message::Heartbeat { from_node, .. } => *from_node,
            Message::Ping { from_node } => *from_node,
            Message::Pong { from_node } => *from_node,
            // ============ NEW: Handle new message types ============
            Message::Election { from_node } => *from_node,
            Message::OK { from_node } => *from_node,
            Message::Coordinator { from_node } => *from_node,
            Message::BecomeLeader { from_node } => *from_node,
            // =======================================================
        }
    }
}
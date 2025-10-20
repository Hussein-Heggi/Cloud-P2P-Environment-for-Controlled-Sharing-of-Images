use serde::{Deserialize, Serialize};

/// Message types for inter-node communication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    /// Initial handshake when connecting
    Hello { from_node: u32 },
    
    /// Response to hello
    HelloAck { from_node: u32 },
    
    /// Periodic heartbeat to detect failures
    Heartbeat { from_node: u32, timestamp: u64 },
    
    /// Test message for verifying connectivity
    Ping { from_node: u32 },
    
    /// Response to ping
    Pong { from_node: u32 },
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
    pub fn sender_id(&self) -> u32 {
        match self {
            Message::Hello { from_node } => *from_node,
            Message::HelloAck { from_node } => *from_node,
            Message::Heartbeat { from_node, .. } => *from_node,
            Message::Ping { from_node } => *from_node,
            Message::Pong { from_node } => *from_node,
        }
    }
}
use serde::{Deserialize, Serialize};

/// Message types for the modified Bully algorithm
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    /// Recovery/Discovery: "Who is the leader?"
    WhoIsLeader { 
        node_id: u32,
        from_address: String,
    },
    
    /// Leader announces its presence and successor
    Coordinator { 
        leader_id: u32,
        successor_id: Option<u32>,
    },
    
    /// Regular heartbeat from nodes to leader
    Heartbeat { 
        node_id: u32,
    },
    
    /// Non-successor node notifies successor of leader failure
    Takeover {
        from_id: u32,
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
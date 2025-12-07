//! DOS (Directory of Service) Management
//! Handles local caching of DOS-C (client directory) and provides query methods

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// DOS Client entry - matches server's DosClient structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DosClient {
    pub client_name: String,
    pub client_ip: String,
    pub client_port: u16,
    pub images: Vec<String>,
    pub last_seen: u64,
    pub online: bool,
}

/// Local DOS state management
#[derive(Debug, Clone, Default)]
pub struct DosState {
    /// Map of username -> DosClient
    pub clients: HashMap<String, DosClient>,
    /// Current DOS version (for cache coherence)
    pub version: u64,
}

impl DosState {
    pub fn new() -> Self {
        Self {
            clients: HashMap::new(),
            version: 0,
        }
    }

    /// Update DOS from server data
    pub fn update(&mut self, clients: HashMap<String, DosClient>, version: u64) {
        self.clients = clients;
        self.version = version;
        println!("[DOS] Updated to version {} with {} clients", version, self.clients.len());
    }

    /// Get all online users
    pub fn list_online_users(&self) -> Vec<String> {
        self.clients
            .iter()
            .filter(|(_, client)| client.online)
            .map(|(name, _)| name.clone())
            .collect()
    }

    /// Get all images for a specific user
    pub fn list_images(&self, username: &str) -> Option<Vec<String>> {
        self.clients.get(username).map(|client| client.images.clone())
    }

    /// Check if a user is online
    pub fn is_online(&self, username: &str) -> bool {
        self.clients
            .get(username)
            .map(|client| client.online)
            .unwrap_or(false)
    }

    /// Get client info
    pub fn get_client(&self, username: &str) -> Option<&DosClient> {
        self.clients.get(username)
    }

    /// Get all clients
    pub fn get_all_clients(&self) -> &HashMap<String, DosClient> {
        &self.clients
    }

    /// Get current version
    pub fn get_version(&self) -> u64 {
        self.version
    }
}

/// Parse DOS-C from JOIN_ACK payload
/// Format: [dos_c_version:u64][num_clients:u32][client_entries...]
/// Each client entry: [name_len:u16][name][ip_len:u16][ip][port:u16][num_images:u32][image_list][last_seen:u64][online:u8]
pub fn parse_dos_c_from_join_ack(payload: &[u8]) -> anyhow::Result<(HashMap<String, DosClient>, u64)> {
    if payload.len() < 12 {
        return Err(anyhow::anyhow!("JOIN_ACK payload too small: {} bytes", payload.len()));
    }

    let mut offset = 0;

    // Parse DOS version
    let dos_c_version = u64::from_le_bytes(payload[offset..offset + 8].try_into()?);
    offset += 8;

    // Parse number of clients
    let num_clients = u32::from_le_bytes(payload[offset..offset + 4].try_into()?) as usize;
    offset += 4;

    println!("[DOS] Parsing JOIN_ACK: version={} num_clients={}", dos_c_version, num_clients);

    let mut clients = HashMap::new();

    for i in 0..num_clients {
        if offset + 2 > payload.len() {
            return Err(anyhow::anyhow!("Unexpected end of payload at client {}", i));
        }

        // Parse client name
        let name_len = u16::from_le_bytes(payload[offset..offset + 2].try_into()?) as usize;
        offset += 2;

        if offset + name_len > payload.len() {
            return Err(anyhow::anyhow!("Invalid name length at client {}", i));
        }

        let client_name = String::from_utf8(payload[offset..offset + name_len].to_vec())?;
        offset += name_len;

        // Parse client IP
        if offset + 2 > payload.len() {
            return Err(anyhow::anyhow!("Unexpected end at IP length for client {}", i));
        }

        let ip_len = u16::from_le_bytes(payload[offset..offset + 2].try_into()?) as usize;
        offset += 2;

        if offset + ip_len > payload.len() {
            return Err(anyhow::anyhow!("Invalid IP length at client {}", i));
        }

        let client_ip = String::from_utf8(payload[offset..offset + ip_len].to_vec())?;
        offset += ip_len;

        // Parse client port
        if offset + 2 > payload.len() {
            return Err(anyhow::anyhow!("Unexpected end at port for client {}", i));
        }

        let client_port = u16::from_le_bytes(payload[offset..offset + 2].try_into()?);
        offset += 2;

        // Parse number of images
        if offset + 4 > payload.len() {
            return Err(anyhow::anyhow!("Unexpected end at num_images for client {}", i));
        }

        let num_images = u32::from_le_bytes(payload[offset..offset + 4].try_into()?) as usize;
        offset += 4;

        // Parse image list
        let mut images = Vec::new();
        for j in 0..num_images {
            if offset + 2 > payload.len() {
                return Err(anyhow::anyhow!("Unexpected end at image {} length for client {}", j, i));
            }

            let img_len = u16::from_le_bytes(payload[offset..offset + 2].try_into()?) as usize;
            offset += 2;

            if offset + img_len > payload.len() {
                return Err(anyhow::anyhow!("Invalid image {} length for client {}", j, i));
            }

            let image_name = String::from_utf8(payload[offset..offset + img_len].to_vec())?;
            offset += img_len;
            images.push(image_name);
        }

        // Parse last_seen
        if offset + 8 > payload.len() {
            return Err(anyhow::anyhow!("Unexpected end at last_seen for client {}", i));
        }

        let last_seen = u64::from_le_bytes(payload[offset..offset + 8].try_into()?);
        offset += 8;

        // Parse online status
        if offset >= payload.len() {
            return Err(anyhow::anyhow!("Unexpected end at online status for client {}", i));
        }

        let online = payload[offset] != 0;
        offset += 1;

        let dos_client = DosClient {
            client_name: client_name.clone(),
            client_ip,
            client_port,
            images,
            last_seen,
            online,
        };

        println!("[DOS] Parsed client: {} (online={}, {} images)", client_name, online, dos_client.images.len());

        clients.insert(client_name, dos_client);
    }

    println!("[DOS] Successfully parsed {} clients from JOIN_ACK", clients.len());

    Ok((clients, dos_c_version))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dos_state_basic() {
        let mut dos = DosState::new();

        let mut clients = HashMap::new();
        clients.insert(
            "alice".to_string(),
            DosClient {
                client_name: "alice".to_string(),
                client_ip: "10.0.0.1".to_string(),
                client_port: 9080,
                images: vec!["secret.png".to_string(), "photo.jpg".to_string()],
                last_seen: 12345,
                online: true,
            },
        );

        dos.update(clients, 1);

        assert_eq!(dos.get_version(), 1);
        assert!(dos.is_online("alice"));
        assert_eq!(dos.list_images("alice").unwrap().len(), 2);
        assert_eq!(dos.list_online_users().len(), 1);
    }
}

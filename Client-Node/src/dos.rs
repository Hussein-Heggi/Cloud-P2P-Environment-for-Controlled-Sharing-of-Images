//! DOS (Directory of Service) Management
//! Handles local caching of DOS-C (client directory) and provides query methods

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// DOS Client entry - P2P v3.0 format (name + IP + port + actual_images)
/// NEW: Includes IP and port for direct P2P connections
/// Excludes: last_seen, online (kept server-side only in DOS-S)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DosClient {
    pub client_name: String,
    /// NEW P2P: Client IP address for direct connection
    pub client_ip: String,
    /// NEW P2P: Client listen port for P2P requests
    pub client_port: u16,
    /// Actual images only (no cover image)
    pub images: Vec<String>,
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
        println!("[DOS DEBUG] Updating DOS state...");
        println!("[DOS DEBUG] Previous version: {}, new version: {}", self.version, version);
        println!("[DOS DEBUG] Previous client count: {}, new client count: {}", self.clients.len(), clients.len());

        for (name, client) in &clients {
            println!("[DOS DEBUG] New client in update: {} ({} images)",
                     name, client.images.len());
        }

        self.clients = clients;
        self.version = version;
        println!("[DOS] Updated to version {} with {} clients", version, self.clients.len());

        println!("[DOS DEBUG] After update, clients in state:");
        for (name, client) in &self.clients {
            println!("[DOS DEBUG]   - {} ({} images)",
                     name, client.images.len());
        }
    }

    /// Get all users (online status not tracked in minimal DOS-C v2.0)
    #[allow(dead_code)]
    pub fn list_all_users(&self) -> Vec<String> {
        self.clients
            .keys()
            .cloned()
            .collect()
    }

    /// Get all images for a specific user
    pub fn list_images(&self, username: &str) -> Option<Vec<String>> {
        self.clients.get(username).map(|client| client.images.clone())
    }

    /// Check if a user exists (online status not tracked in minimal DOS-C v2.0)
    #[allow(dead_code)]
    pub fn user_exists(&self, username: &str) -> bool {
        self.clients.contains_key(username)
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

/// Parse P2P DOS-C v3.0 from JOIN_ACK payload
/// Format: [dos_c_version:u64][num_clients:u32][client_entries...]
/// Each client entry (P2P): [name_len:u16][name][ip_len:u16][ip][port:u16][num_images:u32][image_entries...]
/// NEW: Includes IP and port for P2P connections
/// Excluded: last_seen, online, cover_image (kept server-side)
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

    println!("[DOS] Parsing P2P DOS-C v3.0: version={} num_clients={}", dos_c_version, num_clients);

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

        // NEW P2P: Parse client IP
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

        // NEW P2P: Parse client port
        if offset + 2 > payload.len() {
            return Err(anyhow::anyhow!("Unexpected end at port for client {}", i));
        }

        let client_port = u16::from_le_bytes(payload[offset..offset + 2].try_into()?);
        offset += 2;

        // Parse number of images (actual images only, no cover)
        if offset + 4 > payload.len() {
            return Err(anyhow::anyhow!("Unexpected end at num_images for client {}", i));
        }

        let num_images = u32::from_le_bytes(payload[offset..offset + 4].try_into()?) as usize;
        offset += 4;

        // Parse image list (actual images only)
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

        let dos_client = DosClient {
            client_name: client_name.clone(),
            client_ip: client_ip.clone(),
            client_port,
            images,
        };

        println!("[DOS] Parsed client: {} ({}:{}, {} actual images)",
                 client_name, client_ip, client_port, dos_client.images.len());

        clients.insert(client_name, dos_client);
    }

    println!("[DOS] Successfully parsed {} clients from P2P DOS-C v3.0", clients.len());

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
                client_ip: "192.168.1.100".to_string(),
                client_port: 9080,
                images: vec!["secret.png".to_string(), "photo.jpg".to_string()],
            },
        );

        dos.update(clients, 1);

        assert_eq!(dos.get_version(), 1);
        assert_eq!(dos.list_images("alice").unwrap().len(), 2);
        assert_eq!(dos.get_client("alice").unwrap().client_ip, "192.168.1.100");
        assert_eq!(dos.get_client("alice").unwrap().client_port, 9080);
    }
}

//! Viewer operations for requesting and downloading images from owners

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::sync::RwLock;

use crate::protocol::*;
use crate::simple_client::SharedClientState;

/// Pending view request (viewer side)
#[derive(Debug, Clone)]
pub struct PendingRequest {
    pub request_id: u32,
    pub owner: String,
    pub image_name: String,
    pub requested_views: u32,
    pub status: RequestStatus,
    pub timestamp: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RequestStatus {
    Pending,
    Approved,
    Rejected,
}

// ============================================================================
// Phase 4A: ViewerAccessMap - Local tracking of remaining view counts
// ============================================================================

/// Access grant entry in viewer's local map
/// Tracks remaining views for received encrypted images
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewerAccessGrant {
    pub owner: String,
    pub image_name: String,
    pub remaining_views: u32,
    pub received_at: u64,              // Timestamp when first received
    pub encrypted_path: String,        // Path to encrypted image in encrypted_storage/
}

/// Viewer-side access map - tracks remaining views for received encrypted images
/// Saved to ~/.p2p_client/viewer_access_map.json
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ViewerAccessMap {
    /// Map key: "owner_imagename" -> grant
    pub grants: HashMap<String, ViewerAccessGrant>,
}

impl ViewerAccessMap {
    /// Create new empty viewer access map
    pub fn new() -> Self {
        Self {
            grants: HashMap::new(),
        }
    }

    /// Add or update a grant when receiving encrypted image from owner
    /// Returns true if this is a new grant, false if updating existing
    pub fn add_grant(
        &mut self,
        owner: &str,
        image_name: &str,
        granted_views: u32,
        encrypted_path: &str,
    ) -> bool {
        let key = Self::make_key(owner, image_name);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let is_new = !self.grants.contains_key(&key);

        let grant = ViewerAccessGrant {
            owner: owner.to_string(),
            image_name: image_name.to_string(),
            remaining_views: granted_views,
            received_at: now,
            encrypted_path: encrypted_path.to_string(),
        };

        self.grants.insert(key, grant);
        is_new
    }

    /// Get remaining views for an image (returns 0 if not found)
    pub fn get_remaining_views(&self, owner: &str, image_name: &str) -> u32 {
        let key = Self::make_key(owner, image_name);
        self.grants
            .get(&key)
            .map(|g| g.remaining_views)
            .unwrap_or(0)
    }

    /// Decrement view count for an image
    /// Returns true if successful, false if no views remaining or grant not found
    pub fn decrement_view(&mut self, owner: &str, image_name: &str) -> bool {
        let key = Self::make_key(owner, image_name);
        if let Some(grant) = self.grants.get_mut(&key) {
            if grant.remaining_views > 0 {
                grant.remaining_views -= 1;
                return true;
            }
        }
        false
    }

    /// Get grant entry (for display purposes)
    pub fn get_grant(&self, owner: &str, image_name: &str) -> Option<&ViewerAccessGrant> {
        let key = Self::make_key(owner, image_name);
        self.grants.get(&key)
    }

    /// List all grants with remaining views > 0
    pub fn list_available(&self) -> Vec<&ViewerAccessGrant> {
        self.grants
            .values()
            .filter(|g| g.remaining_views > 0)
            .collect()
    }

    /// Remove grant (when views exhausted or revoked)
    pub fn remove_grant(&mut self, owner: &str, image_name: &str) -> bool {
        let key = Self::make_key(owner, image_name);
        self.grants.remove(&key).is_some()
    }

    /// Save to JSON file
    pub async fn save_to_file(&self, path: &Path) -> Result<()> {
        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let json = serde_json::to_string_pretty(self)?;
        tokio::fs::write(path, json).await?;
        Ok(())
    }

    /// Load from JSON file (returns empty map if file doesn't exist)
    pub async fn load_from_file(path: &Path) -> Result<Self> {
        if !tokio::fs::metadata(path).await.is_ok() {
            // File doesn't exist, return empty map
            return Ok(Self::new());
        }

        let content = tokio::fs::read_to_string(path).await?;
        let map: ViewerAccessMap = serde_json::from_str(&content)?;
        Ok(map)
    }

    /// Get default path: ~/.p2p_client/viewer_access_map.json
    pub fn default_path() -> Result<PathBuf> {
        let home = std::env::var("HOME")
            .context("HOME environment variable not set")?;
        let mut path = PathBuf::from(home);
        path.push(".p2p_client");
        path.push("viewer_access_map.json");
        Ok(path)
    }

    /// Helper: Generate map key from owner and image name
    fn make_key(owner: &str, image_name: &str) -> String {
        format!("{}_{}", owner, image_name)
    }
}

/// Send VIEW_REQUEST to server
/// Format: [req_id:u32][viewer_len:u16][viewer][owner_len:u16][owner][image_len:u16][image][requested_views:u32]
pub async fn send_view_request(
    state: SharedClientState,
    writer: Arc<tokio::sync::Mutex<tokio::net::tcp::OwnedWriteHalf>>,
    owner: String,
    image_name: String,
    requested_views: u32,
) -> Result<u32> {
    // Generate unique request ID
    let request_id = rand::random::<u32>();

    let viewer = {
        let s = state.read().await;
        s.username.clone()
    };

    println!("[VIEWER] Sending VIEW_REQUEST: req_id={}, owner={}, image={}, views={}",
             request_id, owner, image_name, requested_views);

    // Build payload
    let mut payload = Vec::new();

    // Request ID
    payload.extend(request_id.to_le_bytes());

    // Viewer name
    let viewer_bytes = viewer.as_bytes();
    payload.extend((viewer_bytes.len() as u16).to_le_bytes());
    payload.extend_from_slice(viewer_bytes);

    // Owner name
    let owner_bytes = owner.as_bytes();
    payload.extend((owner_bytes.len() as u16).to_le_bytes());
    payload.extend_from_slice(owner_bytes);

    // Image name
    let image_bytes = image_name.as_bytes();
    payload.extend((image_bytes.len() as u16).to_le_bytes());
    payload.extend_from_slice(image_bytes);

    // Requested views
    payload.extend(requested_views.to_le_bytes());

    // Send VIEW_REQUEST
    {
        let mut w = writer.lock().await;
        w.write_all(&(payload.len() as u32 + 1).to_le_bytes()).await?;
        w.write_u8(VIEW_REQUEST).await?;
        w.write_all(&payload).await?;
        w.flush().await?;
    }

    println!("[VIEWER] VIEW_REQUEST sent (req_id={})", request_id);

    // Store pending request
    // Note: In full implementation, this would be stored in ClientState
    // For now, we just return the request_id

    Ok(request_id)
}

// ============================================================================
// Phase 5A: Smart Request Routing (P2P if online, Server if offline)
// ============================================================================

/// Smart request routing: Try P2P if owner online, fallback to server if offline
/// This is the main entry point for requesting image access
pub async fn request_image_access(
    state: SharedClientState,
    writer: Arc<tokio::sync::Mutex<tokio::net::tcp::OwnedWriteHalf>>,
    owner: &str,
    image_name: &str,
    requested_views: u32,
) -> Result<u32> {
    // Check DOS for owner's online status and connection info
    let (owner_online, owner_ip, owner_port) = {
        let s = state.read().await;

        // Find owner in DOS-C
        if let Some(owner_client) = s.dos.clients.get(owner) {
            println!("[DEBUG] Owner '{}' in DOS-C: online={}, ip={}, port={}",
                     owner, owner_client.online, owner_client.client_ip, owner_client.client_port);
            (owner_client.online, owner_client.client_ip.clone(), owner_client.client_port)
        } else {
            return Err(anyhow::anyhow!("Owner '{}' not found in DOS-C", owner));
        }
    };

    if owner_online {
        // Owner is online → Use P2P direct connection
        println!("[REQUEST] 🔵 Owner {} is ONLINE - attempting P2P to {}:{}",
                 owner, owner_ip, owner_port);
        send_peer_view_request(state, owner, image_name, requested_views).await
    } else {
        // Owner is offline → Use server-mediated flow
        println!("[REQUEST] 🔴 Owner {} is OFFLINE - using server-mediated flow", owner);
        send_view_request(
            state,
            writer,
            owner.to_string(),
            image_name.to_string(),
            requested_views,
        ).await
    }
}

// ============================================================================
// Phase 3A: P2P Direct View Request
// ============================================================================

/// Send PEER_VIEW_REQUEST directly to owner's P2P port (TCP)
/// This is the NEW P2P architecture - viewer connects directly to owner
/// Returns request_id if owner is online, or error if offline/not found
pub async fn send_peer_view_request(
    state: SharedClientState,
    owner: &str,
    image_name: &str,
    requested_views: u32,
) -> Result<u32> {
    // Check DOS for owner's online status and connection info
    let (owner_online, owner_ip, owner_port) = {
        let s = state.read().await;

        // Find owner in DOS-C
        let owner_client = s.dos.clients.get(owner)
            .ok_or_else(|| anyhow::anyhow!("Owner '{}' not found in DOS-C", owner))?;

        // Check if owner has the requested image
        if !owner_client.images.contains(&image_name.to_string()) {
            return Err(anyhow::anyhow!(
                "Image '{}' not found in owner '{}' DOS entry",
                image_name, owner
            ));
        }

        (owner_client.online, owner_client.client_ip.clone(), owner_client.client_port)
    };

    // Check if owner is online
    if !owner_online {
        return Err(anyhow::anyhow!(
            "Owner '{}' is offline (online=false in DOS-C). Use offline request flow instead.",
            owner
        ));
    }

    println!("[P2P-VIEWER] 🔵 Owner {} is online at {}:{}", owner, owner_ip, owner_port);

    // Get viewer username
    let viewer = {
        let s = state.read().await;
        s.username.clone()
    };

    // Generate unique request ID
    let request_id = rand::random::<u32>();

    println!("[P2P-VIEWER] 🔌 Attempting TCP connection to {}:{}...", owner_ip, owner_port);

    // Establish TCP connection to owner's P2P port
    let mut stream = match tokio::net::TcpStream::connect(format!("{}:{}", owner_ip, owner_port)).await {
        Ok(s) => {
            println!("[P2P-VIEWER] ✅ TCP connection established to {}:{}", owner_ip, owner_port);
            s
        }
        Err(e) => {
            println!("[P2P-VIEWER] ❌ TCP connection FAILED to {}:{}", owner_ip, owner_port);
            println!("[P2P-VIEWER] ❌ Error details: {}", e);
            return Err(anyhow::anyhow!("Failed to connect to owner's P2P server at {}:{}: {}", owner_ip, owner_port, e));
        }
    };

    // Build PEER_VIEW_REQUEST payload
    // Wire format: [viewer_len:u16][viewer][image_name_len:u16][image_name][requested_views:u32]
    let mut payload = Vec::new();

    // Viewer name
    let viewer_bytes = viewer.as_bytes();
    payload.extend((viewer_bytes.len() as u16).to_le_bytes());
    payload.extend_from_slice(viewer_bytes);

    // Image name
    let image_bytes = image_name.as_bytes();
    payload.extend((image_bytes.len() as u16).to_le_bytes());
    payload.extend_from_slice(image_bytes);

    // Requested views
    payload.extend(requested_views.to_le_bytes());

    // Send message with length prefix: [total_len:u32][msg_type:u8][payload]
    let total_len = 1 + payload.len(); // msg_type (1 byte) + payload
    stream.write_all(&(total_len as u32).to_le_bytes()).await?;
    stream.write_u8(PEER_VIEW_REQUEST).await?;
    stream.write_all(&payload).await?;
    stream.flush().await?;

    println!("[P2P-VIEWER] 📤 Sent PEER_VIEW_REQUEST to {}: image='{}', views={}",
             owner, image_name, requested_views);

    // Note: Connection stays open to receive response
    // The response will be handled by the P2P listener (handle_peer_view_response)
    // For now, we just send the request and return

    // TODO Phase 3D: Store connection and request_id for response handling
    // For now, just close the connection after sending
    drop(stream);

    Ok(request_id)
}

/// Download and save an image from IMAGE_CHUNK messages
/// Returns the path to the saved embedded PNG file
pub async fn save_received_image(
    owner: &str,
    image_name: &str,
    chunks: &HashMap<u32, Vec<u8>>,
    total_chunks: u32,
) -> Result<PathBuf> {
    // Ensure all chunks received
    if chunks.len() != total_chunks as usize {
        return Err(anyhow::anyhow!(
            "Incomplete image: got {}/{} chunks",
            chunks.len(),
            total_chunks
        ));
    }

    // Assemble chunks in order
    let mut full_image = Vec::new();
    for i in 0..total_chunks {
        let chunk = chunks.get(&i)
            .ok_or_else(|| anyhow::anyhow!("Missing chunk {}", i))?;
        full_image.extend_from_slice(chunk);
    }

    // Create downloads directory if it doesn't exist
    let downloads_dir = Path::new("downloads");
    fs::create_dir_all(downloads_dir).await?;

    // Save to downloads/<owner>_<image>_embedded.png
    let filename = format!("{}_{}_embedded.png", owner, image_name.replace(".png", "").replace(".jpg", ""));
    let file_path = downloads_dir.join(&filename);

    fs::write(&file_path, &full_image).await
        .context("Failed to write embedded image")?;

    println!("[VIEWER] ✅ Downloaded embedded image: {} ({} bytes)", file_path.display(), full_image.len());

    Ok(file_path)
}

/// Downloaded image metadata
#[derive(Debug, Clone)]
pub struct DownloadedImage {
    pub owner: String,
    pub image_name: String,
    pub embedded_path: PathBuf,
    pub extracted_path: Option<PathBuf>,
    pub metadata: Option<String>, // JSON metadata if extracted
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pending_request() {
        let req = PendingRequest {
            request_id: 123,
            owner: "alice".to_string(),
            image_name: "secret.png".to_string(),
            requested_views: 5,
            status: RequestStatus::Pending,
            timestamp: 1000,
        };

        assert_eq!(req.status, RequestStatus::Pending);
        assert_eq!(req.requested_views, 5);
    }

    #[test]
    fn test_viewer_access_map_add_grant() {
        let mut map = ViewerAccessMap::new();

        let is_new = map.add_grant("alice", "secret.png", 5, "encrypted_storage/secret.png");
        assert!(is_new);

        // Adding again should return false (updating existing)
        let is_new = map.add_grant("alice", "secret.png", 3, "encrypted_storage/secret.png");
        assert!(!is_new);

        // Check remaining views (should be updated to 3)
        assert_eq!(map.get_remaining_views("alice", "secret.png"), 3);
    }

    #[test]
    fn test_viewer_access_map_decrement() {
        let mut map = ViewerAccessMap::new();
        map.add_grant("alice", "secret.png", 3, "encrypted_storage/secret.png");

        // Decrement should succeed
        assert!(map.decrement_view("alice", "secret.png"));
        assert_eq!(map.get_remaining_views("alice", "secret.png"), 2);

        // Decrement twice more
        assert!(map.decrement_view("alice", "secret.png"));
        assert!(map.decrement_view("alice", "secret.png"));
        assert_eq!(map.get_remaining_views("alice", "secret.png"), 0);

        // Decrement when 0 should fail
        assert!(!map.decrement_view("alice", "secret.png"));
    }

    #[test]
    fn test_viewer_access_map_list_available() {
        let mut map = ViewerAccessMap::new();
        map.add_grant("alice", "secret1.png", 5, "encrypted_storage/secret1.png");
        map.add_grant("bob", "secret2.png", 0, "encrypted_storage/secret2.png");
        map.add_grant("charlie", "secret3.png", 3, "encrypted_storage/secret3.png");

        let available = map.list_available();
        assert_eq!(available.len(), 2);  // Only alice and charlie have views > 0
    }

    #[test]
    fn test_viewer_access_map_remove_grant() {
        let mut map = ViewerAccessMap::new();
        map.add_grant("alice", "secret.png", 5, "encrypted_storage/secret.png");

        assert!(map.remove_grant("alice", "secret.png"));
        assert_eq!(map.get_remaining_views("alice", "secret.png"), 0);

        // Removing again should return false
        assert!(!map.remove_grant("alice", "secret.png"));
    }

    #[tokio::test]
    async fn test_viewer_access_map_save_load() {
        use std::path::PathBuf;

        let mut map = ViewerAccessMap::new();
        map.add_grant("alice", "secret.png", 5, "encrypted_storage/secret.png");
        map.add_grant("bob", "data.png", 3, "encrypted_storage/data.png");

        // Save to temp file
        let temp_path = PathBuf::from("/tmp/test_viewer_access_map.json");
        map.save_to_file(&temp_path).await.unwrap();

        // Load back
        let loaded = ViewerAccessMap::load_from_file(&temp_path).await.unwrap();

        assert_eq!(loaded.get_remaining_views("alice", "secret.png"), 5);
        assert_eq!(loaded.get_remaining_views("bob", "data.png"), 3);

        // Cleanup
        tokio::fs::remove_file(&temp_path).await.ok();
    }
}

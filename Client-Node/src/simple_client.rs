//! Simple client implementation for testing the new protocol
//! Supports JOIN and PING operations
//! Uses TCP for reliable communication with the server

use anyhow::{Context, Result};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::signal;
use tokio::net::TcpStream;
use tokio::sync::RwLock;

use crate::protocol::*;
use crate::dos::DosState;
use crate::viewer::{PendingRequest, DownloadedImage};
use crate::owner::PendingViewRequest;
use crate::local_access_map::LocalAccessMap;

/// Normalize image name by stripping .png extension for logical operations
/// Physical file operations should use the full name with extension
fn normalize_image_name(name: &str) -> String {
    if name.ends_with(".png") {
        name.trim_end_matches(".png").to_string()
    } else {
        name.to_string()
    }
}

/// Fixed client listen/advertised port for TCP (starting port for P2P range)
pub const CLIENT_PORT: u16 = 9080;

/// Pending P2P image download (viewer side)
#[derive(Debug, Clone)]
pub struct PendingImageDownload {
    pub owner: String,
    pub image_name: String,
    pub granted_views: u32,
    pub chunks: HashMap<u32, Vec<u8>>,
    pub total_chunks: Option<u32>,
}

impl PendingImageDownload {
    pub fn new(owner: String, image_name: String, granted_views: u32) -> Self {
        Self {
            owner,
            image_name,
            granted_views,
            chunks: HashMap::new(),
            total_chunks: None,
        }
    }

    /// Check if all chunks have been received
    pub fn is_complete(&self) -> bool {
        if let Some(total) = self.total_chunks {
            self.chunks.len() == total as usize
        } else {
            false
        }
    }

    /// Get key for HashMap lookup
    pub fn make_key(owner: &str, image_name: &str) -> String {
        format!("{}_{}", owner, image_name)
    }
}

/// Pending adjust request from viewer (viewer side)
#[derive(Debug, Clone)]
pub struct PendingAdjustRequest {
    pub request_id: u32,
    pub owner: String,
    pub image_name: String,
    pub requested_views: u32,
    pub timestamp: u64,
}

/// Incoming adjust request from viewer (owner side)
#[derive(Debug, Clone)]
pub struct IncomingAdjustRequest {
    pub request_id: u32,
    pub viewer: String,
    pub image_name: String,
    pub requested_views: u32,
    pub current_views: u32,
    pub peer_addr: SocketAddr,
    pub timestamp: u64,
}

/// Client state
#[derive(Clone)]
pub struct ClientState {
    pub username: String,
    pub server_addr: SocketAddr,
    pub client_port: u16,  // Port registered with server (for backward compatibility)
    /// NEW P2P: Actual P2P listen port (may differ from client_port due to port range binding)
    pub p2p_port: u16,
    /// Cover image (first image in CLI args) - used for steganography, NOT shared in DOS-C
    pub cover_image: Option<String>,
    /// Actual images (remaining CLI args) - shareable images shown in DOS-C
    pub actual_images: Vec<String>,
    pub joined: bool,
    pub dos_version: u32,
    pub dos: DosState,
    // Viewer side: track my outgoing requests
    pub my_requests: HashMap<u32, PendingRequest>,
    // Owner side: track incoming view requests
    pub pending_view_requests: HashMap<u32, PendingViewRequest>,
    // Track downloaded images
    pub downloads: Vec<DownloadedImage>,
    /// NEW P2P: Local access map for owner permissions
    pub local_access_map: LocalAccessMap,
    /// NEW P2P Phase 3D: Track in-progress P2P image downloads (viewer side)
    /// Key: "owner_imagename"
    pub pending_downloads: HashMap<String, PendingImageDownload>,
    /// NEW: Track viewer's pending adjust requests (viewer side)
    pub pending_adjust_requests: HashMap<u32, PendingAdjustRequest>,
    /// NEW: Track owner's incoming adjust requests from viewers (owner side)
    pub incoming_adjust_requests: HashMap<u32, IncomingAdjustRequest>,
}

impl ClientState {
    pub fn new(username: String, server_addr: SocketAddr) -> Self {
        Self {
            username,
            server_addr,
            client_port: 0,
            p2p_port: 0,  // Will be set when P2P listener starts
            cover_image: None,
            actual_images: Vec::new(),
            joined: false,
            dos_version: 0,
            dos: DosState::new(),
            my_requests: HashMap::new(),
            pending_view_requests: HashMap::new(),
            downloads: Vec::new(),
            local_access_map: LocalAccessMap::new(),
            pending_downloads: HashMap::new(),
            pending_adjust_requests: HashMap::new(),
            incoming_adjust_requests: HashMap::new(),
        }
    }

    /// Set images from command line (first = cover if multiple, rest = actual)
    pub fn set_images(&mut self, images: Vec<String>) {
        // Normalize all image names (strip .png)
        let normalized_images: Vec<String> = images.iter()
            .map(|name| normalize_image_name(name))
            .collect();

        if normalized_images.len() > 1 {
            // Multiple images: first = cover, rest = actual
            self.cover_image = Some(normalized_images[0].clone());
            self.actual_images = normalized_images[1..].to_vec();
            println!("[CLIENT] Cover image: {} (normalized)", normalized_images[0]);
            println!("[CLIENT] Actual images (normalized): {:?}", self.actual_images);
        } else if normalized_images.len() == 1 {
            // Single image: actual only, no cover
            self.cover_image = None;
            self.actual_images = normalized_images;
            println!("[CLIENT] Actual images (normalized): {:?}", self.actual_images);
            println!("[CLIENT] No cover image (single image provided)");
        } else {
            // No images
            self.cover_image = None;
            self.actual_images = Vec::new();
            println!("[CLIENT] No images");
        }
    }

    /// Get all images (cover + actual) for JOIN message
    pub fn get_all_images_for_join(&self) -> Vec<String> {
        let mut all_images = Vec::new();
        if let Some(ref cover) = self.cover_image {
            all_images.push(cover.clone());
        }
        all_images.extend(self.actual_images.clone());
        all_images
    }
}

pub type SharedClientState = Arc<RwLock<ClientState>>;

#[derive(Default)]
struct IncomingImage {
    total_chunks: u32,
    chunks: HashMap<u32, Vec<u8>>,
}

async fn send_tcp_message_generic<W: AsyncWrite + Unpin>(
    stream: &mut W,
    msg_type: u8,
    payload: &[u8],
) -> Result<()> {
    let total_len = 1 + payload.len();

    // Write length prefix
    stream.write_all(&(total_len as u32).to_le_bytes()).await?;

    // Write message type + payload
    stream.write_u8(msg_type).await?;
    stream.write_all(payload).await?;
    stream.flush().await?;

    Ok(())
}

async fn recv_tcp_message_generic<R: AsyncRead + Unpin>(
    stream: &mut R,
) -> Result<(u8, Vec<u8>)> {
    use std::sync::atomic::{AtomicU64, Ordering};
    static CALL_COUNTER: AtomicU64 = AtomicU64::new(0);
    let call_num = CALL_COUNTER.fetch_add(1, Ordering::SeqCst);

    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("[CLIENT-RECV-DEBUG] 📞 recv_tcp_message_generic called (call #{})", call_num);
    println!("[CLIENT-RECV-DEBUG] ⏳ About to read 4-byte length prefix...");

    // Read length prefix
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let total_len = u32::from_le_bytes(len_buf) as usize;

    // DIAGNOSTIC: Log length prefix bytes
    println!(
        "[CLIENT-RECV-DEBUG] ✅ Length prefix read successfully!"
    );
    println!(
        "[CLIENT-RECV-DEBUG] 📏 Raw bytes: {:02x?} -> decoded length: {}",
        len_buf,
        total_len
    );

    if total_len == 0 || total_len > 65536 {
        println!("[CLIENT-RECV-DEBUG] ❌ ERROR: Invalid message length: {}", total_len);
        println!("[CLIENT-RECV-DEBUG] 🔍 This suggests TCP stream desynchronization!");
        println!("[CLIENT-RECV-DEBUG] 🔍 Length bytes interpreted as: {:02x?}", len_buf);
        println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        return Err(anyhow::anyhow!("Invalid message length: {}", total_len));
    }

    println!("[CLIENT-RECV-DEBUG] ⏳ Now reading {} bytes of message data...", total_len);

    // Read message data
    let mut buf = vec![0u8; total_len];
    stream.read_exact(&mut buf).await?;

    println!("[CLIENT-RECV-DEBUG] ✅ Message data read successfully!");

    if buf.is_empty() {
        return Err(anyhow::anyhow!("Empty message"));
    }

    let msg_type = buf[0];
    let payload = buf[1..].to_vec();

    // DIAGNOSTIC: Log first 16 bytes of message for debugging
    let preview_len = std::cmp::min(16, buf.len());
    println!(
        "[CLIENT-RECV-DEBUG] 📦 Message details: len={} msg_type={} payload_len={}",
        total_len,
        msg_type,
        payload.len()
    );
    println!(
        "[CLIENT-RECV-DEBUG] 🔍 First {} bytes: {:02x?}",
        preview_len,
        &buf[..preview_len]
    );
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    Ok((msg_type, payload))
}

/// Send CLIENT_LEAVE message to server for graceful shutdown
pub async fn send_client_leave(
    username: &str,
    writer: Arc<tokio::sync::Mutex<tokio::net::tcp::OwnedWriteHalf>>,
) -> Result<()> {
    println!("[CLIENT] Sending CLIENT_LEAVE for graceful shutdown...");

    // Build payload: [username_len:u16][username]
    let mut payload = Vec::new();
    payload.extend((username.len() as u16).to_le_bytes());
    payload.extend(username.as_bytes());

    // Send CLIENT_LEAVE message
    let mut w = writer.lock().await;
    send_tcp_message_generic(&mut *w, CLIENT_LEAVE, &payload).await?;

    println!("[CLIENT] ✅ CLIENT_LEAVE sent to server");
    Ok(())
}

/// Send JOIN message to server and wait for JOIN_ACK
pub async fn join_server(
    state: SharedClientState,
    writer: Arc<tokio::sync::Mutex<tokio::net::tcp::OwnedWriteHalf>>,
    reader: &mut tokio::net::tcp::OwnedReadHalf,
) -> Result<()> {
    let (username, server_addr, images) = {
        let s = state.read().await;
        (s.username.clone(), s.server_addr, s.get_all_images_for_join())
    };

    println!("[CLIENT] Sending JOIN to server {}...", server_addr);

    // Read the P2P port from state (set by P2P listener before JOIN)
    let local_port = {
        let s = state.read().await;
        s.client_port
    };

    // Build JOIN message payload
    // Format: [username_len:u16][username][port:u16][num_images:u32][image_names...]
    let mut payload = Vec::new();

    let username_bytes = username.as_bytes();
    payload.extend((username_bytes.len() as u16).to_le_bytes());
    payload.extend_from_slice(username_bytes);

    println!("[CLIENT-DEBUG] About to add port: local_port={}", local_port);
    payload.extend(local_port.to_le_bytes());
    println!("[CLIENT-DEBUG] After adding port, payload.len()={}", payload.len());

    payload.extend((images.len() as u32).to_le_bytes());
    println!("[CLIENT-DEBUG] After adding num_images={}, payload.len()={}", images.len(), payload.len());

    for image in &images {
        let image_bytes = image.as_bytes();
        payload.extend((image_bytes.len() as u16).to_le_bytes());
        payload.extend_from_slice(image_bytes);
        println!("[CLIENT-DEBUG] After adding image '{}' (len={}), payload.len()={}", image, image_bytes.len(), payload.len());
    }

    // Print complete message as hex for debugging
    println!("[CLIENT-DEBUG] Complete JOIN payload ({} bytes):", payload.len());
    println!("[CLIENT-DEBUG] Hex: {}", payload.iter().map(|b| format!("{:02x}", b)).collect::<Vec<_>>().join(" "));

    // Send JOIN via TCP
    {
        let mut s = writer.lock().await;
        send_tcp_message_generic(&mut *s, JOIN, &payload).await?;
    }
    println!("[CLIENT] JOIN sent ({} bytes), waiting for JOIN_ACK...", payload.len());

    // Wait for JOIN_ACK with timeout
    let timeout = Duration::from_secs(5);

    let result = {
        tokio::time::timeout(timeout, recv_tcp_message_generic(reader)).await
    };

    match result {
        Ok(Ok((msg_type, payload))) => {
            if msg_type == JOIN_ACK {
                println!("[CLIENT] ✅ JOIN_ACK received from server");

                // Parse DOS-C from payload
                match crate::dos::parse_dos_c_from_join_ack(&payload) {
                    Ok((clients, dos_version)) => {
                        println!("[CLIENT] Parsed DOS-C: {} clients, version {}", clients.len(), dos_version);

                        // Update state with DOS-C
                        let mut s = state.write().await;
                        s.joined = true;
                        s.dos.update(clients, dos_version);
                        s.dos_version = dos_version as u32;

                        println!("[CLIENT] DOS-C updated successfully");
                    }
                    Err(e) => {
                        println!("[CLIENT] ⚠️  Failed to parse DOS-C from JOIN_ACK: {}", e);
                        // Still mark as joined, but DOS will be empty
                        let mut s = state.write().await;
                        s.joined = true;
                    }
                }

                Ok(())
            } else {
                Err(anyhow::anyhow!("Unexpected response from server: msg_type={}", msg_type))
            }
        }
        Ok(Err(e)) => {
            println!("[CLIENT] ⚠️ Receive error while waiting for JOIN_ACK: {}", e);
            Err(anyhow::anyhow!("Receive error: {}", e))
        }
        Err(_) => {
            println!("[CLIENT] ⚠️ Timeout waiting for JOIN_ACK (no bytes received during wait)");
            Err(anyhow::anyhow!("Timeout waiting for JOIN_ACK"))
        }
    }
}

/// Send periodic CLIENT_PING to server
pub async fn ping_loop(
    state: SharedClientState,
    writer: Arc<tokio::sync::Mutex<tokio::net::tcp::OwnedWriteHalf>>,
) -> Result<()> {
    loop {
        // Wait 10 seconds
        tokio::time::sleep(Duration::from_secs(10)).await;

        let (joined, username) = {
            let s = state.read().await;
            (s.joined, s.username.clone())
        };

        if !joined {
            println!("[CLIENT] Not joined yet, skipping ping");
            continue;
        }

        // Build CLIENT_PING message payload
        // Format: [username_len:u16][username]
        let mut payload = Vec::new();
        let username_bytes = username.as_bytes();
        payload.extend((username_bytes.len() as u16).to_le_bytes());
        payload.extend_from_slice(username_bytes);

        // Send PING via TCP
        {
            let mut s = writer.lock().await;
            if let Err(e) = send_tcp_message_generic(&mut *s, CLIENT_PING, &payload).await {
                println!("[CLIENT] ⚠️  Failed to send PING: {}", e);
                continue;
            }
        }

        println!("[CLIENT] PING sent to server");
    }
}

/// Update encrypted image index file
/// Maintains a JSON index of all encrypted images received from server
async fn update_encrypted_image_index(
    image_name: &str,
    path: &str,
    size_bytes: usize,
) -> Result<()> {
    use serde::{Serialize, Deserialize};
    use std::collections::HashMap;

    #[derive(Serialize, Deserialize)]
    struct ImageIndexEntry {
        path: String,
        uploaded_at: u64,
        size_bytes: usize,
    }

    let index_path = "encrypted_storage/encrypted_image_index.json";

    // Load existing index or create new
    let mut index: HashMap<String, ImageIndexEntry> = if tokio::fs::metadata(index_path).await.is_ok() {
        let content = tokio::fs::read_to_string(index_path).await?;
        serde_json::from_str(&content).unwrap_or_else(|_| HashMap::new())
    } else {
        HashMap::new()
    };

    // Add/update entry
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();

    index.insert(image_name.to_string(), ImageIndexEntry {
        path: path.to_string(),
        uploaded_at: now,
        size_bytes,
    });

    // Save back to file
    let json = serde_json::to_string_pretty(&index)?;
    tokio::fs::write(index_path, json).await?;

    println!("[INDEX] Updated encrypted image index: {} ({} bytes)", image_name, size_bytes);
    Ok(())
}

/// Run the client listener (receives messages from server)
pub async fn run_listener(
    state: SharedClientState,
    mut reader: tokio::net::tcp::OwnedReadHalf,
    writer: Arc<tokio::sync::Mutex<tokio::net::tcp::OwnedWriteHalf>>,
) -> Result<()> {
    println!("╔════════════════════════════════════════════════════════════════╗");
    println!("║ [CLIENT-LISTENER] 🚀 STARTED - Ready to receive messages      ║");
    println!("║ This listener will receive IMAGE_CHUNK and other messages     ║");
    println!("╚════════════════════════════════════════════════════════════════╝");

    #[derive(Default)]
    struct IncomingImage {
        total_chunks: u32,
        chunks: HashMap<u32, Vec<u8>>,
    }
    let mut images: HashMap<String, IncomingImage> = HashMap::new();
    let mut msg_count = 0u64;

    loop {
        println!("\n[CLIENT-LISTENER] 🔄 Waiting for message #{} from server...", msg_count + 1);
        let (msg_type, data) = match recv_tcp_message_generic(&mut reader).await {
            Ok(msg) => {
                msg_count += 1;
                println!("[CLIENT-LISTENER] ✅ Received message #{}", msg_count);
                msg
            },
            Err(e) => {
                println!("[CLIENT-LISTENER] ❌ Connection closed or error: {}", e);
                return Err(e);
            }
        };

        match msg_type {
            JOIN_ACK => {
                println!("[CLIENT-LISTENER] JOIN_ACK received from server");
                // Handled by join_server function
            }

            SERVER_PONG => {
                if data.len() >= 4 {
                    let dos_version = u32::from_le_bytes(data[0..4].try_into().unwrap());
                    let (old_version, username, version_changed) = {
                        let mut s = state.write().await;
                        let old_version = s.dos_version;
                        s.dos_version = dos_version;
                        let version_changed = dos_version != old_version;
                        (old_version, s.username.clone(), version_changed)
                    };

                    if version_changed {
                        println!("[CLIENT] ✅ PONG received - DOS version updated: {} -> {}", old_version, dos_version);
                        println!("[CLIENT] 🔄 Requesting updated DOS-C from server...");

                        // Send DOS_QUERY to get updated DOS-C
                        let mut payload = Vec::new();
                        let username_bytes = username.as_bytes();
                        payload.extend((username_bytes.len() as u16).to_le_bytes());
                        payload.extend_from_slice(username_bytes);

                        let mut w = writer.lock().await;
                        if let Err(e) = send_tcp_message_generic(&mut *w, DOS_QUERY, &payload).await {
                            println!("[CLIENT] ⚠️  Failed to send DOS_QUERY: {}", e);
                        } else {
                            println!("[CLIENT] ✅ DOS_QUERY sent");
                        }
                    }
                    // Silently accept PONGs with unchanged version (reduces spam from heartbeat)
                } else {
                    println!("[CLIENT-LISTENER] SERVER_PONG too short ({} bytes)", data.len());
                }
            }

            DOS_UPDATE => {
                println!("[CLIENT] 📥 DOS_UPDATE received from server ({} bytes)", data.len());

                // Parse MINIMAL DOS-C v2.0 (same format as JOIN_ACK)
                // Format: [dos_c_version:u64][num_clients:u32][client_entries...]
                match crate::dos::parse_dos_c_from_join_ack(&data) {
                    Ok((clients, dos_c_version)) => {
                        let mut s = state.write().await;
                        let my_username = s.username.clone();

                        // Check if our images are in the DOS and sync them to actual_images
                        if let Some(my_entry) = clients.iter().find(|(name, _)| *name == &my_username) {
                            let my_images = &my_entry.1.images;
                            if !my_images.is_empty() && my_images != &s.actual_images {
                                println!("[CLIENT] 🔄 Syncing own images from DOS: {:?}", my_images);
                                s.actual_images = my_images.clone();
                                println!("[CLIENT] ✅ Updated actual_images to match DOS");
                            }
                        }

                        s.dos.update(clients, dos_c_version);
                        println!("[CLIENT] ✅ DOS-C updated to version {} with {} clients",
                                 dos_c_version, s.dos.get_all_clients().len());

                        // Log all clients for debugging
                        for (name, client) in s.dos.get_all_clients() {
                            println!("[CLIENT]   - {} ({} images)", name, client.images.len());
                        }
                    }
                    Err(e) => {
                        println!("[CLIENT] ⚠️  Failed to parse DOS_UPDATE: {}", e);
                    }
                }
            }

            VIEW_NOTIFICATION => {
                // Parse: [viewer_name_len:u16][viewer_name][image_name_len:u16][image_name][req_id:u32][requested_views:u32]
                let mut offset = 0;

                if data.len() < offset + 2 {
                    println!("[CLIENT-LISTENER] Invalid VIEW_NOTIFICATION: too short");
                    continue;
                }

                let viewer_len = u16::from_le_bytes(data[offset..offset + 2].try_into()?) as usize;
                offset += 2;

                if data.len() < offset + viewer_len {
                    println!("[CLIENT-LISTENER] Invalid VIEW_NOTIFICATION: invalid viewer name length");
                    continue;
                }

                let viewer_name = String::from_utf8(data[offset..offset + viewer_len].to_vec())?;
                offset += viewer_len;

                if data.len() < offset + 2 {
                    println!("[CLIENT-LISTENER] Invalid VIEW_NOTIFICATION: no image name length");
                    continue;
                }

                let image_len = u16::from_le_bytes(data[offset..offset + 2].try_into()?) as usize;
                offset += 2;

                if data.len() < offset + image_len {
                    println!("[CLIENT-LISTENER] Invalid VIEW_NOTIFICATION: invalid image name length");
                    continue;
                }

                let image_name = String::from_utf8(data[offset..offset + image_len].to_vec())?;
                offset += image_len;

                if data.len() < offset + 4 {
                    println!("[CLIENT-LISTENER] Invalid VIEW_NOTIFICATION: no request ID");
                    continue;
                }

                let req_id = u32::from_le_bytes(data[offset..offset + 4].try_into()?);
                offset += 4;

                // Parse requested_views (if present in payload)
                let requested_views = if data.len() >= offset + 4 {
                    u32::from_le_bytes(data[offset..offset + 4].try_into()?)
                } else {
                    0 // Default if not provided
                };

                println!("[OWNER] VIEW_NOTIFICATION received: viewer={}, image={}, req_id={}, views={}",
                         viewer_name, image_name, req_id, requested_views);
                println!("\n📩 [VIEW NOTIFICATION] {} wants to view image: {}", viewer_name, image_name);
                println!("   Request ID: {}", req_id);
                println!("   Requested views: {}", requested_views);
                println!("   Status: ⏳ Pending owner approval (use HTTP API to approve/deny)");

                // Store in pending requests for owner to handle via web UI
                let pending_req = crate::owner::PendingViewRequest {
                    request_id: req_id,
                    viewer: viewer_name.clone(),
                    image_name: image_name.clone(),
                    requested_views,
                    peer_addr: "0.0.0.0:0".parse().unwrap(),  // DEPRECATED: Old server-mediated flow
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                };

                {
                    let mut s = state.write().await;
                    s.pending_view_requests.insert(req_id, pending_req);
                }

                println!("[OWNER] Stored pending request in state");
                println!("   💡 View pending requests via web UI at http://localhost:3000/dashboard");
            }

            PENDING_REQUEST => {
                // Phase 5B: Handle offline request delivery from server
                // Wire format: same as VIEW_NOTIFICATION
                // [viewer_name_len:u16][viewer_name][image_name_len:u16][image_name][req_id:u32][requested_views:u32]
                println!("[OWNER] 📬 PENDING_REQUEST received (offline request delivery)");

                let mut offset = 0;

                if data.len() < offset + 2 {
                    println!("[CLIENT-LISTENER] Invalid PENDING_REQUEST: too short");
                    continue;
                }

                let viewer_len = u16::from_le_bytes(data[offset..offset + 2].try_into()?) as usize;
                offset += 2;

                if data.len() < offset + viewer_len {
                    println!("[CLIENT-LISTENER] Invalid PENDING_REQUEST: invalid viewer name length");
                    continue;
                }

                let viewer_name = String::from_utf8(data[offset..offset + viewer_len].to_vec())?;
                offset += viewer_len;

                if data.len() < offset + 2 {
                    println!("[CLIENT-LISTENER] Invalid PENDING_REQUEST: no image name length");
                    continue;
                }

                let image_len = u16::from_le_bytes(data[offset..offset + 2].try_into()?) as usize;
                offset += 2;

                if data.len() < offset + image_len {
                    println!("[CLIENT-LISTENER] Invalid PENDING_REQUEST: invalid image name length");
                    continue;
                }

                let image_name = String::from_utf8(data[offset..offset + image_len].to_vec())?;
                offset += image_len;

                if data.len() < offset + 4 {
                    println!("[CLIENT-LISTENER] Invalid PENDING_REQUEST: no request ID");
                    continue;
                }

                let req_id = u32::from_le_bytes(data[offset..offset + 4].try_into()?);
                offset += 4;

                let requested_views = if data.len() >= offset + 4 {
                    u32::from_le_bytes(data[offset..offset + 4].try_into()?)
                } else {
                    0
                };

                println!("\n╔═══════════════════════════════════════════════════════════╗");
                println!("║  📬 OFFLINE REQUEST DELIVERED BY SERVER                  ║");
                println!("╠═══════════════════════════════════════════════════════════╣");
                println!("║  Request ID: {}                                    ║", req_id);
                println!("║  From:       {}                                          ║", viewer_name);
                println!("║  Image:      {}                                         ║", image_name);
                println!("║  Views:      {}                                              ║", requested_views);
                println!("╠═══════════════════════════════════════════════════════════╣");
                println!("║  This request was made while you were offline.            ║");
                println!("║  Use CLI commands to approve/deny:                        ║");
                println!("║    approve_p2p <request_id>                               ║");
                println!("║    deny_p2p <request_id>                                  ║");
                println!("╚═══════════════════════════════════════════════════════════╝\n");

                // Store in pending requests
                let pending_req = crate::owner::PendingViewRequest {
                    request_id: req_id,
                    viewer: viewer_name.clone(),
                    image_name: image_name.clone(),
                    requested_views,
                    peer_addr: "0.0.0.0:0".parse().unwrap(),  // Server-mediated, no peer addr
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                };

                {
                    let mut s = state.write().await;
                    s.pending_view_requests.insert(req_id, pending_req);
                }

                println!("[OWNER] Stored offline request in pending queue");
            }

            DOS_UPDATE => {
                println!("[CLIENT-LISTENER] DOS_UPDATE received - refreshing DOS-C");

                // Parse DOS-C from payload (same format as JOIN_ACK)
                match crate::dos::parse_dos_c_from_join_ack(&data) {
                    Ok((clients, dos_version)) => {
                        println!("[CLIENT] Parsed updated DOS-C: {} clients, version {}", clients.len(), dos_version);

                        // Update state with new DOS-C AND update own actual_images if we're in the DOS
                        let mut s = state.write().await;
                        let my_username = s.username.clone();

                        // Check if our images are in the DOS and sync them to actual_images
                        if let Some(my_entry) = clients.iter().find(|(name, _)| *name == &my_username) {
                            let my_images = &my_entry.1.images;
                            if !my_images.is_empty() && my_images != &s.actual_images {
                                println!("[CLIENT] 🔄 Syncing own images from DOS: {:?}", my_images);
                                s.actual_images = my_images.clone();
                                println!("[CLIENT] ✅ Updated actual_images to match DOS");
                            }
                        }

                        s.dos.update(clients, dos_version);
                        s.dos_version = dos_version as u32;

                        println!("[CLIENT] DOS-C refreshed successfully");
                    }
                    Err(e) => {
                        println!("[CLIENT] ⚠️  Failed to parse DOS-C from DOS_UPDATE: {}", e);
                    }
                }
            }

            REJECTED => {
                // Parse: [req_id:u32] or [req_id:u32][reason_len:u16][reason]
                if data.len() >= 4 {
                    let req_id = u32::from_le_bytes(data[0..4].try_into()?);
                    println!("[CLIENT-LISTENER] ⚠️  Request {} REJECTED", req_id);

                    // Update request status
                    let mut s = state.write().await;
                    if let Some(req) = s.my_requests.get_mut(&req_id) {
                        req.status = crate::viewer::RequestStatus::Rejected;
                    }
                } else {
                    println!("[CLIENT-LISTENER] REJECTED message too short");
                }
            }

            APPROVED => {
                // Parse: [req_id:u32]
                if data.len() >= 4 {
                    let req_id = u32::from_le_bytes(data[0..4].try_into()?);
                    println!("[CLIENT-LISTENER] ✅ Request {} APPROVED by owner - waiting for image chunks", req_id);

                    // Update request status
                    let mut s = state.write().await;
                    if let Some(req) = s.my_requests.get_mut(&req_id) {
                        req.status = crate::viewer::RequestStatus::Approved;
                    }
                } else {
                    println!("[CLIENT-LISTENER] APPROVED message too short");
                }
            }

            IMAGE_CHUNK => {
                println!("[CLIENT-LISTENER] 📦 IMAGE_CHUNK message received!");
                // Parse: [name_len:u16][name][seq:u32][total:u32][chunk_len:u16][chunk]
                let mut offset = 0;
                if data.len() < 2 {
                    println!("[CLIENT-LISTENER] IMAGE_CHUNK too short");
                    continue;
                }
                let name_len = u16::from_le_bytes(data[offset..offset + 2].try_into()?) as usize;
                offset += 2;
                if data.len() < offset + name_len + 4 + 4 + 2 {
                    println!("[CLIENT-LISTENER] IMAGE_CHUNK invalid name length");
                    continue;
                }
                let name = String::from_utf8(data[offset..offset + name_len].to_vec())?;
                offset += name_len;
                let seq = u32::from_le_bytes(data[offset..offset + 4].try_into()?);
                offset += 4;
                let total = u32::from_le_bytes(data[offset..offset + 4].try_into()?);
                offset += 4;
                let chunk_len = u16::from_le_bytes(data[offset..offset + 2].try_into()?) as usize;
                offset += 2;
                if data.len() < offset + chunk_len {
                    println!("[CLIENT-LISTENER] IMAGE_CHUNK data too short");
                    continue;
                }
                let chunk = data[offset..offset + chunk_len].to_vec();

                let entry = images.entry(name.clone()).or_insert_with(|| IncomingImage {
                    total_chunks: total,
                    chunks: HashMap::new(),
                });
                entry.total_chunks = total;
                entry.chunks.insert(seq, chunk);

                // Show progress
                let progress = (entry.chunks.len() as f32 / entry.total_chunks as f32) * 100.0;
                println!(
                    "[CLIENT-LISTENER] 📥 Chunk {}/{} for '{}' ({:.1}% complete)",
                    entry.chunks.len(),
                    entry.total_chunks,
                    name,
                    progress
                );

                if entry.chunks.len() as u32 == entry.total_chunks {
                    println!(
                        "╔════════════════════════════════════════════════════════════════╗"
                    );
                    println!(
                        "║ [RECEIVE IMAGE] All {} chunks received for '{}'",
                        entry.total_chunks, name
                    );

                    // Assemble in order
                    let mut assembled = Vec::new();
                    for i in 0..entry.total_chunks {
                        if let Some(c) = entry.chunks.get(&i) {
                            assembled.extend_from_slice(c);
                        } else {
                            println!("[CLIENT-LISTENER] ❌ Missing chunk {} for {}", i, name);
                            break;
                        }
                    }

                    // Remove _encrypted suffix - use clean name
                    let clean_name = name.trim_end_matches("_encrypted");

                    // Determine if this is viewer-received or owner's own image
                    // Check if we have a pending request for this image (means we're a viewer)
                    let (is_viewer_image, owner_name, requested_views) = {
                        let s = state.read().await;

                        println!("[CLIENT-LISTENER] Checking if '{}' is a viewer image", clean_name);
                        println!("[CLIENT-LISTENER] Current my_requests: {:?}", s.my_requests.keys().collect::<Vec<_>>());

                        for req in s.my_requests.values() {
                            println!("[CLIENT-LISTENER]   Request: image={}, owner={}", req.image_name, req.owner);
                        }

                        if let Some(req) = s.my_requests.values().find(|req| req.image_name == clean_name) {
                            println!("[CLIENT-LISTENER] ✓ Found matching request for '{}' from owner '{}'", clean_name, req.owner);
                            (true, req.owner.clone(), req.requested_views)
                        } else {
                            println!("[CLIENT-LISTENER] ✗ No matching request found for '{}'", clean_name);
                            (false, String::new(), 0)
                        }
                    };

                    let (dir, path) = if is_viewer_image {
                        // Viewer receiving someone else's image → client_images
                        let dir = "client_images";
                        if let Err(e) = tokio::fs::create_dir_all(dir).await {
                            println!("[CLIENT-LISTENER] Failed to create dir {}: {}", dir, e);
                        }
                        let path = format!("{}/{}.png", dir, clean_name);
                        (dir, path)
                    } else {
                        // Owner receiving their own encrypted image → encrypted_storage
                        let dir = "encrypted_storage";
                        if let Err(e) = tokio::fs::create_dir_all(dir).await {
                            println!("[CLIENT-LISTENER] Failed to create dir {}: {}", dir, e);
                        }
                        let path = format!("{}/{}.png", dir, clean_name);
                        (dir, path)
                    };

                    match tokio::fs::write(&path, &assembled).await {
                        Ok(_) => {
                            println!(
                                "║ ✅ Image assembled: {} bytes",
                                assembled.len()
                            );
                            println!(
                                "║ 💾 Saved to: {}",
                                path
                            );
                            println!(
                                "╠════════════════════════════════════════════════════════════════╣"
                            );
                            println!(
                                "║ LOCATION: Client-Node/{}",
                                path
                            );
                            println!(
                                "║ You can view this encrypted image at the path above"
                            );
                            println!(
                                "╚════════════════════════════════════════════════════════════════╝"
                            );

                            // If viewer image, add to ViewerAccessMap
                            if is_viewer_image {
                                use crate::viewer::ViewerAccessMap;

                                println!("[CLIENT-LISTENER] 📝 Adding to ViewerAccessMap: {} from {} ({} views)",
                                         clean_name, owner_name, requested_views);

                                let map_path = ViewerAccessMap::default_path()
                                    .unwrap_or_else(|_| std::path::PathBuf::from("viewer_access_map.json"));

                                let mut viewer_map = ViewerAccessMap::load_from_file(&map_path).await
                                    .unwrap_or_else(|_| ViewerAccessMap::new());

                                viewer_map.add_grant(&owner_name, clean_name, requested_views, &path);

                                if let Err(e) = viewer_map.save_to_file(&map_path).await {
                                    println!("[CLIENT-LISTENER] ⚠️  Failed to save ViewerAccessMap: {}", e);
                                } else {
                                    println!("[CLIENT-LISTENER] ✅ Added to ViewerAccessMap with {} views", requested_views);
                                }
                            } else {
                                // Update encrypted image index (for owner's own images)
                                if let Err(e) = update_encrypted_image_index(clean_name, &path, assembled.len()).await {
                                    println!("[CLIENT-LISTENER] ⚠️  Failed to update index: {}", e);
                                }
                            }
                        },
                        Err(e) => println!("[CLIENT-LISTENER] ❌ Failed to write image {}: {}", path, e),
                    }

                    // Clean up from images map
                    images.remove(&name);
                }
            }

            _ => {
                println!("[CLIENT-LISTENER] Unknown message type: {}", msg_type);
            }
        }
    }
}

/// Connect to server via TCP
pub async fn connect_to_server(server_addr: SocketAddr) -> Result<(tokio::net::tcp::OwnedReadHalf, Arc<tokio::sync::Mutex<tokio::net::tcp::OwnedWriteHalf>>)> {
    println!("[CLIENT] Connecting to server at {}...", server_addr);

    // Let OS choose source port - P2P listener will use port 9080
    let stream = TcpStream::connect(server_addr)
        .await
        .context(format!("Failed to connect to server at {}", server_addr))?;

    let local_addr = stream.local_addr()?;
    println!("[CLIENT] ✅ Connected to server from local address {}", local_addr);

    let (read_half, write_half) = stream.into_split();
    Ok((read_half, Arc::new(tokio::sync::Mutex::new(write_half))))
}

/// Owner upload using existing connection (for interactive-upload mode)
/// Server encrypts true image into cover and sends back encrypted image
pub async fn upload_owner_image_with_writer(
    username: &str,
    writer: Arc<tokio::sync::Mutex<tokio::net::tcp::OwnedWriteHalf>>,
    image_name: &str,
    true_path: &str,
    cover_path: &str,
) -> Result<()> {
    println!(
        "╔════════════════════════════════════════════════════════════════╗"
    );
    println!(
        "║ [OWNER UPLOAD] Starting upload for image: {}",
        image_name
    );
    println!(
        "╚════════════════════════════════════════════════════════════════╝"
    );

    println!("[OWNER UPLOAD] 📂 Reading files...");
    let true_bytes = fs::read(true_path).await
        .with_context(|| format!("Failed to read true image {}", true_path))?;
    println!("[OWNER UPLOAD]   ✅ True image: {} ({} bytes)", true_path, true_bytes.len());

    let cover_bytes = fs::read(cover_path).await
        .with_context(|| format!("Failed to read cover image {}", cover_path))?;
    println!("[OWNER UPLOAD]   ✅ Cover image: {} ({} bytes)", cover_path, cover_bytes.len());
    println!("[OWNER UPLOAD]   ℹ️  Server will encrypt true image into cover (no metadata sent)");

    // META message (with meta_size = 0, no metadata sent)
    println!("[OWNER UPLOAD] 📤 Sending image sizes to server...");
    let mut meta_payload = Vec::new();
    meta_payload.extend((username.len() as u16).to_le_bytes());
    meta_payload.extend(username.as_bytes());
    meta_payload.extend((image_name.len() as u16).to_le_bytes());
    meta_payload.extend(image_name.as_bytes());
    meta_payload.extend((true_bytes.len() as u32).to_le_bytes());
    meta_payload.extend((cover_bytes.len() as u32).to_le_bytes());
    meta_payload.extend(0u32.to_le_bytes()); // meta_size = 0 (no metadata)
    {
        let mut w = writer.lock().await;
        send_tcp_message_generic(&mut *w, OWNER_IMAGE_META, &meta_payload).await?;
    }
    println!("[OWNER UPLOAD] ✅ Image sizes sent");

    // Chunk helper
    async fn send_chunk(
        writer: &Arc<tokio::sync::Mutex<tokio::net::tcp::OwnedWriteHalf>>,
        owner: &str,
        image: &str,
        kind: u8,
        data: &[u8],
    ) -> Result<()> {
        let chunk_size = 1000usize;
        for (idx, chunk) in data.chunks(chunk_size).enumerate() {
            let mut payload = Vec::new();
            payload.extend((owner.len() as u16).to_le_bytes());
            payload.extend(owner.as_bytes());
            payload.extend((image.len() as u16).to_le_bytes());
            payload.extend(image.as_bytes());
            payload.push(kind);
            let byte_offset = (idx * chunk_size) as u32;
            payload.extend(byte_offset.to_le_bytes());
            payload.extend((chunk.len() as u16).to_le_bytes());
            payload.extend(chunk);
            let mut w = writer.lock().await;
            send_tcp_message_generic(&mut *w, OWNER_IMAGE_CHUNK, &payload).await?;
        }
        Ok(())
    }

    println!("[OWNER UPLOAD] 📤 Sending image chunks to server...");
    println!("[OWNER UPLOAD]   📦 Sending true image chunks...");
    send_chunk(&writer, username, image_name, 0, &true_bytes).await?;
    println!("[OWNER UPLOAD]   ✅ True image chunks sent");

    println!("[OWNER UPLOAD]   📦 Sending cover image chunks...");
    send_chunk(&writer, username, image_name, 1, &cover_bytes).await?;
    println!("[OWNER UPLOAD]   ✅ Cover image chunks sent");

    println!(
        "╔════════════════════════════════════════════════════════════════╗"
    );
    println!(
        "║ [OWNER UPLOAD] ✅ All data sent to server!"
    );
    println!(
        "║ Image: {}",
        image_name
    );
    println!(
        "║ - True image: {} bytes",
        true_bytes.len()
    );
    println!(
        "║ - Cover image: {} bytes",
        cover_bytes.len()
    );
    println!(
        "║ Server will encrypt true image into cover and send back..."
    );
    println!(
        "║ 📥 The listener task will receive and save the encrypted image"
    );
    println!(
        "╚════════════════════════════════════════════════════════════════╝"
    );
    println!();
    println!("[UPLOAD-TASK] ✅ Upload complete - waiting for encrypted image from server...");
    println!("[UPLOAD-TASK] 💡 The encrypted image will be saved automatically by the listener");
    println!();

    Ok(())
}

/// Owner upload: send true image and cover image to server (NO metadata)
/// Server encrypts true image into cover and sends back encrypted image
pub async fn upload_owner_image(
    username: &str,
    server_addr: SocketAddr,
    image_name: &str,
    true_path: &str,
    cover_path: &str,
    stay_open: bool,
) -> Result<()> {
    println!(
        "╔════════════════════════════════════════════════════════════════╗"
    );
    println!(
        "║ [OWNER UPLOAD] Starting upload for image: {}",
        image_name
    );
    println!(
        "╚════════════════════════════════════════════════════════════════╝"
    );

    println!("[OWNER UPLOAD] 📂 Reading files...");
    let true_bytes = fs::read(true_path).await
        .with_context(|| format!("Failed to read true image {}", true_path))?;
    println!("[OWNER UPLOAD]   ✅ True image: {} ({} bytes)", true_path, true_bytes.len());

    let cover_bytes = fs::read(cover_path).await
        .with_context(|| format!("Failed to read cover image {}", cover_path))?;
    println!("[OWNER UPLOAD]   ✅ Cover image: {} ({} bytes)", cover_path, cover_bytes.len());
    println!("[OWNER UPLOAD]   ℹ️  Server will encrypt true image into cover (no metadata sent)");

    let (mut reader, writer) = connect_to_server(server_addr).await?;

    // META message (with meta_size = 0, no metadata sent)
    println!("[OWNER UPLOAD] 📤 Sending image sizes to server...");
    let mut meta_payload = Vec::new();
    meta_payload.extend((username.len() as u16).to_le_bytes());
    meta_payload.extend(username.as_bytes());
    meta_payload.extend((image_name.len() as u16).to_le_bytes());
    meta_payload.extend(image_name.as_bytes());
    meta_payload.extend((true_bytes.len() as u32).to_le_bytes());
    meta_payload.extend((cover_bytes.len() as u32).to_le_bytes());
    meta_payload.extend(0u32.to_le_bytes()); // meta_size = 0 (no metadata)
    {
        let mut w = writer.lock().await;
        send_tcp_message_generic(&mut *w, OWNER_IMAGE_META, &meta_payload).await?;
    }
    println!("[OWNER UPLOAD] ✅ Image sizes sent");

    // Chunk helper
    async fn send_chunk(
        writer: &Arc<tokio::sync::Mutex<tokio::net::tcp::OwnedWriteHalf>>,
        owner: &str,
        image: &str,
        kind: u8,
        data: &[u8],
    ) -> Result<()> {
        let chunk_size = 1000usize;
        for (idx, chunk) in data.chunks(chunk_size).enumerate() {
            let mut payload = Vec::new();
            payload.extend((owner.len() as u16).to_le_bytes());
            payload.extend(owner.as_bytes());
            payload.extend((image.len() as u16).to_le_bytes());
            payload.extend(image.as_bytes());
            payload.push(kind);
            let byte_offset = (idx * chunk_size) as u32;
            payload.extend(byte_offset.to_le_bytes());
            payload.extend((chunk.len() as u16).to_le_bytes());
            payload.extend(chunk);
            let mut w = writer.lock().await;
            send_tcp_message_generic(&mut *w, OWNER_IMAGE_CHUNK, &payload).await?;
        }
        Ok(())
    }

    println!("[OWNER UPLOAD] 📤 Sending image chunks to server...");
    println!("[OWNER UPLOAD]   📦 Sending true image chunks...");
    send_chunk(&writer, username, image_name, 0, &true_bytes).await?;
    println!("[OWNER UPLOAD]   ✅ True image chunks sent");

    println!("[OWNER UPLOAD]   📦 Sending cover image chunks...");
    send_chunk(&writer, username, image_name, 1, &cover_bytes).await?;
    println!("[OWNER UPLOAD]   ✅ Cover image chunks sent");

    println!(
        "╔════════════════════════════════════════════════════════════════╗"
    );
    println!(
        "║ [OWNER UPLOAD] ✅ All data sent to server!"
    );
    println!(
        "║ Image: {}",
        image_name
    );
    println!(
        "║ - True image: {} bytes",
        true_bytes.len()
    );
    println!(
        "║ - Cover image: {} bytes",
        cover_bytes.len()
    );
    println!(
        "║ Server will encrypt true image into cover and send back..."
    );
    println!(
        "╚════════════════════════════════════════════════════════════════╝"
    );

    if stay_open {
        println!("[OWNER-UPLOAD] Keeping connection open. Press Ctrl+C to exit.");
        // Track incoming image chunks from the server (encrypted image being returned)
        #[derive(Default)]
        struct IncomingImage {
            total_chunks: u32,
            chunks: HashMap<u32, Vec<u8>>,
        }
        let mut images: HashMap<String, IncomingImage> = HashMap::new();

        loop {
            tokio::select! {
                _ = signal::ctrl_c() => {
                    println!("[OWNER-UPLOAD] Ctrl+C received, closing connection.");
                    break;
                }
                recv = recv_tcp_message_generic(&mut reader) => {
                    match recv {
                        Ok((msg_type, payload)) => {
                            if msg_type == IMAGE_CHUNK {
                                // Parse: [name_len:u16][name][seq:u32][total:u32][chunk_len:u16][chunk]
                                let mut offset = 0;
                                if payload.len() < 2 {
                                    println!("[OWNER-UPLOAD-RECV] IMAGE_CHUNK too short");
                                    continue;
                                }
                                let name_len = u16::from_le_bytes(payload[offset..offset + 2].try_into()?) as usize;
                                offset += 2;
                                if payload.len() < offset + name_len + 4 + 4 + 2 {
                                    println!("[OWNER-UPLOAD-RECV] IMAGE_CHUNK invalid name length");
                                    continue;
                                }
                                let name = String::from_utf8(payload[offset..offset + name_len].to_vec())?;
                                offset += name_len;
                                let seq = u32::from_le_bytes(payload[offset..offset + 4].try_into()?);
                                offset += 4;
                                let total = u32::from_le_bytes(payload[offset..offset + 4].try_into()?);
                                offset += 4;
                                let chunk_len = u16::from_le_bytes(payload[offset..offset + 2].try_into()?) as usize;
                                offset += 2;
                                if payload.len() < offset + chunk_len {
                                    println!("[OWNER-UPLOAD-RECV] IMAGE_CHUNK data too short");
                                    continue;
                                }
                                let chunk = payload[offset..offset + chunk_len].to_vec();

                                let entry = images.entry(name.clone()).or_insert_with(|| IncomingImage {
                                    total_chunks: total,
                                    chunks: HashMap::new(),
                                });
                                entry.total_chunks = total;
                                entry.chunks.insert(seq, chunk);

                                println!(
                                    "[OWNER-UPLOAD-RECV] Received chunk {}/{} for '{}'",
                                    entry.chunks.len(),
                                    entry.total_chunks,
                                    name
                                );

                                if entry.chunks.len() as u32 == entry.total_chunks {
                                    // Assemble in order
                                    let mut assembled = Vec::new();
                                    for i in 0..entry.total_chunks {
                                        if let Some(c) = entry.chunks.get(&i) {
                                            assembled.extend_from_slice(c);
                                        } else {
                                            println!("[OWNER-UPLOAD-RECV] ❌ Missing chunk {} for {}", i, name);
                                            break;
                                        }
                                    }

                                    // Save to encrypted_images directory
                                    let dir = "encrypted_images";
                                    if let Err(e) = tokio::fs::create_dir_all(dir).await {
                                        println!("[OWNER-UPLOAD-RECV] Failed to create dir {}: {}", dir, e);
                                    }

                                    let path = format!("{}/{}_encrypted.png", dir, name);
                                    match tokio::fs::write(&path, &assembled).await {
                                        Ok(_) => {
                                            println!(
                                                "║ ✅ Encrypted image assembled ({} bytes) and saved to: {}",
                                                assembled.len(),
                                                path
                                            );
                                            println!("║ LOCATION: Client-Node/{}", path);
                                        },
                                        Err(e) => println!("[OWNER-UPLOAD-RECV] ❌ Failed to write image {}: {}", path, e),
                                    }

                                    // Clean up from images map
                                    images.remove(&name);
                                }
                            } else {
                                println!("[OWNER-UPLOAD-RECV] msg_type={} payload_len={}", msg_type, payload.len());
                            }
                        }
                        Err(e) => {
                            println!("[OWNER-UPLOAD] Connection closed or error: {}", e);
                            break;
                        }
                    }
                }
            }
        }
    } else {
        // Briefly check for acks then exit
        let _ = tokio::time::timeout(Duration::from_millis(500), recv_tcp_message_generic(&mut reader)).await;
    }

    Ok(())
}

/// Approve a view and receive the embedded image back
pub async fn approve_and_receive(
    username: &str,
    server_addr: SocketAddr,
    image_name: &str,
    req_id: u32,
) -> Result<()> {
    let (mut reader, writer) = connect_to_server(server_addr).await?;
    println!("[APPROVE] Connected as {}", username);

    // Send JOIN first to register
    let state = Arc::new(tokio::sync::RwLock::new(ClientState::new(username.to_string(), server_addr)));
    {
        let mut st = state.write().await;
        st.client_port = CLIENT_PORT;
        st.joined = true;
    }

    // Build APPROVE_VIEW: [req_id:u32]
    let mut payload = Vec::new();
    payload.extend(req_id.to_le_bytes());
    {
        let mut w = writer.lock().await;
        send_tcp_message_generic(&mut *w, APPROVE_VIEW, &payload).await?;
    }
    println!("[APPROVE] Sent APPROVE_VIEW req_id={} image={}", req_id, image_name);

    // Listen for IMAGE_CHUNKs until Ctrl+C
    let mut images: HashMap<String, IncomingImage> = HashMap::new();
    loop {
        tokio::select! {
            _ = signal::ctrl_c() => {
                println!("[APPROVE] Ctrl+C received, exiting.");
                break;
            }
            msg = recv_tcp_message_generic(&mut reader) => {
                match msg {
                    Ok((msg_type, data)) => {
                        if msg_type == IMAGE_CHUNK {
                            // reuse the listener logic
                            let mut offset = 0;
                            if data.len() < 2 { continue; }
                            let name_len = u16::from_le_bytes(data[offset..offset+2].try_into().unwrap()) as usize;
                            offset += 2;
                            if data.len() < offset + name_len + 4 + 4 + 2 { continue; }
                            let name = String::from_utf8(data[offset..offset+name_len].to_vec()).unwrap_or_else(|_| "img".into());
                            offset += name_len;
                            let seq = u32::from_le_bytes(data[offset..offset+4].try_into().unwrap());
                            offset += 4;
                            let total = u32::from_le_bytes(data[offset..offset+4].try_into().unwrap());
                            offset += 4;
                            let chunk_len = u16::from_le_bytes(data[offset..offset+2].try_into().unwrap()) as usize;
                            offset += 2;
                            if data.len() < offset + chunk_len { continue; }
                            let chunk = data[offset..offset+chunk_len].to_vec();
                            let entry = images.entry(name.clone()).or_insert_with(|| IncomingImage { total_chunks: total, chunks: HashMap::new() });
                            entry.total_chunks = total;
                            entry.chunks.insert(seq, chunk);
                            if entry.chunks.len() as u32 == entry.total_chunks {
                                let mut assembled = Vec::new();
                                for i in 0..entry.total_chunks {
                                    if let Some(c) = entry.chunks.get(&i) {
                                        assembled.extend_from_slice(c);
                                    }
                                }
                                let dir = "received";
                                let _ = fs::create_dir_all(dir).await;
                                let path = format!("{}/{}.png", dir, name);
                                if let Err(e) = fs::write(&path, &assembled).await {
                                    println!("[APPROVE] Failed to write image {}: {}", path, e);
                                } else {
                                    println!("[APPROVE] ✅ Received and saved {}", path);
                                    break;
                                }
                            }
                        } else {
                            println!("[APPROVE] Received msg_type={} payload_len={}", msg_type, data.len());
                        }
                    }
                    Err(e) => {
                        println!("[APPROVE] Connection closed/error: {}", e);
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}

/// Broadcast REQUEST_EXECUTOR to all servers, return executor address
pub async fn discover_executor(
    server_addresses: &[SocketAddr],
) -> Result<SocketAddr> {
    use tokio::net::TcpStream;

    println!("[DISCOVERY] Broadcasting REQUEST_EXECUTOR to {} servers", server_addresses.len());

    let timeout = Duration::from_secs(5);
    let mut tasks = Vec::new();

    for &addr in server_addresses {
        let task = tokio::spawn(async move {
            match tokio::time::timeout(timeout, TcpStream::connect(addr)).await {
                Ok(Ok(mut stream)) => {
                    // Send REQUEST_EXECUTOR
                    let msg = vec![REQUEST_EXECUTOR];
                    if stream.write_all(&(msg.len() as u32).to_le_bytes()).await.is_ok()
                        && stream.write_all(&msg).await.is_ok()
                    {
                        // Read response
                        let mut len_buf = [0u8; 4];
                        if stream.read_exact(&mut len_buf).await.is_ok() {
                            let len = u32::from_le_bytes(len_buf) as usize;
                            let mut buf = vec![0u8; len];
                            if stream.read_exact(&mut buf).await.is_ok() && buf[0] == EXECUTOR_ACK {
                                println!("[DISCOVERY] ✅ Executor found at {}", addr);
                                return Some(addr);
                            }
                        }
                    }
                }
                _ => {}
            }
            None
        });
        tasks.push(task);
    }

    // Wait for first successful response
    for task in tasks {
        if let Ok(Some(executor_addr)) = task.await {
            return Ok(executor_addr);
        }
    }

    Err(anyhow::anyhow!("No executor found"))
}

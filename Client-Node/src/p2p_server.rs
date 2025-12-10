//! P2P Server for Direct Client-to-Client Communication
//! Listens on a port in the range 9080+ for incoming P2P requests from other clients

use anyhow::{Context, Result};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::simple_client::SharedClientState;
use crate::protocol;

/// Normalize image name by stripping .png extension for logical operations
/// Physical file operations should use the full name with extension
fn normalize_image_name(name: &str) -> String {
    if name.ends_with(".png") {
        name.trim_end_matches(".png").to_string()
    } else {
        name.to_string()
    }
}

/// Start P2P listener by binding to port range 9080+
/// Returns (TcpListener, bound_port)
pub async fn start_p2p_listener(
    start_port: u16,
) -> Result<(TcpListener, u16)> {
    const MAX_PORT_ATTEMPTS: u16 = 100;

    for port in start_port..start_port + MAX_PORT_ATTEMPTS {
        match TcpListener::bind(("0.0.0.0", port)).await {
            Ok(listener) => {
                println!("[P2P_SERVER] Successfully bound to port {}", port);
                return Ok((listener, port));
            }
            Err(e) => {
                if port == start_port + MAX_PORT_ATTEMPTS - 1 {
                    return Err(anyhow::anyhow!(
                        "Failed to bind to any port in range {}-{}: {}",
                        start_port,
                        start_port + MAX_PORT_ATTEMPTS - 1,
                        e
                    ));
                }
                // Try next port
                continue;
            }
        }
    }

    Err(anyhow::anyhow!("No available ports in range"))
}

/// Run P2P server - accept incoming connections and handle P2P messages
pub async fn run_p2p_server(
    listener: TcpListener,
    state: SharedClientState,
) -> Result<()> {
    let local_addr = listener.local_addr()?;
    println!("[P2P_SERVER] Listening for P2P connections on {}", local_addr);

    loop {
        let (stream, peer_addr) = match listener.accept().await {
            Ok(conn) => conn,
            Err(e) => {
                eprintln!("[P2P_SERVER] Failed to accept connection: {}", e);
                continue;
            }
        };

        println!("[P2P_SERVER] Accepted P2P connection from {}", peer_addr);

        let state_clone = state.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_peer_connection(stream, peer_addr, state_clone).await {
                eprintln!("[P2P_SERVER] Error handling peer connection from {}: {}", peer_addr, e);
            }
        });
    }
}

/// Handle incoming P2P connection from another client
async fn handle_peer_connection(
    mut stream: TcpStream,
    peer_addr: SocketAddr,
    state: SharedClientState,
) -> Result<()> {
    println!("[P2P_SERVER] Handling connection from peer: {}", peer_addr);

    loop {
        // Read total length (u32) - standard protocol format: [len][msg_type][payload]
        let mut len_buf = [0u8; 4];
        match stream.read_exact(&mut len_buf).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                println!("[P2P_SERVER] Peer {} disconnected", peer_addr);
                break;
            }
            Err(e) => {
                return Err(anyhow::anyhow!("Failed to read message length: {}", e));
            }
        }

        let total_len = u32::from_le_bytes(len_buf) as usize;

        // Read message type (1 byte)
        let mut msg_type_buf = [0u8; 1];
        stream.read_exact(&mut msg_type_buf).await
            .context("Failed to read message type")?;
        let msg_type = msg_type_buf[0];

        // Calculate payload length (total_len includes msg_type)
        let payload_len = total_len - 1;

        // Read payload
        let mut payload = vec![0u8; payload_len];
        stream.read_exact(&mut payload).await
            .context("Failed to read payload")?;

        println!("[P2P_SERVER] Received P2P message type={} len={} from {}",
                 msg_type, payload_len, peer_addr);

        // Route to appropriate handler
        match msg_type {
            protocol::LIFE_CHECK => {
                // Server is checking if we're alive - respond with ACK
                println!("[P2P_SERVER] Received LIFE_CHECK from server, sending ACK");
                let response = vec![protocol::LIFE_CHECK_ACK];
                if let Err(e) = stream.write_all(&response).await {
                    eprintln!("[P2P_SERVER] Failed to send LIFE_CHECK_ACK: {}", e);
                } else {
                    println!("[P2P_SERVER] ✅ Sent LIFE_CHECK_ACK to server");
                }
                // Continue listening for more messages
            }
            protocol::PEER_VIEW_REQUEST => {
                handle_peer_view_request(&mut stream, peer_addr, &payload, state.clone()).await?;
            }
            protocol::PEER_VIEW_RESPONSE => {
                handle_peer_view_response(&mut stream, peer_addr, &payload, state.clone()).await?;
            }
            protocol::PEER_IMAGE_CHUNK => {
                handle_peer_image_chunk(&mut stream, peer_addr, &payload, state.clone()).await?;
            }
            protocol::PEER_VIEW_REJECTED => {
                handle_peer_view_rejected(&mut stream, peer_addr, &payload, state.clone()).await?;
            }
            protocol::PEER_ADJUST_REQUEST => {
                handle_peer_adjust_request(&mut stream, peer_addr, &payload, state.clone()).await?;
            }
            protocol::PEER_REVOKE => {
                handle_peer_revoke(&mut stream, peer_addr, &payload, state.clone()).await?;
            }
            _ => {
                println!("[P2P_SERVER] Unknown P2P message type: {}", msg_type);
            }
        }
    }

    Ok(())
}

/// Handle PEER_VIEW_REQUEST from viewer
/// Format: [viewer_len:u16][viewer][image_len:u16][image_name][requested_views:u32]
async fn handle_peer_view_request(
    stream: &mut TcpStream,
    peer_addr: SocketAddr,
    data: &[u8],
    state: SharedClientState,
) -> Result<()> {
    println!("[P2P_SERVER] Handling PEER_VIEW_REQUEST from {}", peer_addr);

    let mut offset = 0;

    // Parse viewer name
    if data.len() < 2 {
        return Err(anyhow::anyhow!("Invalid PEER_VIEW_REQUEST: too short"));
    }

    let viewer_len = u16::from_le_bytes(data[offset..offset + 2].try_into()?) as usize;
    offset += 2;

    if data.len() < offset + viewer_len {
        return Err(anyhow::anyhow!("Invalid viewer name length"));
    }

    let viewer_name = String::from_utf8(data[offset..offset + viewer_len].to_vec())?;
    offset += viewer_len;

    // Parse viewer's P2P port
    if data.len() < offset + 2 {
        return Err(anyhow::anyhow!("Invalid viewer P2P port"));
    }

    let viewer_p2p_port = u16::from_le_bytes(data[offset..offset + 2].try_into()?);
    offset += 2;

    // Parse image name
    if data.len() < offset + 2 {
        return Err(anyhow::anyhow!("Invalid image name length"));
    }

    let image_len = u16::from_le_bytes(data[offset..offset + 2].try_into()?) as usize;
    offset += 2;

    if data.len() < offset + image_len {
        return Err(anyhow::anyhow!("Invalid image name"));
    }

    let image_name_raw = String::from_utf8(data[offset..offset + image_len].to_vec())?;
    offset += image_len;

    // Normalize image name (strip .png extension for comparison)
    let image_name = normalize_image_name(&image_name_raw);

    // Parse requested views
    if data.len() < offset + 4 {
        return Err(anyhow::anyhow!("Invalid requested views"));
    }

    let requested_views = u32::from_le_bytes(data[offset..offset + 4].try_into()?);

    println!("[P2P_SERVER] 📥 PEER_VIEW_REQUEST: viewer={}, image={} (normalized from {}), views={}",
             viewer_name, image_name, image_name_raw, requested_views);

    // Phase 3B: Store request in pending_view_requests for owner approval
    // Generate unique request ID
    let request_id = rand::random::<u32>();

    // Get owner username
    let owner_username = {
        let s = state.read().await;
        s.username.clone()
    };

    // Check if we own this image by reading encrypted_storage directory (owner's own encrypted images)
    println!("[P2P_SERVER] 🔍 Checking encrypted_storage directory for image '{}'", image_name);

    let encrypted_storage = std::path::Path::new("encrypted_storage");
    let has_image = if encrypted_storage.exists() {
        match tokio::fs::read_dir(encrypted_storage).await {
            Ok(mut entries) => {
                let mut found = false;
                while let Ok(Some(entry)) = entries.next_entry().await {
                    if let Ok(file_name) = entry.file_name().into_string() {
                        // Normalize filename (strip .png)
                        let normalized_file = normalize_image_name(&file_name);
                        println!("[P2P_SERVER]   - Found file: {} (normalized: {})", file_name, normalized_file);
                        if normalized_file == image_name {
                            println!("[P2P_SERVER] ✅ Match found: {} == {}", normalized_file, image_name);
                            found = true;
                            break;
                        }
                    }
                }
                found
            }
            Err(e) => {
                println!("[P2P_SERVER] ⚠️  Failed to read encrypted_storage directory: {}", e);
                false
            }
        }
    } else {
        println!("[P2P_SERVER] ⚠️  encrypted_storage directory does not exist");
        false
    };

    if !has_image {
        println!("[P2P_SERVER] ❌ Image '{}' not found in encrypted_storage directory", image_name);
        // Send PEER_VIEW_REJECTED
        let mut payload = Vec::new();
        let reason = "Image not found";
        payload.extend((reason.len() as u16).to_le_bytes());
        payload.extend_from_slice(reason.as_bytes());
        send_p2p_message(stream, protocol::PEER_VIEW_REJECTED, &payload).await?;
        return Ok(());
    }

    // Store pending request in ClientState
    {
        use crate::owner::PendingViewRequest;

        // Construct correct viewer address using their IP and P2P listening port
        let viewer_ip = peer_addr.ip();
        let viewer_correct_addr = std::net::SocketAddr::new(viewer_ip, viewer_p2p_port);

        println!("[P2P_SERVER] Viewer address: {} (source) -> {} (P2P port)", peer_addr, viewer_correct_addr);

        let mut s = state.write().await;
        let pending_req = PendingViewRequest {
            request_id,
            viewer: viewer_name.clone(),
            image_name: image_name.clone(),
            requested_views,
            peer_addr: viewer_correct_addr,  // Use P2P listening port, not source port
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };

        s.pending_view_requests.insert(request_id, pending_req);

        println!("[P2P_SERVER] 📝 Stored pending request: ID={}, viewer={}, image={}",
                 request_id, viewer_name, image_name);
    }

    // Notify owner (console for now, UI in Phase 7)
    println!("\n╔═══════════════════════════════════════════════════════════╗");
    println!("║  📢 NEW VIEW REQUEST                                      ║");
    println!("╠═══════════════════════════════════════════════════════════╣");
    println!("║  Request ID: {}                                    ║", request_id);
    println!("║  From:       {}                                          ║", viewer_name);
    println!("║  Image:      {}                                         ║", image_name);
    println!("║  Views:      {}                                              ║", requested_views);
    println!("╠═══════════════════════════════════════════════════════════╣");
    println!("║  Use CLI commands to approve/deny:                        ║");
    println!("║    approve <request_id>                                   ║");
    println!("║    deny <request_id>                                      ║");
    println!("╚═══════════════════════════════════════════════════════════╝\n");

    // Send acknowledgment (connection stays open for response)
    // Note: Approval will be handled by Phase 3C: approve_peer_view_request()
    send_p2p_message(stream, protocol::PEER_ACK, &[]).await?;

    // TODO Phase 3C: When owner approves, call approve_peer_view_request()
    // TODO Phase 3C: When owner denies, send PEER_VIEW_REJECTED

    Ok(())
}

/// Handle PEER_ADJUST_REQUEST from viewer to adjust view count
async fn handle_peer_adjust_request(
    stream: &mut TcpStream,
    peer_addr: SocketAddr,
    data: &[u8],
    _state: SharedClientState,
) -> Result<()> {
    println!("[P2P_SERVER] Handling PEER_ADJUST_REQUEST from {} (not yet implemented)", peer_addr);
    send_p2p_message(stream, protocol::PEER_ACK, &[]).await?;
    Ok(())
}

/// Handle PEER_REVOKE from owner to revoke access
async fn handle_peer_revoke(
    stream: &mut TcpStream,
    peer_addr: SocketAddr,
    data: &[u8],
    _state: SharedClientState,
) -> Result<()> {
    println!("[P2P_SERVER] Handling PEER_REVOKE from {} (not yet implemented)", peer_addr);
    send_p2p_message(stream, protocol::PEER_ACK, &[]).await?;
    Ok(())
}

/// Send a P2P message to peer
/// Format: [msg_type:u8][payload_len:u32][payload]
pub async fn send_p2p_message(
    stream: &mut TcpStream,
    msg_type: u8,
    payload: &[u8],
) -> Result<()> {
    let mut buf = Vec::new();
    buf.push(msg_type);
    buf.extend((payload.len() as u32).to_le_bytes());
    buf.extend_from_slice(payload);

    stream.write_all(&buf).await
        .context("Failed to send P2P message")?;

    Ok(())
}

// ============================================================================
// Phase 3D: Viewer-Side Response Handlers
// ============================================================================

/// Handle PEER_VIEW_RESPONSE from owner (viewer side)
/// Wire format: [image_name_len:u16][image_name][granted_views:u32]
/// This is the first message in the approval flow, followed by PEER_IMAGE_CHUNK messages
async fn handle_peer_view_response(
    stream: &mut TcpStream,
    peer_addr: SocketAddr,
    data: &[u8],
    state: SharedClientState,
) -> Result<()> {
    println!("[P2P_VIEWER] 🔵 Received PEER_VIEW_RESPONSE from {}", peer_addr);

    let mut offset = 0;

    // Parse image name
    if data.len() < 2 {
        return Err(anyhow::anyhow!("Invalid PEER_VIEW_RESPONSE: too short"));
    }

    let image_len = u16::from_le_bytes(data[offset..offset + 2].try_into()?) as usize;
    offset += 2;

    if data.len() < offset + image_len {
        return Err(anyhow::anyhow!("Invalid image name length"));
    }

    let image_name = String::from_utf8(data[offset..offset + image_len].to_vec())?;
    offset += image_len;

    // Parse granted views
    if data.len() < offset + 4 {
        return Err(anyhow::anyhow!("Invalid granted views"));
    }

    let granted_views = u32::from_le_bytes(data[offset..offset + 4].try_into()?);

    println!("[P2P_VIEWER] 📥 Approval received: image='{}', granted_views={}",
             image_name, granted_views);

    // Get owner username from peer_addr by looking up in DOS
    // For now, we'll need to determine owner from the connection
    // Since owner connected to us, we need to track this differently
    // Let's extract owner from pending state or use a different approach

    // For now, we'll use peer_addr to lookup owner in DOS
    let owner = {
        let s = state.read().await;
        // Find owner by matching peer IP
        let peer_ip = peer_addr.ip().to_string();

        s.dos.clients.iter()
            .find(|(_, client)| client.client_ip == peer_ip)
            .map(|(username, _)| username.clone())
            .ok_or_else(|| anyhow::anyhow!("Cannot determine owner from peer_addr {}", peer_addr))?
    };

    println!("[P2P_VIEWER] Identified owner: {}", owner);

    // Create pending download entry to track incoming chunks
    let key = crate::simple_client::PendingImageDownload::make_key(&owner, &image_name);

    {
        use crate::simple_client::PendingImageDownload;

        let mut s = state.write().await;
        let pending = PendingImageDownload::new(owner.clone(), image_name.clone(), granted_views);
        s.pending_downloads.insert(key.clone(), pending);

        println!("[P2P_VIEWER] 📝 Created pending download: key={}", key);
    }

    println!("[P2P_VIEWER] ✅ Ready to receive image chunks");

    Ok(())
}

/// Handle PEER_IMAGE_CHUNK from owner (viewer side)
/// Wire format: [chunk_index:u32][total_chunks:u32][chunk_data]
/// Reassembles chunks and saves to encrypted_storage when complete
async fn handle_peer_image_chunk(
    stream: &mut TcpStream,
    peer_addr: SocketAddr,
    data: &[u8],
    state: SharedClientState,
) -> Result<()> {
    println!("[P2P_VIEWER] 📦 Received PEER_IMAGE_CHUNK from {}", peer_addr);

    let mut offset = 0;

    // Parse chunk index
    if data.len() < 4 {
        return Err(anyhow::anyhow!("Invalid PEER_IMAGE_CHUNK: too short"));
    }

    let chunk_index = u32::from_le_bytes(data[offset..offset + 4].try_into()?);
    offset += 4;

    // Parse total chunks
    if data.len() < offset + 4 {
        return Err(anyhow::anyhow!("Invalid total chunks"));
    }

    let total_chunks = u32::from_le_bytes(data[offset..offset + 4].try_into()?);
    offset += 4;

    // Remaining data is the chunk
    let chunk_data = data[offset..].to_vec();

    println!("[P2P_VIEWER] 📦 Chunk {}/{} ({} bytes)",
             chunk_index + 1, total_chunks, chunk_data.len());

    // Find the pending download by matching peer_addr to owner
    let owner_opt = {
        let s = state.read().await;
        let peer_ip = peer_addr.ip().to_string();

        s.dos.clients.iter()
            .find(|(_, client)| client.client_ip == peer_ip)
            .map(|(username, _)| username.clone())
    };

    let owner = owner_opt.ok_or_else(|| anyhow::anyhow!("Cannot determine owner from peer_addr"))?;

    // Store chunk in pending download
    let mut is_complete = false;
    let mut pending_opt: Option<crate::simple_client::PendingImageDownload> = None;

    {
        let mut s = state.write().await;

        // Find the pending download for this owner (should only be one active at a time)
        let key_opt = s.pending_downloads.keys()
            .find(|k| k.starts_with(&format!("{}_", owner)))
            .cloned();

        if let Some(key) = key_opt {
            if let Some(pending) = s.pending_downloads.get_mut(&key) {
                // Set total chunks if not set
                if pending.total_chunks.is_none() {
                    pending.total_chunks = Some(total_chunks);
                }

                // Store chunk
                pending.chunks.insert(chunk_index, chunk_data);

                println!("[P2P_VIEWER] 📦 Stored chunk {} ({}/{} received)",
                         chunk_index, pending.chunks.len(), total_chunks);

                // Check if complete
                if pending.is_complete() {
                    is_complete = true;
                    pending_opt = Some(pending.clone());
                    println!("[P2P_VIEWER] ✅ All chunks received!");
                }
            }
        } else {
            return Err(anyhow::anyhow!("No pending download found for owner {}", owner));
        }
    }

    // If complete, assemble and save image
    if is_complete {
        if let Some(pending) = pending_opt {
            println!("[P2P_VIEWER] 🔧 Assembling image from {} chunks...", pending.chunks.len());

            // Assemble chunks in order
            let mut full_image = Vec::new();
            for i in 0..total_chunks {
                let chunk = pending.chunks.get(&i)
                    .ok_or_else(|| anyhow::anyhow!("Missing chunk {}", i))?;
                full_image.extend_from_slice(chunk);
            }

            println!("[P2P_VIEWER] 📦 Assembled image: {} bytes", full_image.len());

            // Save to encrypted_storage/
            let encrypted_dir = std::path::Path::new("encrypted_storage");
            tokio::fs::create_dir_all(encrypted_dir).await?;

            let filename = format!("{}.png", pending.image_name.trim_end_matches(".png"));
            let file_path = encrypted_dir.join(&filename);

            tokio::fs::write(&file_path, &full_image).await
                .context("Failed to write encrypted image")?;

            println!("[P2P_VIEWER] 💾 Saved encrypted image to {}", file_path.display());

            // Add to ViewerAccessMap
            {
                use crate::viewer::ViewerAccessMap;

                let mut s = state.write().await;

                // Load existing map or create new
                let map_path = ViewerAccessMap::default_path()?;
                let mut viewer_map = ViewerAccessMap::load_from_file(&map_path).await
                    .unwrap_or_else(|_| ViewerAccessMap::new());

                // Add grant
                let encrypted_path_str = file_path.to_string_lossy().to_string();
                viewer_map.add_grant(
                    &pending.owner,
                    &pending.image_name,
                    pending.granted_views,
                    &encrypted_path_str,
                );

                // Save map
                viewer_map.save_to_file(&map_path).await?;

                println!("[P2P_VIEWER] 📝 Added to viewer_access_map: {} views remaining",
                         pending.granted_views);

                // Remove from pending downloads
                let key = crate::simple_client::PendingImageDownload::make_key(
                    &pending.owner, &pending.image_name);
                s.pending_downloads.remove(&key);
            }

            println!("[P2P_VIEWER] ✅ Download complete! Image ready to view ({} times)",
                     pending.granted_views);
        }
    }

    Ok(())
}

/// Handle PEER_VIEW_REJECTED from owner (viewer side)
/// Wire format: [reason_len:u16][reason]
async fn handle_peer_view_rejected(
    stream: &mut TcpStream,
    peer_addr: SocketAddr,
    data: &[u8],
    state: SharedClientState,
) -> Result<()> {
    println!("[P2P_VIEWER] ❌ Received PEER_VIEW_REJECTED from {}", peer_addr);

    let mut offset = 0;

    // Parse rejection reason
    if data.len() < 2 {
        return Err(anyhow::anyhow!("Invalid PEER_VIEW_REJECTED: too short"));
    }

    let reason_len = u16::from_le_bytes(data[offset..offset + 2].try_into()?) as usize;
    offset += 2;

    if data.len() < offset + reason_len {
        return Err(anyhow::anyhow!("Invalid reason length"));
    }

    let reason = String::from_utf8(data[offset..offset + reason_len].to_vec())?;

    println!("\n╔═══════════════════════════════════════════════════════════╗");
    println!("║  ❌ VIEW REQUEST REJECTED                                 ║");
    println!("╠═══════════════════════════════════════════════════════════╣");
    println!("║  From:   {}                                               ║", peer_addr);
    println!("║  Reason: {}                                               ║", reason);
    println!("╚═══════════════════════════════════════════════════════════╝\n");

    // Clean up any pending state for this peer if exists
    {
        let mut s = state.write().await;
        let peer_ip = peer_addr.ip().to_string();

        // Find owner from peer_addr
        if let Some((owner, _)) = s.dos.clients.iter()
            .find(|(_, client)| client.client_ip == peer_ip) {

            // Remove any pending downloads from this owner
            let keys_to_remove: Vec<String> = s.pending_downloads.keys()
                .filter(|k| k.starts_with(&format!("{}_", owner)))
                .cloned()
                .collect();

            for key in keys_to_remove {
                s.pending_downloads.remove(&key);
                println!("[P2P_VIEWER] 🗑️  Removed pending download: {}", key);
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_port_binding() {
        let result = start_p2p_listener(9080).await;
        assert!(result.is_ok());
        let (_listener, port) = result.unwrap();
        assert!(port >= 9080 && port < 9180);
    }
}

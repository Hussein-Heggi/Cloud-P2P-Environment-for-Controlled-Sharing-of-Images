//! Owner operations for managing image access requests

use anyhow::{Context, Result};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;

use crate::protocol::*;
use crate::simple_client::SharedClientState;

/// Pending view request notification (owner side)
#[derive(Debug, Clone)]
pub struct PendingViewRequest {
    pub request_id: u32,
    pub viewer: String,
    pub image_name: String,
    pub requested_views: u32,
    pub peer_addr: std::net::SocketAddr,  // NEW Phase 3B: For P2P response
    pub timestamp: u64,
}

/// Approve a view request
/// Sends APPROVE_VIEW message to server
pub async fn approve_request(
    state: SharedClientState,
    writer: Arc<tokio::sync::Mutex<tokio::net::tcp::OwnedWriteHalf>>,
    request_id: u32,
) -> Result<()> {
    println!("[OWNER] Approving request {}", request_id);

    // Build payload: just the request ID
    let payload = request_id.to_le_bytes().to_vec();

    // Send APPROVE_VIEW
    {
        let mut w = writer.lock().await;
        w.write_all(&(payload.len() as u32 + 1).to_le_bytes()).await?;
        w.write_u8(APPROVE_VIEW).await?;
        w.write_all(&payload).await?;
        w.flush().await?;
    }

    println!("[OWNER] ✅ APPROVE_VIEW sent for request {}", request_id);

    // Remove from pending requests
    {
        let mut s = state.write().await;
        s.pending_view_requests.remove(&request_id);
    }

    Ok(())
}

/// Deny a view request
/// Sends DENY_VIEW message to server
pub async fn deny_request(
    state: SharedClientState,
    writer: Arc<tokio::sync::Mutex<tokio::net::tcp::OwnedWriteHalf>>,
    request_id: u32,
) -> Result<()> {
    println!("[OWNER] Denying request {}", request_id);

    // Build payload: just the request ID
    let payload = request_id.to_le_bytes().to_vec();

    // Send DENY_VIEW
    {
        let mut w = writer.lock().await;
        w.write_all(&(payload.len() as u32 + 1).to_le_bytes()).await?;
        w.write_u8(DENY_VIEW).await?;
        w.write_all(&payload).await?;
        w.flush().await?;
    }

    println!("[OWNER] ⚠️  DENY_VIEW sent for request {}", request_id);

    // Remove from pending requests
    {
        let mut s = state.write().await;
        s.pending_view_requests.remove(&request_id);
    }

    Ok(())
}

// ============================================================================
// Phase 3C: P2P Approval - Send Pre-Encrypted Image (NO re-encryption!)
// ============================================================================

/// Approve a P2P view request and send pre-encrypted image to viewer
/// CRITICAL: Sends encrypted image AS-IS (no re-encryption per viewer)
/// View count is sent in PROTOCOL MESSAGE, not embedded in image
pub async fn approve_peer_view_request(
    state: SharedClientState,
    request_id: u32,
) -> Result<()> {
    println!("[P2P-OWNER] 🔵 Approving P2P request {}", request_id);

    // Get request details from pending_view_requests
    let (viewer, image_name, requested_views, viewer_addr) = {
        let s = state.read().await;
        let request = s.pending_view_requests.get(&request_id)
            .ok_or_else(|| anyhow::anyhow!("Request {} not found in pending requests", request_id))?;

        (
            request.viewer.clone(),
            request.image_name.clone(),
            request.requested_views,
            request.peer_addr,
        )
    };

    println!("[P2P-OWNER] Request details: viewer={}, image={}, views={}, addr={}",
             viewer, image_name, requested_views, viewer_addr);

    // Get owner username
    let owner = {
        let s = state.read().await;
        s.username.clone()
    };

    // Read pre-encrypted image from encrypted_storage/ (owner's own encrypted images)
    let encrypted_path = format!("encrypted_storage/{}.png", image_name.trim_end_matches(".png"));
    println!("[P2P-OWNER] Reading pre-encrypted image from: {}", encrypted_path);

    let encrypted_image = tokio::fs::read(&encrypted_path).await
        .context(format!("Failed to read encrypted image from {}", encrypted_path))?;

    println!("[P2P-OWNER] ✅ Read encrypted image: {} bytes", encrypted_image.len());

    // Connect back to viewer's P2P port to send response
    println!("[P2P-OWNER] Connecting to viewer at {}...", viewer_addr);
    let mut stream = tokio::net::TcpStream::connect(viewer_addr).await
        .context(format!("Failed to connect to viewer at {}", viewer_addr))?;

    println!("[P2P-OWNER] ✅ Connected to viewer");

    // Send PEER_VIEW_RESPONSE with view count in protocol message
    // Wire format: [image_name_len:u16][image_name][granted_views:u32]
    let mut response_payload = Vec::new();

    let image_bytes = image_name.as_bytes();
    response_payload.extend((image_bytes.len() as u16).to_le_bytes());
    response_payload.extend_from_slice(image_bytes);

    response_payload.extend(requested_views.to_le_bytes());

    // Send PEER_VIEW_RESPONSE message
    let total_len = 1 + response_payload.len();
    stream.write_all(&(total_len as u32).to_le_bytes()).await?;
    stream.write_u8(crate::protocol::PEER_VIEW_RESPONSE).await?;
    stream.write_all(&response_payload).await?;
    stream.flush().await?;

    println!("[P2P-OWNER] 📤 Sent PEER_VIEW_RESPONSE (granted_views={})", requested_views);

    // Send encrypted image in chunks using PEER_IMAGE_CHUNK
    const CHUNK_SIZE: usize = 64 * 1024; // 64KB chunks
    let total_chunks = (encrypted_image.len() + CHUNK_SIZE - 1) / CHUNK_SIZE;

    println!("[P2P-OWNER] Sending encrypted image in {} chunks...", total_chunks);

    for (chunk_idx, chunk) in encrypted_image.chunks(CHUNK_SIZE).enumerate() {
        // Wire format: [chunk_index:u32][total_chunks:u32][chunk_data]
        let mut chunk_payload = Vec::new();
        chunk_payload.extend((chunk_idx as u32).to_le_bytes());
        chunk_payload.extend((total_chunks as u32).to_le_bytes());
        chunk_payload.extend_from_slice(chunk);

        // Send PEER_IMAGE_CHUNK message
        let total_len = 1 + chunk_payload.len();
        stream.write_all(&(total_len as u32).to_le_bytes()).await?;
        stream.write_u8(crate::protocol::PEER_IMAGE_CHUNK).await?;
        stream.write_all(&chunk_payload).await?;
        stream.flush().await?;

        println!("[P2P-OWNER] 📤 Sent chunk {}/{} ({} bytes)",
                 chunk_idx + 1, total_chunks, chunk.len());
    }

    println!("[P2P-OWNER] ✅ All chunks sent successfully");

    // Update local_access_map to track what was granted
    {
        let mut s = state.write().await;
        s.local_access_map.grant_access(
            &viewer,
            &image_name,
            requested_views,
        );

        // Save to disk
        if let Ok(path) = crate::local_access_map::LocalAccessMap::default_path() {
            if let Err(e) = s.local_access_map.save_to_file(&path) {
                eprintln!("[P2P-OWNER] ⚠️  Failed to save local_access_map: {}", e);
            } else {
                println!("[P2P-OWNER] 💾 Saved grant to local_access_map.json");
            }
        }
    }

    // Remove from pending requests
    {
        let mut s = state.write().await;
        s.pending_view_requests.remove(&request_id);
        println!("[P2P-OWNER] 🗑️  Removed request {} from pending", request_id);
    }

    println!("[P2P-OWNER] ✅ Approval complete! Viewer {} can now view {} ({} times)",
             viewer, image_name, requested_views);

    Ok(())
}

/// Deny a P2P view request
pub async fn deny_peer_view_request(
    state: SharedClientState,
    request_id: u32,
) -> Result<()> {
    println!("[P2P-OWNER] ❌ Denying P2P request {}", request_id);

    // Get request details
    let (viewer, image_name, viewer_addr) = {
        let s = state.read().await;
        let request = s.pending_view_requests.get(&request_id)
            .ok_or_else(|| anyhow::anyhow!("Request {} not found", request_id))?;

        (request.viewer.clone(), request.image_name.clone(), request.peer_addr)
    };

    // Connect to viewer to send rejection
    println!("[P2P-OWNER] Connecting to viewer at {}...", viewer_addr);
    let mut stream = tokio::net::TcpStream::connect(viewer_addr).await
        .context(format!("Failed to connect to viewer at {}", viewer_addr))?;

    // Send PEER_VIEW_REJECTED
    let mut payload = Vec::new();
    let reason = "Request denied by owner";
    payload.extend((reason.len() as u16).to_le_bytes());
    payload.extend_from_slice(reason.as_bytes());

    let total_len = 1 + payload.len();
    stream.write_all(&(total_len as u32).to_le_bytes()).await?;
    stream.write_u8(crate::protocol::PEER_VIEW_REJECTED).await?;
    stream.write_all(&payload).await?;
    stream.flush().await?;

    println!("[P2P-OWNER] 📤 Sent PEER_VIEW_REJECTED to {}", viewer);

    // Remove from pending requests
    {
        let mut s = state.write().await;
        s.pending_view_requests.remove(&request_id);
    }

    println!("[P2P-OWNER] ⚠️  Denied request from {} for {}", viewer, image_name);

    Ok(())
}

/// Get all pending view requests for the current user (as owner)
pub async fn get_pending_requests(state: SharedClientState) -> Vec<PendingViewRequest> {
    let s = state.read().await;
    s.pending_view_requests.values().cloned().collect()
}

// ============================================================================
// Phase 3.5: Adjust Request Approval/Rejection (Owner Side)
// ============================================================================

/// Approve viewer's adjust request and send new view count
pub async fn approve_adjust_request(
    state: SharedClientState,
    request_id: u32,
    approved_views: u32,
) -> Result<()> {
    // Get request details
    let (viewer, image_name, viewer_addr) = {
        let s = state.read().await;
        let req = s.incoming_adjust_requests.get(&request_id)
            .ok_or_else(|| anyhow::anyhow!("Adjust request {} not found", request_id))?;

        (req.viewer.clone(), req.image_name.clone(), req.peer_addr)
    };

    println!("[P2P_OWNER] Approving adjust request: viewer={}, image={}, views={}",
             viewer, image_name, approved_views);

    // Update LocalAccessMap
    {
        let mut s = state.write().await;
        s.local_access_map.adjust_views(&viewer, &image_name, approved_views);

        // Save to disk
        if let Ok(path) = crate::local_access_map::LocalAccessMap::default_path() {
            s.local_access_map.save_to_file(&path)?;
        }

        // Remove from incoming requests
        s.incoming_adjust_requests.remove(&request_id);
    }

    // Get viewer's P2P port
    let viewer_p2p_port = {
        let s = state.read().await;
        s.dos.clients.get(&viewer)
            .map(|c| c.client_port)
            .ok_or_else(|| anyhow::anyhow!("Viewer {} not in DOS", viewer))?
    };

    // Connect to viewer
    let viewer_ip = viewer_addr.ip();
    let viewer_p2p_addr = format!("{}:{}", viewer_ip, viewer_p2p_port);
    let mut stream = tokio::net::TcpStream::connect(&viewer_p2p_addr).await?;

    // Build payload: [image_name_len:u16][image_name][approved_views:u32]
    let mut payload = Vec::new();

    let image_bytes = image_name.as_bytes();
    payload.extend((image_bytes.len() as u16).to_le_bytes());
    payload.extend_from_slice(image_bytes);
    payload.extend(approved_views.to_le_bytes());

    // Send PEER_ADJUST_APPROVED
    let total_len = 1 + payload.len();
    stream.write_all(&(total_len as u32).to_le_bytes()).await?;
    stream.write_u8(crate::protocol::PEER_ADJUST_APPROVED).await?;
    stream.write_all(&payload).await?;
    stream.flush().await?;

    println!("[P2P_OWNER] Sent PEER_ADJUST_APPROVED with {} views", approved_views);

    Ok(())
}

/// Reject viewer's adjust request
pub async fn reject_adjust_request(
    state: SharedClientState,
    request_id: u32,
    reason: &str,
) -> Result<()> {
    // Get request details
    let (viewer, image_name, viewer_addr) = {
        let mut s = state.write().await;
        let req = s.incoming_adjust_requests.remove(&request_id)
            .ok_or_else(|| anyhow::anyhow!("Adjust request {} not found", request_id))?;

        (req.viewer.clone(), req.image_name.clone(), req.peer_addr)
    };

    println!("[P2P_OWNER] Rejecting adjust request: viewer={}, image={}, reason={}",
             viewer, image_name, reason);

    // Get viewer's P2P port
    let viewer_p2p_port = {
        let s = state.read().await;
        s.dos.clients.get(&viewer)
            .map(|c| c.client_port)
            .ok_or_else(|| anyhow::anyhow!("Viewer {} not in DOS", viewer))?
    };

    // Connect to viewer
    let viewer_ip = viewer_addr.ip();
    let viewer_p2p_addr = format!("{}:{}", viewer_ip, viewer_p2p_port);
    let mut stream = tokio::net::TcpStream::connect(&viewer_p2p_addr).await?;

    // Build payload: [image_name_len:u16][image_name][reason_len:u16][reason]
    let mut payload = Vec::new();

    let image_bytes = image_name.as_bytes();
    payload.extend((image_bytes.len() as u16).to_le_bytes());
    payload.extend_from_slice(image_bytes);

    let reason_bytes = reason.as_bytes();
    payload.extend((reason_bytes.len() as u16).to_le_bytes());
    payload.extend_from_slice(reason_bytes);

    // Send PEER_ADJUST_REJECTED
    let total_len = 1 + payload.len();
    stream.write_all(&(total_len as u32).to_le_bytes()).await?;
    stream.write_u8(crate::protocol::PEER_ADJUST_REJECTED).await?;
    stream.write_all(&payload).await?;
    stream.flush().await?;

    println!("[P2P_OWNER] Sent PEER_ADJUST_REJECTED");

    Ok(())
}

// ============================================================================
// Phase 4.3: Owner-Initiated Adjust Order
// ============================================================================

/// Send PEER_ADJUST_ORDER to viewer (owner forcing view count change)
pub async fn send_peer_adjust_order(
    state: SharedClientState,
    viewer: &str,
    image_name: &str,
    new_views: u32,
) -> Result<()> {
    println!("[P2P_OWNER] Sending adjust order: viewer={}, image={}, new_views={}",
             viewer, image_name, new_views);

    // Get viewer's P2P address
    let (viewer_ip, viewer_port) = {
        let s = state.read().await;
        let viewer_client = s.dos.clients.get(viewer)
            .ok_or_else(|| anyhow::anyhow!("Viewer {} not in DOS", viewer))?;

        if !viewer_client.online {
            return Err(anyhow::anyhow!("Viewer {} is offline", viewer));
        }

        (viewer_client.client_ip.clone(), viewer_client.client_port)
    };

    // Connect to viewer
    let mut stream = tokio::net::TcpStream::connect(format!("{}:{}", viewer_ip, viewer_port)).await?;

    // Build payload: [image_name_len:u16][image_name][new_views:u32]
    let mut payload = Vec::new();

    let image_bytes = image_name.as_bytes();
    payload.extend((image_bytes.len() as u16).to_le_bytes());
    payload.extend_from_slice(image_bytes);
    payload.extend(new_views.to_le_bytes());

    // Send PEER_ADJUST_ORDER
    let total_len = 1 + payload.len();
    stream.write_all(&(total_len as u32).to_le_bytes()).await?;
    stream.write_u8(crate::protocol::PEER_ADJUST_ORDER).await?;
    stream.write_all(&payload).await?;
    stream.flush().await?;

    println!("[P2P_OWNER] Sent PEER_ADJUST_ORDER");

    Ok(())
}

// ============================================================================
// Phase 5.1: Owner-Initiated Revoke
// ============================================================================

/// Send PEER_REVOKE to viewer (owner revoking access)
pub async fn send_peer_revoke(
    state: SharedClientState,
    viewer: &str,
    image_name: &str,
) -> Result<()> {
    println!("[P2P_OWNER] Sending revoke: viewer={}, image={}", viewer, image_name);

    // Get viewer's P2P address
    let (viewer_ip, viewer_port) = {
        let s = state.read().await;
        let viewer_client = s.dos.clients.get(viewer)
            .ok_or_else(|| anyhow::anyhow!("Viewer {} not in DOS", viewer))?;

        if !viewer_client.online {
            return Err(anyhow::anyhow!("Viewer {} is offline", viewer));
        }

        (viewer_client.client_ip.clone(), viewer_client.client_port)
    };

    // Connect to viewer
    let mut stream = tokio::net::TcpStream::connect(format!("{}:{}", viewer_ip, viewer_port)).await?;

    // Build payload: [image_name_len:u16][image_name]
    let mut payload = Vec::new();

    let image_bytes = image_name.as_bytes();
    payload.extend((image_bytes.len() as u16).to_le_bytes());
    payload.extend_from_slice(image_bytes);

    // Send PEER_REVOKE
    let total_len = 1 + payload.len();
    stream.write_all(&(total_len as u32).to_le_bytes()).await?;
    stream.write_u8(crate::protocol::PEER_REVOKE).await?;
    stream.write_all(&payload).await?;
    stream.flush().await?;

    println!("[P2P_OWNER] Sent PEER_REVOKE");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pending_view_request() {
        let req = PendingViewRequest {
            request_id: 123,
            viewer: "bob".to_string(),
            image_name: "secret.png".to_string(),
            requested_views: 5,
            peer_addr: "127.0.0.1:9080".parse().unwrap(),
            timestamp: 1000,
        };

        assert_eq!(req.viewer, "bob");
        assert_eq!(req.requested_views, 5);
    }
}

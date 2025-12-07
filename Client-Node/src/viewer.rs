//! Viewer operations for requesting and downloading images from owners

use anyhow::{Context, Result};
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

    println!("[VIEWER] Requesting image '{}' from owner '{}' (views={})", image_name, owner, requested_views);

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
}

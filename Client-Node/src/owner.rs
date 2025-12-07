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

/// Get all pending view requests for the current user (as owner)
pub async fn get_pending_requests(state: SharedClientState) -> Vec<PendingViewRequest> {
    let s = state.read().await;
    s.pending_view_requests.values().cloned().collect()
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
            timestamp: 1000,
        };

        assert_eq!(req.viewer, "bob");
        assert_eq!(req.requested_views, 5);
    }
}

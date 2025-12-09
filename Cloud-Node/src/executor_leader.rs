use anyhow::{Context, Result};
use tokio::net::UdpSocket;
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::state::SharedState;
use crate::firebase::{self, DosClient};

// Message types for executor-leader communication
pub const EXEC_ADD_CLIENT: u8 = 40;       // Executor → Leader: Add client to DOS-S
pub const EXEC_ADD_ACCESS: u8 = 41;       // Executor → Leader: Grant access
pub const EXEC_UPDATE_ACCESS: u8 = 42;    // Executor → Leader: Update consumed views
pub const EXEC_REVOKE_ACCESS: u8 = 43;    // Executor → Leader: Revoke access
pub const EXEC_UPDATE_CLIENT_STATUS: u8 = 44;  // Executor → Leader: Update client online status
pub const LEADER_ACK: u8 = 45;            // Leader → Executor: Success
pub const LEADER_ERROR: u8 = 46;          // Leader → Executor: Error

/// Run the executor-leader communication channel
/// Only the leader processes incoming messages
pub async fn run_executor_leader_channel(state: SharedState, cfg: Config) -> Result<()> {
    use std::sync::Arc;

    let bind_addr = cfg.executor_leader_bind_addr()
        .ok_or_else(|| anyhow::anyhow!("No executor-leader bind address"))?;

    info!("Starting executor-leader channel on {}", bind_addr);

    let sock = Arc::new(UdpSocket::bind(bind_addr).await
        .context("Failed to bind executor-leader socket")?);

    let mut buf = vec![0u8; 65536];

    loop {
        let (n, from) = match sock.recv_from(&mut buf).await {
            Ok(res) => res,
            Err(e) => {
                warn!("executor-leader recv error: {}", e);
                continue;
            }
        };

        if n == 0 {
            continue;
        }

        // Only leader processes messages
        let s = state.read().await;
        let is_leader = s.is_leader;
        let ignoring = s.ignoring;
        drop(s);

        if !is_leader || ignoring {
            continue;
        }

        let msg_type = buf[0];
        let data = buf[1..n].to_vec(); // Clone the data to move into spawn

        tokio::spawn({
            let state = state.clone();
            let sock = sock.clone();
            async move {
                let result = match msg_type {
                    EXEC_ADD_CLIENT => handle_add_client(state, &data).await,
                    // REMOVED Phase 2: EXEC_ADD_ACCESS, EXEC_UPDATE_ACCESS, EXEC_REVOKE_ACCESS - Access map now managed locally by clients
                    EXEC_UPDATE_CLIENT_STATUS => handle_update_client_status(state, &data).await,
                    _ => {
                        debug!("Unknown executor-leader message type: {}", msg_type);
                        Ok(())
                    }
                };

                // Send response
                let response = match result {
                    Ok(_) => vec![LEADER_ACK],
                    Err(e) => {
                        warn!("executor-leader handler error: {}", e);
                        let mut resp = vec![LEADER_ERROR];
                        let err_msg = e.to_string();
                        let err_bytes = err_msg.as_bytes();
                        resp.extend((err_bytes.len() as u16).to_le_bytes());
                        resp.extend_from_slice(err_bytes);
                        resp
                    }
                };

                if let Err(e) = sock.send_to(&response, from).await {
                    warn!("Failed to send executor-leader response: {}", e);
                }
            }
        });
    }
}

/// Handle ADD_CLIENT: Add or update client in DOS-S
async fn handle_add_client(state: SharedState, data: &[u8]) -> Result<()> {
    // Parse: [username_len:u16][username][ip_len:u16][ip][port:u16][num_images:u32][image_names...]
    let mut offset = 0;

    let username_len = u16::from_le_bytes(data[offset..offset + 2].try_into()?) as usize;
    offset += 2;
    let username = String::from_utf8(data[offset..offset + username_len].to_vec())?;
    offset += username_len;

    let ip_len = u16::from_le_bytes(data[offset..offset + 2].try_into()?) as usize;
    offset += 2;
    let client_ip = String::from_utf8(data[offset..offset + ip_len].to_vec())?;
    offset += ip_len;

    let client_port = u16::from_le_bytes(data[offset..offset + 2].try_into()?);
    offset += 2;

    let num_images = u32::from_le_bytes(data[offset..offset + 4].try_into()?) as usize;
    offset += 4;

    let mut actual_images = Vec::new();
    for _ in 0..num_images {
        let img_len = u16::from_le_bytes(data[offset..offset + 2].try_into()?) as usize;
        offset += 2;
        let img_name = String::from_utf8(data[offset..offset + img_len].to_vec())?;
        offset += img_len;
        actual_images.push(img_name);
    }

    // NEW P2P Architecture: All images are actual_images (no cover split)
    // Owner will provide cover per-request when encrypting

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis() as u64;

    // 🆕 MERGE images instead of replacing: Check if client already exists
    let db_opt = {
        let s = state.read().await;
        s.firestore_db.clone()
    };

    // If client already exists in Firebase, read and merge images
    let merged_images = if let Some(db) = &db_opt {
        match firebase::read_client(db, &username).await {
            Ok(Some(firebase_client)) => {
                // Merge: existing images + new images (avoiding duplicates)
                let mut all_images = firebase_client.actual_images.clone();
                let new_count = actual_images.len();
                for img in actual_images.into_iter() {
                    if !all_images.contains(&img) {
                        all_images.push(img.clone());
                        println!("[LEADER] 📝 Merging new image '{}' for client {}",
                                 img, username);
                    }
                }
                println!("[LEADER] ✅ Merged images for {}: {} existing + {} new = {} total",
                         username, firebase_client.actual_images.len(),
                         new_count, all_images.len());
                all_images
            }
            Ok(None) => {
                println!("[LEADER] ℹ️  New client {}, using {} provided images", username, actual_images.len());
                actual_images
            }
            Err(e) => {
                warn!("Failed to read existing client {} from Firebase: {}", username, e);
                println!("[LEADER] ⚠️  Using provided images due to Firebase error");
                actual_images
            }
        }
    } else {
        actual_images
    };

    let client = DosClient {
        client_name: username.clone(),
        client_ip,
        client_port,
        actual_images: merged_images,
        last_seen: now,
        online: true,
    };

    debug!("Leader writing client {} to Firebase with {} images", username, client.actual_images.len());

    // Write to Firebase
    if let Some(db) = &db_opt {
        firebase::write_client(db, &client).await?;
    }

    // Update local state
    let num_images = client.actual_images.len();
    let mut s = state.write().await;
    s.dos_clients.insert(username.clone(), client);
    s.dos_c_version += 1;
    drop(s);

    info!("Client {} added/updated in DOS-S with {} images", username, num_images);

    Ok(())
}

/// Handle ADD_ACCESS: Grant access to an image (DEPRECATED Phase 2 - removed)
#[allow(dead_code)]
async fn handle_add_access(_state: SharedState, _data: &[u8]) -> Result<()> {
    // REMOVED Phase 2: Access map now managed locally by clients only
    Err(anyhow::anyhow!("DEPRECATED: Server-side access management removed in Phase 2"))
}

/// Handle UPDATE_ACCESS: Update consumed views (DEPRECATED Phase 2 - unused)
#[allow(dead_code)]
/// Handle UPDATE_ACCESS: Update consumed views (DEPRECATED Phase 2 - unused)
#[allow(dead_code)]
async fn handle_update_access(_state: SharedState, _data: &[u8]) -> Result<()> {
    // REMOVED Phase 2: Access map now managed locally by clients only
    Err(anyhow::anyhow!("DEPRECATED: Server-side access management removed in Phase 2"))
}

/// Handle REVOKE_ACCESS: Revoke access to an image (DEPRECATED Phase 2 - unused)
#[allow(dead_code)]
/// Handle REVOKE_ACCESS: Revoke access to an image (DEPRECATED Phase 2 - unused)
#[allow(dead_code)]
async fn handle_revoke_access(_state: SharedState, _data: &[u8]) -> Result<()> {
    // REMOVED Phase 2: Access map now managed locally by clients only
    Err(anyhow::anyhow!("DEPRECATED: Server-side access management removed in Phase 2"))
}

/// Handle UPDATE_CLIENT_STATUS: Update client online status in DOS-S
async fn handle_update_client_status(state: SharedState, data: &[u8]) -> Result<()> {
    // Parse: [username_len:u16][username][online:u8]
    let username_len = u16::from_le_bytes(data[0..2].try_into()?) as usize;
    let username = String::from_utf8(data[2..2 + username_len].to_vec())?;
    let online = data[2 + username_len] != 0;

    debug!("Leader updating {} to online={}", username, online);

    // Update Firebase
    let s = state.read().await;
    if let Some(db) = &s.firestore_db {
        if let Some(client) = s.dos_clients.get(&username) {
            let mut updated = client.clone();
            updated.online = online;
            firebase::write_client(db, &updated).await?;
        }
    }
    drop(s);

    // Update local state
    let mut s = state.write().await;
    if let Some(client) = s.dos_clients.get_mut(&username) {
        client.online = online;
    }
    s.dos_c_version += 1;

    info!("Client {} online={}", username, online);
    Ok(())
}

/// Send a message to the leader (called by executor) with 5-retry exponential backoff
pub async fn send_to_leader(
    cfg: &Config,
    msg_type: u8,
    data: &[u8],
) -> Result<()> {
    let leader_addr = cfg.executor_leader_peer_addrs()
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("No leader address configured"))?;

    let mut msg = vec![msg_type];
    msg.extend_from_slice(data);

    // Retry configuration: 5 attempts with exponential backoff
    // Delays: 1s, 2s, 4s, 8s, 16s (total max wait: 31 seconds)
    let max_retries = 5;
    let mut last_error = None;

    for attempt in 1..=max_retries {
        // Create a new socket for each attempt (clean state)
        let sock = match UdpSocket::bind("0.0.0.0:0").await {
            Ok(s) => s,
            Err(e) => {
                warn!("Failed to bind UDP socket (attempt {}): {}", attempt, e);
                last_error = Some(anyhow::anyhow!("Socket bind failed: {}", e));

                // Wait before retrying (exponential backoff: 2^(attempt-1) seconds)
                if attempt < max_retries {
                    let delay_secs = 2u64.pow(attempt - 1);
                    debug!("Retrying in {} seconds...", delay_secs);
                    tokio::time::sleep(tokio::time::Duration::from_secs(delay_secs)).await;
                }
                continue;
            }
        };

        // Send message to leader
        if let Err(e) = sock.send_to(&msg, leader_addr).await {
            warn!("Failed to send to leader (attempt {}): {}", attempt, e);
            last_error = Some(anyhow::anyhow!("Send failed: {}", e));

            // Wait before retrying (exponential backoff)
            if attempt < max_retries {
                let delay_secs = 2u64.pow(attempt - 1);
                debug!("Retrying in {} seconds...", delay_secs);
                tokio::time::sleep(tokio::time::Duration::from_secs(delay_secs)).await;
            }
            continue;
        }

        // Wait for response with timeout
        let mut buf = vec![0u8; 65536];
        let timeout = tokio::time::Duration::from_secs(5);

        match tokio::time::timeout(timeout, sock.recv_from(&mut buf)).await {
            Ok(Ok((n, _))) => {
                if n > 0 && buf[0] == LEADER_ACK {
                    // Success!
                    if attempt > 1 {
                        info!("Leader communication succeeded on attempt {}/{}", attempt, max_retries);
                    }
                    return Ok(());
                } else if n > 2 && buf[0] == LEADER_ERROR {
                    let err_len = u16::from_le_bytes(buf[1..3].try_into()?) as usize;
                    let err_msg = String::from_utf8_lossy(&buf[3..3 + err_len]);
                    // Leader explicitly returned error - don't retry
                    return Err(anyhow::anyhow!("Leader error: {}", err_msg));
                } else {
                    warn!("Invalid response from leader (attempt {})", attempt);
                    last_error = Some(anyhow::anyhow!("Invalid response from leader"));
                }
            }
            Ok(Err(e)) => {
                warn!("Failed to receive from leader (attempt {}): {}", attempt, e);
                last_error = Some(anyhow::anyhow!("Receive failed: {}", e));
            }
            Err(_) => {
                warn!("Timeout waiting for leader response (attempt {})", attempt);
                last_error = Some(anyhow::anyhow!("Timeout waiting for leader response"));
            }
        }

        // Wait before retrying (exponential backoff: 2^(attempt-1) seconds)
        if attempt < max_retries {
            let delay_secs = 2u64.pow(attempt - 1);
            debug!("Retrying in {} seconds... (attempt {}/{})", delay_secs, attempt, max_retries);
            tokio::time::sleep(tokio::time::Duration::from_secs(delay_secs)).await;
        }
    }

    // All retries exhausted
    let final_error = last_error.unwrap_or_else(|| anyhow::anyhow!("All {} retries failed", max_retries));
    warn!("Failed to communicate with leader after {} attempts: {}", max_retries, final_error);
    Err(final_error)
}

use anyhow::{Context, Result};
use std::net::SocketAddr;
use tokio::net::UdpSocket;
use tracing::{debug, info, warn, error};

use crate::config::Config;
use crate::state::SharedState;
use crate::firebase::{self, DosClient, DosAccess};

// Message types for executor-leader communication
pub const EXEC_ADD_CLIENT: u8 = 40;       // Executor → Leader: Add client to DOS-S
pub const EXEC_ADD_ACCESS: u8 = 41;       // Executor → Leader: Grant access
pub const EXEC_UPDATE_ACCESS: u8 = 42;    // Executor → Leader: Update consumed views
pub const EXEC_REVOKE_ACCESS: u8 = 43;    // Executor → Leader: Revoke access
pub const EXEC_DELETE_CLIENT: u8 = 44;    // Executor → Leader: Remove offline client
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
                    EXEC_ADD_ACCESS => handle_add_access(state, &data).await,
                    EXEC_UPDATE_ACCESS => handle_update_access(state, &data).await,
                    EXEC_REVOKE_ACCESS => handle_revoke_access(state, &data).await,
                    EXEC_DELETE_CLIENT => handle_delete_client(state, &data).await,
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

    let mut images = Vec::new();
    for _ in 0..num_images {
        let img_len = u16::from_le_bytes(data[offset..offset + 2].try_into()?) as usize;
        offset += 2;
        let img_name = String::from_utf8(data[offset..offset + img_len].to_vec())?;
        offset += img_len;
        images.push(img_name);
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis();

    let client = DosClient {
        client_name: username.clone(),
        client_ip,
        client_port,
        images,
        last_seen: now,
        online: true,
    };

    debug!("Leader writing client {} to Firebase", username);

    // Write to Firebase
    let s = state.read().await;
    if let Some(db) = &s.firestore_db {
        firebase::write_client(db, &client).await?;
    }
    drop(s);

    // Update local state
    let mut s = state.write().await;
    s.dos_clients.insert(username.clone(), client);
    s.dos_c_version += 1;
    drop(s);

    info!("Client {} added to DOS-S", username);

    Ok(())
}

/// Handle ADD_ACCESS: Grant access to an image
async fn handle_add_access(state: SharedState, data: &[u8]) -> Result<()> {
    // Parse: [access_id_len:u16][access_id][owner_len:u16][owner][viewer_len:u16][viewer]
    //        [image_name_len:u16][image_name][granted_views:u32][image_uuid_len:u16][image_uuid]
    let mut offset = 0;

    let access_id_len = u16::from_le_bytes(data[offset..offset + 2].try_into()?) as usize;
    offset += 2;
    let access_id = String::from_utf8(data[offset..offset + access_id_len].to_vec())?;
    offset += access_id_len;

    let owner_len = u16::from_le_bytes(data[offset..offset + 2].try_into()?) as usize;
    offset += 2;
    let owner = String::from_utf8(data[offset..offset + owner_len].to_vec())?;
    offset += owner_len;

    let viewer_len = u16::from_le_bytes(data[offset..offset + 2].try_into()?) as usize;
    offset += 2;
    let viewer = String::from_utf8(data[offset..offset + viewer_len].to_vec())?;
    offset += viewer_len;

    let image_name_len = u16::from_le_bytes(data[offset..offset + 2].try_into()?) as usize;
    offset += 2;
    let image_name = String::from_utf8(data[offset..offset + image_name_len].to_vec())?;
    offset += image_name_len;

    let granted_views = u32::from_le_bytes(data[offset..offset + 4].try_into()?);
    offset += 4;

    let image_uuid_len = u16::from_le_bytes(data[offset..offset + 2].try_into()?) as usize;
    offset += 2;
    let image_uuid = String::from_utf8(data[offset..offset + image_uuid_len].to_vec())?;

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis();

    let access = DosAccess {
        owner,
        viewer,
        image_name,
        granted_views,
        consumed_views: 0,
        revoked: false,
        granted_at: now,
        image_uuid,
    };

    debug!("Leader writing access {} to Firebase", access_id);

    // Write to Firebase
    let s = state.read().await;
    if let Some(db) = &s.firestore_db {
        firebase::write_access(db, &access_id, &access).await?;
    }
    drop(s);

    // Update local state
    let mut s = state.write().await;
    s.dos_access.insert(access_id.clone(), access);
    drop(s);

    info!("Access {} added to DOS-S", access_id);

    Ok(())
}

/// Handle UPDATE_ACCESS: Update consumed views
async fn handle_update_access(state: SharedState, data: &[u8]) -> Result<()> {
    // Parse: [access_id_len:u16][access_id][consumed_views:u32]
    let mut offset = 0;

    let access_id_len = u16::from_le_bytes(data[offset..offset + 2].try_into()?) as usize;
    offset += 2;
    let access_id = String::from_utf8(data[offset..offset + access_id_len].to_vec())?;
    offset += access_id_len;

    let consumed_views = u32::from_le_bytes(data[offset..offset + 4].try_into()?);

    debug!("Leader updating access {} consumed views to {}", access_id, consumed_views);

    // Clone firestore_db first
    let db = {
        let s = state.read().await;
        s.firestore_db.clone()
    };

    // Update local state
    let mut s = state.write().await;
    if let Some(access) = s.dos_access.get_mut(&access_id) {
        access.consumed_views = consumed_views;
        let access_clone = access.clone();
        drop(s);

        // Write to Firebase
        if let Some(db) = db {
            firebase::write_access(&db, &access_id, &access_clone).await?;
        }

        info!("Access {} consumed views updated to {}", access_id, consumed_views);
        Ok(())
    } else {
        drop(s);
        Err(anyhow::anyhow!("Access record not found: {}", access_id))
    }
}

/// Handle REVOKE_ACCESS: Revoke access to an image
async fn handle_revoke_access(state: SharedState, data: &[u8]) -> Result<()> {
    // Parse: [access_id_len:u16][access_id]
    let access_id_len = u16::from_le_bytes(data[0..2].try_into()?) as usize;
    let access_id = String::from_utf8(data[2..2 + access_id_len].to_vec())?;

    debug!("Leader revoking access {}", access_id);

    // Clone firestore_db first
    let db = {
        let s = state.read().await;
        s.firestore_db.clone()
    };

    // Update local state
    let mut s = state.write().await;
    if let Some(access) = s.dos_access.get_mut(&access_id) {
        access.revoked = true;
        let access_clone = access.clone();
        drop(s);

        // Write to Firebase
        if let Some(db) = db {
            firebase::write_access(&db, &access_id, &access_clone).await?;
        }

        info!("Access {} revoked", access_id);
        Ok(())
    } else {
        drop(s);
        Err(anyhow::anyhow!("Access record not found: {}", access_id))
    }
}

/// Handle DELETE_CLIENT: Remove client from DOS-S (when offline)
async fn handle_delete_client(state: SharedState, data: &[u8]) -> Result<()> {
    // Parse: [username_len:u16][username]
    let username_len = u16::from_le_bytes(data[0..2].try_into()?) as usize;
    let username = String::from_utf8(data[2..2 + username_len].to_vec())?;

    debug!("Leader deleting client {} from Firebase", username);

    // Delete from Firebase (but keep access records)
    let s = state.read().await;
    if let Some(db) = &s.firestore_db {
        firebase::delete_client(db, &username).await?;
    }
    drop(s);

    // Update local state
    let mut s = state.write().await;
    s.dos_clients.remove(&username);
    s.dos_c_version += 1;
    drop(s);

    info!("Client {} removed from DOS-S", username);

    Ok(())
}

/// Send a message to the leader (called by executor)
pub async fn send_to_leader(
    cfg: &Config,
    msg_type: u8,
    data: &[u8],
) -> Result<()> {
    let leader_addr = cfg.executor_leader_peer_addrs()
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("No leader address configured"))?;

    let sock = UdpSocket::bind("0.0.0.0:0").await?;

    let mut msg = vec![msg_type];
    msg.extend_from_slice(data);

    sock.send_to(&msg, leader_addr).await?;

    // Wait for response with timeout
    let mut buf = vec![0u8; 65536];
    let timeout = tokio::time::Duration::from_secs(5);

    match tokio::time::timeout(timeout, sock.recv_from(&mut buf)).await {
        Ok(Ok((n, _))) => {
            if n > 0 && buf[0] == LEADER_ACK {
                Ok(())
            } else if n > 2 && buf[0] == LEADER_ERROR {
                let err_len = u16::from_le_bytes(buf[1..3].try_into()?) as usize;
                let err_msg = String::from_utf8_lossy(&buf[3..3 + err_len]);
                Err(anyhow::anyhow!("Leader error: {}", err_msg))
            } else {
                Err(anyhow::anyhow!("Invalid response from leader"))
            }
        }
        Ok(Err(e)) => Err(anyhow::anyhow!("Failed to receive from leader: {}", e)),
        Err(_) => Err(anyhow::anyhow!("Timeout waiting for leader response")),
    }
}

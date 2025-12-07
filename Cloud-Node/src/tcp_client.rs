//! TCP server for client connections
//! Handles all client-facing communication over reliable TCP connections

use anyhow::{Context, Result};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{info, warn};

use crate::{
    client_protocol,
    config::Config,
    owner_image,
    state::{SharedState, StoredImageAssets},
};

/// Run TCP server for client connections
pub async fn run_tcp_client_server(
    state: SharedState,
    cfg: Config,
) -> Result<()> {
    let bind_addr = cfg.tcp_client_bind_addr()
        .context("TCP client bind address not configured")?;

    let listener = TcpListener::bind(bind_addr).await
        .context(format!("Failed to bind TCP listener to {}", bind_addr))?;

    println!("🚀 TCP client server listening on {}", bind_addr);
    info!(%bind_addr, "TCP client server started");

    loop {
        match listener.accept().await {
            Ok((stream, peer_addr)) => {
                info!(%peer_addr, "New client TCP connection");
                let state_clone = state.clone();
                let cfg_clone = cfg.clone();

                tokio::spawn(async move {
                    if let Err(e) = handle_client_connection(stream, peer_addr, state_clone, cfg_clone).await {
                        warn!(%peer_addr, error=%e, "Client connection error");
                    }
                });
            }
            Err(e) => {
                warn!(error=%e, "Failed to accept TCP connection");
            }
        }
    }
}

/// Handle a single client TCP connection
async fn handle_client_connection(
    stream: TcpStream,
    peer_addr: SocketAddr,
    state: SharedState,
    cfg: Config,
) -> Result<()> {
    info!(%peer_addr, "Client connection handler started");

    // Store stream in state for sending responses
    let stream = Arc::new(tokio::sync::Mutex::new(stream));

    // Track username for this connection (set when JOIN is received)
    let mut connected_username: Option<String> = None;

    let mut buf = vec![0u8; 65536];

    loop {
        // Read message length prefix (4 bytes)
        let mut len_buf = [0u8; 4];
        let stream_clone = stream.clone();
        let n = {
            let mut s = stream_clone.lock().await;
            match s.read_exact(&mut len_buf).await {
                Ok(_) => u32::from_le_bytes(len_buf) as usize,
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    info!(%peer_addr, "Client disconnected");
                    break;
                }
                Err(e) => {
                    warn!(%peer_addr, error=%e, "Failed to read message length");
                    break;
                }
            }
        };

        if n == 0 || n > 65536 {
            warn!(%peer_addr, len=n, "Invalid message length");
            break;
        }

        // Read message data
        let stream_clone = stream.clone();
        let data = {
            let mut s = stream_clone.lock().await;
            match s.read_exact(&mut buf[..n]).await {
                Ok(_) => buf[..n].to_vec(),
                Err(e) => {
                    warn!(%peer_addr, error=%e, "Failed to read message data");
                    break;
                }
            }
        };

        if data.is_empty() {
            continue;
        }

        let msg_type = data[0];
        let payload = data[1..].to_vec(); // Clone payload for 'static lifetime

        // Check if this is a JOIN message to track username
        if msg_type == crate::client_protocol::JOIN && connected_username.is_none() {
            // Parse username from JOIN payload
            if payload.len() >= 2 {
                let username_len = u16::from_le_bytes([payload[0], payload[1]]) as usize;
                if payload.len() >= 2 + username_len {
                    if let Ok(username) = String::from_utf8(payload[2..2 + username_len].to_vec()) {
                        connected_username = Some(username.clone());
                        println!("[CONNECTION] Client {} connected from {}", username, peer_addr);
                    }
                }
            }
        }

        // Route message to appropriate handler synchronously so responses are sent
        // on the same task without any scheduling delay/races.
        if let Err(e) = route_client_message(
            msg_type,
            &payload,
            peer_addr,
            state.clone(),
            cfg.clone(),
            stream.clone(),
        ).await {
            warn!(%peer_addr, msg_type, error=%e, "Message handler error");
            break;
        }
    }

    // Cleanup on disconnect
    if let Some(username) = connected_username {
        println!("[CONNECTION] Client {} disconnected from {}", username, peer_addr);

        let mut s = state.write().await;
        s.client_connections.remove(&username);

        // Mark client as offline in dos_clients
        if let Some(client) = s.dos_clients.get_mut(&username) {
            client.online = false;
        }

        // Increment DOS version to signal change
        s.dos_c_version += 1;

        println!("[CONNECTION] Removed connection for {} from registry", username);
    }

    Ok(())
}

fn save_embedded(owner: &str, image: &str, data: &[u8]) -> Result<()> {
    use std::fs;
    use std::path::Path;
    let dir = Path::new("received");
    fs::create_dir_all(dir)?;
    let path = dir.join(format!("{}_{}_embedded.png", owner, image));
    fs::write(path, data)?;
    Ok(())
}

/// Route client message to appropriate handler
async fn route_client_message(
    msg_type: u8,
    payload: &[u8],
    peer_addr: SocketAddr,
    state: SharedState,
    cfg: Config,
    stream: Arc<tokio::sync::Mutex<TcpStream>>,
) -> Result<()> {
    match msg_type {
        x if x == client_protocol::REQ => {
            info!(%peer_addr, len=payload.len(), "Received REQ from client");
            handle_req_tcp(state, &cfg, stream, peer_addr, payload).await
        }

        x if x == client_protocol::JOIN => {
            info!(%peer_addr, len=payload.len(), "Received JOIN from client");
            handle_join_tcp(state, &cfg, stream, peer_addr, payload).await
        }

        x if x == client_protocol::CLIENT_PING => {
            info!(%peer_addr, len=payload.len(), "Received CLIENT_PING from client");
            handle_client_ping_tcp(state, stream, peer_addr, payload).await
        }

        x if x == client_protocol::VIEW_REQUEST => {
            info!(%peer_addr, len=payload.len(), "Received VIEW_REQUEST from client");
            handle_view_request_tcp(state, &cfg, stream, peer_addr, payload).await
        }

        x if x == client_protocol::DENY_VIEW => {
            info!(%peer_addr, len=payload.len(), "Received DENY_VIEW from client");
            handle_deny_view_tcp(state, stream, peer_addr, payload).await
        }

        x if x == client_protocol::APPROVE_VIEW => {
            info!(%peer_addr, len=payload.len(), "Received APPROVE_VIEW from client");
            handle_approve_view_tcp(state, &cfg, stream, peer_addr, payload).await
        }

        x if x == client_protocol::OWNER_IMAGE_META => {
            info!(%peer_addr, len=payload.len(), "Received OWNER_IMAGE_META from client");
            handle_owner_image_meta(state, payload).await
        }

        x if x == client_protocol::OWNER_IMAGE_CHUNK => {
            info!(%peer_addr, len=payload.len(), "Received OWNER_IMAGE_CHUNK from client");
            handle_owner_image_chunk(state, stream, peer_addr, payload).await
        }

        x if x == client_protocol::SYNC_USAGE => {
            info!(%peer_addr, len=payload.len(), "Received SYNC_USAGE from client");
            handle_sync_usage_tcp(state, &cfg, stream, peer_addr, payload).await
        }

        x if x == client_protocol::DOS_QUERY => {
            info!(%peer_addr, len=payload.len(), "Received DOS_QUERY from client");
            handle_dos_query_tcp(state, stream, peer_addr, payload).await
        }

        _ => {
            warn!(%peer_addr, msg_type, "Unknown message type from client");
            Ok(())
        }
    }
}

/// Send TCP response to client
async fn send_tcp_response(
    stream: Arc<tokio::sync::Mutex<TcpStream>>,
    msg_type: u8,
    payload: &[u8],
) -> Result<()> {
    let mut msg = Vec::new();

    // Length prefix (message type + payload)
    let total_len = 1 + payload.len();
    msg.extend((total_len as u32).to_le_bytes());

    // Message type + payload
    msg.push(msg_type);
    msg.extend_from_slice(payload);

    let mut s = stream.lock().await;
    s.write_all(&msg).await?;
    s.flush().await?;

    // Structured log (shows up in output.txt) with exact bytes written
    let len_prefix = u32::from_le_bytes(msg[0..4].try_into().unwrap());
    info!(
        peer=?s.peer_addr().ok(),
        len_prefix,
        total_len,
        msg_type,
        payload_len=payload.len(),
        bytes=?msg,
        "[TCP-SEND] response"
    );
    println!(
        "[TCP-SEND] to {} len_prefix={} total_len={} msg_type={} payload_len={} bytes={:02x?}",
        s.peer_addr().unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], 0))),
        len_prefix,
        total_len,
        msg_type,
        payload.len(),
        msg
    );

    Ok(())
}

// TCP-specific handlers that wrap the original protocol handlers

async fn handle_req_tcp(
    state: SharedState,
    cfg: &Config,
    _stream: Arc<tokio::sync::Mutex<TcpStream>>,
    peer_addr: SocketAddr,
    data: &[u8],
) -> Result<()> {
    // For now, call the original handler
    // In the future, we'll need to refactor to use TCP stream for responses
    let temp_sock = tokio::net::UdpSocket::bind("0.0.0.0:0").await?;
    client_protocol::handle_req(state, cfg, &temp_sock, peer_addr, data).await
}

async fn handle_join_tcp(
    state: SharedState,
    cfg: &Config,
    stream: Arc<tokio::sync::Mutex<TcpStream>>,
    peer_addr: SocketAddr,
    data: &[u8],
) -> Result<()> {
    println!("[HANDLE_JOIN_TCP] Called with {} bytes from {}", data.len(), peer_addr);

    // Parse: [username_len:u16][username][port:u16][num_images:u32][image_names...]
    let mut offset = 0;

    if data.len() < 2 {
        return Err(anyhow::anyhow!("JOIN message too short: {} bytes", data.len()));
    }

    let username_len = u16::from_le_bytes(data[offset..offset + 2].try_into()?) as usize;
    offset += 2;
    println!("[HANDLE_JOIN_TCP] username_len={}, offset={}", username_len, offset);

    if offset + username_len > data.len() {
        return Err(anyhow::anyhow!("Invalid username length: {} (data len={})", username_len, data.len()));
    }

    let username = String::from_utf8(data[offset..offset + username_len].to_vec())?;
    offset += username_len;
    println!("[HANDLE_JOIN_TCP] username={}, offset={}", username, offset);

    if offset + 2 > data.len() {
        return Err(anyhow::anyhow!("Not enough data for port"));
    }
    let client_port = u16::from_le_bytes(data[offset..offset + 2].try_into()?);
    offset += 2;
    println!("[HANDLE_JOIN_TCP] client_port={}, offset={}", client_port, offset);

    if offset + 4 > data.len() {
        return Err(anyhow::anyhow!("Not enough data for num_images"));
    }
    let num_images = u32::from_le_bytes(data[offset..offset + 4].try_into()?) as usize;
    offset += 4;
    println!("[HANDLE_JOIN_TCP] num_images={}, offset={}", num_images, offset);

    // Parse image names
    let mut images = Vec::new();
    for i in 0..num_images {
        if offset + 2 > data.len() {
            return Err(anyhow::anyhow!("Not enough data for image {} name length", i));
        }
        let img_len = u16::from_le_bytes(data[offset..offset + 2].try_into()?) as usize;
        offset += 2;

        if offset + img_len > data.len() {
            return Err(anyhow::anyhow!("Not enough data for image {} name", i));
        }
        let img_name = String::from_utf8(data[offset..offset + img_len].to_vec())?;
        offset += img_len;
        images.push(img_name);
    }

    println!("[HANDLE_JOIN_TCP] ✅ Parsed: username={} port={} images={:?}", username, client_port, images);

    // Split images into cover and actual
    // If num_images > 1: first = cover, rest = actual
    // If num_images == 1: single image = actual, no cover
    let (cover_image, actual_images) = if images.len() > 1 {
        (Some(images[0].clone()), images[1..].to_vec())
    } else {
        (None, images)
    };

    println!("[HANDLE_JOIN_TCP] Split images: cover={:?}, actual={:?}", cover_image, actual_images);

    // Register client in state
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis() as u64;

    {
        let mut s = state.write().await;
        s.dos_clients.insert(
            username.clone(),
            crate::firebase::DosClient {
                client_name: username.clone(),
                client_ip: peer_addr.ip().to_string(),
                client_port: client_port,
                cover_image: cover_image.clone(),
                actual_images: actual_images.clone(),
                online: true,
                last_seen: now_ms,
            },
        );
        s.dos_c_version += 1;

        // Store TCP connection in registry
        s.client_connections.insert(username.clone(), stream.clone());
        println!("[HANDLE_JOIN_TCP] Stored TCP connection for {} in registry", username);
    }

    // Build MINIMAL DOS-C payload for JOIN_ACK (v2.0)
    // Excludes: requesting client (self-exclusion), IP, port, last_seen, online
    // Includes: dos_version, name, actual_images only
    let dos_payload = {
        let s = state.read().await;
        let mut payload = Vec::new();

        // DOS version (u64) - Cast u32 to u64 for wire format!
        let dos_version_u64 = s.dos_c_version as u64;
        payload.extend(dos_version_u64.to_le_bytes());

        // Self-exclusion: filter out requesting client
        let clients_to_send: Vec<_> = s.dos_clients
            .iter()
            .filter(|(name, _)| *name != &username)  // EXCLUDE SELF
            .collect();

        // Number of clients (u32) - excludes requesting client
        let num_clients = clients_to_send.len() as u32;
        payload.extend(num_clients.to_le_bytes());

        println!("[HANDLE_JOIN_TCP] Building MINIMAL DOS-C v2.0: version={}, num_clients={} (excluded: {})",
                 dos_version_u64, num_clients, username);

        // For each client (MINIMAL FORMAT: name + actual_images only)
        for (client_name, client) in clients_to_send {
            // Username
            let name_bytes = client_name.as_bytes();
            payload.extend((name_bytes.len() as u16).to_le_bytes());
            payload.extend_from_slice(name_bytes);

            // Number of images (u32) - ONLY actual images (no cover)
            payload.extend((client.actual_images.len() as u32).to_le_bytes());

            // Image names (actual images only, cover not included in DOS-C)
            for img in &client.actual_images {
                let img_bytes = img.as_bytes();
                payload.extend((img_bytes.len() as u16).to_le_bytes());
                payload.extend_from_slice(img_bytes);
            }

            // NO IP, NO PORT, NO LAST_SEEN, NO ONLINE - minimal format!

            println!("[HANDLE_JOIN_TCP]   ✅ {} -> actual_images={:?} (cover={:?} kept server-side)",
                client_name, client.actual_images, client.cover_image);
        }

        payload
    };

    // Send JOIN_ACK with DOS-C payload
    println!("[HANDLE_JOIN_TCP] Sending JOIN_ACK with DOS-C ({} bytes) to {}", dos_payload.len(), peer_addr);
    send_tcp_response(stream.clone(), client_protocol::JOIN_ACK, &dos_payload).await?;

    // Notify leader to write to Firebase (send cover + actual images)
    if let Err(e) = notify_leader_add_client(cfg, &username, peer_addr.ip().to_string(), client_port, cover_image.clone(), actual_images.clone()).await {
        warn!("Failed to notify leader about new client: {}", e);
    }

    println!("[HANDLE_JOIN_TCP] ✅ Client {} registered successfully", username);
    Ok(())
}

async fn handle_client_ping_tcp(
    state: SharedState,
    stream: Arc<tokio::sync::Mutex<TcpStream>>,
    _peer_addr: SocketAddr,
    data: &[u8],
) -> Result<()> {
    // Parse username
    if data.len() < 2 {
        return Err(anyhow::anyhow!("PING too short"));
    }

    let username_len = u16::from_le_bytes(data[0..2].try_into()?) as usize;
    if data.len() < 2 + username_len {
        return Err(anyhow::anyhow!("PING invalid username length"));
    }

    let username = String::from_utf8(data[2..2 + username_len].to_vec())?;

    // Update last seen
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis() as u64;

    let dos_version = {
        let mut s = state.write().await;
        if let Some(client) = s.dos_clients.get_mut(&username) {
            client.last_seen = now_ms;
            client.online = true;
        }
        s.dos_c_version
    };

    // Send SERVER_PONG with DOS version
    let mut pong_data = Vec::new();
    pong_data.extend(dos_version.to_le_bytes());
    send_tcp_response(stream, client_protocol::SERVER_PONG, &pong_data).await?;

    println!("[PING] {} (DOS version={})", username, dos_version);
    Ok(())
}

/// Handle DOS_QUERY: Client requests updated DOS-C
async fn handle_dos_query_tcp(
    state: SharedState,
    stream: Arc<tokio::sync::Mutex<TcpStream>>,
    peer_addr: SocketAddr,
    data: &[u8],
) -> Result<()> {
    // Parse: [username_len:u16][username]
    if data.len() < 2 {
        return Err(anyhow::anyhow!("DOS_QUERY too short: {} bytes", data.len()));
    }

    let username_len = u16::from_le_bytes(data[0..2].try_into()?) as usize;
    if data.len() < 2 + username_len {
        return Err(anyhow::anyhow!("DOS_QUERY invalid username length"));
    }

    let username = String::from_utf8(data[2..2 + username_len].to_vec())?;

    println!("[DOS_QUERY] Request from {} ({})", username, peer_addr);

    // Build MINIMAL DOS-C v2.0 payload (same format as JOIN_ACK)
    // Excludes: requesting client (self-exclusion), IP, port, last_seen, online
    // Includes: dos_version, name, actual_images only
    let dos_payload = {
        let s = state.read().await;
        let mut payload = Vec::new();

        // DOS version (u64) - Cast u32 to u64 for wire format!
        let dos_version_u64 = s.dos_c_version as u64;
        payload.extend(dos_version_u64.to_le_bytes());

        // Self-exclusion: filter out requesting client
        let clients_to_send: Vec<_> = s.dos_clients
            .iter()
            .filter(|(name, _)| *name != &username)  // EXCLUDE SELF
            .collect();

        // Number of clients (u32) - excludes requesting client
        let num_clients = clients_to_send.len() as u32;
        payload.extend(num_clients.to_le_bytes());

        println!("[DOS_QUERY] Building MINIMAL DOS-C v2.0: version={}, num_clients={} (excluded: {})",
                 dos_version_u64, num_clients, username);

        // For each client (MINIMAL FORMAT: name + actual_images only)
        for (client_name, client) in clients_to_send {
            // Username
            let name_bytes = client_name.as_bytes();
            payload.extend((name_bytes.len() as u16).to_le_bytes());
            payload.extend_from_slice(name_bytes);

            // Number of images (u32) - ONLY actual images (no cover)
            payload.extend((client.actual_images.len() as u32).to_le_bytes());

            // Image names (actual images only, cover not included in DOS-C)
            for img in &client.actual_images {
                let img_bytes = img.as_bytes();
                payload.extend((img_bytes.len() as u16).to_le_bytes());
                payload.extend_from_slice(img_bytes);
            }

            println!("[DOS_QUERY]   ✅ {} -> actual_images={:?}",
                client_name, client.actual_images);
        }

        payload
    };

    // Send DOS_UPDATE with DOS-C payload
    println!("[DOS_QUERY] Sending DOS_UPDATE with DOS-C ({} bytes) to {}", dos_payload.len(), peer_addr);
    send_tcp_response(stream.clone(), client_protocol::DOS_UPDATE, &dos_payload).await?;

    Ok(())
}

async fn handle_view_request_tcp(
    state: SharedState,
    _cfg: &Config,
    viewer_stream: Arc<tokio::sync::Mutex<TcpStream>>,
    peer_addr: SocketAddr,
    data: &[u8],
) -> Result<()> {
    // Parse VIEW_REQUEST: [req_id:u32][viewer_len:u16][viewer][owner_len:u16][owner]
    //                      [image_name_len:u16][image_name][requested_views:u32]
    let mut offset = 0;

    if data.len() < 4 {
        return Err(anyhow::anyhow!("VIEW_REQUEST too short"));
    }

    if data.len() < offset + 4 {
        return Err(anyhow::anyhow!("VIEW_REQUEST too short for req_id"));
    }
    let req_id = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
    offset += 4;

    println!("[SERVER] VIEW_REQUEST received: req_id={} from {}", req_id, peer_addr);

    if data.len() < offset + 2 {
        return Err(anyhow::anyhow!("VIEW_REQUEST too short for viewer_len"));
    }
    let viewer_len = u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap()) as usize;
    offset += 2;

    if data.len() < offset + viewer_len {
        return Err(anyhow::anyhow!("VIEW_REQUEST too short for viewer_name"));
    }
    let viewer_name = String::from_utf8(data[offset..offset + viewer_len].to_vec())?;
    offset += viewer_len;

    if data.len() < offset + 2 {
        return Err(anyhow::anyhow!("VIEW_REQUEST too short for owner_len"));
    }
    let owner_len = u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap()) as usize;
    offset += 2;

    if data.len() < offset + owner_len {
        return Err(anyhow::anyhow!("VIEW_REQUEST too short for owner_name"));
    }
    let owner_name = String::from_utf8(data[offset..offset + owner_len].to_vec())?;
    offset += owner_len;

    if data.len() < offset + 2 {
        return Err(anyhow::anyhow!("VIEW_REQUEST too short for image_len"));
    }
    let image_name_len = u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap()) as usize;
    offset += 2;

    if data.len() < offset + image_name_len {
        return Err(anyhow::anyhow!("VIEW_REQUEST too short for image_name"));
    }
    let image_name = String::from_utf8(data[offset..offset + image_name_len].to_vec())?;
    offset += image_name_len;

    if data.len() < offset + 4 {
        return Err(anyhow::anyhow!("VIEW_REQUEST too short for requested_views"));
    }
    let requested_views = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());

    println!("[SERVER] VIEW_REQUEST details: viewer={}, owner={}, image={}, views={}",
             viewer_name, owner_name, image_name, requested_views);

    // Look up owner in dos_clients
    println!("[SERVER] Looking up owner '{}' in dos_clients...", owner_name);

    let (owner_client, owner_connection) = {
        let s = state.read().await;

        let owner_client = match s.dos_clients.get(&owner_name) {
            Some(c) => c.clone(),
            None => {
                println!("[SERVER] ⚠️  Owner '{}' not found in dos_clients", owner_name);
                // Send REJECTED to viewer
                let mut resp = vec![crate::client_protocol::REJECTED];
                resp.extend(req_id.to_le_bytes());
                let reason = "Owner not found";
                resp.extend((reason.len() as u16).to_le_bytes());
                resp.extend(reason.as_bytes());
                send_tcp_response(viewer_stream, crate::client_protocol::REJECTED, &resp[1..]).await?;
                return Ok(());
            }
        };

        let owner_connection = s.client_connections.get(&owner_name).cloned();

        (owner_client, owner_connection)
    };

    println!("[SERVER] Owner found: ip={}, port={}, online={}, connected={}",
             owner_client.client_ip, owner_client.client_port, owner_client.online,
             owner_connection.is_some());

    // Check if owner is connected
    let owner_stream = match owner_connection {
        Some(stream) => stream,
        None => {
            println!("[SERVER] ⚠️  Owner '{}' not connected (no active TCP connection)", owner_name);
            // Send REJECTED to viewer
            let mut resp = vec![crate::client_protocol::REJECTED];
            resp.extend(req_id.to_le_bytes());
            let reason = "Owner not connected";
            resp.extend((reason.len() as u16).to_le_bytes());
            resp.extend(reason.as_bytes());
            send_tcp_response(viewer_stream, crate::client_protocol::REJECTED, &resp[1..]).await?;
            return Ok(());
        }
    };

    // Create pending request
    {
        let mut s = state.write().await;
        s.pending_requests.insert(req_id, crate::state::PendingRequest {
            req_id,
            executor_ip: peer_addr.ip(),
            req_type: crate::state::RequestType::View,
            owner_name: owner_name.clone(),
            viewer_name: viewer_name.clone(),
            image_name: image_name.clone(),
            initiated_at: client_protocol::now_ms(),
        });
    }

    // Build VIEW_NOTIFICATION payload
    let mut notif = Vec::new();
    notif.extend((viewer_name.len() as u16).to_le_bytes());
    notif.extend(viewer_name.as_bytes());
    notif.extend((image_name.len() as u16).to_le_bytes());
    notif.extend(image_name.as_bytes());
    notif.extend(req_id.to_le_bytes());
    notif.extend(requested_views.to_le_bytes());

    // Send VIEW_NOTIFICATION to owner via TCP
    println!("[SERVER] Sending VIEW_NOTIFICATION to owner '{}' via TCP...", owner_name);
    if let Err(e) = send_tcp_response(owner_stream, crate::client_protocol::VIEW_NOTIFICATION, &notif).await {
        println!("[SERVER] ⚠️  Failed to send VIEW_NOTIFICATION to owner: {}", e);
        // Send REJECTED to viewer
        let mut resp = vec![crate::client_protocol::REJECTED];
        resp.extend(req_id.to_le_bytes());
        let reason = "Failed to notify owner";
        resp.extend((reason.len() as u16).to_le_bytes());
        resp.extend(reason.as_bytes());
        send_tcp_response(viewer_stream, crate::client_protocol::REJECTED, &resp[1..]).await?;
        return Ok(());
    }

    println!("[SERVER] ✅ VIEW_NOTIFICATION sent to owner '{}'", owner_name);
    Ok(())
}

async fn handle_deny_view_tcp(
    state: SharedState,
    _stream: Arc<tokio::sync::Mutex<TcpStream>>,
    peer_addr: SocketAddr,
    data: &[u8],
) -> Result<()> {
    let temp_sock = tokio::net::UdpSocket::bind("0.0.0.0:0").await?;
    client_protocol::handle_deny_view(state, &temp_sock, peer_addr, data).await
}

async fn handle_approve_view_tcp(
    state: SharedState,
    _cfg: &Config,
    stream: Arc<tokio::sync::Mutex<TcpStream>>,
    peer_addr: SocketAddr,
    data: &[u8],
) -> Result<()> {
    // Parse request ID
    if data.len() < 4 {
        return Err(anyhow::anyhow!("APPROVE_VIEW too short"));
    }

    let req_id = u32::from_le_bytes(data[0..4].try_into()?);

    println!("[APPROVE_VIEW] req_id={} from {}", req_id, peer_addr);

    // For now, pick the first ready image for this owner and send it back as IMAGE_CHUNK
    let (owner, image_name, assets) = {
        let s = state.read().await;
        // naive: match peer IP string to client_ip in dos_clients
        let owner_name = s
            .dos_clients
            .iter()
            .find_map(|(name, c)| if c.client_ip == peer_addr.ip().to_string() { Some(name.clone()) } else { None })
            .unwrap_or_else(|| "unknown".to_string());

        let ready = s.owner_images.get(&owner_name).and_then(|imgs| {
            imgs.iter()
                .find(|(_, v)| v.ready())
                .map(|(name, v)| (owner_name.clone(), name.clone(), v.clone()))
        });
        ready.unwrap_or_else(|| (owner_name, "no_image".to_string(), crate::state::StoredImageAssets::default()))
    };

    if assets.ready() {
        let embedded = crate::stego_service::embed_meta_return_png(
            &assets.true_buf,
            &assets.cover_buf,
            &assets.meta_buf,
        )?;
        if let Err(e) = save_embedded(&owner, &image_name, &embedded) {
            warn!(error=%e, "Failed to save embedded image locally");
        }
        println!(
            "[APPROVE_VIEW] Sending embedded image back to client owner={} image={} size={}",
            owner,
            image_name,
            embedded.len()
        );
        send_embedded_image(stream.clone(), &image_name, &embedded).await?;
    } else {
        println!(
            "[APPROVE_VIEW] No ready image to send for owner={}, image={}",
            owner, image_name
        );
    }

    Ok(())
}

async fn handle_sync_usage_tcp(
    state: SharedState,
    cfg: &Config,
    _stream: Arc<tokio::sync::Mutex<TcpStream>>,
    peer_addr: SocketAddr,
    data: &[u8],
) -> Result<()> {
    let temp_sock = tokio::net::UdpSocket::bind("0.0.0.0:0").await?;
    client_protocol::handle_sync_usage(state, cfg, &temp_sock, peer_addr, data).await
}

// -------- Owner image upload (TCP stego pipeline) --------

/// OWNER_IMAGE_META: [owner_len:u16][owner][image_len:u16][image_name][true_size:u32][cover_size:u32][meta_len:u32]
async fn handle_owner_image_meta(
    state: SharedState,
    payload: &[u8],
) -> Result<()> {
    if payload.len() < 2 {
        return Err(anyhow::anyhow!("OWNER_IMAGE_META too short"));
    }
    let mut offset = 0;
    let owner_len = u16::from_le_bytes(payload[offset..offset + 2].try_into()?) as usize;
    offset += 2;
    if payload.len() < offset + owner_len + 2 {
        return Err(anyhow::anyhow!("OWNER_IMAGE_META invalid owner length"));
    }
    let owner = String::from_utf8(payload[offset..offset + owner_len].to_vec())?;
    offset += owner_len;

    let image_len = u16::from_le_bytes(payload[offset..offset + 2].try_into()?) as usize;
    offset += 2;
    if payload.len() < offset + image_len + 12 {
        return Err(anyhow::anyhow!("OWNER_IMAGE_META invalid image name length"));
    }
    let image_name = String::from_utf8(payload[offset..offset + image_len].to_vec())?;
    offset += image_len;

    let true_size = u32::from_le_bytes(payload[offset..offset + 4].try_into()?) as usize;
    offset += 4;
    let cover_size = u32::from_le_bytes(payload[offset..offset + 4].try_into()?) as usize;
    offset += 4;
    let meta_size = u32::from_le_bytes(payload[offset..offset + 4].try_into()?) as usize;

    owner_image::init_owner_image(
        state,
        owner.clone(),
        image_name.clone(),
        true_size,
        cover_size,
        meta_size,
    )
    .await;

    println!(
        "[OWNER_IMAGE_META] owner={} image={} true_size={} cover_size={} meta_size={}",
        owner, image_name, true_size, cover_size, meta_size
    );
    Ok(())
}

/// OWNER_IMAGE_CHUNK: [owner_len:u16][owner][image_len:u16][image_name][kind:u8][offset:u32][data_len:u16][data]
/// kind: 0=true, 1=cover, 2=meta
async fn handle_owner_image_chunk(
    state: SharedState,
    stream: Arc<tokio::sync::Mutex<TcpStream>>,
    peer_addr: SocketAddr,
    payload: &[u8],
) -> Result<()> {
    let mut offset = 0;
    if payload.len() < 2 {
        return Err(anyhow::anyhow!("OWNER_IMAGE_CHUNK too short"));
    }
    let owner_len = u16::from_le_bytes(payload[offset..offset + 2].try_into()?) as usize;
    offset += 2;
    if payload.len() < offset + owner_len + 2 {
        return Err(anyhow::anyhow!("OWNER_IMAGE_CHUNK invalid owner length"));
    }
    let owner = String::from_utf8(payload[offset..offset + owner_len].to_vec())?;
    offset += owner_len;

    let image_len = u16::from_le_bytes(payload[offset..offset + 2].try_into()?) as usize;
    offset += 2;
    if payload.len() < offset + image_len + 1 + 4 + 2 {
        return Err(anyhow::anyhow!("OWNER_IMAGE_CHUNK invalid image name length"));
    }
    let image_name = String::from_utf8(payload[offset..offset + image_len].to_vec())?;
    offset += image_len;

    let kind = payload[offset];
    offset += 1;
    let chunk_offset = u32::from_le_bytes(payload[offset..offset + 4].try_into()?) as usize;
    offset += 4;

    let data_len = u16::from_le_bytes(payload[offset..offset + 2].try_into()?) as usize;
    offset += 2;
    if payload.len() < offset + data_len {
        return Err(anyhow::anyhow!("OWNER_IMAGE_CHUNK data too short"));
    }
    let data = &payload[offset..offset + data_len];

    if let Some(assets) = owner_image::append_chunk(state.clone(), &owner, &image_name, kind, chunk_offset, data).await {
        if assets.ready() {
            println!(
                "[OWNER_IMAGE_CHUNK] Upload complete owner={} image={} (true={} cover={} meta={})",
                owner,
                image_name,
                assets.true_buf.len(),
                assets.cover_buf.len(),
                assets.meta_buf.len()
            );
            if let Err(e) = save_raw_assets(&owner, &image_name, &assets) {
                warn!(error=%e, "Failed to save raw assets locally");
            }
        }
    }

    // For now, acknowledge with SERVER_PONG (reuse) to keep client aware
    let mut ack = Vec::new();
    ack.extend(0u32.to_le_bytes()); // dummy payload
    send_tcp_response(stream, client_protocol::SERVER_PONG, &ack).await?;
    println!("[OWNER_IMAGE_CHUNK] ack sent to {}", peer_addr);
    Ok(())
}

/// Helper: send an embedded image as IMAGE_CHUNK messages to the connected client
async fn send_embedded_image(
    stream: Arc<tokio::sync::Mutex<TcpStream>>,
    image_name: &str,
    data: &[u8],
) -> Result<()> {
    let chunk_size = 1000usize;
    let total_chunks = ((data.len() as f32) / (chunk_size as f32)).ceil() as u32;
    for (idx, chunk) in data.chunks(chunk_size).enumerate() {
        let mut payload = Vec::new();
        payload.extend((image_name.len() as u16).to_le_bytes());
        payload.extend(image_name.as_bytes());
        payload.extend((idx as u32).to_le_bytes());
        payload.extend(total_chunks.to_le_bytes());
        payload.extend((chunk.len() as u16).to_le_bytes());
        payload.extend(chunk);
        send_tcp_response(stream.clone(), client_protocol::IMAGE_CHUNK, &payload).await?;
    }
    Ok(())
}

fn save_raw_assets(owner: &str, image: &str, assets: &StoredImageAssets) -> Result<()> {
    use std::fs;
    use std::path::Path;
    let dir = Path::new("received");
    fs::create_dir_all(dir)?;
    let true_path = dir.join(format!("{}_{}_true.png", owner, image));
    let cover_path = dir.join(format!("{}_{}_cover.png", owner, image));
    let meta_path = dir.join(format!("{}_{}_meta.json", owner, image));
    fs::write(true_path, &assets.true_buf)?;
    fs::write(cover_path, &assets.cover_buf)?;
    fs::write(meta_path, &assets.meta_buf)?;
    Ok(())
}
async fn notify_leader_add_client(
    cfg: &Config,
    username: &str,
    ip: String,
    port: u16,
    cover_image: Option<String>,
    actual_images: Vec<String>,
) -> Result<()> {
    // Build EXEC_ADD_CLIENT message
    let mut data = Vec::new();

    let username_bytes = username.as_bytes();
    data.extend((username_bytes.len() as u16).to_le_bytes());
    data.extend_from_slice(username_bytes);

    let ip_bytes = ip.as_bytes();
    data.extend((ip_bytes.len() as u16).to_le_bytes());
    data.extend_from_slice(ip_bytes);

    data.extend(port.to_le_bytes());

    // Combine cover + actual images for wire format (cover first if exists)
    let mut all_images = Vec::new();
    if let Some(cover) = cover_image {
        all_images.push(cover);
    }
    all_images.extend(actual_images);

    data.extend((all_images.len() as u32).to_le_bytes());
    for img in all_images {
        let img_bytes = img.as_bytes();
        data.extend((img_bytes.len() as u16).to_le_bytes());
        data.extend_from_slice(img_bytes);
    }

    crate::executor_leader::send_to_leader(cfg, crate::executor_leader::EXEC_ADD_CLIENT, &data).await
}

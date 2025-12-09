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

        // Check if we are the leader
        let is_leader = {
            let my_ip = cfg.service_bind_addr()
                .expect("service_bind_addr not configured")
                .ip();

            if let (Some(exec_ip), Some(deadline)) = (&s.executor_ip, s.executor_lease_deadline_ms) {
                exec_ip == &my_ip && std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() <= deadline
            } else {
                false
            }
        };
        drop(s);

        // If not leader, notify leader of status change
        if !is_leader {
            let mut data = Vec::new();
            data.extend((username.len() as u16).to_le_bytes());
            data.extend(username.as_bytes());
            data.push(0u8); // online = false

            if let Err(e) = crate::executor_leader::send_to_leader(
                &cfg,
                crate::executor_leader::EXEC_UPDATE_CLIENT_STATUS,
                &data,
            ).await {
                eprintln!("[CONNECTION] Failed to notify leader: {}", e);
            } else {
                println!("[CONNECTION] ✅ Notified leader: {} is offline", username);
            }
        }
    }

    Ok(())
}

fn save_embedded(owner: &str, image: &str, data: &[u8]) -> Result<()> {
    use std::fs;
    use std::path::Path;
    let dir = Path::new("embedded");
    fs::create_dir_all(dir)?;
    let path = dir.join(format!("{}_{}_encrypted.png", owner, image));
    fs::write(&path, data)?;
    println!("[SAVE_EMBEDDED] 💾 Saved to: {}", path.display());
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

        // REMOVED: Old server-mediated view request forwarding (replaced by P2P)
        // VIEW_REQUEST, DENY_VIEW, APPROVE_VIEW no longer handled by server
        // Clients now communicate directly via P2P connections

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

        x if x == client_protocol::OFFLINE_REQUESTS_QUERY => {
            info!(%peer_addr, len=payload.len(), "Received OFFLINE_REQUESTS_QUERY from client");
            handle_offline_requests_query(state, stream, peer_addr, payload).await
        }

        x if x == client_protocol::ACCESS_MAP_QUERY => {
            info!(%peer_addr, len=payload.len(), "Received ACCESS_MAP_QUERY from client");
            handle_access_map_query(state, stream, peer_addr, payload).await
        }

        x if x == client_protocol::REQUEST_EXECUTOR => {
            info!(%peer_addr, len=payload.len(), "Received REQUEST_EXECUTOR from client");
            handle_request_executor(state, &cfg, stream, peer_addr).await
        }

        x if x == client_protocol::CLIENT_LEAVE => {
            info!(%peer_addr, len=payload.len(), "Received CLIENT_LEAVE from client");
            handle_client_leave(state, &cfg, stream, peer_addr, payload).await
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

    // NEW P2P Architecture: All images are actual_images (no cover split)
    // Owner will provide cover per-request when encrypting
    let actual_images = images;

    println!("[HANDLE_JOIN_TCP] Registered images: actual={:?}", actual_images);

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

    // Build P2P DOS-C payload for JOIN_ACK (v3.0 - with IP/port for P2P)
    // Excludes: requesting client (self-exclusion), last_seen, online
    // Includes: dos_version, name, IP, port, actual_images
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

        println!("[HANDLE_JOIN_TCP] Building P2P DOS-C v3.0: version={}, num_clients={} (excluded: {})",
                 dos_version_u64, num_clients, username);

        // For each client (P2P FORMAT: name + IP + port + actual_images)
        for (client_name, client) in clients_to_send {
            // Username
            let name_bytes = client_name.as_bytes();
            payload.extend((name_bytes.len() as u16).to_le_bytes());
            payload.extend_from_slice(name_bytes);

            // NEW P2P: Client IP (for direct connection)
            let ip_bytes = client.client_ip.as_bytes();
            payload.extend((ip_bytes.len() as u16).to_le_bytes());
            payload.extend_from_slice(ip_bytes);

            // NEW P2P: Client port (P2P listen port)
            payload.extend(client.client_port.to_le_bytes());

            // Number of images (u32) - ONLY actual images (no cover)
            payload.extend((client.actual_images.len() as u32).to_le_bytes());

            // Image names (actual images only, cover not included in DOS-C)
            for img in &client.actual_images {
                let img_bytes = img.as_bytes();
                payload.extend((img_bytes.len() as u16).to_le_bytes());
                payload.extend_from_slice(img_bytes);
            }

            println!("[HANDLE_JOIN_TCP]   ✅ {} -> ip={}, port={}, actual_images={:?}",
                client_name, client.client_ip, client.client_port, client.actual_images);
        }

        payload
    };

    // Send JOIN_ACK with DOS-C payload
    println!("[HANDLE_JOIN_TCP] Sending JOIN_ACK with DOS-C ({} bytes) to {}", dos_payload.len(), peer_addr);
    send_tcp_response(stream.clone(), client_protocol::JOIN_ACK, &dos_payload).await?;

    // Notify leader to write to Firebase (NEW P2P: only actual images, no cover)
    if let Err(e) = notify_leader_add_client(cfg, &username, peer_addr.ip().to_string(), client_port, actual_images.clone()).await {
        warn!("Failed to notify leader about new client: {}", e);
    }

    // Check if we are the executor/leader and deliver offline requests
    let is_executor = {
        let s = state.read().await;
        let my_ip = cfg.service_bind_addr()
            .expect("service_bind_addr not configured")
            .ip();

        if let (Some(exec_ip), Some(deadline)) = (&s.executor_ip, s.executor_lease_deadline_ms) {
            exec_ip == &my_ip && std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() <= deadline
        } else {
            false
        }
    };

    if is_executor {
        let s = state.read().await;
        if let Some(db) = &s.firestore_db {
            match crate::firebase::get_and_delete_offline_requests(db, &username).await {
                Ok(requests) if !requests.is_empty() => {
                    println!("[JOIN] Delivering {} offline requests to {}", requests.len(), username);

                    // Send each request to client
                    for req in requests {
                        // Format: PENDING_REQUEST message
                        // [requester_len:u16][requester][image_name_len:u16][image_name][request_id:u32][requested_views:u32]
                        let mut payload = Vec::new();
                        payload.extend((req.requester.len() as u16).to_le_bytes());
                        payload.extend(req.requester.as_bytes());
                        payload.extend((req.image_name.len() as u16).to_le_bytes());
                        payload.extend(req.image_name.as_bytes());
                        payload.extend(req.request_id.to_le_bytes());
                        payload.extend(req.requested_views.to_le_bytes());

                        // Send to client
                        if let Err(e) = send_tcp_response(stream.clone(), client_protocol::PENDING_REQUEST, &payload).await {
                            warn!("Failed to send pending request to {}: {}", username, e);
                        } else {
                            println!("[JOIN] ✅ Sent pending request from {} to {}", req.requester, username);
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to retrieve offline requests for {}: {}", username, e);
                }
                _ => {}
            }
        }
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

    // Build P2P DOS-C v3.0 payload (same format as JOIN_ACK)
    // Excludes: requesting client (self-exclusion), last_seen, online
    // Includes: dos_version, name, IP, port, actual_images
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

        println!("[DOS_QUERY] Building P2P DOS-C v3.0: version={}, num_clients={} (excluded: {})",
                 dos_version_u64, num_clients, username);

        // For each client (P2P FORMAT: name + IP + port + actual_images)
        for (client_name, client) in clients_to_send {
            // Username
            let name_bytes = client_name.as_bytes();
            payload.extend((name_bytes.len() as u16).to_le_bytes());
            payload.extend_from_slice(name_bytes);

            // NEW P2P: Client IP (for direct connection)
            let ip_bytes = client.client_ip.as_bytes();
            payload.extend((ip_bytes.len() as u16).to_le_bytes());
            payload.extend_from_slice(ip_bytes);

            // NEW P2P: Client port (P2P listen port)
            payload.extend(client.client_port.to_le_bytes());

            // Number of images (u32) - ONLY actual images (no cover)
            payload.extend((client.actual_images.len() as u32).to_le_bytes());

            // Image names (actual images only, cover not included in DOS-C)
            for img in &client.actual_images {
                let img_bytes = img.as_bytes();
                payload.extend((img_bytes.len() as u16).to_le_bytes());
                payload.extend_from_slice(img_bytes);
            }

            println!("[DOS_QUERY]   ✅ {} -> ip={}, port={}, actual_images={:?}",
                client_name, client.client_ip, client.client_port, client.actual_images);
        }

        payload
    };

    // Send DOS_UPDATE with DOS-C payload
    println!("[DOS_QUERY] Sending DOS_UPDATE with DOS-C ({} bytes) to {}", dos_payload.len(), peer_addr);
    send_tcp_response(stream.clone(), client_protocol::DOS_UPDATE, &dos_payload).await?;

    Ok(())
}

/// Handle REQUEST_EXECUTOR: Client discovery protocol - respond if this server is executor
async fn handle_request_executor(
    state: SharedState,
    cfg: &Config,
    stream: Arc<tokio::sync::Mutex<TcpStream>>,
    peer_addr: SocketAddr,
) -> Result<()> {
    // Check if this server is the current executor
    let is_executor = {
        let s = state.read().await;
        let my_ip = cfg.service_bind_addr()
            .expect("service_bind_addr not configured")
            .ip();

        if let (Some(exec_ip), Some(deadline)) = (&s.executor_ip, s.executor_lease_deadline_ms) {
            exec_ip == &my_ip && std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() <= deadline
        } else {
            false
        }
    };

    if is_executor {
        println!("[REQUEST_EXECUTOR] ✅ I am executor, responding to {}", peer_addr);
        // Send EXECUTOR_ACK
        send_tcp_response(stream, client_protocol::EXECUTOR_ACK, &[]).await?;
    } else {
        println!("[REQUEST_EXECUTOR] ❌ I am not executor, ignoring request from {}", peer_addr);
    }

    Ok(())
}

/// Handle CLIENT_LEAVE: Client sends graceful shutdown notification
async fn handle_client_leave(
    state: SharedState,
    cfg: &Config,
    _stream: Arc<tokio::sync::Mutex<TcpStream>>,
    peer_addr: SocketAddr,
    data: &[u8],
) -> Result<()> {
    // Parse username: [username_len:u16][username]
    if data.len() < 2 {
        return Err(anyhow::anyhow!("CLIENT_LEAVE too short"));
    }

    let username_len = u16::from_le_bytes(data[0..2].try_into()?) as usize;
    if data.len() < 2 + username_len {
        return Err(anyhow::anyhow!("CLIENT_LEAVE invalid username length"));
    }

    let username = String::from_utf8(data[2..2 + username_len].to_vec())?;

    println!("[LEAVE] Client {} ({}) is leaving gracefully", username, peer_addr);

    // Remove from local state
    {
        let mut s = state.write().await;
        s.client_connections.remove(&username);

        // Mark as offline in dos_clients
        if let Some(client) = s.dos_clients.get_mut(&username) {
            client.online = false;
        }

        // Increment DOS version to signal change
        s.dos_c_version += 1;
    }

    // Notify leader to mark offline in Firebase
    let mut data = Vec::new();
    data.extend((username.len() as u16).to_le_bytes());
    data.extend(username.as_bytes());
    data.push(0u8); // online = false

    if let Err(e) = crate::executor_leader::send_to_leader(
        cfg,
        crate::executor_leader::EXEC_UPDATE_CLIENT_STATUS,
        &data,
    ).await {
        eprintln!("[LEAVE] Failed to notify leader: {}", e);
    } else {
        println!("[LEAVE] ✅ Notified leader: {} is offline", username);
    }

    println!("[LEAVE] ✅ Client {} removed from registry", username);
    Ok(())
}

/// Handle OFFLINE_REQUESTS_QUERY: Client requests pending offline requests on startup
async fn handle_offline_requests_query(
    state: SharedState,
    stream: Arc<tokio::sync::Mutex<TcpStream>>,
    peer_addr: SocketAddr,
    data: &[u8],
) -> Result<()> {
    // Parse username
    if data.len() < 2 {
        return Err(anyhow::anyhow!("OFFLINE_REQUESTS_QUERY too short"));
    }

    let username_len = u16::from_le_bytes(data[0..2].try_into()?) as usize;
    if data.len() < 2 + username_len {
        return Err(anyhow::anyhow!("Invalid username length"));
    }

    let username = String::from_utf8(data[2..2 + username_len].to_vec())?;

    println!("[OFFLINE_REQUESTS_QUERY] Request from user: {} ({})", username, peer_addr);

    // Get offline requests for this user
    let requests = {
        let mut s = state.write().await;
        s.offline_requests.remove(&username).unwrap_or_default()
    };

    let num_requests = requests.len();
    println!("[OFFLINE_REQUESTS_QUERY] Found {} offline requests for {}", num_requests, username);

    // Build response payload
    let mut payload = Vec::new();

    // Number of requests
    payload.extend((num_requests as u32).to_le_bytes());

    // Each request: [type_len:u16][type][sender_len:u16][sender][image_len:u16][image][data_len:u32][data][timestamp:u64]
    for req in requests {
        // Request type
        payload.extend((req.request_type.len() as u16).to_le_bytes());
        payload.extend(req.request_type.as_bytes());

        // Sender
        payload.extend((req.sender.len() as u16).to_le_bytes());
        payload.extend(req.sender.as_bytes());

        // Image name
        payload.extend((req.image_name.len() as u16).to_le_bytes());
        payload.extend(req.image_name.as_bytes());

        // Data
        payload.extend((req.data.len() as u32).to_le_bytes());
        payload.extend(&req.data);

        // Timestamp
        payload.extend(req.timestamp.to_le_bytes());

        println!("[OFFLINE_REQUESTS_QUERY]   - type={}, sender={}, image={}",
                 req.request_type, req.sender, req.image_name);
    }

    // Send response
    send_tcp_response(stream, client_protocol::OFFLINE_REQUESTS_RESPONSE, &payload).await?;

    println!("[OFFLINE_REQUESTS_QUERY] Sent {} offline requests to {}", num_requests, username);
    Ok(())
}

/// Handle ACCESS_MAP_QUERY: Client requests access map on startup
async fn handle_access_map_query(
    _state: SharedState,
    stream: Arc<tokio::sync::Mutex<TcpStream>>,
    peer_addr: SocketAddr,
    data: &[u8],
) -> Result<()> {
    // Parse username
    if data.len() < 2 {
        return Err(anyhow::anyhow!("ACCESS_MAP_QUERY too short"));
    }

    let username_len = u16::from_le_bytes(data[0..2].try_into()?) as usize;
    if data.len() < 2 + username_len {
        return Err(anyhow::anyhow!("Invalid username length"));
    }

    let username = String::from_utf8(data[2..2 + username_len].to_vec())?;

    println!("[ACCESS_MAP_QUERY] Request from user: {} ({})", username, peer_addr);

    // Load access map from filesystem
    let access_map = crate::access_map_storage::load_access_map(&username).await?;

    println!("[ACCESS_MAP_QUERY] Loaded {} grants for {}", access_map.grants.len(), username);

    // Serialize access map to JSON
    let json = serde_json::to_vec(&access_map)?;

    // Build response payload: [json_len:u32][json]
    let mut payload = Vec::new();
    payload.extend((json.len() as u32).to_le_bytes());
    payload.extend(&json);

    // Send response
    send_tcp_response(stream, client_protocol::ACCESS_MAP_RESPONSE, &payload).await?;

    println!("[ACCESS_MAP_QUERY] Sent {} grants to {}", access_map.grants.len(), username);
    Ok(())
}

// REMOVED: Old server-mediated view request handlers (replaced by P2P architecture)
// - handle_view_request_tcp: Server no longer forwards VIEW_REQUEST
// - handle_deny_view_tcp: Clients handle denials directly via P2P
// - handle_approve_view_tcp: Clients handle approvals directly via P2P
// In the new architecture, clients communicate peer-to-peer using PEER_VIEW_REQUEST, etc.

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
                "╔════════════════════════════════════════════════════════════════╗"
            );
            println!(
                "║ [OWNER UPLOAD] All chunks received for {}/{}",
                owner, image_name
            );
            println!(
                "║ - True image size: {} bytes",
                assets.true_buf.len()
            );
            println!(
                "║ - Cover image size: {} bytes",
                assets.cover_buf.len()
            );
            println!(
                "║ - Metadata size: {} bytes",
                assets.meta_buf.len()
            );
            println!(
                "╚════════════════════════════════════════════════════════════════╝"
            );

            // Save raw assets to server's received/ directory
            if let Err(e) = save_raw_assets(&owner, &image_name, &assets) {
                warn!(error=%e, "Failed to save raw assets locally");
            } else {
                println!("[OWNER UPLOAD] ✅ Raw assets saved to Cloud-Node/received/");
            }

            // Perform steganographic encryption
            println!("[OWNER UPLOAD] 🔐 Starting encryption for {}/{}...", owner, image_name);

            // NO metadata generation on server - pass empty buffer as-is
            match crate::stego_service::embed_meta_return_png(
                &assets.true_buf,
                &assets.cover_buf,
                &assets.meta_buf,
            ) {
                Ok(encrypted_png) => {
                    println!(
                        "[OWNER UPLOAD] ✅ Encryption complete! Size: {} bytes",
                        encrypted_png.len()
                    );

                    // Save encrypted image on server
                    if let Err(e) = save_embedded(&owner, &image_name, &encrypted_png) {
                        warn!(error=%e, "Failed to save encrypted image on server");
                    } else {
                        println!("[OWNER UPLOAD] ✅ Encrypted image saved to Cloud-Node/embedded/");
                    }

                    // DIAGNOSTIC: Check for duplicate send_embedded_image calls
                    use std::sync::atomic::{AtomicBool, Ordering};
                    static SEND_GUARD: AtomicBool = AtomicBool::new(false);

                    if SEND_GUARD.swap(true, Ordering::SeqCst) {
                        warn!("⚠️  DUPLICATE send_embedded_image call detected for {}/{}!", owner, image_name);
                        println!("[OWNER UPLOAD] ⚠️  WARNING: Duplicate send attempt blocked!");
                    } else {
                        // Send encrypted image back to owner
                        println!("[OWNER UPLOAD] 📤 Sending encrypted image back to owner...");
                        if let Err(e) = send_embedded_image(stream.clone(), &image_name, &encrypted_png).await {
                            warn!(error=%e, "Failed to send encrypted image to owner");
                            SEND_GUARD.store(false, Ordering::SeqCst); // Reset on error
                        } else {
                            println!("[OWNER UPLOAD] ✅ Encrypted image sent to owner successfully!");
                            println!(
                                "╔════════════════════════════════════════════════════════════════╗"
                            );
                            println!(
                                "║ ENCRYPTION COMPLETE: {}/{}",
                                owner, image_name
                            );
                            println!(
                                "║ Owner should receive and save encrypted image locally"
                            );
                            println!(
                                "╚════════════════════════════════════════════════════════════════╝"
                            );
                            // Keep SEND_GUARD true to prevent future duplicates
                        }
                    }
                }
                Err(e) => {
                    eprintln!("[OWNER UPLOAD] ❌ Encryption failed: {}", e);
                    warn!(error=%e, "Failed to encrypt owner image");
                }
            }

            // CRITICAL: Remove the entry from state to prevent re-processing
            println!("[OWNER UPLOAD] 🧹 Cleaning up state for {}/{}", owner, image_name);
            let mut s = state.write().await;
            if let Some(imgs) = s.owner_images.get_mut(&owner) {
                imgs.remove(&image_name);
            }
        } else {
            // Still receiving chunks - just ack
            let mut ack = Vec::new();
            ack.extend(0u32.to_le_bytes());
            send_tcp_response(stream, client_protocol::SERVER_PONG, &ack).await?;
        }
    } else {
        // No assets found or chunk failed - just ack
        let mut ack = Vec::new();
        ack.extend(0u32.to_le_bytes());
        send_tcp_response(stream, client_protocol::SERVER_PONG, &ack).await?;
    }

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

    println!("[SEND-IMAGE] Starting to send {} chunks for image '{}'", total_chunks, image_name);

    for (idx, chunk) in data.chunks(chunk_size).enumerate() {
        let mut payload = Vec::new();
        payload.extend((image_name.len() as u16).to_le_bytes());
        payload.extend(image_name.as_bytes());
        payload.extend((idx as u32).to_le_bytes());
        payload.extend(total_chunks.to_le_bytes());
        payload.extend((chunk.len() as u16).to_le_bytes());
        payload.extend(chunk);

        // DIAGNOSTIC: Log each chunk being sent
        println!(
            "[SEND-IMAGE-CHUNK] {}/{}: name='{}' seq={} chunk_len={} payload_len={}",
            idx + 1,
            total_chunks,
            image_name,
            idx,
            chunk.len(),
            payload.len()
        );

        send_tcp_response(stream.clone(), client_protocol::IMAGE_CHUNK, &payload).await?;
    }

    println!("[SEND-IMAGE] ✅ Completed sending all {} chunks for '{}'", total_chunks, image_name);
    Ok(())
}

fn save_raw_assets(owner: &str, image: &str, assets: &StoredImageAssets) -> Result<()> {
    use std::fs;
    use std::path::Path;
    let dir = Path::new("received");
    fs::create_dir_all(dir)?;
    let true_path = dir.join(format!("{}_{}_true.png", owner, image));
    let cover_path = dir.join(format!("{}_{}_cover.png", owner, image));
    fs::write(&true_path, &assets.true_buf)?;
    fs::write(&cover_path, &assets.cover_buf)?;
    println!("[SAVE_RAW] 💾 True image: {}", true_path.display());
    println!("[SAVE_RAW] 💾 Cover image: {}", cover_path.display());

    // Only save metadata if it's not empty
    if !assets.meta_buf.is_empty() {
        let meta_path = dir.join(format!("{}_{}_meta.json", owner, image));
        fs::write(&meta_path, &assets.meta_buf)?;
        println!("[SAVE_RAW] 💾 Metadata: {}", meta_path.display());
    } else {
        println!("[SAVE_RAW] ℹ️  No metadata (0 bytes) - skipped saving");
    }
    Ok(())
}
async fn notify_leader_add_client(
    cfg: &Config,
    username: &str,
    ip: String,
    port: u16,
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

    // NEW P2P: Only actual_images (no cover)
    data.extend((actual_images.len() as u32).to_le_bytes());
    for img in actual_images {
        let img_bytes = img.as_bytes();
        data.extend((img_bytes.len() as u16).to_le_bytes());
        data.extend_from_slice(img_bytes);
    }

    crate::executor_leader::send_to_leader(cfg, crate::executor_leader::EXEC_ADD_CLIENT, &data).await
}

/// Broadcast DOS from Firebase to all connected clients (NO local caching)
/// This function reads from Firebase data and broadcasts directly to clients
/// Servers do NOT cache DOS - Firebase is the ONLY persistent storage
pub async fn broadcast_dos_to_clients(
    state: &SharedState,
    firebase_dos: std::collections::HashMap<String, crate::firebase::DosClient>,
) {
    use std::collections::HashMap;
    use tokio::io::AsyncWriteExt;
    use tokio::sync::Mutex;

    // Get all active client connections
    let connections: HashMap<String, Arc<Mutex<TcpStream>>> = {
        let s = state.read().await;
        s.client_connections.clone()
    };

    println!("[DOS-BROADCAST] Broadcasting to {} connected clients (from Firebase, NO local caching)", connections.len());

    for (username, stream_mutex) in connections {
        // Build DOS payload excluding this client (self-exclusion)
        let dos_payload = build_dos_payload_from_firebase(&firebase_dos, &username);

        // Send DOS_UPDATE via TCP
        let msg_type = client_protocol::DOS_UPDATE;
        let mut message = Vec::with_capacity(1 + dos_payload.len());
        message.push(msg_type);
        message.extend_from_slice(&dos_payload);

        // Lock the stream and write the message
        let mut stream = stream_mutex.lock().await;
        match stream.write_all(&message).await {
            Ok(_) => {
                println!("[DOS-BROADCAST] ✅ Sent {} bytes to {}", message.len(), username);
            }
            Err(e) => {
                eprintln!("[DOS-BROADCAST] ❌ Failed to send to {}: {}", username, e);
            }
        }
    }
}

/// Build DOS payload from Firebase data (NOT from local cache)
/// Excludes the requesting client (self-exclusion)
fn build_dos_payload_from_firebase(
    firebase_dos: &std::collections::HashMap<String, crate::firebase::DosClient>,
    exclude_username: &str,
) -> Vec<u8> {
    let mut payload = Vec::new();

    // DOS version (use max last_seen as version proxy since servers don't cache version)
    let version = firebase_dos.values()
        .map(|c| c.last_seen)
        .max()
        .unwrap_or(0);
    payload.extend(version.to_le_bytes());

    // Filter clients (exclude the requesting client)
    let clients_to_send: Vec<_> = firebase_dos
        .iter()
        .filter(|(name, _)| *name != exclude_username)
        .collect();

    // Number of clients (u32)
    payload.extend((clients_to_send.len() as u32).to_le_bytes());

    // For each client: name, IP, port, images
    for (client_name, client) in clients_to_send {
        // Client name
        payload.extend((client_name.len() as u16).to_le_bytes());
        payload.extend_from_slice(client_name.as_bytes());

        // Client IP
        payload.extend((client.client_ip.len() as u16).to_le_bytes());
        payload.extend_from_slice(client.client_ip.as_bytes());

        // Client port
        payload.extend(client.client_port.to_le_bytes());

        // Actual images (no cover images in unified DOS)
        payload.extend((client.actual_images.len() as u32).to_le_bytes());
        for img in &client.actual_images {
            payload.extend((img.len() as u16).to_le_bytes());
            payload.extend_from_slice(img.as_bytes());
        }
    }

    payload
}

/// Send LIFE_CHECK to client's P2P TCP port and wait for ACK
/// This is used for system recovery to verify which clients are still alive
/// CRITICAL: Uses TCP (NOT UDP) - creates TCP connection to client's P2P port
pub async fn send_life_check(client_ip: &str, client_port: u16, username: &str) -> Result<bool> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::time::{timeout, Duration};

    let client_addr = format!("{}:{}", client_ip, client_port);
    let socket_addr: SocketAddr = client_addr.parse()
        .context(format!("Invalid client address: {}", client_addr))?;

    // Create TCP connection to client's P2P port (1 second timeout)
    let mut tcp_stream = match timeout(
        Duration::from_secs(1),
        TcpStream::connect(socket_addr)
    ).await {
        Ok(Ok(stream)) => stream,
        Ok(Err(e)) => {
            eprintln!("[LIFE_CHECK] Failed to connect to {}: {}", client_addr, e);
            return Ok(false);
        }
        Err(_) => {
            eprintln!("[LIFE_CHECK] Timeout connecting to {}", client_addr);
            return Ok(false);
        }
    };

    // Build LIFE_CHECK message: [msg_type:u8][username_len:u16][username]
    let mut payload = Vec::new();
    payload.push(client_protocol::LIFE_CHECK);
    payload.extend((username.len() as u16).to_le_bytes());
    payload.extend(username.as_bytes());

    // Send LIFE_CHECK via TCP
    if let Err(e) = tcp_stream.write_all(&payload).await {
        eprintln!("[LIFE_CHECK] Failed to send to {}: {}", client_addr, e);
        return Ok(false);
    }

    // Wait for ACK (1 second timeout)
    let mut buf = [0u8; 1];
    match timeout(Duration::from_secs(1), tcp_stream.read_exact(&mut buf)).await {
        Ok(Ok(_)) if buf[0] == client_protocol::LIFE_CHECK_ACK => {
            println!("[LIFE_CHECK] ✅ Received ACK from {} ({})", username, client_addr);
            Ok(true)
        }
        Ok(Ok(_)) => {
            eprintln!("[LIFE_CHECK] Wrong response from {}: {}", client_addr, buf[0]);
            Ok(false)
        }
        Ok(Err(e)) => {
            eprintln!("[LIFE_CHECK] Read error from {}: {}", client_addr, e);
            Ok(false)
        }
        Err(_) => {
            eprintln!("[LIFE_CHECK] Timeout waiting for ACK from {}", client_addr);
            Ok(false)
        }
    }
    // TCP connection automatically closed when tcp_stream goes out of scope
}

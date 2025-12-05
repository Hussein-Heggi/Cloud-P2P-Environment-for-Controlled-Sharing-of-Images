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
    state::SharedState,
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
    mut stream: TcpStream,
    peer_addr: SocketAddr,
    state: SharedState,
    cfg: Config,
) -> Result<()> {
    info!(%peer_addr, "Client connection handler started");

    // Store stream in state for sending responses
    let stream = Arc::new(tokio::sync::Mutex::new(stream));

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

        // Route message to appropriate handler
        let state_clone = state.clone();
        let cfg_clone = cfg.clone();
        let stream_clone = stream.clone();

        tokio::spawn(async move {
            if let Err(e) = route_client_message(
                msg_type,
                &payload,
                peer_addr,
                state_clone,
                cfg_clone,
                stream_clone,
            ).await {
                warn!(%peer_addr, msg_type, error=%e, "Message handler error");
            }
        });
    }

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

        x if x == client_protocol::SYNC_USAGE => {
            info!(%peer_addr, len=payload.len(), "Received SYNC_USAGE from client");
            handle_sync_usage_tcp(state, &cfg, stream, peer_addr, payload).await
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

    Ok(())
}

// TCP-specific handlers that wrap the original protocol handlers

async fn handle_req_tcp(
    state: SharedState,
    cfg: &Config,
    stream: Arc<tokio::sync::Mutex<TcpStream>>,
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

    // Register client in state
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis();

    {
        let mut s = state.write().await;
        s.dos_clients.insert(
            username.clone(),
            crate::firebase::DosClient {
                client_name: username.clone(),
                client_ip: peer_addr.ip().to_string(),
                client_port: client_port,
                images: images.clone(),
                online: true,
                last_seen: now_ms,
            },
        );
        s.dos_c_version += 1;
    }

    // Send JOIN_ACK
    println!("[HANDLE_JOIN_TCP] Sending JOIN_ACK to {}", peer_addr);
    send_tcp_response(stream, client_protocol::JOIN_ACK, &[]).await?;

    // Notify leader to write to Firebase
    if let Err(e) = notify_leader_add_client(cfg, &username, peer_addr.ip().to_string(), client_port, images).await {
        warn!("Failed to notify leader about new client: {}", e);
    }

    println!("[HANDLE_JOIN_TCP] ✅ Client {} registered successfully", username);
    Ok(())
}

async fn handle_client_ping_tcp(
    state: SharedState,
    stream: Arc<tokio::sync::Mutex<TcpStream>>,
    peer_addr: SocketAddr,
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
        .as_millis();

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

async fn handle_view_request_tcp(
    state: SharedState,
    cfg: &Config,
    stream: Arc<tokio::sync::Mutex<TcpStream>>,
    peer_addr: SocketAddr,
    data: &[u8],
) -> Result<()> {
    let temp_sock = tokio::net::UdpSocket::bind("0.0.0.0:0").await?;
    client_protocol::handle_view_request(state, cfg, &temp_sock, peer_addr, data).await
}

async fn handle_deny_view_tcp(
    state: SharedState,
    stream: Arc<tokio::sync::Mutex<TcpStream>>,
    peer_addr: SocketAddr,
    data: &[u8],
) -> Result<()> {
    let temp_sock = tokio::net::UdpSocket::bind("0.0.0.0:0").await?;
    client_protocol::handle_deny_view(state, &temp_sock, peer_addr, data).await
}

async fn handle_approve_view_tcp(
    state: SharedState,
    cfg: &Config,
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

    // TODO: Find pending request and send APPROVED to viewer
    // For now, just acknowledge

    Ok(())
}

async fn handle_sync_usage_tcp(
    state: SharedState,
    cfg: &Config,
    stream: Arc<tokio::sync::Mutex<TcpStream>>,
    peer_addr: SocketAddr,
    data: &[u8],
) -> Result<()> {
    let temp_sock = tokio::net::UdpSocket::bind("0.0.0.0:0").await?;
    client_protocol::handle_sync_usage(state, cfg, &temp_sock, peer_addr, data).await
}

async fn notify_leader_add_client(
    cfg: &Config,
    username: &str,
    ip: String,
    port: u16,
    images: Vec<String>,
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

    data.extend((images.len() as u32).to_le_bytes());
    for img in images {
        let img_bytes = img.as_bytes();
        data.extend((img_bytes.len() as u16).to_le_bytes());
        data.extend_from_slice(img_bytes);
    }

    crate::executor_leader::send_to_leader(cfg, crate::executor_leader::EXEC_ADD_CLIENT, &data).await
}

//! Simple client implementation for testing the new protocol
//! Supports JOIN and PING operations
//! Uses TCP for reliable communication with the server

use anyhow::{Context, Result};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpSocket;
use tokio::sync::RwLock;

use crate::protocol::*;

/// Fixed client listen/advertised port for TCP
pub const CLIENT_PORT: u16 = 9080;

/// Client state
#[derive(Clone)]
pub struct ClientState {
    pub username: String,
    pub server_addr: SocketAddr,
    pub client_port: u16,
    pub images: Vec<String>,
    pub joined: bool,
    pub dos_version: u32,
}

impl ClientState {
    pub fn new(username: String, server_addr: SocketAddr) -> Self {
        Self {
            username,
            server_addr,
            client_port: 0,
            images: Vec::new(),
            joined: false,
            dos_version: 0,
        }
    }
}

pub type SharedClientState = Arc<RwLock<ClientState>>;

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
    // Read length prefix
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let total_len = u32::from_le_bytes(len_buf) as usize;

    if total_len == 0 || total_len > 65536 {
        return Err(anyhow::anyhow!("Invalid message length: {}", total_len));
    }

    // Read message data
    let mut buf = vec![0u8; total_len];
    stream.read_exact(&mut buf).await?;

    if buf.is_empty() {
        return Err(anyhow::anyhow!("Empty message"));
    }

    let msg_type = buf[0];
    let payload = buf[1..].to_vec();

    println!(
        "[CLIENT-RECV] len={} msg_type={} payload_len={}",
        total_len,
        msg_type,
        payload.len()
    );

    Ok((msg_type, payload))
}

/// Send JOIN message to server and wait for JOIN_ACK
pub async fn join_server(
    state: SharedClientState,
    writer: Arc<tokio::sync::Mutex<tokio::net::tcp::OwnedWriteHalf>>,
    reader: &mut tokio::net::tcp::OwnedReadHalf,
) -> Result<()> {
    let (username, server_addr, images) = {
        let s = state.read().await;
        (s.username.clone(), s.server_addr, s.images.clone())
    };

    println!("[CLIENT] Sending JOIN to server {}...", server_addr);

    // Advertise the fixed local port used for the TCP connection
    let local_port = CLIENT_PORT;
    {
        let mut s = state.write().await;
        s.client_port = local_port;
    }

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
        Ok(Ok((msg_type, _payload))) => {
            if msg_type == JOIN_ACK {
                println!("[CLIENT] ✅ JOIN_ACK received from server");

                // Update state
                let mut s = state.write().await;
                s.joined = true;

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

/// Run the client listener (receives messages from server)
pub async fn run_listener(
    _state: SharedClientState,
    mut reader: tokio::net::tcp::OwnedReadHalf,
    writer: Arc<tokio::sync::Mutex<tokio::net::tcp::OwnedWriteHalf>>,
) -> Result<()> {
    println!("[CLIENT-LISTENER] Started");

    loop {
        let (msg_type, data) = match recv_tcp_message_generic(&mut reader).await {
            Ok(msg) => msg,
            Err(e) => {
                println!("[CLIENT-LISTENER] Connection closed or error: {}", e);
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
                    let mut s = _state.write().await;
                    let old_version = s.dos_version;
                    s.dos_version = dos_version;
                    if dos_version != old_version {
                        println!("[CLIENT] ✅ PONG received - DOS version updated: {} -> {}", old_version, dos_version);
                    } else {
                        println!("[CLIENT] ✅ PONG received - DOS version: {}", dos_version);
                    }
                } else {
                    println!("[CLIENT-LISTENER] SERVER_PONG too short ({} bytes)", data.len());
                }
            }

            VIEW_NOTIFICATION => {
                // Parse: [viewer_name_len:u16][viewer_name][image_name_len:u16][image_name][req_id:u32]
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

                println!("\n📩 [VIEW NOTIFICATION] {} wants to view image: {}", viewer_name, image_name);
                println!("   Request ID: {}", req_id);
                println!("   Action: Auto-approving (demo mode)");

                // Auto-approve for demo
                let mut response_payload = Vec::new();
                response_payload.extend(req_id.to_le_bytes());

                let mut s = writer.lock().await;
                if let Err(e) = send_tcp_message_generic(&mut *s, APPROVE_VIEW, &response_payload).await {
                    println!("   ⚠️  Failed to send APPROVE_VIEW: {}", e);
                } else {
                    println!("   ✅ APPROVE_VIEW sent");
                }
            }

            DOS_UPDATE => {
                println!("[CLIENT-LISTENER] DOS_UPDATE received - clients/access lists updated");
                // In full implementation, would update local DOS-C here
            }

            REJECTED => {
                println!("[CLIENT-LISTENER] ⚠️  Request REJECTED by server");
            }

            APPROVED => {
                println!("[CLIENT-LISTENER] ✅ Request APPROVED by owner - waiting for image chunks");
            }

            IMAGE_CHUNK => {
                println!("[CLIENT-LISTENER] IMAGE_CHUNK received - implement chunked download");
                // TODO: Implement chunked image reception
            }

            _ => {
                println!("[CLIENT-LISTENER] Unknown message type: {}", msg_type);
            }
        }
    }
}

/// Connect to server via TCP
pub async fn connect_to_server(server_addr: SocketAddr) -> Result<(tokio::net::tcp::OwnedReadHalf, Arc<tokio::sync::Mutex<tokio::net::tcp::OwnedWriteHalf>>)> {
    println!("[CLIENT] Connecting to server at {} from local port {}...", server_addr, CLIENT_PORT);

    // Bind local port explicitly so the connection shows up as :9080
    let socket = TcpSocket::new_v4()
        .context("Failed to create TCP socket")?;
    socket
        .bind(SocketAddr::from(([0, 0, 0, 0], CLIENT_PORT)))
        .context(format!("Failed to bind local TCP port {}", CLIENT_PORT))?;

    let stream = socket
        .connect(server_addr)
        .await
        .context(format!("Failed to connect to server at {}", server_addr))?;

    let local_addr = stream.local_addr()?;
    println!("[CLIENT] ✅ Connected to server from local address {}", local_addr);

    let (read_half, write_half) = stream.into_split();
    Ok((read_half, Arc::new(tokio::sync::Mutex::new(write_half))))
}

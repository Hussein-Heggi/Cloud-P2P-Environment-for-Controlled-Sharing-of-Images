//! Simple client implementation for testing the new protocol
//! Supports JOIN and PING operations

use anyhow::{Context, Result};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::RwLock;

use crate::protocol::*;

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

/// Send JOIN message to server and wait for JOIN_ACK
pub async fn join_server(
    state: SharedClientState,
    sock: Arc<UdpSocket>,
) -> Result<()> {
    let (username, server_addr, images) = {
        let s = state.read().await;
        (s.username.clone(), s.server_addr, s.images.clone())
    };

    println!("[CLIENT] Sending JOIN to server {}...", server_addr);

    // Build JOIN message
    // Format: [JOIN][username_len:u16][username][num_images:u32][image_names...]
    let mut msg = vec![JOIN];

    let username_bytes = username.as_bytes();
    msg.extend((username_bytes.len() as u16).to_le_bytes());
    msg.extend_from_slice(username_bytes);

    msg.extend((images.len() as u32).to_le_bytes());
    for image in &images {
        let image_bytes = image.as_bytes();
        msg.extend((image_bytes.len() as u16).to_le_bytes());
        msg.extend_from_slice(image_bytes);
    }

    // Send JOIN
    sock.send_to(&msg, server_addr).await?;
    println!("[CLIENT] JOIN sent, waiting for JOIN_ACK...");

    // Wait for JOIN_ACK with timeout
    let mut buf = vec![0u8; 65536];
    let timeout = Duration::from_secs(5);

    match tokio::time::timeout(timeout, sock.recv_from(&mut buf)).await {
        Ok(Ok((n, from))) => {
            if n > 0 && buf[0] == JOIN_ACK {
                println!("[CLIENT] ✅ JOIN_ACK received from {}", from);

                // Update state
                let mut s = state.write().await;
                s.joined = true;

                Ok(())
            } else {
                Err(anyhow::anyhow!("Unexpected response from server"))
            }
        }
        Ok(Err(e)) => Err(anyhow::anyhow!("Receive error: {}", e)),
        Err(_) => Err(anyhow::anyhow!("Timeout waiting for JOIN_ACK")),
    }
}

/// Send periodic CLIENT_PING to server
pub async fn ping_loop(
    state: SharedClientState,
    sock: Arc<UdpSocket>,
) -> Result<()> {
    loop {
        // Wait 10 seconds
        tokio::time::sleep(Duration::from_secs(10)).await;

        let (joined, username, server_addr) = {
            let s = state.read().await;
            (s.joined, s.username.clone(), s.server_addr)
        };

        if !joined {
            println!("[CLIENT] Not joined yet, skipping ping");
            continue;
        }

        // Build CLIENT_PING message
        // Format: [CLIENT_PING][username_len:u16][username]
        let mut msg = vec![CLIENT_PING];
        let username_bytes = username.as_bytes();
        msg.extend((username_bytes.len() as u16).to_le_bytes());
        msg.extend_from_slice(username_bytes);

        // Send PING
        if let Err(e) = sock.send_to(&msg, server_addr).await {
            println!("[CLIENT] ⚠️  Failed to send PING: {}", e);
            continue;
        }

        println!("[CLIENT] PING sent to server");

        // Wait for SERVER_PONG (non-blocking, best effort)
        let mut buf = vec![0u8; 1024];
        match tokio::time::timeout(Duration::from_millis(500), sock.recv_from(&mut buf)).await {
            Ok(Ok((n, _))) => {
                if n > 1 && buf[0] == SERVER_PONG {
                    // Parse DOS version from PONG
                    if n >= 5 {
                        let dos_version = u32::from_le_bytes(buf[1..5].try_into().unwrap());

                        let mut s = state.write().await;
                        let old_version = s.dos_version;
                        s.dos_version = dos_version;

                        if dos_version != old_version {
                            println!("[CLIENT] ✅ PONG received - DOS version updated: {} -> {}", old_version, dos_version);
                        } else {
                            println!("[CLIENT] ✅ PONG received - DOS version: {}", dos_version);
                        }
                    }
                }
            }
            _ => {
                println!("[CLIENT] ⚠️  No PONG received (timeout)");
            }
        }
    }
}

/// Run the client listener (receives messages from server)
pub async fn run_listener(
    state: SharedClientState,
    sock: Arc<UdpSocket>,
) -> Result<()> {
    println!("[CLIENT-LISTENER] Started on {}", sock.local_addr()?);

    let mut buf = vec![0u8; 65536];

    loop {
        let (n, from) = sock.recv_from(&mut buf).await?;

        if n == 0 {
            continue;
        }

        let msg_type = buf[0];
        let data = &buf[1..n];

        match msg_type {
            JOIN_ACK => {
                println!("[CLIENT-LISTENER] JOIN_ACK received from {}", from);
                // Handled by join_server function
            }

            SERVER_PONG => {
                // Handled by ping_loop
            }

            VIEW_NOTIFICATION => {
                // Parse: [viewer_name_len:u16][viewer_name][image_name_len:u16][image_name][req_id:u32]
                let mut offset = 0;

                let viewer_len = u16::from_le_bytes(data[offset..offset + 2].try_into()?) as usize;
                offset += 2;
                let viewer_name = String::from_utf8(data[offset..offset + viewer_len].to_vec())?;
                offset += viewer_len;

                let image_len = u16::from_le_bytes(data[offset..offset + 2].try_into()?) as usize;
                offset += 2;
                let image_name = String::from_utf8(data[offset..offset + image_len].to_vec())?;
                offset += image_len;

                let req_id = u32::from_le_bytes(data[offset..offset + 4].try_into()?);

                println!("\n📩 [VIEW NOTIFICATION] {} wants to view image: {}", viewer_name, image_name);
                println!("   Request ID: {}", req_id);
                println!("   Action: Auto-approving (demo mode)");

                // Auto-approve for demo
                let mut response = vec![APPROVE_VIEW];
                response.extend(req_id.to_le_bytes());

                if let Err(e) = sock.send_to(&response, from).await {
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

/// Initialize client socket (binds to any available port)
pub async fn init_socket() -> Result<Arc<UdpSocket>> {
    let sock = UdpSocket::bind("0.0.0.0:0").await
        .context("Failed to bind UDP socket")?;

    let local_addr = sock.local_addr()?;
    println!("[CLIENT] Socket bound to {}", local_addr);

    Ok(Arc::new(sock))
}

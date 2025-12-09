//! P2P Server for Direct Client-to-Client Communication
//! Listens on a port in the range 9080+ for incoming P2P requests from other clients

use anyhow::{Context, Result};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::simple_client::SharedClientState;
use crate::protocol;

/// Start P2P listener by binding to port range 9080+
/// Returns (TcpListener, bound_port)
pub async fn start_p2p_listener(
    start_port: u16,
) -> Result<(TcpListener, u16)> {
    const MAX_PORT_ATTEMPTS: u16 = 100;

    for port in start_port..start_port + MAX_PORT_ATTEMPTS {
        match TcpListener::bind(("0.0.0.0", port)).await {
            Ok(listener) => {
                println!("[P2P_SERVER] Successfully bound to port {}", port);
                return Ok((listener, port));
            }
            Err(e) => {
                if port == start_port + MAX_PORT_ATTEMPTS - 1 {
                    return Err(anyhow::anyhow!(
                        "Failed to bind to any port in range {}-{}: {}",
                        start_port,
                        start_port + MAX_PORT_ATTEMPTS - 1,
                        e
                    ));
                }
                // Try next port
                continue;
            }
        }
    }

    Err(anyhow::anyhow!("No available ports in range"))
}

/// Run P2P server - accept incoming connections and handle P2P messages
pub async fn run_p2p_server(
    listener: TcpListener,
    state: SharedClientState,
) -> Result<()> {
    let local_addr = listener.local_addr()?;
    println!("[P2P_SERVER] Listening for P2P connections on {}", local_addr);

    loop {
        let (stream, peer_addr) = match listener.accept().await {
            Ok(conn) => conn,
            Err(e) => {
                eprintln!("[P2P_SERVER] Failed to accept connection: {}", e);
                continue;
            }
        };

        println!("[P2P_SERVER] Accepted P2P connection from {}", peer_addr);

        let state_clone = state.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_peer_connection(stream, peer_addr, state_clone).await {
                eprintln!("[P2P_SERVER] Error handling peer connection from {}: {}", peer_addr, e);
            }
        });
    }
}

/// Handle incoming P2P connection from another client
async fn handle_peer_connection(
    mut stream: TcpStream,
    peer_addr: SocketAddr,
    state: SharedClientState,
) -> Result<()> {
    println!("[P2P_SERVER] Handling connection from peer: {}", peer_addr);

    loop {
        // Read message type (1 byte)
        let mut msg_type_buf = [0u8; 1];
        match stream.read_exact(&mut msg_type_buf).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                println!("[P2P_SERVER] Peer {} disconnected", peer_addr);
                break;
            }
            Err(e) => {
                return Err(anyhow::anyhow!("Failed to read message type: {}", e));
            }
        }

        let msg_type = msg_type_buf[0];

        // Read payload length (u32)
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await
            .context("Failed to read payload length")?;
        let payload_len = u32::from_le_bytes(len_buf) as usize;

        // Read payload
        let mut payload = vec![0u8; payload_len];
        stream.read_exact(&mut payload).await
            .context("Failed to read payload")?;

        println!("[P2P_SERVER] Received P2P message type={} len={} from {}",
                 msg_type, payload_len, peer_addr);

        // Route to appropriate handler
        match msg_type {
            protocol::LIFE_CHECK => {
                // Server is checking if we're alive - respond with ACK
                println!("[P2P_SERVER] Received LIFE_CHECK from server, sending ACK");
                let response = vec![protocol::LIFE_CHECK_ACK];
                if let Err(e) = stream.write_all(&response).await {
                    eprintln!("[P2P_SERVER] Failed to send LIFE_CHECK_ACK: {}", e);
                } else {
                    println!("[P2P_SERVER] ✅ Sent LIFE_CHECK_ACK to server");
                }
                // Continue listening for more messages
            }
            protocol::PEER_VIEW_REQUEST => {
                handle_peer_view_request(&mut stream, peer_addr, &payload, state.clone()).await?;
            }
            protocol::PEER_ADJUST_REQUEST => {
                handle_peer_adjust_request(&mut stream, peer_addr, &payload, state.clone()).await?;
            }
            protocol::PEER_REVOKE => {
                handle_peer_revoke(&mut stream, peer_addr, &payload, state.clone()).await?;
            }
            _ => {
                println!("[P2P_SERVER] Unknown P2P message type: {}", msg_type);
            }
        }
    }

    Ok(())
}

/// Handle PEER_VIEW_REQUEST from viewer
/// Format: [viewer_len:u16][viewer][image_len:u16][image_name][requested_views:u32]
async fn handle_peer_view_request(
    stream: &mut TcpStream,
    peer_addr: SocketAddr,
    data: &[u8],
    state: SharedClientState,
) -> Result<()> {
    println!("[P2P_SERVER] Handling PEER_VIEW_REQUEST from {}", peer_addr);

    let mut offset = 0;

    // Parse viewer name
    if data.len() < 2 {
        return Err(anyhow::anyhow!("Invalid PEER_VIEW_REQUEST: too short"));
    }

    let viewer_len = u16::from_le_bytes(data[offset..offset + 2].try_into()?) as usize;
    offset += 2;

    if data.len() < offset + viewer_len {
        return Err(anyhow::anyhow!("Invalid viewer name length"));
    }

    let viewer_name = String::from_utf8(data[offset..offset + viewer_len].to_vec())?;
    offset += viewer_len;

    // Parse image name
    if data.len() < offset + 2 {
        return Err(anyhow::anyhow!("Invalid image name length"));
    }

    let image_len = u16::from_le_bytes(data[offset..offset + 2].try_into()?) as usize;
    offset += 2;

    if data.len() < offset + image_len {
        return Err(anyhow::anyhow!("Invalid image name"));
    }

    let image_name = String::from_utf8(data[offset..offset + image_len].to_vec())?;
    offset += image_len;

    // Parse requested views
    if data.len() < offset + 4 {
        return Err(anyhow::anyhow!("Invalid requested views"));
    }

    let requested_views = u32::from_le_bytes(data[offset..offset + 4].try_into()?);

    println!("[P2P_SERVER] PEER_VIEW_REQUEST: viewer={}, image={}, views={}",
             viewer_name, image_name, requested_views);

    // TODO: Check local_access_map for permissions
    // TODO: If approved, encrypt image and send PEER_VIEW_RESPONSE
    // TODO: If rejected, send PEER_VIEW_REJECTED

    // For now, send a simple ACK
    send_p2p_message(stream, protocol::PEER_ACK, &[]).await?;

    Ok(())
}

/// Handle PEER_ADJUST_REQUEST from viewer to adjust view count
async fn handle_peer_adjust_request(
    stream: &mut TcpStream,
    peer_addr: SocketAddr,
    data: &[u8],
    _state: SharedClientState,
) -> Result<()> {
    println!("[P2P_SERVER] Handling PEER_ADJUST_REQUEST from {} (not yet implemented)", peer_addr);
    send_p2p_message(stream, protocol::PEER_ACK, &[]).await?;
    Ok(())
}

/// Handle PEER_REVOKE from owner to revoke access
async fn handle_peer_revoke(
    stream: &mut TcpStream,
    peer_addr: SocketAddr,
    data: &[u8],
    _state: SharedClientState,
) -> Result<()> {
    println!("[P2P_SERVER] Handling PEER_REVOKE from {} (not yet implemented)", peer_addr);
    send_p2p_message(stream, protocol::PEER_ACK, &[]).await?;
    Ok(())
}

/// Send a P2P message to peer
/// Format: [msg_type:u8][payload_len:u32][payload]
pub async fn send_p2p_message(
    stream: &mut TcpStream,
    msg_type: u8,
    payload: &[u8],
) -> Result<()> {
    let mut buf = Vec::new();
    buf.push(msg_type);
    buf.extend((payload.len() as u32).to_le_bytes());
    buf.extend_from_slice(payload);

    stream.write_all(&buf).await
        .context("Failed to send P2P message")?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_port_binding() {
        let result = start_p2p_listener(9080).await;
        assert!(result.is_ok());
        let (_listener, port) = result.unwrap();
        assert!(port >= 9080 && port < 9180);
    }
}

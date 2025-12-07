//! Simple client implementation for testing the new protocol
//! Supports JOIN and PING operations
//! Uses TCP for reliable communication with the server

use anyhow::{Context, Result};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::signal;
use tokio::net::TcpSocket;
use tokio::sync::RwLock;

use crate::protocol::*;
use crate::dos::DosState;
use crate::viewer::{PendingRequest, DownloadedImage};
use crate::owner::PendingViewRequest;

/// Fixed client listen/advertised port for TCP
pub const CLIENT_PORT: u16 = 9080;

/// Client state
#[derive(Clone)]
pub struct ClientState {
    pub username: String,
    pub server_addr: SocketAddr,
    pub client_port: u16,
    /// Cover image (first image in CLI args) - used for steganography, NOT shared in DOS-C
    pub cover_image: Option<String>,
    /// Actual images (remaining CLI args) - shareable images shown in DOS-C
    pub actual_images: Vec<String>,
    pub joined: bool,
    pub dos_version: u32,
    pub dos: DosState,
    // Viewer side: track my outgoing requests
    pub my_requests: HashMap<u32, PendingRequest>,
    // Owner side: track incoming view requests
    pub pending_view_requests: HashMap<u32, PendingViewRequest>,
    // Track downloaded images
    pub downloads: Vec<DownloadedImage>,
}

impl ClientState {
    pub fn new(username: String, server_addr: SocketAddr) -> Self {
        Self {
            username,
            server_addr,
            client_port: 0,
            cover_image: None,
            actual_images: Vec::new(),
            joined: false,
            dos_version: 0,
            dos: DosState::new(),
            my_requests: HashMap::new(),
            pending_view_requests: HashMap::new(),
            downloads: Vec::new(),
        }
    }

    /// Set images from command line (first = cover if multiple, rest = actual)
    pub fn set_images(&mut self, images: Vec<String>) {
        if images.len() > 1 {
            // Multiple images: first = cover, rest = actual
            self.cover_image = Some(images[0].clone());
            self.actual_images = images[1..].to_vec();
            println!("[CLIENT] Cover image: {}", images[0]);
            println!("[CLIENT] Actual images: {:?}", self.actual_images);
        } else if images.len() == 1 {
            // Single image: actual only, no cover
            self.cover_image = None;
            self.actual_images = images;
            println!("[CLIENT] Actual images: {:?}", self.actual_images);
            println!("[CLIENT] No cover image (single image provided)");
        } else {
            // No images
            self.cover_image = None;
            self.actual_images = Vec::new();
            println!("[CLIENT] No images");
        }
    }

    /// Get all images (cover + actual) for JOIN message
    pub fn get_all_images_for_join(&self) -> Vec<String> {
        let mut all_images = Vec::new();
        if let Some(ref cover) = self.cover_image {
            all_images.push(cover.clone());
        }
        all_images.extend(self.actual_images.clone());
        all_images
    }
}

pub type SharedClientState = Arc<RwLock<ClientState>>;

#[derive(Default)]
struct IncomingImage {
    total_chunks: u32,
    chunks: HashMap<u32, Vec<u8>>,
}

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
        (s.username.clone(), s.server_addr, s.get_all_images_for_join())
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
        Ok(Ok((msg_type, payload))) => {
            if msg_type == JOIN_ACK {
                println!("[CLIENT] ✅ JOIN_ACK received from server");

                // Parse DOS-C from payload
                match crate::dos::parse_dos_c_from_join_ack(&payload) {
                    Ok((clients, dos_version)) => {
                        println!("[CLIENT] Parsed DOS-C: {} clients, version {}", clients.len(), dos_version);

                        // Update state with DOS-C
                        let mut s = state.write().await;
                        s.joined = true;
                        s.dos.update(clients, dos_version);
                        s.dos_version = dos_version as u32;

                        println!("[CLIENT] DOS-C updated successfully");
                    }
                    Err(e) => {
                        println!("[CLIENT] ⚠️  Failed to parse DOS-C from JOIN_ACK: {}", e);
                        // Still mark as joined, but DOS will be empty
                        let mut s = state.write().await;
                        s.joined = true;
                    }
                }

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
    state: SharedClientState,
    mut reader: tokio::net::tcp::OwnedReadHalf,
    writer: Arc<tokio::sync::Mutex<tokio::net::tcp::OwnedWriteHalf>>,
) -> Result<()> {
    println!("[CLIENT-LISTENER] Started");

    #[derive(Default)]
    struct IncomingImage {
        total_chunks: u32,
        chunks: HashMap<u32, Vec<u8>>,
    }
    let mut images: HashMap<String, IncomingImage> = HashMap::new();

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
                    let mut s = state.write().await;
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
                // Parse: [viewer_name_len:u16][viewer_name][image_name_len:u16][image_name][req_id:u32][requested_views:u32]
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
                offset += 4;

                // Parse requested_views (if present in payload)
                let requested_views = if data.len() >= offset + 4 {
                    u32::from_le_bytes(data[offset..offset + 4].try_into()?)
                } else {
                    0 // Default if not provided
                };

                println!("\n📩 [VIEW NOTIFICATION] {} wants to view image: {}", viewer_name, image_name);
                println!("   Request ID: {}", req_id);
                println!("   Requested views: {}", requested_views);
                println!("   Status: ⏳ Pending owner approval (use HTTP API to approve/deny)");

                // Store in pending requests for owner to handle via web UI
                let pending_req = crate::owner::PendingViewRequest {
                    request_id: req_id,
                    viewer: viewer_name.clone(),
                    image_name: image_name.clone(),
                    requested_views,
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                };

                {
                    let mut s = state.write().await;
                    s.pending_view_requests.insert(req_id, pending_req);
                }

                println!("   💡 View pending requests via web UI at http://localhost:3000/dashboard");
            }

            DOS_UPDATE => {
                println!("[CLIENT-LISTENER] DOS_UPDATE received - refreshing DOS-C");

                // Parse DOS-C from payload (same format as JOIN_ACK)
                match crate::dos::parse_dos_c_from_join_ack(&data) {
                    Ok((clients, dos_version)) => {
                        println!("[CLIENT] Parsed updated DOS-C: {} clients, version {}", clients.len(), dos_version);

                        // Update state with new DOS-C
                        let mut s = state.write().await;
                        s.dos.update(clients, dos_version);
                        s.dos_version = dos_version as u32;

                        println!("[CLIENT] DOS-C refreshed successfully");
                    }
                    Err(e) => {
                        println!("[CLIENT] ⚠️  Failed to parse DOS-C from DOS_UPDATE: {}", e);
                    }
                }
            }

            REJECTED => {
                // Parse: [req_id:u32] or [req_id:u32][reason_len:u16][reason]
                if data.len() >= 4 {
                    let req_id = u32::from_le_bytes(data[0..4].try_into()?);
                    println!("[CLIENT-LISTENER] ⚠️  Request {} REJECTED", req_id);

                    // Update request status
                    let mut s = state.write().await;
                    if let Some(req) = s.my_requests.get_mut(&req_id) {
                        req.status = crate::viewer::RequestStatus::Rejected;
                    }
                } else {
                    println!("[CLIENT-LISTENER] REJECTED message too short");
                }
            }

            APPROVED => {
                // Parse: [req_id:u32]
                if data.len() >= 4 {
                    let req_id = u32::from_le_bytes(data[0..4].try_into()?);
                    println!("[CLIENT-LISTENER] ✅ Request {} APPROVED by owner - waiting for image chunks", req_id);

                    // Update request status
                    let mut s = state.write().await;
                    if let Some(req) = s.my_requests.get_mut(&req_id) {
                        req.status = crate::viewer::RequestStatus::Approved;
                    }
                } else {
                    println!("[CLIENT-LISTENER] APPROVED message too short");
                }
            }

            IMAGE_CHUNK => {
                // Parse: [name_len:u16][name][seq:u32][total:u32][chunk_len:u16][chunk]
                let mut offset = 0;
                if data.len() < 2 {
                    println!("[CLIENT-LISTENER] IMAGE_CHUNK too short");
                    continue;
                }
                let name_len = u16::from_le_bytes(data[offset..offset + 2].try_into()?) as usize;
                offset += 2;
                if data.len() < offset + name_len + 4 + 4 + 2 {
                    println!("[CLIENT-LISTENER] IMAGE_CHUNK invalid name length");
                    continue;
                }
                let name = String::from_utf8(data[offset..offset + name_len].to_vec())?;
                offset += name_len;
                let seq = u32::from_le_bytes(data[offset..offset + 4].try_into()?);
                offset += 4;
                let total = u32::from_le_bytes(data[offset..offset + 4].try_into()?);
                offset += 4;
                let chunk_len = u16::from_le_bytes(data[offset..offset + 2].try_into()?) as usize;
                offset += 2;
                if data.len() < offset + chunk_len {
                    println!("[CLIENT-LISTENER] IMAGE_CHUNK data too short");
                    continue;
                }
                let chunk = data[offset..offset + chunk_len].to_vec();

                let entry = images.entry(name.clone()).or_insert_with(|| IncomingImage {
                    total_chunks: total,
                    chunks: HashMap::new(),
                });
                entry.total_chunks = total;
                entry.chunks.insert(seq, chunk);

                if entry.chunks.len() as u32 == entry.total_chunks {
                    // Assemble in order
                    let mut assembled = Vec::new();
                    for i in 0..entry.total_chunks {
                        if let Some(c) = entry.chunks.get(&i) {
                            assembled.extend_from_slice(c);
                        } else {
                            println!("[CLIENT-LISTENER] Missing chunk {} for {}", i, name);
                            break;
                        }
                    }
                    let dir = "received";
                    if let Err(e) = tokio::fs::create_dir_all(dir).await {
                        println!("[CLIENT-LISTENER] Failed to create dir {}: {}", dir, e);
                    }

                    let path = format!("{}/{}.png", dir, name);
                    match tokio::fs::write(&path, &assembled).await {
                        Ok(_) => println!("[CLIENT-LISTENER] ✅ Image {} assembled and saved to {}", name, path),
                        Err(e) => println!("[CLIENT-LISTENER] Failed to write image {}: {}", path, e),
                    }
                }
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

/// Owner upload: send true image, cover image, and metadata to server
pub async fn upload_owner_image(
    username: &str,
    server_addr: SocketAddr,
    image_name: &str,
    true_path: &str,
    cover_path: &str,
    meta_path: &str,
    stay_open: bool,
) -> Result<()> {
    let true_bytes = fs::read(true_path).await
        .with_context(|| format!("Failed to read true image {}", true_path))?;
    let cover_bytes = fs::read(cover_path).await
        .with_context(|| format!("Failed to read cover image {}", cover_path))?;
    let meta_bytes = fs::read(meta_path).await
        .with_context(|| format!("Failed to read metadata {}", meta_path))?;

    let (mut reader, writer) = connect_to_server(server_addr).await?;

    // META message
    let mut meta_payload = Vec::new();
    meta_payload.extend((username.len() as u16).to_le_bytes());
    meta_payload.extend(username.as_bytes());
    meta_payload.extend((image_name.len() as u16).to_le_bytes());
    meta_payload.extend(image_name.as_bytes());
    meta_payload.extend((true_bytes.len() as u32).to_le_bytes());
    meta_payload.extend((cover_bytes.len() as u32).to_le_bytes());
    meta_payload.extend((meta_bytes.len() as u32).to_le_bytes());
    {
        let mut w = writer.lock().await;
        send_tcp_message_generic(&mut *w, OWNER_IMAGE_META, &meta_payload).await?;
    }

    // Chunk helper
    async fn send_chunk(
        writer: &Arc<tokio::sync::Mutex<tokio::net::tcp::OwnedWriteHalf>>,
        owner: &str,
        image: &str,
        kind: u8,
        data: &[u8],
    ) -> Result<()> {
        let chunk_size = 1000usize;
        for (idx, chunk) in data.chunks(chunk_size).enumerate() {
            let mut payload = Vec::new();
            payload.extend((owner.len() as u16).to_le_bytes());
            payload.extend(owner.as_bytes());
            payload.extend((image.len() as u16).to_le_bytes());
            payload.extend(image.as_bytes());
            payload.push(kind);
            let byte_offset = (idx * chunk_size) as u32;
            payload.extend(byte_offset.to_le_bytes());
            payload.extend((chunk.len() as u16).to_le_bytes());
            payload.extend(chunk);
            let mut w = writer.lock().await;
            send_tcp_message_generic(&mut *w, OWNER_IMAGE_CHUNK, &payload).await?;
        }
        Ok(())
    }

    send_chunk(&writer, username, image_name, 0, &true_bytes).await?;
    send_chunk(&writer, username, image_name, 1, &cover_bytes).await?;
    send_chunk(&writer, username, image_name, 2, &meta_bytes).await?;

    println!(
        "[OWNER-UPLOAD] Uploaded image '{}' (true={} bytes, cover={} bytes, meta={})",
        image_name,
        true_bytes.len(),
        cover_bytes.len(),
        meta_bytes.len()
    );

    if stay_open {
        println!("[OWNER-UPLOAD] Keeping connection open. Press Ctrl+C to exit.");
        loop {
            tokio::select! {
                _ = signal::ctrl_c() => {
                    println!("[OWNER-UPLOAD] Ctrl+C received, closing connection.");
                    break;
                }
                recv = recv_tcp_message_generic(&mut reader) => {
                    match recv {
                        Ok((msg_type, payload)) => {
                            println!("[OWNER-UPLOAD-RECV] msg_type={} payload_len={}", msg_type, payload.len());
                        }
                        Err(e) => {
                            println!("[OWNER-UPLOAD] Connection closed or error: {}", e);
                            break;
                        }
                    }
                }
            }
        }
    } else {
        // Briefly check for acks then exit
        let _ = tokio::time::timeout(Duration::from_millis(500), recv_tcp_message_generic(&mut reader)).await;
    }

    Ok(())
}

/// Approve a view and receive the embedded image back
pub async fn approve_and_receive(
    username: &str,
    server_addr: SocketAddr,
    image_name: &str,
    req_id: u32,
) -> Result<()> {
    let (mut reader, writer) = connect_to_server(server_addr).await?;
    println!("[APPROVE] Connected as {}", username);

    // Send JOIN first to register
    let state = Arc::new(tokio::sync::RwLock::new(ClientState::new(username.to_string(), server_addr)));
    {
        let mut st = state.write().await;
        st.client_port = CLIENT_PORT;
        st.joined = true;
    }

    // Build APPROVE_VIEW: [req_id:u32]
    let mut payload = Vec::new();
    payload.extend(req_id.to_le_bytes());
    {
        let mut w = writer.lock().await;
        send_tcp_message_generic(&mut *w, APPROVE_VIEW, &payload).await?;
    }
    println!("[APPROVE] Sent APPROVE_VIEW req_id={} image={}", req_id, image_name);

    // Listen for IMAGE_CHUNKs until Ctrl+C
    let mut images: HashMap<String, IncomingImage> = HashMap::new();
    loop {
        tokio::select! {
            _ = signal::ctrl_c() => {
                println!("[APPROVE] Ctrl+C received, exiting.");
                break;
            }
            msg = recv_tcp_message_generic(&mut reader) => {
                match msg {
                    Ok((msg_type, data)) => {
                        if msg_type == IMAGE_CHUNK {
                            // reuse the listener logic
                            let mut offset = 0;
                            if data.len() < 2 { continue; }
                            let name_len = u16::from_le_bytes(data[offset..offset+2].try_into().unwrap()) as usize;
                            offset += 2;
                            if data.len() < offset + name_len + 4 + 4 + 2 { continue; }
                            let name = String::from_utf8(data[offset..offset+name_len].to_vec()).unwrap_or_else(|_| "img".into());
                            offset += name_len;
                            let seq = u32::from_le_bytes(data[offset..offset+4].try_into().unwrap());
                            offset += 4;
                            let total = u32::from_le_bytes(data[offset..offset+4].try_into().unwrap());
                            offset += 4;
                            let chunk_len = u16::from_le_bytes(data[offset..offset+2].try_into().unwrap()) as usize;
                            offset += 2;
                            if data.len() < offset + chunk_len { continue; }
                            let chunk = data[offset..offset+chunk_len].to_vec();
                            let entry = images.entry(name.clone()).or_insert_with(|| IncomingImage { total_chunks: total, chunks: HashMap::new() });
                            entry.total_chunks = total;
                            entry.chunks.insert(seq, chunk);
                            if entry.chunks.len() as u32 == entry.total_chunks {
                                let mut assembled = Vec::new();
                                for i in 0..entry.total_chunks {
                                    if let Some(c) = entry.chunks.get(&i) {
                                        assembled.extend_from_slice(c);
                                    }
                                }
                                let dir = "received";
                                let _ = fs::create_dir_all(dir).await;
                                let path = format!("{}/{}.png", dir, name);
                                if let Err(e) = fs::write(&path, &assembled).await {
                                    println!("[APPROVE] Failed to write image {}: {}", path, e);
                                } else {
                                    println!("[APPROVE] ✅ Received and saved {}", path);
                                    break;
                                }
                            }
                        } else {
                            println!("[APPROVE] Received msg_type={} payload_len={}", msg_type, data.len());
                        }
                    }
                    Err(e) => {
                        println!("[APPROVE] Connection closed/error: {}", e);
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}

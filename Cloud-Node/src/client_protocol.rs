use anyhow::Result;
use std::net::{SocketAddr, IpAddr};
use tokio::net::UdpSocket;
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::state::{SharedState, PendingRequest, RequestType};
use crate::executor_leader;

// ============================================================================
// MESSAGE TYPE CONSTANTS
// ============================================================================

// Phase 1: Initial handshake
pub const REQ: u8 = 10;                    // Client → Server: "I have a request"
pub const ACCEPT: u8 = 11;                 // Server → Client: "I'm the executor"

// Phase 2: Request details - DEPRECATED (replaced by P2P)
pub const VIEW_REQUEST: u8 = 12;           // DEPRECATED: Viewer → Executor (use PEER_VIEW_REQUEST)
pub const ADJUST_REQUEST: u8 = 13;         // DEPRECATED: Viewer → Executor (use PEER_ADJUST_REQUEST)
pub const REVOKE_REQUEST: u8 = 14;         // DEPRECATED: Owner → Executor (use PEER_REVOKE)

// Phase 3: Owner notifications (DEPRECATED - replaced by P2P)
pub const VIEW_NOTIFICATION: u8 = 15;      // DEPRECATED: Executor → Owner: Someone wants to view
pub const ADJUST_NOTIFICATION: u8 = 16;    // DEPRECATED: Executor → Owner: Someone wants more views

// Phase 4: Owner responses - DEPRECATED (replaced by P2P)
pub const APPROVE_VIEW: u8 = 17;           // DEPRECATED: Owner → Executor (use PEER_VIEW_RESPONSE)
pub const DENY_VIEW: u8 = 18;              // DEPRECATED: Owner → Executor (use PEER_VIEW_REJECTED)
pub const APPROVE_ADJUST: u8 = 19;         // DEPRECATED: Owner → Executor (use PEER_ACK)
pub const DENY_ADJUST: u8 = 20;            // DEPRECATED: Owner → Executor (use PEER_VIEW_REJECTED)

// Phase 5: Viewer responses
pub const APPROVED: u8 = 21;               // Executor → Viewer: Approved, chunks coming
pub const REJECTED: u8 = 22;               // Executor → Viewer: Rejected
pub const IMAGE_CHUNK: u8 = 23;            // Executor → Viewer: Image data chunk
pub const ADJUSTED_VIEWS: u8 = 24;         // Executor → Viewer: New view count + chunks
pub const REVOKED: u8 = 25;                // Executor → Viewer: Access revoked

// Management messages
pub const DELETE_IMAGE: u8 = 26;           // Executor → Viewer: Delete local image
pub const JOIN: u8 = 27;                   // Client → Executor: Join system
pub const JOIN_ACK: u8 = 28;               // Executor → Client: Welcome, here's DOS-C
pub const DOS_UPDATE: u8 = 29;             // Executor → All clients: DOS-C changed

// Client heartbeat
pub const CLIENT_PING: u8 = 50;            // Client → All servers: Heartbeat
pub const SERVER_PONG: u8 = 51;            // Server → Client: Pong + DOS version
pub const DOS_QUERY: u8 = 52;              // Client → Server: Request updated DOS-C

// Sync messages
pub const SYNC_USAGE: u8 = 30;             // Client → Executor: Report offline usage
pub const SYNC_ACK: u8 = 31;               // Executor → Client: Sync accepted
pub const REQUEST_VIEW_PERMISSION: u8 = 32;// Client → Executor: Can I view?
pub const VIEW_PERMISSION_GRANTED: u8 = 33;// Executor → Client: Yes
pub const VIEW_PERMISSION_DENIED: u8 = 34; // Executor → Client: No

// Image upload from owner
pub const OWNER_IMAGE_META: u8 = 35;       // Owner → Executor: Image metadata
pub const OWNER_IMAGE_CHUNK: u8 = 36;      // Owner → Executor: Image chunk

// P2P Client-to-Client Messages (NEW for P2P architecture)
pub const PEER_VIEW_REQUEST: u8 = 60;     // Viewer → Owner direct
pub const PEER_VIEW_RESPONSE: u8 = 61;    // Owner → Viewer (approved)
pub const PEER_VIEW_REJECTED: u8 = 62;    // Owner → Viewer (denied)
pub const PEER_IMAGE_CHUNK: u8 = 63;      // Owner → Viewer (image data)
pub const PEER_ADJUST_REQUEST: u8 = 64;   // Viewer → Owner (adjust views)
pub const PEER_REVOKE: u8 = 65;           // Owner → Viewer (revoke access)
pub const PEER_ACK: u8 = 66;              // Generic acknowledgment

// Server Offline/Access Map Messages (NEW for P2P architecture)
pub const OFFLINE_REQUESTS_QUERY: u8 = 53;    // Client → Server on startup
pub const OFFLINE_REQUESTS_RESPONSE: u8 = 54; // Server → Client
pub const ACCESS_MAP_QUERY: u8 = 55;          // Client → Server on startup
pub const ACCESS_MAP_RESPONSE: u8 = 56;       // Server → Client
pub const PENDING_REQUEST: u8 = 57;           // Server → Client: Deliver offline request

// Multi-Server Discovery Messages
pub const REQUEST_EXECUTOR: u8 = 70;          // Client → Server: Find executor
pub const EXECUTOR_ACK: u8 = 71;              // Server → Client: I am executor

// System Recovery and Failover (TCP-based)
pub const LIFE_CHECK: u8 = 72;                // Server → Client P2P TCP: "Are you alive?"
pub const LIFE_CHECK_ACK: u8 = 73;            // Client → Server TCP: "Yes, I'm alive"
pub const CLIENT_LEAVE: u8 = 74;              // Client → Server TCP: Graceful shutdown

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

pub fn now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
}

pub fn is_executor(state: &crate::state::ServerState, my_ip: IpAddr) -> bool {
    if let (Some(exec_ip), Some(deadline)) = (&state.executor_ip, state.executor_lease_deadline_ms) {
        exec_ip == &my_ip && now_ms() <= deadline
    } else {
        false
    }
}

pub fn generate_req_id() -> u32 {
    use std::sync::atomic::{AtomicU32, Ordering};
    static COUNTER: AtomicU32 = AtomicU32::new(1);
    COUNTER.fetch_add(1, Ordering::SeqCst)
}

// ============================================================================
// HANDLER FUNCTIONS (Skeletons - To be implemented)
// ============================================================================

/// Handle REQ message: Client requests service
pub async fn handle_req(
    state: SharedState,
    _cfg: &Config,
    sock: &UdpSocket,
    client_addr: SocketAddr,
    _data: &[u8],
) -> Result<()> {
    let s = state.read().await;
    let executor_ip = match s.executor_ip {
        Some(ip) => ip,
        None => {
            drop(s);
            debug!("No executor assigned, ignoring REQ");
            return Ok(());
        }
    };
    drop(s);

    let req_id = generate_req_id();

    // Send ACCEPT with executor IP
    let mut resp = vec![ACCEPT];
    resp.extend(req_id.to_le_bytes());
    let executor_ip_u32 = match executor_ip {
        IpAddr::V4(ip) => u32::from_be_bytes(ip.octets()),
        _ => return Err(anyhow::anyhow!("IPv6 not supported")),
    };
    resp.extend(executor_ip_u32.to_le_bytes());

    sock.send_to(&resp, client_addr).await?;

    debug!("Sent ACCEPT to {} for req_id {}", client_addr, req_id);

    Ok(())
}

/// Handle JOIN message: Client joins the system
pub async fn handle_join(
    state: SharedState,
    cfg: &Config,
    sock: &UdpSocket,
    client_addr: SocketAddr,
    data: &[u8],
) -> Result<()> {
    println!("[HANDLE_JOIN] Called with {} bytes from {}", data.len(), client_addr);

    // Parse: [username_len:u16][username][port:u16][num_images:u32][image_names...]
    let mut offset = 0;

    if data.len() < 2 {
        return Err(anyhow::anyhow!("JOIN message too short: {} bytes", data.len()));
    }

    let username_len = u16::from_le_bytes(data[offset..offset + 2].try_into()?) as usize;
    offset += 2;
    println!("[HANDLE_JOIN] username_len={}, offset={}", username_len, offset);

    if offset + username_len > data.len() {
        return Err(anyhow::anyhow!("Invalid username length: {} (data len={})", username_len, data.len()));
    }

    let username = String::from_utf8(data[offset..offset + username_len].to_vec())?;
    offset += username_len;
    println!("[HANDLE_JOIN] username={}, offset={}, data.len()={}, remaining={}",
             username, offset, data.len(), data.len() - offset);

    if offset + 2 > data.len() {
        return Err(anyhow::anyhow!("Not enough data for port: need offset+2={}, have data.len()={}", offset + 2, data.len()));
    }
    let client_port = u16::from_le_bytes(data[offset..offset + 2].try_into()?);
    offset += 2;
    println!("[HANDLE_JOIN] client_port={}, offset={}", client_port, offset);

    if offset + 4 > data.len() {
        return Err(anyhow::anyhow!("Not enough data for num_images: need offset+4={}, have data.len()={}", offset + 4, data.len()));
    }
    let num_images = u32::from_le_bytes(data[offset..offset + 4].try_into()?) as usize;
    offset += 4;
    println!("[HANDLE_JOIN] num_images={}, offset={}", num_images, offset);

    let mut images = Vec::new();
    for _ in 0..num_images {
        let img_len = u16::from_le_bytes(data[offset..offset + 2].try_into()?) as usize;
        offset += 2;
        let img_name = String::from_utf8(data[offset..offset + img_len].to_vec())?;
        offset += img_len;
        images.push(img_name);
    }

    println!("[HANDLE_JOIN] Parsed: username={}, port={}, {} images", username, client_port, images.len());
    info!("Client {} joining with {} images", username, images.len());

    // Check if executor
    let s = state.read().await;
    // Use our bound socket IP (not the client's IP) to determine executor role
    let my_ip = sock.local_addr()?.ip();
    let exec_ip = s.executor_ip;
    let lease = s.executor_lease_deadline_ms;
    let is_exec = is_executor(&*s, my_ip);
    println!("[HANDLE_JOIN] Executor check: my_ip={}, exec_ip={:?}, lease={:?}, is_exec={}", my_ip, exec_ip, lease, is_exec);

    if !is_exec {
        drop(s);
        println!(
            "[HANDLE_JOIN] Not executor, ignoring JOIN from {}:{} (my_ip={:?}, executor_ip={:?}, lease={:?})",
            client_addr.ip(),
            client_port,
            my_ip,
            exec_ip,
            lease
        );
        debug!(
            "Not executor, ignoring JOIN from {}:{} (my_ip={:?}, executor_ip={:?}, lease={:?})",
            client_addr.ip(),
            client_port,
            my_ip,
            exec_ip,
            lease
        );
        return Ok(());
    }
    drop(s);
    println!("[HANDLE_JOIN] I am executor, processing JOIN");

    // Send to leader via executor_leader channel
    let mut msg_data = Vec::new();
    msg_data.extend((username.len() as u16).to_le_bytes());
    msg_data.extend(username.as_bytes());

    let client_ip = client_addr.ip().to_string();
    msg_data.extend((client_ip.len() as u16).to_le_bytes());
    msg_data.extend(client_ip.as_bytes());

    msg_data.extend(client_port.to_le_bytes());
    msg_data.extend((images.len() as u32).to_le_bytes());

    for img in &images {
        msg_data.extend((img.len() as u16).to_le_bytes());
        msg_data.extend(img.as_bytes());
    }

    println!("[HANDLE_JOIN] Sending EXEC_ADD_CLIENT to leader...");
    info!(
        "Forwarding EXEC_ADD_CLIENT to leader for {} (client={}:{})",
        username,
        client_addr.ip(),
        client_port
    );
    executor_leader::send_to_leader(cfg, executor_leader::EXEC_ADD_CLIENT, &msg_data).await?;
    println!("[HANDLE_JOIN] Successfully sent to leader");

    // Build MINIMAL DOS-C v2.0 and send JOIN_ACK
    // Excludes: requesting client (self-exclusion), IP, port, last_seen, online
    // Includes: dos_version, name, actual_images only
    let (dos_c_version, clients_to_send_count, mut resp) = {
        let s = state.read().await;
        let dos_c_version = s.dos_c_version;

        // Self-exclusion: filter out requesting client
        let clients_to_send: Vec<_> = s.dos_clients
            .iter()
            .filter(|(name, _)| *name != &username)  // EXCLUDE SELF
            .collect();

        let clients_count = clients_to_send.len();

        let mut resp = vec![JOIN_ACK];
        resp.extend(dos_c_version.to_le_bytes());
        resp.extend((clients_count as u32).to_le_bytes());

        for (name, client) in clients_to_send {
            resp.extend((name.len() as u16).to_le_bytes());
            resp.extend(name.as_bytes());
            // Send only actual images in DOS-C (not cover)
            resp.extend((client.actual_images.len() as u32).to_le_bytes());
            for img in &client.actual_images {
                resp.extend((img.len() as u16).to_le_bytes());
                resp.extend(img.as_bytes());
            }
            // NO IP, NO PORT, NO LAST_SEEN, NO ONLINE - minimal format!
        }

        (dos_c_version, clients_count, resp)
    };

    let target_addr = SocketAddr::new(client_addr.ip(), client_port);
    println!("[HANDLE_JOIN] Sending JOIN_ACK ({} bytes) to {} (MINIMAL DOS-C v2.0, excluded: {})", resp.len(), target_addr, username);
    info!(
        "Sending JOIN_ACK to {} at {}:{} (clients={}, version={}, self-excluded)",
        username,
        client_addr.ip(),
        client_port,
        clients_to_send_count,
        dos_c_version
    );
    sock.send_to(&resp, target_addr).await?;

    println!("[HANDLE_JOIN] Successfully sent JOIN_ACK to {}", target_addr);
    info!(
        "Sent JOIN_ACK to {} at {}:{}",
        username,
        client_addr.ip(),
        client_port
    );

    Ok(())
}

/// Handle CLIENT_PING: Update last_seen timestamp
pub async fn handle_client_ping(
    state: SharedState,
    sock: &UdpSocket,
    client_addr: SocketAddr,
    data: &[u8],
) -> Result<()> {
    // Parse: [username_len:u16][username]
    let username_len = u16::from_le_bytes(data[0..2].try_into()?) as usize;
    let username = String::from_utf8(data[2..2 + username_len].to_vec())?;

    let now = now_ms() as u64;

    // Update last_seen
    let mut s = state.write().await;
    if let Some(client) = s.dos_clients.get_mut(&username) {
        client.last_seen = now;
        client.online = true;
    }
    let dos_c_version = s.dos_c_version;
    drop(s);

    // Send PONG with DOS version
    let mut resp = vec![SERVER_PONG];
    resp.extend(dos_c_version.to_le_bytes());

    sock.send_to(&resp, client_addr).await?;

    debug!("Ping from {}, responded with version {}", username, dos_c_version);

    Ok(())
}

/// DEPRECATED: Old server-mediated view request handler (replaced by P2P)
/// This function is no longer used in the P2P architecture
/// Clients now use PEER_VIEW_REQUEST for direct peer-to-peer communication
#[deprecated(note = "Replaced by P2P direct communication")]
pub async fn handle_view_request(
    state: SharedState,
    _cfg: &Config,
    sock: &UdpSocket,
    viewer_addr: SocketAddr,
    data: &[u8],
) -> Result<()> {
    // Parse: [req_id:u32][viewer_len:u16][viewer][owner_len:u16][owner]
    //        [image_name_len:u16][image_name][requested_views:u32]
    let mut offset = 0;

    let req_id = u32::from_le_bytes(data[offset..offset + 4].try_into()?);
    offset += 4;

    let viewer_len = u16::from_le_bytes(data[offset..offset + 2].try_into()?) as usize;
    offset += 2;
    let viewer_name = String::from_utf8(data[offset..offset + viewer_len].to_vec())?;
    offset += viewer_len;

    let owner_len = u16::from_le_bytes(data[offset..offset + 2].try_into()?) as usize;
    offset += 2;
    let owner_name = String::from_utf8(data[offset..offset + owner_len].to_vec())?;
    offset += owner_len;

    let image_name_len = u16::from_le_bytes(data[offset..offset + 2].try_into()?) as usize;
    offset += 2;
    let image_name = String::from_utf8(data[offset..offset + image_name_len].to_vec())?;
    offset += image_name_len;

    let requested_views = u32::from_le_bytes(data[offset..offset + 4].try_into()?);

    info!("VIEW_REQUEST: {} wants to view {}/{} ({} views)",
          viewer_name, owner_name, image_name, requested_views);

    // Check if executor
    let s = state.read().await;
    // Use our bound socket IP (not the viewer's IP) to determine executor role
    let my_ip = sock.local_addr()?.ip();
    if !is_executor(&*s, my_ip) {
        drop(s);
        return Ok(());
    }

    // Check if owner online
    let owner_client = match s.dos_clients.get(&owner_name) {
        Some(c) => c.clone(),
        None => {
            drop(s);
            // Send REJECTED: owner not found
            let mut resp = vec![REJECTED];
            resp.extend(req_id.to_le_bytes());
            let reason = "Owner not found";
            resp.extend((reason.len() as u16).to_le_bytes());
            resp.extend(reason.as_bytes());
            sock.send_to(&resp, viewer_addr).await?;
            return Ok(());
        }
    };

    if !owner_client.online {
        drop(s);
        // Send REJECTED: owner offline
        let mut resp = vec![REJECTED];
        resp.extend(req_id.to_le_bytes());
        let reason = "Owner offline";
        resp.extend((reason.len() as u16).to_le_bytes());
        resp.extend(reason.as_bytes());
        sock.send_to(&resp, viewer_addr).await?;
        return Ok(());
    }
    drop(s);

    // Create pending request
    let mut s = state.write().await;
    s.pending_requests.insert(req_id, PendingRequest {
        req_id,
        executor_ip: my_ip,
        req_type: RequestType::View,
        owner_name: owner_name.clone(),
        viewer_name: viewer_name.clone(),
        image_name: image_name.clone(),
        initiated_at: now_ms(),
    });
    drop(s);

    // Send VIEW_NOTIFICATION to owner
    let owner_addr = SocketAddr::new(
        owner_client.client_ip.parse()?,
        owner_client.client_port
    );

    let mut notif = vec![VIEW_NOTIFICATION];
    notif.extend(req_id.to_le_bytes());
    notif.extend((viewer_name.len() as u16).to_le_bytes());
    notif.extend(viewer_name.as_bytes());
    notif.extend((image_name.len() as u16).to_le_bytes());
    notif.extend(image_name.as_bytes());
    notif.extend(requested_views.to_le_bytes());

    sock.send_to(&notif, owner_addr).await?;

    info!("Sent VIEW_NOTIFICATION to owner {}", owner_name);

    Ok(())
}

/// DEPRECATED: Old server-mediated deny view handler (replaced by P2P)
/// This function is no longer used in the P2P architecture
/// Owners now use PEER_VIEW_REJECTED for direct peer-to-peer communication
#[deprecated(note = "Replaced by P2P direct communication")]
pub async fn handle_deny_view(
    state: SharedState,
    sock: &UdpSocket,
    _owner_addr: SocketAddr,
    data: &[u8],
) -> Result<()> {
    // Parse: [req_id:u32]
    let req_id = u32::from_le_bytes(data[0..4].try_into()?);

    info!("Owner denied view request {}", req_id);

    // Lookup pending request
    let s = state.read().await;
    let pending = match s.pending_requests.get(&req_id) {
        Some(p) => p.clone(),
        None => {
            drop(s);
            warn!("Pending request {} not found", req_id);
            return Ok(());
        }
    };
    drop(s);

    // Get viewer address from dos_clients
    let s = state.read().await;
    let viewer_client = match s.dos_clients.get(&pending.viewer_name) {
        Some(c) => c.clone(),
        None => {
            drop(s);
            warn!("Viewer {} not found", pending.viewer_name);
            return Ok(());
        }
    };
    drop(s);

    let viewer_addr = SocketAddr::new(
        viewer_client.client_ip.parse()?,
        viewer_client.client_port
    );

    // Send REJECTED to viewer
    let mut resp = vec![REJECTED];
    resp.extend(req_id.to_le_bytes());
    let reason = "Owner denied request";
    resp.extend((reason.len() as u16).to_le_bytes());
    resp.extend(reason.as_bytes());

    sock.send_to(&resp, viewer_addr).await?;

    // Remove from pending requests
    state.write().await.pending_requests.remove(&req_id);

    info!("Sent REJECTED to viewer {}", pending.viewer_name);

    Ok(())
}

/// Handle SYNC_USAGE: Client reports offline usage
pub async fn handle_sync_usage(
    state: SharedState,
    cfg: &Config,
    sock: &UdpSocket,
    client_addr: SocketAddr,
    data: &[u8],
) -> Result<()> {
    // Parse: [req_id:u32][image_uuid_len:u16][image_uuid][consumed_offline:u32]
    let mut offset = 0;

    let req_id = u32::from_le_bytes(data[offset..offset + 4].try_into()?);
    offset += 4;

    let uuid_len = u16::from_le_bytes(data[offset..offset + 2].try_into()?) as usize;
    offset += 2;
    let image_uuid = String::from_utf8(data[offset..offset + uuid_len].to_vec())?;
    offset += uuid_len;

    let consumed_offline = u32::from_le_bytes(data[offset..offset + 4].try_into()?);

    info!("SYNC_USAGE (DEPRECATED Phase 2): {} consumed {} views offline", image_uuid, consumed_offline);

    // REMOVED Phase 2: Access map now managed locally by clients only
    // Send ACK to maintain protocol compatibility
    let mut resp = vec![SYNC_ACK];
    resp.extend(req_id.to_le_bytes());
    sock.send_to(&resp, client_addr).await?;
    return Ok(());

    /* OLD CODE - REMOVED:
    let s = state.read().await;
    let access_entry = s.dos_access.iter()
        .find(|(_, access)| access.image_uuid == image_uuid)
        .map(|(id, access)| (id.clone(), access.clone()));
    drop(s);

    let (access_id, mut access) = match access_entry {
        Some(e) => e,
        None => {
            // Access record not found - might be deleted
            let mut resp = vec![SYNC_ACK];
            resp.extend(req_id.to_le_bytes());
            sock.send_to(&resp, client_addr).await?;
            return Ok(());
        }
    };

    // Check if revoked
    if access.revoked {
        // Send REVOKED + DELETE_IMAGE
        let mut resp = vec![REVOKED];
        resp.extend(req_id.to_le_bytes());
        sock.send_to(&resp, client_addr).await?;

        // Send DELETE_IMAGE
        let mut del_msg = vec![DELETE_IMAGE];
        del_msg.extend((access.owner.len() as u16).to_le_bytes());
        del_msg.extend(access.owner.as_bytes());
        del_msg.extend((access.image_name.len() as u16).to_le_bytes());
        del_msg.extend(access.image_name.as_bytes());
        sock.send_to(&del_msg, client_addr).await?;

        info!("Sent REVOKED to {}", access.viewer);
        return Ok(());
    }

    // Update consumed views
    let new_consumed = access.consumed_views + consumed_offline;
    access.consumed_views = new_consumed;

    // Send to leader via executor_leader channel
    let mut msg_data = Vec::new();
    msg_data.extend((access_id.len() as u16).to_le_bytes());
    msg_data.extend(access_id.as_bytes());
    msg_data.extend(new_consumed.to_le_bytes());

    executor_leader::send_to_leader(cfg, executor_leader::EXEC_UPDATE_ACCESS, &msg_data).await?;

    // Send SYNC_ACK
    let mut resp = vec![SYNC_ACK];
    resp.extend(req_id.to_le_bytes());
    sock.send_to(&resp, client_addr).await?;

    info!("Synced {} offline views for {}", consumed_offline, image_uuid);

    Ok(())
    */  // END OLD CODE
}

/// Helper: Generate access_id from owner, viewer, image_name
pub fn generate_access_id(owner: &str, viewer: &str, image_name: &str) -> String {
    format!("{}:{}:{}", owner, viewer, image_name)
}

use crate::{config::Config, state::SharedState};
use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::net::UdpSocket;
use tokio::time::{interval, Duration};
use tracing::{debug, info, warn};

/// Message types for history table
pub const HISTORY_UPDATE: u8 = 8;
pub const FORWARD_REQUEST: u8 = 9;
pub const TABLE_SYNC: u8 = 10;
pub const TABLE_UPDATE: u8 = 11;

#[inline]
fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}

/// Multicast HISTORY_UPDATE to all peers after completing a request
/// Format: [8][req_id: u32][executor_ip: 4 bytes][timestamp: u128 as 16 bytes]
pub async fn multicast_history_update(
    sock: &UdpSocket,
    cfg: &Config,
    req_id: u32,
    executor_ip: IpAddr,
    timestamp: u128,
) {
    let ipv4_bytes = match executor_ip {
        IpAddr::V4(v4) => u32::from(v4).to_be_bytes(),
        _ => return, // Only IPv4 supported
    };

    let mut pkt = Vec::with_capacity(1 + 4 + 4 + 16);
    pkt.push(HISTORY_UPDATE);
    pkt.extend(req_id.to_le_bytes());
    pkt.extend(ipv4_bytes);
    pkt.extend(timestamp.to_le_bytes());

    for peer in cfg.assignment_peer_addrs() {
        let _ = sock.send_to(&pkt, peer).await;
    }

    debug!(req_id, ?executor_ip, timestamp, "HISTORY_UPDATE multicast sent");
}

/// Send FORWARD_REQUEST to original executor
/// Format: [9][req_id: u32][client_ip: 4 bytes][client_port: u16][data_len: u32][data...]
pub async fn send_forward_request(
    sock: &UdpSocket,
    cfg: &Config,
    original_executor_ip: IpAddr,
    req_id: u32,
    client_addr: SocketAddr,
    request_data: &[u8],
) {
    let client_ip_bytes = match client_addr.ip() {
        IpAddr::V4(v4) => u32::from(v4).to_be_bytes(),
        _ => return,
    };

    let mut pkt = Vec::with_capacity(1 + 4 + 4 + 2 + 4 + request_data.len());
    pkt.push(FORWARD_REQUEST);
    pkt.extend(req_id.to_le_bytes());
    pkt.extend(client_ip_bytes);
    pkt.extend(client_addr.port().to_le_bytes());
    pkt.extend((request_data.len() as u32).to_le_bytes());
    pkt.extend(request_data);

    // Find matching assignment port for the executor IP
    for peer_addr in cfg.assignment_peer_addrs() {
        if peer_addr.ip() == original_executor_ip {
            let _ = sock.send_to(&pkt, peer_addr).await;
            info!(
                req_id,
                ?original_executor_ip,
                ?client_addr,
                "FORWARD_REQUEST sent to original executor"
            );
            return;
        }
    }

    warn!(
        req_id,
        ?original_executor_ip,
        "Failed to find assignment port for executor IP"
    );
}

/// Handle received HISTORY_UPDATE messages
/// Updates local history table with completed request info
pub async fn handle_history_update(state: SharedState, buf: &[u8]) {
    if buf.len() < 1 + 4 + 4 + 16 {
        return;
    }

    let req_id = u32::from_le_bytes(buf[1..5].try_into().unwrap());
    let executor_ip_be = u32::from_be_bytes(buf[5..9].try_into().unwrap());
    let executor_ip = IpAddr::V4(std::net::Ipv4Addr::from(executor_ip_be));
    let timestamp = u128::from_le_bytes(buf[9..25].try_into().unwrap());

    let mut s = state.write().await;
    
    // Check if this is an update from self (executor)
    let is_self = match executor_ip {
        IpAddr::V4(v4) => {
            if let Some(self_ip) = s.executor_ip {
                if let IpAddr::V4(self_v4) = self_ip {
                    u32::from(v4) == u32::from(self_v4) && s.node_id == get_node_id_from_ip(executor_ip)
                } else {
                    false
                }
            } else {
                false
            }
        }
        _ => false,
    };

    // Get existing path if this is self (before mutable borrow)
    let existing_path = if is_self {
        s.history.get(&req_id).and_then(|r| r.path_to_output_image.clone())
    } else {
        None
    };

    // Insert or update record
    use crate::state::HistoryRecord;
    s.history.insert(
        req_id,
        HistoryRecord {
            req_id,
            executor_node: executor_ip,
            path_to_output_image: existing_path,
            timestamp,
        },
    );

    debug!(req_id, ?executor_ip, timestamp, "HISTORY_UPDATE received and processed");
}

/// Handle received FORWARD_REQUEST messages
/// Original executor receives forwarded request and serves saved image
pub async fn handle_forward_request(
    state: SharedState,
    sock: Arc<UdpSocket>,
    cfg: Config,
    buf: &[u8],
) {
    if buf.len() < 1 + 4 + 4 + 2 + 4 {
        return;
    }

    let req_id = u32::from_le_bytes(buf[1..5].try_into().unwrap());
    let client_ip_be = u32::from_be_bytes(buf[5..9].try_into().unwrap());
    let client_ip = IpAddr::V4(std::net::Ipv4Addr::from(client_ip_be));
    let client_port = u16::from_le_bytes(buf[9..11].try_into().unwrap());
    let client_addr = SocketAddr::new(client_ip, client_port);
    let _data_len = u32::from_le_bytes(buf[11..15].try_into().unwrap()) as usize;

    info!(req_id, ?client_addr, "FORWARD_REQUEST received");

    // Check local history
    let path_opt = {
        let s = state.read().await;
        s.history.get(&req_id).and_then(|r| r.path_to_output_image.clone())
    };

    let path = match path_opt {
        Some(p) => p,
        None => {
            warn!(req_id, "FORWARD_REQUEST: history record not found");
            return;
        }
    };

    // Read saved image from disk
    let img_bytes = match tokio::fs::read(&path).await {
        Ok(bytes) => bytes,
        Err(e) => {
            warn!(req_id, ?path, error=%e, "FORWARD_REQUEST: failed to read saved image");
            return;
        }
    };

    info!(req_id, ?path, size=img_bytes.len(), "FORWARD_REQUEST: loaded saved image");

    // Send response to client (same logic as original response)
    send_response_to_client(sock, client_addr, req_id, &img_bytes, cfg.pacing_us).await;

    info!(req_id, ?client_addr, "FORWARD_REQUEST: response sent to client");
}

/// Send response packets to client with pacing
async fn send_response_to_client(
    sock: Arc<UdpSocket>,
    client_addr: SocketAddr,
    req_id: u32,
    data: &[u8],
    pacing_us: u64,
) {
    const RESP_DATA: u8 = 3;
    const MAX_PAYLOAD: usize = 1400;

    let total_len = data.len();
    let num_packets = (total_len + MAX_PAYLOAD - 1) / MAX_PAYLOAD;

    for i in 0..num_packets {
        let start = i * MAX_PAYLOAD;
        let end = std::cmp::min(start + MAX_PAYLOAD, total_len);
        let chunk = &data[start..end];

        let mut pkt = Vec::with_capacity(1 + 4 + 4 + 4 + chunk.len());
        pkt.push(RESP_DATA);
        pkt.extend(req_id.to_le_bytes());
        pkt.extend((i as u32).to_le_bytes());
        pkt.extend((num_packets as u32).to_le_bytes());
        pkt.extend(chunk);

        let _ = sock.send_to(&pkt, client_addr).await;

        if pacing_us > 0 {
            tokio::time::sleep(Duration::from_micros(pacing_us)).await;
        }
    }
}

/// Periodic cleanup task - runs every 5 seconds
/// Deletes history records older than 5 seconds and associated image files
pub async fn run_cleanup_task(state: SharedState, cfg: Config) {
    let mut tick = interval(Duration::from_secs(5));

    loop {
        tick.tick().await;

        let now = now_ms();
        let mut to_delete = Vec::new();
        let node_ip = {
            let s = state.read().await;
            cfg.node_id_to_ip(s.node_id)
        };

        {
            let s = state.read().await;
            for (req_id, record) in &s.history {
                let age_ms = now.saturating_sub(record.timestamp);
                if age_ms > 5000 {
                    to_delete.push((*req_id, record.clone()));
                }
            }
        }

        if !to_delete.is_empty() {
            let delete_count = to_delete.len(); // Get count before consuming
            let mut s = state.write().await;
            for (req_id, record) in to_delete {
                // If this node was the executor, delete the image file
                if let (Some(node_ip), Some(path)) = (node_ip, &record.path_to_output_image) {
                    if record.executor_node == node_ip {
                        if let Err(e) = tokio::fs::remove_file(path).await {
                            debug!(req_id, ?path, error=%e, "Failed to delete image file");
                        } else {
                            debug!(req_id, ?path, "Deleted image file");
                        }
                    }
                }

                s.history.remove(&req_id);
            }

            info!(deleted_count=delete_count, "Cleanup: removed old history records");
        }
    }
}

/// Periodic sync task - sends local table to leader every 10-15 seconds
pub async fn run_sync_to_leader_task(
    state: SharedState,
    sock: Arc<UdpSocket>,
    cfg: Config,
) {
    let mut tick = interval(Duration::from_millis(12000)); // 12 seconds

    loop {
        tick.tick().await;

        let (is_leader, ignoring, node_id, records) = {
            let s = state.read().await;
            if s.ignoring {
                continue;
            }
            (
                s.is_leader,
                s.ignoring,
                s.node_id,
                s.history.values().cloned().collect::<Vec<_>>(),
            )
        };

        if ignoring || is_leader {
            continue; // Don't send if failing or if I'm the leader
        }

        // Build TABLE_SYNC message
        // Format: [10][node_id: u32][num_records: u32][records...]
        // Each record: [req_id: u32][executor_ip: 4 bytes][timestamp: 16 bytes]
        let mut pkt = Vec::with_capacity(1 + 4 + 4 + records.len() * (4 + 4 + 16));
        pkt.push(TABLE_SYNC);
        pkt.extend(node_id.to_le_bytes());
        pkt.extend((records.len() as u32).to_le_bytes());

        for record in &records {
            pkt.extend(record.req_id.to_le_bytes());
            let ip_bytes = match record.executor_node {
                IpAddr::V4(v4) => u32::from(v4).to_be_bytes(),
                _ => continue,
            };
            pkt.extend(ip_bytes);
            pkt.extend(record.timestamp.to_le_bytes());
        }

        // Send to leader's assignment port
        let leader_id = { state.read().await.leader_id };
        if let Some(leader_ip) = cfg.node_id_to_ip(leader_id) {
            // Find leader's assignment port
            for peer in cfg.assignment_peer_addrs() {
                if peer.ip() == leader_ip {
                    let _ = sock.send_to(&pkt, peer).await;
                    debug!(node_id, leader_id, records_count=records.len(), "TABLE_SYNC sent to leader");
                    break;
                }
            }
        }
    }
}

/// Leader sync task - receives tables, merges, and broadcasts
/// Only runs when this node is leader
pub async fn run_leader_sync_task(
    state: SharedState,
    sock: Arc<UdpSocket>,
    cfg: Config,
) {
    let mut tick = interval(Duration::from_millis(15000)); // 15 seconds

    loop {
        tick.tick().await;

        let is_leader = { state.read().await.is_leader };
        if !is_leader {
            continue;
        }

        // Collect all received tables (stored temporarily)
        // In practice, TABLE_SYNC messages are handled in recv_assign_loop
        // Leader will merge on-the-fly as TABLE_SYNC arrives
        
        // Broadcast merged table
        let records = {
            let s = state.read().await;
            s.history.values().cloned().collect::<Vec<_>>()
        };

        // Build TABLE_UPDATE message
        // Format: [11][num_records: u32][records...]
        let mut pkt = Vec::with_capacity(1 + 4 + records.len() * (4 + 4 + 16));
        pkt.push(TABLE_UPDATE);
        pkt.extend((records.len() as u32).to_le_bytes());

        for record in &records {
            pkt.extend(record.req_id.to_le_bytes());
            let ip_bytes = match record.executor_node {
                IpAddr::V4(v4) => u32::from(v4).to_be_bytes(),
                _ => continue,
            };
            pkt.extend(ip_bytes);
            pkt.extend(record.timestamp.to_le_bytes());
        }

        // Broadcast to all peers
        for peer in cfg.assignment_peer_addrs() {
            let _ = sock.send_to(&pkt, peer).await;
        }

        info!(records_count=records.len(), "TABLE_UPDATE broadcast by leader");
    }
}

/// Handle TABLE_SYNC received by leader
pub async fn handle_table_sync(state: SharedState, buf: &[u8]) {
    if buf.len() < 1 + 4 + 4 {
        return;
    }

    let sender_node_id = u32::from_le_bytes(buf[1..5].try_into().unwrap());
    let num_records = u32::from_le_bytes(buf[5..9].try_into().unwrap()) as usize;

    let mut offset = 9;
    let record_size = 4 + 4 + 16; // req_id + ip + timestamp

    if buf.len() < 9 + num_records * record_size {
        return;
    }

    let mut s = state.write().await;
    
    for _ in 0..num_records {
        if offset + record_size > buf.len() {
            break;
        }

        let req_id = u32::from_le_bytes(buf[offset..offset + 4].try_into().unwrap());
        let executor_ip_be = u32::from_be_bytes(buf[offset + 4..offset + 8].try_into().unwrap());
        let executor_ip = IpAddr::V4(std::net::Ipv4Addr::from(executor_ip_be));
        let timestamp = u128::from_le_bytes(buf[offset + 8..offset + 24].try_into().unwrap());

        offset += record_size;

        // Merge logic: keep record with latest timestamp
        use crate::state::HistoryRecord;
        let should_update = if let Some(existing) = s.history.get(&req_id) {
            timestamp > existing.timestamp
        } else {
            true
        };

        if should_update {
            s.history.insert(
                req_id,
                HistoryRecord {
                    req_id,
                    executor_node: executor_ip,
                    path_to_output_image: None, // Leader doesn't have paths from other nodes
                    timestamp,
                },
            );
        }
    }

    debug!(sender_node_id, num_records, "TABLE_SYNC processed by leader");
}

/// Handle TABLE_UPDATE received from leader
pub async fn handle_table_update(state: SharedState, buf: &[u8]) {
    if buf.len() < 1 + 4 {
        return;
    }

    let num_records = u32::from_le_bytes(buf[1..5].try_into().unwrap()) as usize;

    let mut offset = 5;
    let record_size = 4 + 4 + 16;

    if buf.len() < 5 + num_records * record_size {
        return;
    }

    let mut s = state.write().await;

    for _ in 0..num_records {
        if offset + record_size > buf.len() {
            break;
        }

        let req_id = u32::from_le_bytes(buf[offset..offset + 4].try_into().unwrap());
        let executor_ip_be = u32::from_be_bytes(buf[offset + 4..offset + 8].try_into().unwrap());
        let executor_ip = IpAddr::V4(std::net::Ipv4Addr::from(executor_ip_be));
        let timestamp = u128::from_le_bytes(buf[offset + 8..offset + 24].try_into().unwrap());

        offset += record_size;

        // Update local table: keep newer timestamp
        use crate::state::HistoryRecord;
        let should_update = if let Some(existing) = s.history.get(&req_id) {
            timestamp > existing.timestamp
        } else {
            true
        };

        if should_update {
            // Preserve local path if this node is executor
            let path = s.history.get(&req_id).and_then(|r| r.path_to_output_image.clone());
            
            s.history.insert(
                req_id,
                HistoryRecord {
                    req_id,
                    executor_node: executor_ip,
                    path_to_output_image: path,
                    timestamp,
                },
            );
        }
    }

    debug!(num_records, "TABLE_UPDATE received and processed");
}

/// Helper to get node_id from IP (reverse lookup)
fn get_node_id_from_ip(ip: IpAddr) -> u32 {
    for (idx, peer_str) in Config::service_peers().iter().enumerate() {
        let peer_addr = Config::parse_addr(peer_str);
        if peer_addr.ip() == ip {
            return (idx + 1) as u32;
        }
    }
    0 // Unknown
}
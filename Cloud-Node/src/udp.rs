//! UDP service (client port) – SELECT/ACCEPT + upload/echo pipeline
//! Wire (LE):
//!   REQ_META  (type=0): [u8][u32 req_id][u32 total_chunks][u32 img_bytes][u32 meta_bytes]
//!   REQ_CHUNK (type=1): [u8][u32 req_id][u32 seq][bytes...]
//!   RESP_META (type=2): [u8][u32 req_id][u32 total_chunks][u32 out_bytes]
//!   RESP_CHUNK(type=3): [u8][u32 req_id][u32 seq][bytes...]
//!   SELECT    (type=4): [u8][u32 req_id][u32 sender_id][u8 op_code][u32 image_len]
//!   ACCEPT    (type=5): [u8][u32 req_id]

use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::net::UdpSocket;
use socket2::SockRef;
use tracing::{debug, info, warn};
use serde::{Deserialize, Serialize};

use crate::{
    config::Config,
    state::{ServerState, SharedState},
    history,
};

const REQ_META: u8 = 0;
const REQ_CHUNK: u8 = 1;
const RESP_META: u8 = 2;
const RESP_CHUNK: u8 = 3;
const SELECT: u8 = 4;
const ACCEPT: u8 = 5;

const MAX_DGRAM: usize = 1200;
const HDR_META: usize = 1 + 4 + 4 + 4 + 4;
const HDR_CHUNK: usize = 1 + 4 + 4;

#[inline]
fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}

fn is_executor(state: &ServerState, my_client_ip: IpAddr) -> bool {
    if let (Some(exec_ip), Some(deadline)) = (&state.executor_ip, state.executor_lease_deadline_ms)
    {
        exec_ip == &my_client_ip && now_ms() <= deadline
    } else {
        false
    }
}

// ============================================================================
// Metadata structures for transformation
// ============================================================================

/// Client's "allow" entry format
#[derive(Debug, Deserialize)]
struct ClientAllow {
    user: String,
    views: u32,
}

/// Client's full metadata format (what they send)
#[derive(Debug, Deserialize)]
struct ClientMeta {
    owner: String,
    allow: Vec<ClientAllow>,
    // We ignore other fields (op, sender_id, etc.) for steganography
}

/// Stego library's AccessEntry format
#[derive(Debug, Serialize)]
struct StegoAccessEntry {
    user: String,
    remaining_views: u32,
}

/// Stego library's Meta format (what embed_meta_return_png expects)
#[derive(Debug, Serialize)]
struct StegoMeta {
    owner: String,
    allow: Vec<StegoAccessEntry>,
}

/// Transform client metadata to stego format
fn transform_metadata(client_meta_json: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    // Parse client's full metadata
    let client_meta: ClientMeta = serde_json::from_slice(client_meta_json)?;
    
    // Transform allow entries: views -> remaining_views
    let stego_allow: Vec<StegoAccessEntry> = client_meta
        .allow
        .into_iter()
        .map(|entry| StegoAccessEntry {
            user: entry.user,
            remaining_views: entry.views,
        })
        .collect();
    
    // Create simplified stego metadata
    let stego_meta = StegoMeta {
        owner: client_meta.owner,
        allow: stego_allow,
    };
    
    // Serialize to JSON for steganography
    let stego_json = serde_json::to_vec(&stego_meta)?;
    Ok(stego_json)
}

// ============================================================================
// Request context - FIXED: Store chunks by sequence number
// ============================================================================

struct ReqCtx {
    expect_chunks: u32,
    image_len: usize,
    chunks: HashMap<u32, Vec<u8>>,  // ← FIXED: Store chunks by seq number
    meta_json: Vec<u8>,
    received: u32,
    first_chunk_logged: bool,
}

impl Default for ReqCtx {
    fn default() -> Self {
        Self {
            expect_chunks: 0,
            image_len: 0,
            chunks: HashMap::new(),
            meta_json: Vec::new(),
            received: 0,
            first_chunk_logged: false,
        }
    }
}

pub async fn run_udp_server(state: SharedState, cfg: Config) -> anyhow::Result<()> {
    // Extract pacing parameter
    let pacing_us = cfg.pacing_us;
    
    // Bind client-facing service socket (fixed port per node)
    let bind_addr = cfg
        .service_bind_addr()
        .expect("udp_bind (service) not configured");
    let sock = UdpSocket::bind(bind_addr).await?;
    
    // Increase receive buffer to handle large bursts (20MB should handle ~10MB image)
    let sock_ref = SockRef::from(&sock);
    if let Err(e) = sock_ref.set_recv_buffer_size(20 * 1024 * 1024) {
        eprintln!("Warning: failed to set recv buffer size: {}", e);
    }
    
    println!("Client port bound: {}", bind_addr);
    info!(%bind_addr, "UDP listening (service)");

    // Remember my client IP (for executor comparison)
    let my_client_ip = bind_addr.ip();

    // Per-request assembly (executor only)
    let mut ctxs: HashMap<u32, ReqCtx> = HashMap::new();

    let mut buf = [0u8; 64 * 1024];
    loop {
        let (n, peer) = sock.recv_from(&mut buf).await?;
        if state.read().await.ignoring || n == 0 {
            continue;
        }

        match buf[0] {
            // --------------------- SELECT ---------------------
            x if x == SELECT => {
                if n < 1 + 4 + 4 + 1 + 4 {
                    continue;
                }
                let req_id = u32::from_le_bytes(buf[1..5].try_into().unwrap());

                let am_exec = {
                    let s = state.read().await;
                    is_executor(&*s, my_client_ip)
                };

                if am_exec {
                    let mut pkt = [0u8; 1 + 4];
                    pkt[0] = ACCEPT;
                    pkt[1..5].copy_from_slice(&req_id.to_le_bytes());
                    let _ = sock.send_to(&pkt, peer).await;
                    println!("[EXECUTOR] ACCEPT sent to {} | req_id={}", peer, req_id);
                    debug!(%peer, req_id, "ACCEPT sent (executor)");
                } else {
                    // Non-executors stay silent (no redirect).
                }
            }

            // --------------------- REQ_META ---------------------
            x if x == REQ_META => {
                if n < HDR_META {
                    continue;
                }
                let req_id = u32::from_le_bytes(buf[1..5].try_into().unwrap());
                let total_chunks = u32::from_le_bytes(buf[5..9].try_into().unwrap());
                let img_bytes = u32::from_le_bytes(buf[9..13].try_into().unwrap()) as usize;
                let meta_bytes = u32::from_le_bytes(buf[13..17].try_into().unwrap()) as usize;

                let am_exec = {
                    let s = state.read().await;
                    is_executor(&*s, my_client_ip)
                };
                if !am_exec {
                    println!(
                        "[NON-EXECUTOR] Ignoring REQ_META from {} | req_id={}",
                        peer, req_id
                    );
                    continue;
                }

                // ============================================================
                // HISTORY CHECK: Check if this request was already completed
                // ============================================================
                let history_check = {
                    let s = state.read().await;
                    s.history.get(&req_id).cloned()
                };

                if let Some(record) = history_check {
                    println!(
                        "[EXECUTOR] HISTORY HIT | req_id={} was completed by {}",
                        req_id, record.executor_node
                    );

                    // Check if self was the original executor
                    let self_ip = my_client_ip;
                    if record.executor_node == self_ip {
                        // Self was executor - load saved image and resend
                        println!(
                            "[EXECUTOR] Loading saved image | req_id={} path={:?}",
                            req_id, record.path_to_output_image
                        );

                        if let Some(path) = record.path_to_output_image {
                            match tokio::fs::read(&path).await {
                                Ok(encrypted_png) => {
                                    println!(
                                        "[EXECUTOR] Saved image loaded | req_id={} size={}",
                                        req_id, encrypted_png.len()
                                    );

                                    // Send response to client (PRIORITIZE CLIENT RESPONSE)
                                    send_response_to_client(
                                        &sock,
                                        peer,
                                        req_id,
                                        &encrypted_png,
                                        pacing_us,
                                    ).await;

                                    {
                                        let mut s = state.write().await;
                                        s.requests_served = s.requests_served.saturating_add(1);
                                    }

                                    println!(
                                        "[EXECUTOR] Response resent from history | req_id={}",
                                        req_id
                                    );
                                    continue; // Done, don't re-process
                                }
                                Err(e) => {
                                    eprintln!(
                                        "[EXECUTOR] Failed to read saved image | req_id={} path={} error={}",
                                        req_id, path, e
                                    );
                                    warn!(req_id, ?path, error=%e, "Failed to read saved image");
                                    // Fall through to re-process
                                }
                            }
                        } else {
                            eprintln!(
                                "[EXECUTOR] History record missing path | req_id={}",
                                req_id
                            );
                            // Fall through to re-process
                        }
                    } else {
                        // Different executor - forward request
                        println!(
                            "[EXECUTOR] Forwarding to original executor | req_id={} original_executor={}",
                            req_id, record.executor_node
                        );

                        // Capture full request data for forwarding
                        let forward_data = buf[..n].to_vec();

                        // Forward to original executor (fire-and-forget)
                        history::send_forward_request(
                            &sock,
                            &cfg,
                            record.executor_node,
                            req_id,
                            peer,
                            &forward_data,
                        ).await;

                        println!(
                            "[EXECUTOR] Request forwarded | req_id={} to {}",
                            req_id, record.executor_node
                        );
                        continue; // Done, don't process locally
                    }
                }

                // ============================================================
                // NOT IN HISTORY - Process normally
                // ============================================================

                // Extract metadata JSON from packet
                let meta_json = if meta_bytes > 0 && n >= HDR_META + meta_bytes {
                    buf[HDR_META..HDR_META + meta_bytes].to_vec()
                } else {
                    Vec::new() // No metadata or packet too short
                };

                let mut c = ReqCtx::default();
                c.expect_chunks = total_chunks;
                c.image_len = img_bytes;
                c.meta_json = meta_json;
                c.chunks = HashMap::with_capacity(total_chunks as usize);  // Pre-allocate

                ctxs.insert(req_id, c);

                // Count as a "received" request
                {
                    let mut s = state.write().await;
                    s.requests_received = s.requests_received.saturating_add(1);
                }

                println!(
                    "[EXECUTOR] REQ_META accepted from {} | req_id={} total_chunks={} image_len={} meta_len={}",
                    peer, req_id, total_chunks, img_bytes, meta_bytes
                );
                debug!(%peer, req_id, total_chunks, img_bytes, meta_bytes, "REQ_META accepted (executor)");
            }

            // --------------------- REQ_CHUNK ---------------------
            x if x == REQ_CHUNK => {
                if n < HDR_CHUNK {
                    continue;
                }
                let req_id = u32::from_le_bytes(buf[1..5].try_into().unwrap());
                let seq = u32::from_le_bytes(buf[5..9].try_into().unwrap());
                let payload = &buf[9..n];

                let am_exec = {
                    let s = state.read().await;
                    is_executor(&*s, my_client_ip)
                };
                if !am_exec {
                    println!(
                        "[NON-EXECUTOR] Ignoring REQ_CHUNK from {} | req_id={} seq={}",
                        peer, req_id, seq
                    );
                    continue;
                }

                if let Some(c) = ctxs.get_mut(&req_id) {
                    // First-chunk visibility (once per request)
                    if !c.first_chunk_logged {
                        c.first_chunk_logged = true;
                        println!(
                            "[EXECUTOR] REQ_CHUNK first seen from {} | req_id={} seq={} ({} bytes)",
                            peer, req_id, seq, payload.len()
                        );
                        debug!(%peer, req_id, seq, len=payload.len(), "first REQ_CHUNK seen");
                    }

                    // ← FIXED: Store chunk by sequence number instead of appending
                    c.chunks.insert(seq, payload.to_vec());
                    c.received += 1;

                    // Progress every 1000 chunks (tune as needed)
                    if c.received % 1000 == 0 || c.received == c.expect_chunks {
                        println!(
                            "[EXECUTOR] REQ_CHUNK progress | req_id={} {}/{} chunks ({:.1}%)",
                            req_id,
                            c.received,
                            c.expect_chunks,
                            (c.received as f64 * 100.0) / (c.expect_chunks.max(1) as f64)
                        );
                        debug!(req_id, received=c.received, expect=c.expect_chunks, "chunk progress");
                        
                        // Explicit 100% notification
                        if c.received == c.expect_chunks {
                            println!(
                                "✅ [EXECUTOR] 100% RECEIVED | req_id={} - ALL {} chunks arrived!",
                                req_id, c.expect_chunks
                            );
                        }
                    }

                    // Done?
                    if c.received == c.expect_chunks {
                        println!(
                            "[EXECUTOR] All chunks received | req_id={} chunks={}",
                            req_id, c.received
                        );
                        debug!(req_id, chunks=c.received, "all chunks in");

                        // ============================================================
                        // FIXED: Reassemble chunks in sequence order
                        // ============================================================
                        let mut buffer = Vec::with_capacity(c.image_len);
                        let mut missing_chunks = Vec::new();
                        
                        for seq in 0..c.expect_chunks {
                            if let Some(chunk_data) = c.chunks.get(&seq) {
                                buffer.extend_from_slice(chunk_data);
                            } else {
                                missing_chunks.push(seq);
                            }
                        }
                        
                        if !missing_chunks.is_empty() {
                            eprintln!(
                                "[EXECUTOR] Missing chunks {:?} | req_id={} - dropping request",
                                missing_chunks, req_id
                            );
                            warn!(req_id, missing=?missing_chunks, "Missing chunks - dropping request");
                            ctxs.remove(&req_id);
                            continue;
                        }

                        println!(
                            "[EXECUTOR] Chunks reassembled in order | req_id={} bytes={}",
                            req_id, buffer.len()
                        );
                        debug!(req_id, bytes=buffer.len(), "chunks reassembled");

                        // ============================================================
                        // STEP A: Save original image (validation)
                        // ============================================================
                        // let original_path = format!("./server_test/req_{}_original.png", req_id);
                        // match tokio::fs::write(&original_path, &buffer).await {
                        //     Ok(_) => {
                        //         println!("[EXECUTOR] Original image saved: {}", original_path);
                        //         debug!(path=%original_path, "Original image saved");
                        //     }
                        //     Err(e) => {
                        //         eprintln!("[EXECUTOR] Failed to save original image: {} | Error: {}", original_path, e);
                        //         warn!(path=%original_path, error=%e, "Failed to save original image");
                        //         // Continue despite error (per your silent drop policy)
                        //     }
                        // }

                        // ============================================================
                        // STEP B: Transform metadata
                        // ============================================================
                        let stego_meta_json = match transform_metadata(&c.meta_json) {
                            Ok(json) => json,
                            Err(e) => {
                                eprintln!("[EXECUTOR] Metadata transformation failed | req_id={} | Error: {}", req_id, e);
                                warn!(req_id, error=%e, "Metadata transformation failed - dropping request");
                                ctxs.remove(&req_id);
                                continue;
                            }
                        };

                        println!("[EXECUTOR] Metadata transformed | req_id={}", req_id);
                        debug!(req_id, "Metadata transformed for steganography");

                        // ============================================================
                        // STEP C: Apply steganography
                        // ============================================================
                        let encrypted_png = match crate::stego_service::embed_meta_return_png(&buffer, &stego_meta_json) {
                            Ok(png) => png,
                            Err(e) => {
                                eprintln!("[EXECUTOR] Steganography failed | req_id={} | Error: {}", req_id, e);
                                warn!(req_id, error=%e, "Steganography failed - dropping request");
                                ctxs.remove(&req_id);
                                continue;
                            }
                        };

                        println!(
                            "[EXECUTOR] Steganography complete | req_id={} original_size={} encrypted_size={}",
                            req_id, buffer.len(), encrypted_png.len()
                        );
                        debug!(req_id, original_size=buffer.len(), encrypted_size=encrypted_png.len(), "Steganography complete");

                        // ============================================================
                        // STEP D: Send encrypted image back to client
                        // ============================================================
                        let out_len = encrypted_png.len();
                        let payload_cap = MAX_DGRAM - (1 + 4 + 4); // RESP_CHUNK header space
                        let total_out_chunks = ((out_len + payload_cap - 1) / payload_cap) as u32;

                        // RESP_META: [2][req_id][total_chunks][out_len]
                        let mut hdr = [0u8; 1 + 4 + 4 + 4];
                        hdr[0] = RESP_META;
                        hdr[1..5].copy_from_slice(&req_id.to_le_bytes());
                        hdr[5..9].copy_from_slice(&total_out_chunks.to_le_bytes());
                        hdr[9..13].copy_from_slice(&(out_len as u32).to_le_bytes());
                        let _ = sock.send_to(&hdr, peer).await;

                        println!(
                            "[EXECUTOR] RESP_META sent | req_id={} total_chunks={} out_len={}",
                            req_id, total_out_chunks, out_len
                        );
                        debug!(%peer, req_id, total_chunks=%total_out_chunks, out_len, "RESP_META sent");

                        // RESP_CHUNK(s) - with configurable pacing
                        let mut off = 0usize;
                        let mut seq_out = 0u32;
                        while off < out_len {
                            let take = (out_len - off).min(payload_cap);
                            let mut pkt = Vec::with_capacity(1 + 4 + 4 + take);
                            pkt.push(RESP_CHUNK);
                            pkt.extend(req_id.to_le_bytes());
                            pkt.extend(seq_out.to_le_bytes());
                            pkt.extend_from_slice(&encrypted_png[off..off + take]);
                            let _ = sock.send_to(&pkt, peer).await;
                            
                            off += take;
                            seq_out += 1;
                            
                            // Configurable pacing to prevent client buffer overflow
                            if pacing_us > 0 {
                                std::thread::sleep(Duration::from_micros(pacing_us));
                            }
                        }

                        // increment 'served' counter when we finish a response
                        {
                            let mut s = state.write().await;
                            s.requests_served = s.requests_served.saturating_add(1);
                        }

                        println!(
                            "[EXECUTOR] RESP echoed to {} | req_id={} out_len={} chunks={}",
                            peer, req_id, out_len, total_out_chunks
                        );
                        debug!(%peer, req_id, out_len, chunks=%total_out_chunks, "RESP echoed (executor)");

                        // ============================================================
                        // HISTORY UPDATE: Save image, update history, multicast
                        // IMPORTANT: This happens AFTER client response is sent (priority)
                        // ============================================================
                        let self_ip = my_client_ip;
                        let timestamp = now_ms();
                        
                        // Create directory if not exists
                        let _ = tokio::fs::create_dir_all("./server_images").await;
                        
                        // Save image to disk
                        let image_path = format!("./server_images/req_{}.png", req_id);
                        match tokio::fs::write(&image_path, &encrypted_png).await {
                            Ok(_) => {
                                println!(
                                    "[EXECUTOR] Image saved to disk | req_id={} path={}",
                                    req_id, image_path
                                );

                                // Update local history table
                                {
                                    use crate::state::HistoryRecord;
                                    let mut s = state.write().await;
                                    s.history.insert(
                                        req_id,
                                        HistoryRecord {
                                            req_id,
                                            executor_node: self_ip,
                                            path_to_output_image: Some(image_path.clone()),
                                            timestamp,
                                        },
                                    );
                                }

                                println!(
                                    "[EXECUTOR] History updated | req_id={} executor={}",
                                    req_id, self_ip
                                );

                                // Multicast HISTORY_UPDATE to all peers
                                // Get assignment socket from somewhere - we'll need to pass it
                                // For now, we'll spawn a task that does this
                                let cfg_clone = cfg.clone();
                                tokio::spawn(async move {
                                    // Create temporary socket for multicast
                                    if let Ok(temp_sock) = UdpSocket::bind("0.0.0.0:0").await {
                                        history::multicast_history_update(
                                            &temp_sock,
                                            &cfg_clone,
                                            req_id,
                                            self_ip,
                                            timestamp,
                                        ).await;
                                        println!(
                                            "[EXECUTOR] HISTORY_UPDATE multicast sent | req_id={}",
                                            req_id
                                        );
                                    }
                                });
                            }
                            Err(e) => {
                                eprintln!(
                                    "[EXECUTOR] Failed to save image | req_id={} path={} error={}",
                                    req_id, image_path, e
                                );
                                warn!(req_id, ?image_path, error=%e, "Failed to save image to disk");
                            }
                        }

                        ctxs.remove(&req_id);
                    }
                } else {
                    println!(
                        "[EXECUTOR] Ignoring REQ_CHUNK with no ctx | from {} req_id={} seq={}",
                        peer, req_id, seq
                    );
                    debug!(%peer, req_id, seq, "REQ_CHUNK ignored (no REQ_META ctx)");
                }
            }

            // --------------------- Unknown ----------------------
            other => {
                println!("Unknown UDP packet type {} from {}", other, peer);
                debug!(%peer, ty = other, "Unknown UDP packet type");
            }
        }
    }
}

/// Helper function to send response packets to client with pacing
/// Used for both new responses and history-based responses
async fn send_response_to_client(
    sock: &UdpSocket,
    client_addr: SocketAddr,
    req_id: u32,
    data: &[u8],
    pacing_us: u64,
) {
    let out_len = data.len();
    let payload_cap = MAX_DGRAM - (1 + 4 + 4); // RESP_CHUNK header space
    let total_out_chunks = ((out_len + payload_cap - 1) / payload_cap) as u32;

    // RESP_META: [2][req_id][total_chunks][out_len]
    let mut hdr = [0u8; 1 + 4 + 4 + 4];
    hdr[0] = RESP_META;
    hdr[1..5].copy_from_slice(&req_id.to_le_bytes());
    hdr[5..9].copy_from_slice(&total_out_chunks.to_le_bytes());
    hdr[9..13].copy_from_slice(&(out_len as u32).to_le_bytes());
    let _ = sock.send_to(&hdr, client_addr).await;

    println!(
        "[EXECUTOR] RESP_META sent | req_id={} total_chunks={} out_len={}",
        req_id, total_out_chunks, out_len
    );

    // RESP_CHUNK(s) - with configurable pacing
    let mut off = 0usize;
    let mut seq_out = 0u32;
    while off < out_len {
        let take = (out_len - off).min(payload_cap);
        let mut pkt = Vec::with_capacity(1 + 4 + 4 + take);
        pkt.push(RESP_CHUNK);
        pkt.extend(req_id.to_le_bytes());
        pkt.extend(seq_out.to_le_bytes());
        pkt.extend_from_slice(&data[off..off + take]);
        let _ = sock.send_to(&pkt, client_addr).await;
        
        off += take;
        seq_out += 1;
        
        // Configurable pacing to prevent client buffer overflow
        if pacing_us > 0 {
            std::thread::sleep(Duration::from_micros(pacing_us));
        }
    }

    println!(
        "[EXECUTOR] RESP echoed to {} | req_id={} out_len={} chunks={}",
        client_addr, req_id, out_len, total_out_chunks
    );
}
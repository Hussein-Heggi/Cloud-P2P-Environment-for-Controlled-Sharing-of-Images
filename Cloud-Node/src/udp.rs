//! UDP service (client port) – Multi-socket receiver pool + worker pool architecture
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
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
#[cfg(target_os = "linux")]
use std::os::unix::io::AsRawFd;

use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use socket2::{Socket, Domain, Type, Protocol, SockRef};
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

// Number of receiver sockets
const NUM_RECEIVERS: usize = 15;

// Number of worker tasks
const NUM_WORKERS: usize = 15;

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
// Request context - stores chunks by sequence number
// ============================================================================

struct ReqCtx {
    expect_chunks: u32,
    image_len: usize,
    chunks: HashMap<u32, Vec<u8>>,  // Store chunks by seq number
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

// ============================================================================
// Job structure - passed from receivers to workers
// ============================================================================

struct Job {
    req_id: u32,
    peer: SocketAddr,
    image_buffer: Vec<u8>,  // Fully assembled image (in sequence order)
    meta_json: Vec<u8>,     // Metadata JSON
}

// ============================================================================
// Main entry point: spawn receiver pool + worker pool
// ============================================================================

pub async fn run_udp_server(state: SharedState, cfg: Config) -> anyhow::Result<()> {
    // Extract config parameters
    let bind_addr = cfg
        .service_bind_addr()
        .expect("udp_bind (service) not configured");
    let my_client_ip = bind_addr.ip();
    let _pacing_us = cfg.pacing_us; // Stored in cfg for workers to access

    // Create job queue (MPSC channel)
    let (tx_jobs, rx_jobs) = mpsc::unbounded_channel::<Job>();

    println!(
        "🚀 Starting UDP server with {} receivers and {} workers on {}",
        NUM_RECEIVERS, NUM_WORKERS, bind_addr
    );
    info!(
        bind_addr=%bind_addr,
        num_receivers=NUM_RECEIVERS,
        num_workers=NUM_WORKERS,
        "UDP server starting with multi-socket architecture"
    );

    // ============================================================================
    // Create a shared response socket (for workers to send responses)
    // This socket is bound to the same service port so responses come from
    // the expected address that clients are listening on
    // ============================================================================
    let response_socket = {
        let sock = create_reuse_port_socket(bind_addr)?;
        Arc::new(sock)
    };

    // ============================================================================
    // SPAWN RECEIVER POOL (6 tasks, each with SO_REUSEPORT socket)
    // ============================================================================
    for receiver_id in 0..NUM_RECEIVERS {
        // Create socket with SO_REUSEPORT
        let sock = create_reuse_port_socket(bind_addr)?;
        let sock = Arc::new(sock);

        let state_clone = state.clone();
        let tx_clone = tx_jobs.clone();
        let my_ip = my_client_ip;

        tokio::spawn(async move {
            receiver_task(receiver_id, sock, state_clone, tx_clone, my_ip).await;
        });

        println!("✅ Receiver {} spawned on {}", receiver_id, bind_addr);
    }

    // ============================================================================
    // SPAWN WORKER POOL (6 tasks)
    // ============================================================================
    // Wrap receiver in Arc for sharing across workers
    let rx_jobs = Arc::new(tokio::sync::Mutex::new(rx_jobs));
    
    for worker_id in 0..NUM_WORKERS {
        let rx_clone = rx_jobs.clone();
        let state_clone = state.clone();
        let cfg_clone = cfg.clone();
        let my_ip = my_client_ip;
        let response_sock_clone = response_socket.clone(); // Share the response socket

        tokio::spawn(async move {
            worker_task(worker_id, rx_clone, state_clone, cfg_clone, my_ip, response_sock_clone).await;
        });

        println!("✅ Worker {} spawned", worker_id);
    }

    println!("🎉 All receivers and workers spawned successfully!");
    info!("UDP server fully initialized");

    // Keep this task alive
    loop {
        tokio::time::sleep(Duration::from_secs(3600)).await;
    }
}

// ============================================================================
// Helper: Create UDP socket with SO_REUSEPORT
// ============================================================================

fn create_reuse_port_socket(bind_addr: SocketAddr) -> anyhow::Result<UdpSocket> {
    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    
    // Enable SO_REUSEADDR (allows multiple sockets on same port)
    socket.set_reuse_address(true)?;
    
    // On Linux, we need to set SO_REUSEPORT manually using setsockopt
    // This is a workaround for socket2 API differences across versions
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        let fd = socket.as_raw_fd();
        unsafe {
            let optval: libc::c_int = 1;
            let ret = libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_REUSEPORT,
                &optval as *const _ as *const libc::c_void,
                std::mem::size_of_val(&optval) as libc::socklen_t,
            );
            if ret != 0 {
                return Err(std::io::Error::last_os_error().into());
            }
        }
    }
    
    // Bind to address
    socket.bind(&bind_addr.into())?;
    
    // Increase receive buffer to handle large bursts (20MB)
    let sock_ref = SockRef::from(&socket);
    if let Err(e) = sock_ref.set_recv_buffer_size(20 * 1024 * 1024) {
        eprintln!("Warning: failed to set recv buffer size: {}", e);
    }
    
    // Convert to tokio UdpSocket
    socket.set_nonblocking(true)?;
    let tokio_sock = UdpSocket::from_std(socket.into())?;
    
    Ok(tokio_sock)
}

// ============================================================================
// RECEIVER TASK (Fast path - no blocking operations)
// ============================================================================

async fn receiver_task(
    receiver_id: usize,
    sock: Arc<UdpSocket>,
    state: SharedState,
    tx_jobs: mpsc::UnboundedSender<Job>,
    my_client_ip: IpAddr,
) {
    println!("[RECEIVER-{}] Started", receiver_id);
    debug!(receiver_id, "Receiver task started");

    // Each receiver has its own independent request context
    let mut ctxs: HashMap<u32, ReqCtx> = HashMap::new();
    let mut buf = [0u8; 64 * 1024];

    loop {
        let (n, peer) = match sock.recv_from(&mut buf).await {
            Ok(res) => res,
            Err(e) => {
                eprintln!("[RECEIVER-{}] recv_from error: {}", receiver_id, e);
                warn!(receiver_id, error=%e, "recv_from error");
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }
        };

        // Skip if ignoring or empty packet
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
                    println!(
                        "[RECEIVER-{}] [EXECUTOR] ACCEPT sent to {} | req_id={}",
                        receiver_id, peer, req_id
                    );
                    debug!(receiver_id, %peer, req_id, "ACCEPT sent (executor)");
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
                        "[RECEIVER-{}] [NON-EXECUTOR] Ignoring REQ_META from {} | req_id={}",
                        receiver_id, peer, req_id
                    );
                    continue;
                }

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
                c.chunks = HashMap::with_capacity(total_chunks as usize);

                ctxs.insert(req_id, c);

                // Count as a "received" request
                {
                    let mut s = state.write().await;
                    s.requests_received = s.requests_received.saturating_add(1);
                }

                println!(
                    "[RECEIVER-{}] [EXECUTOR] REQ_META accepted from {} | req_id={} total_chunks={} image_len={} meta_len={}",
                    receiver_id, peer, req_id, total_chunks, img_bytes, meta_bytes
                );
                debug!(
                    receiver_id,
                    %peer,
                    req_id,
                    total_chunks,
                    img_bytes,
                    meta_bytes,
                    "REQ_META accepted (executor)"
                );
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
                        "[RECEIVER-{}] [NON-EXECUTOR] Ignoring REQ_CHUNK from {} | req_id={} seq={}",
                        receiver_id, peer, req_id, seq
                    );
                    continue;
                }

                if let Some(c) = ctxs.get_mut(&req_id) {
                    // First-chunk visibility (once per request)
                    if !c.first_chunk_logged {
                        c.first_chunk_logged = true;
                        println!(
                            "[RECEIVER-{}] [EXECUTOR] REQ_CHUNK first seen from {} | req_id={} seq={} ({} bytes)",
                            receiver_id, peer, req_id, seq, payload.len()
                        );
                        debug!(
                            receiver_id,
                            %peer,
                            req_id,
                            seq,
                            len = payload.len(),
                            "first REQ_CHUNK seen"
                        );
                    }

                    // Store chunk by sequence number
                    c.chunks.insert(seq, payload.to_vec());
                    c.received += 1;

                    // Progress logging every 1000 chunks
                    if c.received % 1000 == 0 || c.received == c.expect_chunks {
                        println!(
                            "[RECEIVER-{}] [EXECUTOR] REQ_CHUNK progress | req_id={} {}/{} chunks ({:.1}%)",
                            receiver_id,
                            req_id,
                            c.received,
                            c.expect_chunks,
                            (c.received as f64 * 100.0) / (c.expect_chunks.max(1) as f64)
                        );
                        debug!(
                            receiver_id,
                            req_id,
                            received = c.received,
                            expect = c.expect_chunks,
                            "chunk progress"
                        );

                        // Explicit 100% notification
                        if c.received == c.expect_chunks {
                            println!(
                                "✅ [RECEIVER-{}] [EXECUTOR] 100% RECEIVED | req_id={} - ALL {} chunks arrived!",
                                receiver_id, req_id, c.expect_chunks
                            );
                        }
                    }

                    // All chunks received?
                    if c.received == c.expect_chunks {
                        println!(
                            "[RECEIVER-{}] [EXECUTOR] All chunks received | req_id={} chunks={}",
                            receiver_id, req_id, c.received
                        );
                        debug!(receiver_id, req_id, chunks = c.received, "all chunks in");

                        // ============================================================
                        // Reassemble chunks in sequence order
                        // ============================================================
                        let mut buffer = Vec::with_capacity(c.image_len);
                        let mut missing_chunks = Vec::new();

                        for seq_idx in 0..c.expect_chunks {
                            if let Some(chunk_data) = c.chunks.get(&seq_idx) {
                                buffer.extend_from_slice(chunk_data);
                            } else {
                                missing_chunks.push(seq_idx);
                            }
                        }

                        if !missing_chunks.is_empty() {
                            eprintln!(
                                "[RECEIVER-{}] [EXECUTOR] Missing chunks {:?} | req_id={} - dropping request",
                                receiver_id, missing_chunks, req_id
                            );
                            warn!(
                                receiver_id,
                                req_id,
                                missing = ?missing_chunks,
                                "Missing chunks - dropping request"
                            );
                            ctxs.remove(&req_id);
                            continue;
                        }

                        println!(
                            "[RECEIVER-{}] [EXECUTOR] Chunks reassembled in order | req_id={} bytes={}",
                            receiver_id, req_id, buffer.len()
                        );
                        debug!(receiver_id, req_id, bytes = buffer.len(), "chunks reassembled");

                        // ============================================================
                        // Create Job and send to worker pool
                        // ============================================================
                        let job = Job {
                            req_id,
                            peer,
                            image_buffer: buffer,
                            meta_json: c.meta_json.clone(),
                        };

                        if let Err(e) = tx_jobs.send(job) {
                            eprintln!(
                                "[RECEIVER-{}] Failed to send job to worker pool: {}",
                                receiver_id, e
                            );
                            warn!(receiver_id, error=%e, "Failed to send job to worker pool");
                        } else {
                            println!(
                                "[RECEIVER-{}] ✅ Job sent to worker pool | req_id={}",
                                receiver_id, req_id
                            );
                            debug!(receiver_id, req_id, "Job sent to worker pool");
                        }

                        // Clean up context
                        ctxs.remove(&req_id);
                    }
                } else {
                    println!(
                        "[RECEIVER-{}] [EXECUTOR] Ignoring REQ_CHUNK with no ctx | from {} req_id={} seq={}",
                        receiver_id, peer, req_id, seq
                    );
                    debug!(receiver_id, %peer, req_id, seq, "REQ_CHUNK ignored (no REQ_META ctx)");
                }
            }

            // --------------------- Unknown ----------------------
            other => {
                println!(
                    "[RECEIVER-{}] Unknown UDP packet type {} from {}",
                    receiver_id, other, peer
                );
                debug!(receiver_id, %peer, ty = other, "Unknown UDP packet type");
            }
        }
    }
}

// ============================================================================
// WORKER TASK (Slow path - all heavy lifting)
// ============================================================================

async fn worker_task(
    worker_id: usize,
    rx_jobs: Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<Job>>>,
    state: SharedState,
    cfg: Config,
    my_client_ip: IpAddr,
    response_sock: Arc<UdpSocket>, // Shared response socket on service port
) {
    println!("[WORKER-{}] Started", worker_id);
    debug!(worker_id, "Worker task started");

    let pacing_us = cfg.pacing_us;

    loop {
        // Wait for job from queue (lock to receive)
        let job = {
            let mut rx = rx_jobs.lock().await;
            match rx.recv().await {
                Some(j) => j,
                None => {
                    println!("[WORKER-{}] Job channel closed, exiting", worker_id);
                    info!(worker_id, "Job channel closed");
                    break;
                }
            }
        };

        let req_id = job.req_id;
        let peer = job.peer;

        println!(
            "[WORKER-{}] 🔨 Processing job | req_id={} from {}",
            worker_id, req_id, peer
        );
        debug!(worker_id, req_id, %peer, "Processing job");

        // ============================================================
        // HISTORY CHECK: Check if this request was already completed
        // ============================================================
        let history_check = {
            let s = state.read().await;
            s.history.get(&req_id).cloned()
        };

        if let Some(record) = history_check {
            println!(
                "[WORKER-{}] [EXECUTOR] HISTORY HIT | req_id={} was completed by {}",
                worker_id, req_id, record.executor_node
            );

            // Check if self was the original executor
            let self_ip = my_client_ip;
            if record.executor_node == self_ip {
                // Self was executor - load saved image and resend
                println!(
                    "[WORKER-{}] [EXECUTOR] Loading saved image | req_id={} path={:?}",
                    worker_id, req_id, record.path_to_output_image
                );

                if let Some(path) = record.path_to_output_image {
                    match tokio::fs::read(&path).await {
                        Ok(encrypted_png) => {
                            println!(
                                "[WORKER-{}] [EXECUTOR] Saved image loaded | req_id={} size={}",
                                worker_id, req_id, encrypted_png.len()
                            );

                            // Send response to client (PRIORITIZE CLIENT RESPONSE)
                            send_response_to_client(
                                &response_sock,
                                peer,
                                req_id,
                                &encrypted_png,
                                pacing_us,
                            )
                            .await;

                            {
                                let mut s = state.write().await;
                                s.requests_served = s.requests_served.saturating_add(1);
                            }

                            println!(
                                "[WORKER-{}] [EXECUTOR] Response resent from history | req_id={}",
                                worker_id, req_id
                            );
                            continue; // Done, go to next job
                        }
                        Err(e) => {
                            eprintln!(
                                "[WORKER-{}] [EXECUTOR] Failed to read saved image | req_id={} path={} error={}",
                                worker_id, req_id, path, e
                            );
                            warn!(worker_id, req_id, ?path, error=%e, "Failed to read saved image");
                            // Fall through to re-process
                        }
                    }
                } else {
                    eprintln!(
                        "[WORKER-{}] [EXECUTOR] History record missing path | req_id={}",
                        worker_id, req_id
                    );
                    // Fall through to re-process
                }
            } else {
                // Different executor - forward request
                println!(
                    "[WORKER-{}] [EXECUTOR] Forwarding to original executor | req_id={} original_executor={}",
                    worker_id, req_id, record.executor_node
                );

                // Note: We need the original request data to forward, but in worker we only have the job
                // For now, just log this case. In production, you'd need to reconstruct or cache the original packet
                // This matches your existing logic where forwarding happens in REQ_META handler
                println!(
                    "[WORKER-{}] [EXECUTOR] Forward logic requires original packet data (skipping)",
                    worker_id
                );
                continue; // Done, go to next job
            }
        }

        // ============================================================
        // NOT IN HISTORY - Process normally
        // ============================================================

        // ============================================================
        // STEP A: Transform metadata
        // ============================================================
        let stego_meta_json = match transform_metadata(&job.meta_json) {
            Ok(json) => json,
            Err(e) => {
                eprintln!(
                    "[WORKER-{}] [EXECUTOR] Metadata transformation failed | req_id={} | Error: {}",
                    worker_id, req_id, e
                );
                warn!(worker_id, req_id, error=%e, "Metadata transformation failed - dropping request");
                continue; // Drop request, go to next job
            }
        };

        println!(
            "[WORKER-{}] [EXECUTOR] Metadata transformed | req_id={}",
            worker_id, req_id
        );
        debug!(worker_id, req_id, "Metadata transformed for steganography");

        // ============================================================
        // STEP B: Apply steganography (CPU-BOUND - This is why we need workers!)
        // ============================================================
        let encrypted_png = match crate::stego_service::embed_meta_return_png(
            &job.image_buffer,
            &stego_meta_json,
        ) {
            Ok(png) => png,
            Err(e) => {
                eprintln!(
                    "[WORKER-{}] [EXECUTOR] Steganography failed | req_id={} | Error: {}",
                    worker_id, req_id, e
                );
                warn!(worker_id, req_id, error=%e, "Steganography failed - dropping request");
                continue; // Drop request, go to next job
            }
        };

        println!(
            "[WORKER-{}] [EXECUTOR] Steganography complete | req_id={} original_size={} encrypted_size={}",
            worker_id,
            req_id,
            job.image_buffer.len(),
            encrypted_png.len()
        );
        debug!(
            worker_id,
            req_id,
            original_size = job.image_buffer.len(),
            encrypted_size = encrypted_png.len(),
            "Steganography complete"
        );

        // ============================================================
        // STEP C: Send encrypted image back to client (PRIORITY!)
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
        let _ = response_sock.send_to(&hdr, peer).await;

        println!(
            "[WORKER-{}] [EXECUTOR] RESP_META sent | req_id={} total_chunks={} out_len={}",
            worker_id, req_id, total_out_chunks, out_len
        );
        debug!(
            worker_id,
            %peer,
            req_id,
            total_chunks = %total_out_chunks,
            out_len,
            "RESP_META sent"
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
            pkt.extend_from_slice(&encrypted_png[off..off + take]);
            let _ = response_sock.send_to(&pkt, peer).await;

            off += take;
            seq_out += 1;

            // Configurable pacing to prevent client buffer overflow
            if pacing_us > 0 {
                std::thread::sleep(Duration::from_micros(pacing_us));
            }
        }

        // Increment 'served' counter when we finish a response
        {
            let mut s = state.write().await;
            s.requests_served = s.requests_served.saturating_add(1);
        }

        println!(
            "[WORKER-{}] [EXECUTOR] RESP echoed to {} | req_id={} out_len={} chunks={}",
            worker_id, peer, req_id, out_len, total_out_chunks
        );
        debug!(
            worker_id,
            %peer,
            req_id,
            out_len,
            chunks = %total_out_chunks,
            "RESP echoed (executor)"
        );

        // ============================================================
        // STEP D: HISTORY UPDATE: Save image, update history, multicast
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
                    "[WORKER-{}] [EXECUTOR] Image saved to disk | req_id={} path={}",
                    worker_id, req_id, image_path
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
                    "[WORKER-{}] [EXECUTOR] History updated | req_id={} executor={}",
                    worker_id, req_id, self_ip
                );

                // Multicast HISTORY_UPDATE to all peers
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
                        )
                        .await;
                        println!(
                            "[WORKER] HISTORY_UPDATE multicast sent | req_id={}",
                            req_id
                        );
                    }
                });
            }
            Err(e) => {
                eprintln!(
                    "[WORKER-{}] [EXECUTOR] Failed to save image | req_id={} path={} error={}",
                    worker_id, req_id, image_path, e
                );
                warn!(
                    worker_id,
                    req_id,
                    ?image_path,
                    error = %e,
                    "Failed to save image to disk"
                );
            }
        }

        println!(
            "[WORKER-{}] ✅ Job completed | req_id={}",
            worker_id, req_id
        );
        debug!(worker_id, req_id, "Job completed");
    }
}

// ============================================================================
// Helper function to send response packets to client with pacing
// ============================================================================

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
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


 use tokio::net::UdpSocket;
 use tokio::sync::mpsc;
 use socket2::{Socket, Domain, Type, Protocol, SockRef};
 use tracing::{debug, info, warn};
 use serde::{Deserialize, Serialize};
 
 
 use crate::{
    config::Config,
    state::{ServerState, SharedState},
    history,
    client_protocol,
 };
 
 
 const REQ_META: u8 = 0;
 const REQ_CHUNK: u8 = 1;
 const RESP_META: u8 = 2;
 const RESP_CHUNK: u8 = 3;
 const SELECT: u8 = 4;
 const ACCEPT: u8 = 5;
 
 
 const MAX_DGRAM: usize = 1200;
 const HDR_META: usize = 1 + 4 + 4 + 4 + 4 + 4 + 4; // type + req_id + true_chunks + true_bytes + cover_chunks + cover_bytes + meta_bytes
 const HDR_CHUNK: usize = 1 + 4 + 1 + 4; // type + req_id + image_type + seq
 
 
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
    // True image
    expect_true_chunks: u32,
    true_image_len: usize,
    true_chunks: HashMap<u32, Vec<u8>>,
    true_received: u32,

    // Cover image
    expect_cover_chunks: u32,
    cover_image_len: usize,
    cover_chunks: HashMap<u32, Vec<u8>>,
    cover_received: u32,

    // Metadata
    meta_json: Vec<u8>,

    first_chunk_logged: bool,
 }
 
 
 impl Default for ReqCtx {
    fn default() -> Self {
        Self {
            expect_true_chunks: 0,
            true_image_len: 0,
            true_chunks: HashMap::new(),
            true_received: 0,

            expect_cover_chunks: 0,
            cover_image_len: 0,
            cover_chunks: HashMap::new(),
            cover_received: 0,

            meta_json: Vec::new(),
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
    true_image_buffer: Vec<u8>,   // True image (to be hidden)
    cover_image_buffer: Vec<u8>,  // Cover image (container)
    meta_json: Vec<u8>,           // Metadata JSON
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
        let cfg_clone = cfg.clone();


        tokio::spawn(async move {
            receiver_task(receiver_id, sock, state_clone, tx_clone, my_ip, cfg_clone).await;
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
    cfg: Config,
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
                let true_chunks = u32::from_le_bytes(buf[5..9].try_into().unwrap());
                let true_bytes = u32::from_le_bytes(buf[9..13].try_into().unwrap()) as usize;
                let cover_chunks = u32::from_le_bytes(buf[13..17].try_into().unwrap());
                let cover_bytes = u32::from_le_bytes(buf[17..21].try_into().unwrap()) as usize;
                let meta_bytes = u32::from_le_bytes(buf[21..25].try_into().unwrap()) as usize;
 
 
                // Check if I should accept this REQ_META:
                // 1. I'm the current executor (for new requests or forwarding)
                // 2. I'm the original executor (for cache responses on retries)
                let (should_accept, is_retry) = {
                    let s = state.read().await;
                    let am_current_exec = is_executor(&*s, my_client_ip);
                    let record = s.history.get(&req_id);
                    let am_original_exec = record.map_or(false, |r| r.path_to_output_image.is_some());
                   
                    (am_current_exec || am_original_exec, record.is_some())
                };
 
 
                if !should_accept {
                    println!(
                        "[RECEIVER-{}] [NON-EXECUTOR] Ignoring REQ_META from {} | req_id={}{}",
                        receiver_id, peer, req_id,
                        if is_retry { " (retry)" } else { "" }
                    );
                    continue;
                }
 
 
                if is_retry {
                    println!(
                        "[RECEIVER-{}] REQ_META retry detected | req_id={} (worker will handle)",
                        receiver_id, req_id
                    );
                }
 
 
                // Extract metadata JSON from packet
                let meta_json = if meta_bytes > 0 && n >= HDR_META + meta_bytes {
                    buf[HDR_META..HDR_META + meta_bytes].to_vec()
                } else {
                    Vec::new() // No metadata or packet too short
                };
 
 
                let mut c = ReqCtx::default();
                c.expect_true_chunks = true_chunks;
                c.true_image_len = true_bytes;
                c.expect_cover_chunks = cover_chunks;
                c.cover_image_len = cover_bytes;
                c.meta_json = meta_json;
                c.true_chunks = HashMap::with_capacity(true_chunks as usize);
                c.cover_chunks = HashMap::with_capacity(cover_chunks as usize);
 
 
                ctxs.insert(req_id, c);
 
 
                // Count as a "received" request AND create history entry immediately (in-progress marker)
                {
                    use crate::state::HistoryRecord;
                    let mut s = state.write().await;
                    s.requests_received = s.requests_received.saturating_add(1);
                    
                    // CRITICAL: Create history entry NOW (before chunks arrive)
                    // This marks the request as "in-progress" on this server
                    // path_to_output_image: None = still processing
                    // timestamp: 0 = not completed yet
                    s.history.insert(
                        req_id,
                        HistoryRecord {
                            req_id,
                            executor_node: my_client_ip,
                            path_to_output_image: None, // None = in-progress
                            timestamp: 0, // 0 = not completed yet
                        },
                    );
                }
 
 
                println!(
                    "[RECEIVER-{}] [EXECUTOR] REQ_META accepted from {} | req_id={} true_chunks={} true_len={} cover_chunks={} cover_len={} meta_len={} (history entry created)",
                    receiver_id, peer, req_id, true_chunks, true_bytes, cover_chunks, cover_bytes, meta_bytes
                );
                debug!(
                    receiver_id,
                    %peer,
                    req_id,
                    true_chunks,
                    true_bytes,
                    cover_chunks,
                    cover_bytes,
                    meta_bytes,
                    "REQ_META accepted (executor) - history entry created as in-progress"
                );
            }
 
 
            // --------------------- REQ_CHUNK ---------------------
            x if x == REQ_CHUNK => {
                if n < HDR_CHUNK {
                    continue;
                }
                let req_id = u32::from_le_bytes(buf[1..5].try_into().unwrap());
                let image_type = buf[5]; // 0 = true_image, 1 = cover_image
                let seq = u32::from_le_bytes(buf[6..10].try_into().unwrap());
                let payload = &buf[10..n];

                // ============================================================
                // STICKY EXECUTOR LOGIC - History-Based Acceptance
                // ============================================================
                // Accept chunks if:
                // 1. We have local context (same receiver that accepted REQ_META)
                // 2. Request in history table with our IP as executor (ANY receiver on this server)
                //    - path_to_output_image: None = in-progress (sticky executor)
                //    - path_to_output_image: Some = completed (serve from cache)
                // 3. We are current executor (new request, should have REQ_META)
                // Reject chunks if:
                // 4. Request in history with different executor IP (not our request)
                // 5. Neither context nor current executor (not our request)
                
                let has_local_context = ctxs.contains_key(&req_id);
                
                let (should_accept, acceptance_reason) = if has_local_context {
                    // CASE 1: We have local context - we're the sticky executor
                    // This receiver accepted REQ_META and is processing chunks
                    (true, "local_context")
                } else {
                    // No local context - check shared history table
                    let s = state.read().await;
                    
                    if let Some(record) = s.history.get(&req_id) {
                        // Request is in history table
                        let is_our_request = record.executor_node == my_client_ip;
                        
                        if is_our_request {
                            if record.path_to_output_image.is_none() {
                                // CASE 2a: In history with our IP and no path = in-progress
                                // Another receiver on this server accepted REQ_META
                                // This is the sticky executor case (KEY FIX)
                                (true, "history_in_progress")
                            } else {
                                // CASE 2b: In history with our IP and has path = completed
                                // Already processed and saved, serve from cache
                                (true, "history_completed")
                            }
                        } else {
                            // CASE 4: In history but different executor IP
                            // Not our request
                            (false, "history_different_executor")
                        }
                    } else {
                        // Not in history - check if we're current executor
                        let is_current_exec = is_executor(&*s, my_client_ip);
                        
                        if is_current_exec {
                            // CASE 3: We're current executor but no history/context
                            // This shouldn't happen (should have REQ_META first)
                            // But accept anyway in case REQ_META was lost
                            (true, "current_executor_no_history")
                        } else {
                            // CASE 5: Not in history, not current executor
                            // Not our request
                            (false, "not_executor")
                        }
                    }
                };

                if !should_accept {
                    // Log rejection with clear reason
                    match acceptance_reason {
                        "history_completed" => {
                            // Already processed, client might be retrying
                            // Only log occasionally to avoid spam
                            if seq == 0 || seq % 1000 == 0 {
                                println!(
                                    "[RECEIVER-{}] [ALREADY-COMPLETED] Ignoring REQ_CHUNK from {} | req_id={} seq={} (request already completed - in history with path)",
                                    receiver_id, peer, req_id, seq
                                );
                                debug!(receiver_id, %peer, req_id, seq, "Ignoring chunk - request already completed");
                            }
                        }
                        "history_different_executor" => {
                            // Request belongs to different server
                            if seq == 0 || seq % 1000 == 0 {
                                println!(
                                    "[RECEIVER-{}] [DIFFERENT-EXECUTOR] Ignoring REQ_CHUNK from {} | req_id={} seq={} (request belongs to different server)",
                                    receiver_id, peer, req_id, seq
                                );
                                debug!(receiver_id, %peer, req_id, seq, "Ignoring chunk - different executor in history");
                            }
                        }
                        "not_executor" => {
                            // Standard non-executor rejection
                            println!(
                                "[RECEIVER-{}] [NON-EXECUTOR] Ignoring REQ_CHUNK from {} | req_id={} seq={} (no context, no history, not current executor)",
                                receiver_id, peer, req_id, seq
                            );
                            debug!(receiver_id, %peer, req_id, seq, "Ignoring chunk - not executor");
                        }
                        _ => {
                            // Unknown reason (shouldn't happen)
                            println!(
                                "[RECEIVER-{}] [REJECTED] Ignoring REQ_CHUNK from {} | req_id={} seq={} reason={}",
                                receiver_id, peer, req_id, seq, acceptance_reason
                            );
                        }
                    }
                    continue;
                }

                // ============================================================
                // CHUNK ACCEPTED - Log acceptance reason
                // ============================================================
                
                // Special logging for sticky executor cases
                match acceptance_reason {
                    "local_context" => {
                        // Normal case - same receiver that accepted REQ_META
                        let is_still_current = {
                            let s = state.read().await;
                            is_executor(&*s, my_client_ip)
                        };
                        
                        if !is_still_current {
                            // Log that we're accepting as sticky executor (not current executor anymore)
                            // Only log on first chunk to avoid spam
                            if let Some(c) = ctxs.get(&req_id) {
                                if !c.first_chunk_logged {
                                    println!(
                                        "[RECEIVER-{}] ⚡ [STICKY-EXECUTOR-LOCAL] Accepting REQ_CHUNK from {} | req_id={} seq={} (executor changed but receiver has context - finishing request)",
                                        receiver_id, peer, req_id, seq
                                    );
                                    info!(
                                        receiver_id,
                                        %peer,
                                        req_id,
                                        seq,
                                        "Sticky executor (local context): accepting chunk despite not being current executor"
                                    );
                                }
                            }
                        }
                    }
                    "history_in_progress" => {
                        // CRITICAL FIX: Different receiver, same server
                        // Another receiver on this server accepted REQ_META
                        // Request is in history table with path=None (in-progress)
                        // This is the key case that fixes the chunk drop issue
                        if seq == 0 || seq % 1000 == 0 {  // Log first chunk and every 1000th
                            println!(
                                "[RECEIVER-{}] ⚡ [STICKY-EXECUTOR-HISTORY] Accepting REQ_CHUNK from {} | req_id={} seq={} (request in history as in-progress - another receiver accepted REQ_META)",
                                receiver_id, peer, req_id, seq
                            );
                            info!(
                                receiver_id,
                                %peer,
                                req_id,
                                seq,
                                "Sticky executor (history in-progress): accepting chunk from different receiver using shared history table"
                            );
                        }
                    }
                    "history_completed" => {
                        // Request already completed, chunks arriving at wrong receiver
                        // This shouldn't happen often (would be rejected above)
                        // But if it does, worker will serve from cache
                        if seq == 0 {
                            println!(
                                "[RECEIVER-{}] [CACHE-PATH] Accepting REQ_CHUNK from {} | req_id={} seq={} (request already completed - will serve from cache)",
                                receiver_id, peer, req_id, seq
                            );
                        }
                    }
                    "current_executor_no_history" => {
                        // Warn if we're current executor but have no history (missing REQ_META)
                        println!(
                            "[RECEIVER-{}] ⚠️  [CURRENT-EXECUTOR-NO-HISTORY] Accepting REQ_CHUNK from {} | req_id={} seq={} (WARNING: no REQ_META received yet - not in history)",
                            receiver_id, peer, req_id, seq
                        );
                        warn!(
                            receiver_id,
                            %peer,
                            req_id,
                            seq,
                            "Current executor accepting chunk without history - REQ_META might be missing"
                        );
                    }
                    _ => {
                        // Other acceptance reasons (shouldn't happen)
                    }
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
 
 
                    // Store chunk in appropriate HashMap based on image_type
                    if image_type == 0 {
                        c.true_chunks.insert(seq, payload.to_vec());
                        c.true_received += 1;
                    } else {
                        c.cover_chunks.insert(seq, payload.to_vec());
                        c.cover_received += 1;
                    }

                    let total_received = c.true_received + c.cover_received;
                    let total_expected = c.expect_true_chunks + c.expect_cover_chunks;
 
 
                    // Progress logging every 1000 chunks
                    if total_received % 1000 == 0 || total_received == total_expected {
                        println!(
                            "[RECEIVER-{}] [EXECUTOR] REQ_CHUNK progress | req_id={} {}/{} chunks ({:.1}%) [true:{}/{} cover:{}/{}]",
                            receiver_id,
                            req_id,
                            total_received,
                            total_expected,
                            (total_received as f64 * 100.0) / (total_expected.max(1) as f64),
                            c.true_received, c.expect_true_chunks,
                            c.cover_received, c.expect_cover_chunks
                        );
                        debug!(
                            receiver_id,
                            req_id,
                            total_received,
                            total_expected,
                            true_received = c.true_received,
                            cover_received = c.cover_received,
                            "chunk progress"
                        );
 
 
                        // Explicit 100% notification
                        if total_received == total_expected {
                            println!(
                                "✅ [RECEIVER-{}] [EXECUTOR] 100% RECEIVED | req_id={} - ALL {} chunks arrived (true:{} cover:{})!",
                                receiver_id, req_id, total_expected, c.expect_true_chunks, c.expect_cover_chunks
                            );
                        }
                    }
 
 
                    // All chunks received for BOTH images?
                    let both_complete = c.true_received == c.expect_true_chunks && c.cover_received == c.expect_cover_chunks;
                    if both_complete {
                        println!(
                            "[RECEIVER-{}] [EXECUTOR] All chunks received | req_id={} total={} (true:{} cover:{})",
                            receiver_id, req_id, total_received, c.true_received, c.cover_received
                        );
                        debug!(receiver_id, req_id, total_received, true_received = c.true_received, cover_received = c.cover_received, "all chunks in");
 
 
                        // ============================================================
                        // Reassemble TRUE IMAGE chunks in sequence order
                        // ============================================================
                        let mut true_buffer = Vec::with_capacity(c.true_image_len);
                        let mut missing_true = Vec::new();
 
 
                        for seq_idx in 0..c.expect_true_chunks {
                            if let Some(chunk_data) = c.true_chunks.get(&seq_idx) {
                                true_buffer.extend_from_slice(chunk_data);
                            } else {
                                missing_true.push(seq_idx);
                            }
                        }


                        // ============================================================
                        // Reassemble COVER IMAGE chunks in sequence order
                        // ============================================================
                        let mut cover_buffer = Vec::with_capacity(c.cover_image_len);
                        let mut missing_cover = Vec::new();

                        for seq_idx in 0..c.expect_cover_chunks {
                            if let Some(chunk_data) = c.cover_chunks.get(&seq_idx) {
                                cover_buffer.extend_from_slice(chunk_data);
                            } else {
                                missing_cover.push(seq_idx);
                            }
                        }
 
 
                        if !missing_true.is_empty() || !missing_cover.is_empty() {
                            eprintln!(
                                "[RECEIVER-{}] [EXECUTOR] Missing chunks | req_id={} - true:{:?} cover:{:?} - dropping request",
                                receiver_id, req_id, missing_true, missing_cover
                            );
                            warn!(
                                receiver_id,
                                req_id,
                                missing_true = ?missing_true,
                                missing_cover = ?missing_cover,
                                "Missing chunks - dropping request"
                            );
                            // NOTE: History entry remains with path=None, timestamp=0
                            // Cleanup task will remove it after timeout
                            ctxs.remove(&req_id);
                            continue;
                        }
 
 
                        println!(
                            "[RECEIVER-{}] [EXECUTOR] Chunks reassembled in order | req_id={} true_bytes={} cover_bytes={}",
                            receiver_id, req_id, true_buffer.len(), cover_buffer.len()
                        );
                        debug!(receiver_id, req_id, true_bytes = true_buffer.len(), cover_bytes = cover_buffer.len(), "chunks reassembled");
 
 
                        // ============================================================
                        // Send job to worker pool
                        // ============================================================
                        let job = Job {
                            req_id,
                            peer,
                            true_image_buffer: true_buffer,
                            cover_image_buffer: cover_buffer,
                            meta_json: c.meta_json.clone(),
                        };
 
 
                        if let Err(e) = tx_jobs.send(job) {
                            eprintln!(
                                "[RECEIVER-{}] [EXECUTOR] Failed to send job to workers | req_id={} error={}",
                                receiver_id, req_id, e
                            );
                            warn!(receiver_id, req_id, error=%e, "Failed to send job to workers");
                        } else {
                            println!(
                                "[RECEIVER-{}] [EXECUTOR] Job sent to workers | req_id={}",
                                receiver_id, req_id
                            );
                            debug!(receiver_id, req_id, "job queued for workers");
                        }
 
 
                        // Clean up context
                        ctxs.remove(&req_id);
                    }
                } else if acceptance_reason == "history_in_progress" {
                    // ============================================================
                    // HISTORY IN-PROGRESS BUT NO LOCAL CONTEXT
                    // ============================================================
                    // Another receiver on this server accepted REQ_META but this
                    // receiver doesn't have the context. Due to SO_REUSEPORT,
                    // chunks can arrive at different receivers.
                    //
                    // We accept the chunk (no error log) but can't process it here.
                    // Most chunks should go to the receiver with context due to
                    // kernel flow hashing. This is a rare edge case.
                    //
                    // Log once at the start to indicate what's happening
                    if seq == 0 {
                        println!(
                            "[RECEIVER-{}] ⚠️  [STICKY-EXECUTOR-NO-CONTEXT] REQ_CHUNK accepted but cannot process | req_id={} (REQ_META was accepted by different receiver due to SO_REUSEPORT)",
                            receiver_id, req_id
                        );
                        warn!(
                            receiver_id,
                            req_id,
                            "Chunk accepted (sticky executor via history) but no local context - REQ_META at different receiver"
                        );
                    }
                    // Drop chunk silently - the receiver with context will handle the request
                    // If too many chunks arrive here, the request will fail and client will retry
                } else {
                    // No context and not history_in_progress - shouldn't reach here for accepted chunks
                    println!(
                        "[RECEIVER-{}] ⚠️  [WARNING] REQ_CHUNK accepted without context | req_id={} seq={} reason={} from {}",
                        receiver_id, req_id, seq, acceptance_reason, peer
                    );
                    warn!(
                        receiver_id,
                        req_id,
                        seq,
                        %peer,
                        reason=acceptance_reason,
                        "REQ_CHUNK accepted without context - unusual case"
                    );
                }
            }


            // --------------------- NEW PROTOCOL MESSAGES ---------------------
            // REQ: Initial handshake (type=10)
            x if x == client_protocol::REQ => {
                info!(%peer, len=n-1, "Received REQ from client");
                let state_clone = state.clone();
                let cfg_clone = cfg.clone();
                let sock_clone = sock.clone();
                let data = buf[1..n].to_vec();

                tokio::spawn(async move {
                    if let Err(e) = client_protocol::handle_req(
                        state_clone,
                        &cfg_clone,
                        &*sock_clone,
                        peer,
                        &data,
                    ).await {
                        warn!("handle_req error: {}", e);
                    }
                });
            }

            // JOIN: Client registration (type=27)
            x if x == client_protocol::JOIN => {
                info!(%peer, len=n-1, "Received JOIN from client");
                let state_clone = state.clone();
                let cfg_clone = cfg.clone();
                let sock_clone = sock.clone();
                let data = buf[1..n].to_vec();

                tokio::spawn(async move {
                    if let Err(e) = client_protocol::handle_join(
                        state_clone,
                        &cfg_clone,
                        &*sock_clone,
                        peer,
                        &data,
                    ).await {
                        warn!("handle_join error: {}", e);
                    }
                });
            }

            // CLIENT_PING: Heartbeat (type=50)
            x if x == client_protocol::CLIENT_PING => {
                info!(%peer, len=n-1, "Received CLIENT_PING from client");
                let state_clone = state.clone();
                let sock_clone = sock.clone();
                let data = buf[1..n].to_vec();

                tokio::spawn(async move {
                    if let Err(e) = client_protocol::handle_client_ping(
                        state_clone,
                        &*sock_clone,
                        peer,
                        &data,
                    ).await {
                        warn!("handle_client_ping error: {}", e);
                    }
                });
            }

            // VIEW_REQUEST: Viewer requests access (type=12)
            x if x == client_protocol::VIEW_REQUEST => {
                info!(%peer, len=n-1, "Received VIEW_REQUEST from client");
                let state_clone = state.clone();
                let cfg_clone = cfg.clone();
                let sock_clone = sock.clone();
                let data = buf[1..n].to_vec();

                tokio::spawn(async move {
                    if let Err(e) = client_protocol::handle_view_request(
                        state_clone,
                        &cfg_clone,
                        &*sock_clone,
                        peer,
                        &data,
                    ).await {
                        warn!("handle_view_request error: {}", e);
                    }
                });
            }

            // DENY_VIEW: Owner denies view request (type=18)
            x if x == client_protocol::DENY_VIEW => {
                info!(%peer, len=n-1, "Received DENY_VIEW from client");
                let state_clone = state.clone();
                let sock_clone = sock.clone();
                let data = buf[1..n].to_vec();

                tokio::spawn(async move {
                    if let Err(e) = client_protocol::handle_deny_view(
                        state_clone,
                        &*sock_clone,
                        peer,
                        &data,
                    ).await {
                        warn!("handle_deny_view error: {}", e);
                    }
                });
            }

            // SYNC_USAGE: Client syncs offline usage (type=30)
            x if x == client_protocol::SYNC_USAGE => {
                let state_clone = state.clone();
                let cfg_clone = cfg.clone();
                let sock_clone = sock.clone();
                let data = buf[1..n].to_vec();

                tokio::spawn(async move {
                    if let Err(e) = client_protocol::handle_sync_usage(
                        state_clone,
                        &cfg_clone,
                        &*sock_clone,
                        peer,
                        &data,
                    ).await {
                        warn!("handle_sync_usage error: {}", e);
                    }
                });
            }


            _ => {
                // Unknown packet type
            }
        }
    }
 }
 
 
 // ============================================================================
 // WORKER TASK (Slow path - CPU-intensive operations)
 // ============================================================================
 
 
 async fn worker_task(
    worker_id: usize,
    rx_jobs: Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<Job>>>,
    state: SharedState,
    cfg: Config,
    my_client_ip: IpAddr,
    response_sock: Arc<UdpSocket>,
 ) {
    println!("[WORKER-{}] Started", worker_id);
    debug!(worker_id, "Worker task started");
 
 
    let pacing_us = cfg.pacing_us;
 
 
    loop {
        // Get next job from queue
        let job = {
            let mut rx = rx_jobs.lock().await;
            match rx.recv().await {
                Some(j) => j,
                None => {
                    eprintln!("[WORKER-{}] Channel closed, exiting", worker_id);
                    warn!(worker_id, "Job channel closed");
                    return;
                }
            }
        };
 
 
        let Job {
            req_id,
            peer,
            true_image_buffer,
            cover_image_buffer,
            meta_json,
        } = job;
 
 
        println!(
            "[WORKER-{}] Processing job | req_id={} true_len={} cover_len={} meta_len={}",
            worker_id,
            req_id,
            true_image_buffer.len(),
            cover_image_buffer.len(),
            meta_json.len()
        );
        debug!(
            worker_id,
            req_id,
            true_len = true_image_buffer.len(),
            cover_len = cover_image_buffer.len(),
            meta_len = meta_json.len(),
            "processing job"
        );
 
 
        // ============================================================
        // STEP A: Check history for duplicate/retry
        // ============================================================
        let (in_history, has_local_path, original_executor_ip) = {
            let s = state.read().await;
            if let Some(record) = s.history.get(&req_id) {
                (
                    true,
                    record.path_to_output_image.is_some(),
                    Some(record.executor_node),
                )
            } else {
                (false, false, None)
            }
        };
 
 
        if in_history {
            if has_local_path {
                // We have local path - we're the original executor
                // Serve cached response
                println!(
                    "[WORKER-{}] [CACHE-HIT] Request already processed | req_id={} - serving cached response",
                    worker_id, req_id
                );
                info!(worker_id, req_id, "Cache hit - serving cached response");
 
 
                let path = format!("./server_images/req_{}.png", req_id);
                match tokio::fs::read(&path).await {
                    Ok(cached_data) => {
                        send_response_to_client(&response_sock, peer, req_id, &cached_data, pacing_us).await;
                        
                        println!(
                            "[WORKER-{}] [CACHE-HIT] Cached response sent | req_id={} path={}",
                            worker_id, req_id, path
                        );
                        debug!(worker_id, req_id, ?path, "cached response sent");
                        continue; // Skip to next job
                    }
                    Err(e) => {
                        eprintln!(
                            "[WORKER-{}] [CACHE-MISS] Failed to read cached image | req_id={} path={} error={}",
                            worker_id, req_id, path, e
                        );
                        warn!(worker_id, req_id, ?path, error=%e, "Failed to read cached image");
                        // Fall through to reprocess
                    }
                }
            } else if let Some(orig_exec_ip) = original_executor_ip {
                // In history but no local path - another server is the original executor
                // Forward the request to the original executor
                println!(
                    "[WORKER-{}] [FORWARD] Request belongs to another executor | req_id={} original_executor={}",
                    worker_id, req_id, orig_exec_ip
                );
                info!(
                    worker_id,
                    req_id,
                    ?orig_exec_ip,
                    "Forwarding request to original executor (found in history)"
                );
 
 
                // Forward to original executor using history module
                if let Ok(temp_sock) = UdpSocket::bind("0.0.0.0:0").await {
                    // Reconstruct request data (simple format for forwarding)
                    let request_data = Vec::new(); // Empty for now - original executor has the image
                    
                    history::send_forward_request(
                        &temp_sock,
                        &cfg,
                        orig_exec_ip,
                        req_id,
                        peer,
                        &request_data,
                    ).await;
                    
                    println!(
                        "[WORKER-{}] [FORWARD] Forward request sent | req_id={} to={}",
                        worker_id, req_id, orig_exec_ip
                    );
                }
                continue; // Skip to next job
            }
        }
 
 
        // ============================================================
        // STEP B: Transform metadata and perform steganography
        // ============================================================
        
        // Transform client metadata to stego format
        let stego_meta_json = if !meta_json.is_empty() {
            match transform_metadata(&meta_json) {
                Ok(transformed) => {
                    println!(
                        "[WORKER-{}] [EXECUTOR] Metadata transformed | req_id={} client_meta_len={} stego_meta_len={}",
                        worker_id, req_id, meta_json.len(), transformed.len()
                    );
                    debug!(
                        worker_id,
                        req_id,
                        client_meta_len = meta_json.len(),
                        stego_meta_len = transformed.len(),
                        "Metadata transformed"
                    );
                    transformed
                }
                Err(e) => {
                    eprintln!(
                        "[WORKER-{}] [EXECUTOR] Metadata transformation failed | req_id={} | Error: {}",
                        worker_id, req_id, e
                    );
                    warn!(worker_id, req_id, error=%e, "Metadata transformation failed - using empty metadata");
                    Vec::new()
                }
            }
        } else {
            println!(
                "[WORKER-{}] [EXECUTOR] No metadata provided | req_id={} - proceeding with empty metadata",
                worker_id, req_id
            );
            Vec::new()
        };
 
 
        println!(
            "[WORKER-{}] [EXECUTOR] Starting steganography | req_id={} true_len={} cover_len={} meta_len={}",
            worker_id,
            req_id,
            true_image_buffer.len(),
            cover_image_buffer.len(),
            stego_meta_json.len()
        );
        debug!(
            worker_id,
            req_id,
            true_len = true_image_buffer.len(),
            cover_len = cover_image_buffer.len(),
            meta_len = stego_meta_json.len(),
            "starting steganography"
        );
 
 
        let encrypted_png = match crate::stego_service::embed_meta_return_png(
            &true_image_buffer,
            &cover_image_buffer,
            &stego_meta_json,
        ) {
            Ok(png) => png,
            Err(e) => {
                eprintln!(
                    "[WORKER-{}] [EXECUTOR] Steganography failed | req_id={} | Error: {}",
                    worker_id, req_id, e
                );
                warn!(worker_id, req_id, error=%e, "Steganography failed - dropping request");
                // NOTE: History entry remains with path=None, timestamp=0
                // Cleanup task will remove it after timeout
                continue; // Drop request, go to next job
            }
        };
 
 
        let total_input_size = true_image_buffer.len() + cover_image_buffer.len();
        println!(
            "[WORKER-{}] [EXECUTOR] Steganography complete | req_id={} input_size={} (true:{} cover:{}) encrypted_size={}",
            worker_id,
            req_id,
            total_input_size,
            true_image_buffer.len(),
            cover_image_buffer.len(),
            encrypted_png.len()
        );
        debug!(
            worker_id,
            req_id,
            total_input_size,
            true_size = true_image_buffer.len(),
            cover_size = cover_image_buffer.len(),
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
            // NOTE: We don't remove from history here - the history entry will be updated
            // below with the actual path and timestamp when we save to disk
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

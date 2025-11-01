use anyhow::{bail, Context, Result};
use clap::{Parser, ValueEnum};
use serde::{Serialize, Deserialize};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::fs;
use std::hash::{Hash, Hasher};
use std::net::{IpAddr, SocketAddr, UdpSocket};
use std::process::Command;
use std::sync::atomic::{AtomicU32, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// NOTE: Import the stego library for metadata extraction
// This assumes the stego library is available as a dependency
// Adjust the import path based on your project structure
// If stego is a separate crate: use stego::{extract, Meta, StegoError};
// If stego is a local module: use crate::stego::{extract, Meta, StegoError};
// For now, I'll inline the necessary structures and assume you'll link the library


/// Wire constants (must match server)
const REQ_META: u8 = 0;
const REQ_CHUNK: u8 = 1;
const RESP_META: u8 = 2;
const RESP_CHUNK: u8 = 3;
const SELECT: u8 = 4;
const ACCEPT: u8 = 5;


/// Keep UDP payload safe for typical MTUs
const MAX_DGRAM: usize = 1200;


/// Timeout configurations
const INITIAL_TIMEOUT_MS: u64 = 1000;      // SELECT → ACCEPT
const SECONDARY_TIMEOUT_MS: u64 = 10000;   // REQ_META → RESP_META
const TERTIARY_TIMEOUT_MS: u64 = 10000;    // RESP_META → all RESP_CHUNKs


/// Retry configuration
const MAX_ATTEMPTS: u32 = 3;
const BASE_BACKOFF_MS: u64 = 100;


/// Global sequence counter for request ID generation
static SEQUENCE_COUNTER: AtomicU32 = AtomicU32::new(0);


#[derive(ValueEnum, Clone, Debug)]
enum Operation {
   Encrypt,
   AdjustViews,
   Grant,
   Revoke,
}


impl Operation {
   fn as_str(&self) -> &'static str {
       match self {
           Operation::Encrypt => "encrypt",
           Operation::AdjustViews => "adjust_views",
           Operation::Grant => "grant",
           Operation::Revoke => "revoke",
       }
   }
   fn to_code(&self) -> u8 {
       match self {
           Operation::Encrypt => 1,
           Operation::AdjustViews => 2,
           Operation::Grant => 3,
           Operation::Revoke => 4,
       }
   }
}


/// CLI for the client launcher and per-user child
#[derive(Parser, Debug, Clone)]
#[command(name = "client-node", version)]
struct Cli {
   /// Comma-separated peers (ip:port)
   #[arg(long)]
   peers: String,


   /// Total users (processes) to spawn. Set to 1 inside children.
   #[arg(long, default_value_t = 1)]
   processes: usize,


   /// Requests per user (per process), done with retry support.
   #[arg(long, default_value_t = 1)]
   requests_per_user: usize,


   /// Path to the image to send
   #[arg(long)]
   image: String,


   /// Operation to perform
   #[arg(long, value_enum, default_value_t = Operation::Encrypt)]
   op: Operation,


   /// Logical owner for policy
   #[arg(long, default_value = "owner")]
   owner: String,


   /// Optional "allow" CSV: user:views,user2:views
   #[arg(long, default_value = "bob")]
   allow: String,


   /// Optional logical image id/name
   #[arg(long, default_value = "")]
   image_id: String,


   /// Optional explicit sender id (if 0, child's sender_id == proc_idx)
   #[arg(long, default_value_t = 0)]
   sender_id: u32,


   /// Pacing delay in microseconds per packet sent (0 = no pacing)
   #[arg(long, default_value_t = 150)]
   pacing_us: u64,


   /// INTERNAL: set by parent for child users
   #[arg(long, default_value_t = false, hide = true)]
   child: bool,


   /// INTERNAL: which user/process index is this (0-based)
   #[arg(long, default_value_t = 0, hide = true)]
   proc_idx: u16,
}


/// Policy structs for meta JSON (sending to server)
#[derive(Serialize, Clone)]
struct Allow {
   user: String,
   views: u32,
}


#[derive(Serialize)]
struct Adjust<'a> {
   user: &'a str,
   delta: i32,
}


#[derive(Serialize)]
struct Grant<'a> {
   user: &'a str,
   views: u32,
}


#[derive(Serialize)]
struct Revoke<'a> {
   user: &'a str,
}


#[derive(Serialize)]
struct Meta<'a> {
   op: &'a str,
   sender_id: u32,
   request_id: u32,
   image_id: &'a str,
   owner: &'a str,
   allow: Vec<Allow>,
   adjust: Option<Adjust<'a>>,
   grant: Option<Grant<'a>>,
   revoke: Option<Revoke<'a>>,
   ts_unix_ms: u128,
   selection_phase: &'a str,
}


// ============================================================================
// Steganography structures for extraction (matches lib.rs)
// ============================================================================

/// AccessEntry from stego library (what we extract)
#[derive(Debug, Clone, Serialize, Deserialize)]
struct AccessEntry {
   user: String,
   remaining_views: u32,
}

/// Meta from stego library (what we extract)
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ExtractedMeta {
   owner: String,
   allow: Vec<AccessEntry>,
}


/// Request state tracking
#[derive(Debug, Clone, Copy, PartialEq)]
enum RequestState {
   SelectSent,
   AcceptReceived,
   ChunksSent,
   RespMetaReceived { expect_chunks: u32 },
   Completed,
   Failed,
}


/// Context for a single request attempt
struct RequestContext {
   req_id: u32,
   attempt: u32,
   state: RequestState,
   target: Option<SocketAddr>,
}


impl RequestContext {
   fn new(req_id: u32) -> Self {
       Self {
           req_id,
           attempt: 1,
           state: RequestState::SelectSent,
           target: None,
       }
   }


   fn reset_for_retry(&mut self) {
       self.attempt += 1;
       self.state = RequestState::SelectSent;
       self.target = None;
   }
}


/// Request execution result types
enum RequestResult {
   Success,
   InitialTimeout,
   SecondaryTimeout,
   TertiaryTimeout,
   OtherFailure,
}


fn main() -> Result<()> {
   let cli = Cli::parse();
   let peers = parse_peers(&cli.peers)?;

   if !cli.child && cli.processes > 1 {
       // Parent: spawn N child processes (users)
       spawn_child_processes(&cli)?;
       thread::sleep(Duration::from_secs(3));
       println!("[PARENT] All {} child processes spawned", cli.processes);
       Ok(())
   } else {
       // Child or single-process mode: run the actual client logic
       run_client_process(&cli, &peers)
   }
}


fn spawn_child_processes(cli: &Cli) -> Result<()> {
   let exe = std::env::current_exe().context("current_exe")?;
   for i in 0..cli.processes {
       let mut child_args: Vec<String> = vec![];
       child_args.push(exe.to_string_lossy().to_string());
       child_args.push("--peers".into());
       child_args.push(cli.peers.clone());
       child_args.push("--processes".into());
       child_args.push("1".into());
       child_args.push("--requests-per-user".into());
       child_args.push(cli.requests_per_user.to_string());
       child_args.push("--image".into());
       child_args.push(cli.image.clone());
       child_args.push("--op".into());
       child_args.push(cli.op.as_str().into());
       child_args.push("--owner".into());
       child_args.push(cli.owner.clone());
       child_args.push("--allow".into());
       child_args.push(cli.allow.clone());
       child_args.push("--image-id".into());
       child_args.push(cli.image_id.clone());
       child_args.push("--pacing-us".into());
       child_args.push(cli.pacing_us.to_string());
       child_args.push("--child".into());
       child_args.push("--proc-idx".into());
       child_args.push(i.to_string());
       if cli.sender_id != 0 {
           child_args.push("--sender-id".into());
           child_args.push(cli.sender_id.to_string());
       }


       let _ = Command::new(&exe).args(&child_args[1..]).spawn()?;
   }
   Ok(())
}


fn run_client_process(cli: &Cli, peers: &[SocketAddr]) -> Result<()> {
   let sender_id = if cli.sender_id != 0 {
       cli.sender_id
   } else {
       cli.proc_idx as u32 + 1
   };


   println!("[CLIENT] Process started: sender_id={}, requests={}", sender_id, cli.requests_per_user);


   let sock = UdpSocket::bind("0.0.0.0:0").context("bind local UDP")?;
   sock.set_read_timeout(Some(Duration::from_millis(100)))?;
  
   let local_ip = get_local_ip()?;
   println!("[CLIENT] Local IP detected: {}", local_ip);


   let image_bytes = fs::read(&cli.image)
       .with_context(|| format!("read image file '{}'", cli.image))?;


   let allow_vec = parse_allow(&cli.allow);


   for i in 1..=cli.requests_per_user {
       println!("\n[CLIENT] ===== Starting Request {}/{} =====", i, cli.requests_per_user);
      
       let req_id = generate_request_id(local_ip);
       let mut ctx = RequestContext::new(req_id);

       let result = execute_request_with_retry(
           cli,
           &sock,
           peers,
           &mut ctx,
           sender_id,
           &image_bytes,
           &allow_vec,
       );

       match result {
           Ok(_) => println!("[CLIENT] ===== Request {}/{} COMPLETED =====\n", i, cli.requests_per_user),
           Err(e) => {
               eprintln!("[CLIENT] ===== Request {}/{} FAILED: {} =====\n", i, cli.requests_per_user, e);
           }
       }

       if i < cli.requests_per_user {
           thread::sleep(Duration::from_millis(100));
       }
   }

   println!("[CLIENT] All {} requests completed", cli.requests_per_user);
   Ok(())
}


fn execute_request_with_retry(
   cli: &Cli,
   sock: &UdpSocket,
   peers: &[SocketAddr],
   ctx: &mut RequestContext,
   sender_id: u32,
   image_bytes: &[u8],
   allow_vec: &[Allow],
) -> Result<()> {
   let mut last_error: Option<anyhow::Error> = None;

   while ctx.attempt <= MAX_ATTEMPTS {
       println!("[ATTEMPT] req_id={} attempt={}/{}", ctx.req_id, ctx.attempt, MAX_ATTEMPTS);
      
       match execute_single_attempt(cli, sock, peers, ctx, sender_id, image_bytes, allow_vec) {
           Ok(_) => {
               println!("[ATTEMPT] req_id={} succeeded on attempt {}", ctx.req_id, ctx.attempt);
               return Ok(());
           }
           Err(e) => {
               last_error = Some(e);
               println!("[ATTEMPT] req_id={} failed on attempt {}: {}", 
                       ctx.req_id, ctx.attempt, last_error.as_ref().unwrap());
              
               if ctx.attempt < MAX_ATTEMPTS {
                   let backoff = BASE_BACKOFF_MS * (1 << (ctx.attempt - 1));
                   println!("[RETRY] Waiting {}ms before retry...", backoff);
                   thread::sleep(Duration::from_millis(backoff));
                   ctx.reset_for_retry();
               }
           }
       }
   }

   Err(last_error.unwrap_or_else(|| anyhow::anyhow!("All attempts failed")))
}


fn execute_single_attempt(
   cli: &Cli,
   sock: &UdpSocket,
   peers: &[SocketAddr],
   ctx: &mut RequestContext,
   sender_id: u32,
   image_bytes: &[u8],
   allow_vec: &[Allow],
) -> Result<()> {
   let attempt_start = Instant::now();

   // Phase 1: SELECT → ACCEPT
   send_select(sock, peers, ctx.req_id, sender_id, cli.op.to_code(), image_bytes.len())?;
   ctx.state = RequestState::SelectSent;

   let target = match wait_first_accept(sock, ctx.req_id, Duration::from_millis(INITIAL_TIMEOUT_MS))? {
       Some(addr) => {
           println!("[ACCEPT] Received from {} (req_id={})", addr, ctx.req_id);
           addr
       }
       None => {
           bail!("Timeout waiting for ACCEPT (Initial timeout)")
       }
   };

   ctx.target = Some(target);
   ctx.state = RequestState::AcceptReceived;

   // Phase 2: Upload image chunks
   upload_request(cli, sock, target, ctx.req_id, sender_id, image_bytes, allow_vec)?;
   ctx.state = RequestState::ChunksSent;

   // Phase 3: Wait for RESP_META
   let (expect_chunks, out_len) = wait_resp_meta(sock, target, ctx.req_id)?;
   ctx.state = RequestState::RespMetaReceived { expect_chunks };
  
   println!("[RESPONSE] RESP_META received: {} chunks, {} bytes (req_id={})",
            expect_chunks, out_len, ctx.req_id);


   // Phase 4: Wait for all RESP_CHUNKs and save encrypted image
   receive_and_save_encrypted_image(sock, target, ctx.req_id, expect_chunks, out_len)?;
   ctx.state = RequestState::Completed;


   let total_time = attempt_start.elapsed();
   println!("[SUCCESS] req_id={} completed in {:.2}s (attempt={}, to={})",
            ctx.req_id, total_time.as_secs_f64(), ctx.attempt, target);


   Ok(())
}


fn send_select(
   sock: &UdpSocket,
   peers: &[SocketAddr],
   req_id: u32,
   sender_id: u32,
   op_code: u8,
   image_len: usize,
) -> Result<()> {
   let mut pkt = Vec::with_capacity(1 + 4 + 4 + 1 + 4);
   pkt.push(SELECT);
   pkt.extend(req_id.to_le_bytes());
   pkt.extend(sender_id.to_le_bytes());
   pkt.push(op_code);
   pkt.extend((image_len as u32).to_le_bytes());


   for &peer in peers {
       sock.send_to(&pkt, peer)?;
   }


   println!("[SELECT] Multicast to {} peers (req_id={})", peers.len(), req_id);
   Ok(())
}


fn wait_first_accept(sock: &UdpSocket, req_id: u32, timeout: Duration) -> Result<Option<SocketAddr>> {
   let deadline = Instant::now() + timeout;
   let mut buf = [0u8; 256];


   while Instant::now() < deadline {
       match sock.recv_from(&mut buf) {
           Ok((n, from)) => {
               if n >= 1 + 4 && buf[0] == ACCEPT {
                   let r = u32::from_le_bytes(buf[1..5].try_into().unwrap());
                   if r == req_id {
                       return Ok(Some(from));
                   }
               }
           }
           Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock
                      || e.kind() == std::io::ErrorKind::TimedOut => {
               continue;
           }
           Err(e) => return Err(e.into()),
       }
   }


   Ok(None)
}


fn upload_request(
   cli: &Cli,
   sock: &UdpSocket,
   target: SocketAddr,
   req_id: u32,
   sender_id: u32,
   image_bytes: &[u8],
   allow_vec: &[Allow],
) -> Result<()> {
   let now_ms = now_unix_ms();
  
   let (adjust, grant, revoke) = match cli.op {
       Operation::AdjustViews => (Some(Adjust { user: "alice", delta: -1 }), None, None),
       Operation::Grant => (None, Some(Grant { user: "charlie", views: 5 }), None),
       Operation::Revoke => (None, None, Some(Revoke { user: "dave" })),
       Operation::Encrypt => (None, None, None),
   };


   let meta = Meta {
       op: cli.op.as_str(),
       sender_id,
       request_id: req_id,
       image_id: &cli.image_id,
       owner: &cli.owner,
       allow: allow_vec.to_vec(),
       adjust,
       grant,
       revoke,
       ts_unix_ms: now_ms,
       selection_phase: "upload",
   };
   let meta_json = serde_json::to_vec(&meta).context("serialize meta")?;


   // Send REQ_META
   let chunk_payload = MAX_DGRAM - (1 + 4 + 4);
   let total_chunks = ((image_bytes.len() + chunk_payload - 1) / chunk_payload) as u32;


   let mut meta_hdr = Vec::with_capacity(1 + 4 + 4 + 4 + 4 + meta_json.len());
   meta_hdr.push(REQ_META);
   meta_hdr.extend(req_id.to_le_bytes());
   meta_hdr.extend(total_chunks.to_le_bytes());
   meta_hdr.extend((image_bytes.len() as u32).to_le_bytes());
   meta_hdr.extend((meta_json.len() as u32).to_le_bytes());
   meta_hdr.extend(&meta_json);
   sock.send_to(&meta_hdr, target)?;


   println!("[UPLOAD] Starting {} chunks to {} (req_id={})", total_chunks, target, req_id);


   // Send REQ_CHUNKs with pacing
   let mut offset = 0usize;
   let mut seq_idx = 0u32;


   while offset < image_bytes.len() {
       let take = (image_bytes.len() - offset).min(chunk_payload);
       let mut pkt = Vec::with_capacity(1 + 4 + 4 + take);
       pkt.push(REQ_CHUNK);
       pkt.extend(req_id.to_le_bytes());
       pkt.extend(seq_idx.to_le_bytes());
       pkt.extend(&image_bytes[offset..offset + take]);
       sock.send_to(&pkt, target)?;


       if seq_idx % 1000 == 0 && seq_idx > 0 {
           println!("[UPLOAD] Progress: {}/{} chunks ({:.1}%)",
                    seq_idx, total_chunks, (seq_idx as f64 * 100.0) / total_chunks as f64);
       }


       offset += take;
       seq_idx += 1;


       if cli.pacing_us > 0 {
           thread::sleep(Duration::from_micros(cli.pacing_us));
       }
   }


   println!("[UPLOAD] All {} chunks sent (req_id={})", total_chunks, req_id);
   Ok(())
}


fn wait_resp_meta(sock: &UdpSocket, target: SocketAddr, req_id: u32) -> Result<(u32, usize)> {
   let deadline = Instant::now() + Duration::from_millis(SECONDARY_TIMEOUT_MS);
   let mut buf = [0u8; 65536];


   while Instant::now() < deadline {
       match sock.recv_from(&mut buf) {
           Ok((n, from)) => {
               if from != target {
                   continue;
               }
               if n >= 1 + 4 + 4 + 4 && buf[0] == RESP_META {
                   let rid = u32::from_le_bytes(buf[1..5].try_into().unwrap());
                   if rid != req_id {
                       continue;
                   }
                   let total_chunks = u32::from_le_bytes(buf[5..9].try_into().unwrap());
                   let out_len = u32::from_le_bytes(buf[9..13].try_into().unwrap()) as usize;
                   return Ok((total_chunks, out_len));
               }
           }
           Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock
                      || e.kind() == std::io::ErrorKind::TimedOut => {
               continue;
           }
           Err(e) => return Err(e.into()),
       }
   }


   bail!("Timeout waiting for RESP_META (Secondary timeout)")
}


/// Receive RESP_CHUNKs with ordered reassembly, save encrypted image, then extract and save metadata
fn receive_and_save_encrypted_image(
   sock: &UdpSocket,
   target: SocketAddr,
   req_id: u32,
   expect_chunks: u32,
   expected_size: usize,
) -> Result<()> {
   if expect_chunks == 0 {
       return Ok(());
   }

   let deadline = Instant::now() + Duration::from_millis(TERTIARY_TIMEOUT_MS);
   let mut buf = [0u8; 65536];
   let mut received = 0u32;
   
   // Store chunks by sequence number
   let mut chunks_map: HashMap<u32, Vec<u8>> = HashMap::with_capacity(expect_chunks as usize);


   while received < expect_chunks {
       if Instant::now() > deadline {
           bail!("Timeout waiting for RESP_CHUNKs: got {}/{} (Tertiary timeout)",
                 received, expect_chunks);
       }


       match sock.recv_from(&mut buf) {
           Ok((n, from)) => {
               if from != target {
                   continue;
               }
               if n >= 1 + 4 + 4 && buf[0] == RESP_CHUNK {
                   let rid = u32::from_le_bytes(buf[1..5].try_into().unwrap());
                   if rid != req_id {
                       continue;
                   }
                   
                   // Extract sequence number and payload
                   let seq = u32::from_le_bytes(buf[5..9].try_into().unwrap());
                   let payload = buf[9..n].to_vec();
                   
                   // Store by sequence number
                   chunks_map.insert(seq, payload);
                   
                   received += 1;


                   if received % 1000 == 0 && received < expect_chunks {
                       println!("[RESPONSE] Progress: {}/{} chunks ({:.1}%)",
                                received, expect_chunks,
                                (received as f64 * 100.0) / expect_chunks as f64);
                   }
               }
           }
           Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock
                      || e.kind() == std::io::ErrorKind::TimedOut => {
               continue;
           }
           Err(e) => return Err(e.into()),
       }
   }


   println!("[RESPONSE] All {} chunks received (req_id={})", expect_chunks, req_id);
   
   // Reassemble chunks in sequence order
   let mut encrypted_buffer = Vec::with_capacity(expected_size);
   let mut missing_chunks = Vec::new();
   
   for seq in 0..expect_chunks {
       if let Some(chunk_data) = chunks_map.get(&seq) {
           encrypted_buffer.extend_from_slice(chunk_data);
       } else {
           missing_chunks.push(seq);
       }
   }
   
   if !missing_chunks.is_empty() {
       bail!("Missing chunks {:?} in response", missing_chunks);
   }
   
   println!("[RESPONSE] Chunks reassembled in order | req_id={} bytes={}", req_id, encrypted_buffer.len());
   
   // ============================================================================
   // STEP 1: Save encrypted image to disk
   // ============================================================================
   let encrypted_path = format!("./client_test/req_{}_encrypted.png", req_id);
   match fs::write(&encrypted_path, &encrypted_buffer) {
       Ok(_) => {
           println!("[CLIENT] Encrypted image saved: {} ({} bytes)", encrypted_path, encrypted_buffer.len());
       }
       Err(e) => {
           eprintln!("[CLIENT] Failed to save encrypted image: {} | Error: {}", encrypted_path, e);
           bail!("Failed to save encrypted image: {}", e);
       }
   }
   
   // ============================================================================
   // STEP 2: Extract metadata from encrypted image
   // ============================================================================
   println!("[DECRYPT] Starting metadata extraction | req_id={}", req_id);
   
   let extracted_meta = extract_metadata_from_image(&encrypted_buffer, req_id)?;
   
   // ============================================================================
   // STEP 3: Save extracted metadata to text file
   // ============================================================================
   let metadata_path = format!("./decrypted_data/req_{}_metadata.txt", req_id);
   
   // Create pretty-printed JSON string
   let metadata_json = serde_json::to_string_pretty(&extracted_meta)
       .context("Failed to serialize extracted metadata")?;
   
   match fs::write(&metadata_path, &metadata_json) {
       Ok(_) => {
           println!("[DECRYPT] Metadata saved: {}", metadata_path);
           println!("[DECRYPT] Extracted metadata:");
           println!("  Owner: {}", extracted_meta.owner);
           println!("  Access List:");
           for entry in &extracted_meta.allow {
               println!("    - User: {}, Remaining Views: {}", entry.user, entry.remaining_views);
           }
       }
       Err(e) => {
           eprintln!("[DECRYPT] Failed to save metadata: {} | Error: {}", metadata_path, e);
           bail!("Failed to save metadata: {}", e);
       }
   }
   
   Ok(())
}


/// Extract metadata from encrypted image bytes
fn extract_metadata_from_image(image_bytes: &[u8], req_id: u32) -> Result<ExtractedMeta> {
   // Load image using the image crate
   let img = image::load_from_memory(image_bytes)
       .with_context(|| format!("Failed to load encrypted image for req_id={}", req_id))?;
   
   // Extract metadata using steganography
   // NOTE: This uses the stego library's extract function
   // The actual implementation depends on how you've structured the stego library
   // Here we inline the extraction logic based on lib.rs
   
   let rgba = img.to_rgba8();
   let mut bytes = Vec::new();
   let mut cur: u8 = 0;
   let mut bitpos = 0;
   
   const MAGIC: &[u8; 4] = b"SP2P";
   
   for y in 0..rgba.height() {
       for x in 0..rgba.width() {
           let p = rgba.get_pixel(x, y);
           for c in 0..4 {
               let bit = p[c] & 1;
               cur |= bit << (bitpos as u8);
               bitpos += 1;
               if bitpos == 8 {
                   bytes.push(cur);
                   cur = 0;
                   bitpos = 0;
                   
                   if bytes.len() >= 8 && &bytes[0..4] == MAGIC {
                       let len = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
                       if bytes.len() >= 8 + len {
                           let meta: ExtractedMeta = serde_json::from_slice(&bytes[8..8 + len])
                               .context("Failed to deserialize extracted metadata")?;
                           println!("[DECRYPT] Metadata extraction successful | req_id={}", req_id);
                           return Ok(meta);
                       }
                   }
               }
           }
       }
   }
   
   bail!("No metadata found in encrypted image for req_id={}", req_id)
}


fn generate_request_id(client_ip: IpAddr) -> u32 {
   let mut hasher = DefaultHasher::new();
  
   client_ip.hash(&mut hasher);
   std::process::id().hash(&mut hasher);
   SystemTime::now()
       .duration_since(SystemTime::UNIX_EPOCH)
       .unwrap()
       .as_nanos()
       .hash(&mut hasher);
   SEQUENCE_COUNTER.fetch_add(1, Ordering::SeqCst).hash(&mut hasher);
  
   (hasher.finish() & 0xFFFFFFFF) as u32
}


fn get_local_ip() -> Result<IpAddr> {
   // Use a separate temporary socket to determine local IP without affecting main socket
   let temp_sock = UdpSocket::bind("0.0.0.0:0")?;
   temp_sock.connect("8.8.8.8:80")?;
   let local = temp_sock.local_addr()?;
   Ok(local.ip())
}


fn parse_peers(s: &str) -> Result<Vec<SocketAddr>> {
   let mut v = Vec::new();
   for part in s.split(',') {
       let p = part.trim();
       if p.is_empty() {
           continue;
       }
       let addr: SocketAddr = p.parse().with_context(|| format!("invalid peer '{}'", p))?;
       v.push(addr);
   }
   if v.is_empty() {
       bail!("no peers provided");
   }
   Ok(v)
}


fn parse_allow(csv: &str) -> Vec<Allow> {
   csv.split(',')
       .filter(|s| !s.trim().is_empty())
       .filter_map(|pair| {
           let mut it = pair.split(':');
           let user = it.next()?.trim().to_string();
           let views = it.next()?.trim().parse::<u32>().ok()?;
           Some(Allow { user, views })
       })
       .collect()
}


fn now_unix_ms() -> u128 {
   SystemTime::now()
       .duration_since(UNIX_EPOCH)
       .unwrap()
       .as_millis()
}

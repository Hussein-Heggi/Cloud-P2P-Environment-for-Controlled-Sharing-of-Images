use anyhow::{bail, Context, Result};
use clap::{Parser, ValueEnum};
use serde::Serialize;
use std::fs;
use std::net::{SocketAddr, UdpSocket};
use std::process::Command;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::thread;
use socket2::SockRef;


/// Wire constants (must match server)
const REQ_META: u8 = 0;
const REQ_CHUNK: u8 = 1;
const RESP_META: u8 = 2;
const RESP_CHUNK: u8 = 3;


/// New tiny control messages for selection fan-out
const SELECT: u8 = 4;
const ACCEPT: u8 = 5;


/// Keep UDP payload safe for typical MTUs
const MAX_DGRAM: usize = 1200;


/// Default timeouts
const DEFAULT_SELECT_TIMEOUT_MS: u64 = 100000; // selection window
const DEFAULT_IO_TIMEOUT_MS: u64 = 100000;    // recv/send after upload


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


 /// Requests per user (per process), done strictly synchronously.
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
 #[arg(long, default_value = "")]
 allow: String,




 /// Optional logical image id/name
 #[arg(long, default_value = "")]
 image_id: String,


 /// Optional explicit sender id (if 0, child's sender_id == proc_idx)
 #[arg(long, default_value_t = 0)]
 sender_id: u32,




 /// Selection timeout (ms) for waiting ACCEPT
 #[arg(long, default_value_t = DEFAULT_SELECT_TIMEOUT_MS)]
 selection_timeout_ms: u64,


 /// IO timeout (ms) for request/response phases
 #[arg(long, default_value_t = DEFAULT_IO_TIMEOUT_MS)]
 io_timeout_ms: u64,


 /// INTERNAL: set by parent for child users
 #[arg(long, default_value_t = false, hide = true)]
 child: bool,


 /// INTERNAL: which user/process index is this (0-based)
 #[arg(long, default_value_t = 0, hide = true)]
 proc_idx: u16,
}


/// Policy structs for meta JSON
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
 // informative hint; server may ignore
 selection_phase: &'a str, // "meta_only" for SELECT; "upload" for REQ_META
}


fn main() -> Result<()> {
 let cli = Cli::parse();
 let peers = parse_peers(&cli.peers)?;


 if !cli.child && cli.processes > 1 {
     // Parent: spawn N child processes (users)
     let exe = std::env::current_exe().context("current_exe")?;
     for i in 0..cli.processes {
         let mut child_args: Vec<String> = std::env::args().collect();
         // Rebuild args but force child=true, proc_idx=i, processes=1
         // Simpler: pass all flags explicitly
         child_args.clear();
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
         child_args.push("--selection-timeout-ms".into());
         child_args.push(cli.selection_timeout_ms.to_string());
         child_args.push("--io-timeout-ms".into());
         child_args.push(cli.io_timeout_ms.to_string());
         child_args.push("--child".into());
         child_args.push("--proc-idx".into());
         child_args.push(i.to_string());
         if cli.sender_id != 0 {
             child_args.push("--sender-id".into());
             child_args.push(cli.sender_id.to_string());
         }


         let status = Command::new(&exe).args(&child_args[1..]).spawn()?;
         // Optionally capture handles; here we just spawn and continue.
         let _ = status;
     }
     // Parent waits for all children to exit
     // (Simple approach: block until no more children)
     // In many labs it's fine to just wait on the shell 'wait'; here we block a bit.
     // For correctness, you can omit waiting; children run independently.
     return Ok(());
 }


 // Child (or single-process mode)
 let image_bytes = fs::read(&cli.image)
     .with_context(|| format!("reading image file {}", &cli.image))?;


 run_user_process(
     &cli,
     peers,
     image_bytes,
     cli.proc_idx,
     if cli.sender_id == 0 { cli.proc_idx as u32 } else { cli.sender_id },
 )
}


fn run_user_process(
 cli: &Cli,
 peers: Vec<SocketAddr>,
 image_bytes: Vec<u8>,
 proc_idx: u16,
 sender_id: u32,
) -> Result<()> {
 // One socket per user/process (synchronous pattern)
 let sock = UdpSocket::bind("0.0.0.0:0").context("bind udp")?;
 let sock_ref = SockRef::from(&sock);
 sock_ref.set_send_buffer_size(20 * 1024 * 1024)?;
 sock_ref.set_recv_buffer_size(20 * 1024 * 1024)?;
  // Diagnostic: check actual buffer sizes
 if let Ok(actual_send) = sock_ref.send_buffer_size() {
     println!("[DIAGNOSTIC] Send buffer: {} MB", actual_send / (1024*1024));
 }
 if let Ok(actual_recv) = sock_ref.recv_buffer_size() {
     println!("[DIAGNOSTIC] Recv buffer: {} MB", actual_recv / (1024*1024));
 }
  sock.set_read_timeout(Some(Duration::from_millis(cli.io_timeout_ms)))?;
 sock.set_write_timeout(Some(Duration::from_millis(cli.io_timeout_ms)))?;


 let allow_vec = parse_allow(&cli.allow);


 for seq in 0..cli.requests_per_user {
     let req_id = make_req_id(proc_idx, seq as u16);
     let now_ms = now_unix_ms();


     // Build op-specific fields
     let (adjust, grant, revoke) = match cli.op {
         Operation::AdjustViews => (Some(Adjust { user: "alice", delta: -1 }), None, None),
         Operation::Grant => (None, Some(Grant { user: "charlie", views: 5 }), None),
         Operation::Revoke => (None, None, Some(Revoke { user: "dave" })),
         Operation::Encrypt => (None, None, None),
     };


     // ---- Phase A: SELECT fan-out (tiny control) ----
     // SELECT payload: [u8 ty=SELECT][u32 req_id][u32 sender_id][u8 op_code][u32 image_len]
     let mut select_pkt = Vec::with_capacity(1 + 4 + 4 + 1 + 4);
     select_pkt.push(SELECT);
     select_pkt.extend(req_id.to_le_bytes());
     select_pkt.extend(sender_id.to_le_bytes());
     select_pkt.push(cli.op.to_code());
     select_pkt.extend((image_bytes.len() as u32).to_le_bytes());


     // Send SELECT to all peers
     for &p in &peers {
         let _ = sock.send_to(&select_pkt, p);
     }


     // Wait for first ACCEPT within selection window (shorter than IO timeout)
     let chosen = wait_first_accept(&sock, req_id, Duration::from_millis(cli.selection_timeout_ms))?;
     let target = match chosen {
         Some(addr) => addr,
         None => {
             eprintln!(
                 "TIMEOUT_SELECT user={} req_id={} peers={}",
                 sender_id,
                 req_id,
                 peers.len()
             );
             continue; // move to next request
         }
     };


     // ---- Phase B: upload control+data to chosen server ----
     // Build meta JSON (selection_phase="upload" to indicate data follows)
     let meta = Meta {
         op: cli.op.as_str(),
         sender_id,
         request_id: req_id,
         image_id: &cli.image_id,
         owner: &cli.owner,
         allow: allow_vec.clone(),
         adjust,
         grant,
         revoke,
         ts_unix_ms: now_ms,
         selection_phase: "upload",
     };
     let meta_json = serde_json::to_vec(&meta).context("serialize meta")?;


     let started = Instant::now();


     // Send REQ_META with meta appended
     // Layout: [u8 ty=REQ_META][u32 req_id][u32 total_chunks][u32 img_len][u32 meta_len][meta_json...]
     let chunk_payload = MAX_DGRAM - (1 + 4 + 4); // for chunks (ty+req_id+seq)
     let total_chunks = ((image_bytes.len() + chunk_payload - 1) / chunk_payload) as u32;


     let mut meta_hdr = Vec::with_capacity(1 + 4 + 4 + 4 + 4 + meta_json.len());
     meta_hdr.push(REQ_META);
     meta_hdr.extend(req_id.to_le_bytes());
     meta_hdr.extend(total_chunks.to_le_bytes());
     meta_hdr.extend((image_bytes.len() as u32).to_le_bytes());
     meta_hdr.extend((meta_json.len() as u32).to_le_bytes());
     meta_hdr.extend(&meta_json);
     sock.send_to(&meta_hdr, target)
         .with_context(|| format!("send REQ_META to {}", target))?;
    
     println!("[CLIENT] Starting chunk upload: {} chunks to {}", total_chunks, target);


     // Send REQ_CHUNKs: [u8 ty=REQ_CHUNK][u32 req_id][u32 seq][bytes...]
    let mut offset = 0usize;
    let mut seq_idx = 0u32;
    let send_start = Instant::now();
   
    println!("[CLIENT] Sending {} chunks with 1ms pacing...", total_chunks);
  
    while offset < image_bytes.len() {
        let take = (image_bytes.len() - offset).min(chunk_payload);
        let mut pkt = Vec::with_capacity(1 + 4 + 4 + take);
        pkt.push(REQ_CHUNK);
        pkt.extend(req_id.to_le_bytes());
        pkt.extend(seq_idx.to_le_bytes());
        pkt.extend(&image_bytes[offset..offset + take]);
        sock.send_to(&pkt, target)
            .with_context(|| format!("send REQ_CHUNK seq={} to {}", seq_idx, target))?;
      
        // Progress logging every 1000 chunks
        if seq_idx % 1000 == 0 {
            println!("[CLIENT] Sent {}/{} chunks ({:.1}%)",
                     seq_idx, total_chunks,
                     (seq_idx as f64 * 100.0) / total_chunks as f64);
        }
      
        offset += take;
        seq_idx += 1;
       
        // *** FIX: Sleep 1ms after EVERY packet (no conditional!) ***
        thread::sleep(Duration::from_millis(1));
    }
  
    let send_duration = send_start.elapsed();
    println!("[CLIENT] All {} chunks sent in {:.2}s (paced)", total_chunks, send_duration.as_secs_f64());




     // ---- Phase C: receive RESP_META + RESP_CHUNK(s) from target ----
     println!("[CLIENT] Waiting for response from {}", target);
     match recv_response(&sock, target, req_id) {
         Ok((bytes_in, out_len)) => {
             let ms = started.elapsed().as_millis();
             println!(
                 "OK user={} req_id={} op={} to={} latency_ms={} in_bytes={} out_bytes={}",
                 sender_id,
                 req_id,
                 cli.op.as_str(),
                 target,
                 ms,
                 bytes_in,
                 out_len
             );
         }
         Err(err) => {
             eprintln!(
                 "TIMEOUT_OR_ERR user={} req_id={} to={} err={}",
                 sender_id, req_id, target, err
             );
         }
     }
 }


 Ok(())
}


/// Wait for the first ACCEPT for this req_id within the selection window.
/// ACCEPT layout: [u8 ty=ACCEPT][u32 req_id]
fn wait_first_accept(sock: &UdpSocket, req_id: u32, window: Duration) -> Result<Option<SocketAddr>> {
 let deadline = Instant::now() + window;
 let mut buf = [0u8; 256];


 // Temporarily shrink the read timeout to not exceed the selection window
 let original = sock.read_timeout()?;
 sock.set_read_timeout(Some(Duration::from_millis(50)))?;


 while Instant::now() < deadline {
     match sock.recv_from(&mut buf) {
         Ok((n, from)) => {
             if n >= 1 + 4 && buf[0] == ACCEPT {
                 let r = u32::from_le_bytes(buf[1..5].try_into().unwrap());
                 if r == req_id {
                     // restore original timeout before returning
                     sock.set_read_timeout(original)?;
                     return Ok(Some(from));
                 }
             }
             // Ignore anything else during selection
         }
         Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock || e.kind() == std::io::ErrorKind::TimedOut => {
             // small poll; continue until deadline
         }
         Err(e) => {
             // restore and propagate
             sock.set_read_timeout(original)?;
             return Err(e.into());
         }
     }
 }
 sock.set_read_timeout(original)?;
 Ok(None)
}


/// Receive RESP_META then RESP_CHUNKs for this req_id from the chosen target.
/// RESP_META: [u8 ty=RESP_META][u32 req_id][u32 total_chunks][u32 out_len]
/// RESP_CHUNK: [u8 ty=RESP_CHUNK][u32 req_id][u32 seq][bytes...]
fn recv_response(sock: &UdpSocket, target: SocketAddr, expect_req_id: u32) -> Result<(usize, usize)> {
 let mut buf = [0u8; 65536];
 let mut bytes_in = 0usize;
 let mut total_chunks: Option<u32> = None;
 let mut got_chunks: u32 = 0;
 let mut out_len: usize = 0;


 // First, wait RESP_META from target
 loop {
     let (n, from) = sock.recv_from(&mut buf)?;
     if from != target {
         continue; // ignore other sources
     }
     bytes_in += n;
     if n >= 1 + 4 + 4 + 4 && buf[0] == RESP_META {
         let rid = u32::from_le_bytes(buf[1..5].try_into().unwrap());
         if rid != expect_req_id {
             continue;
         }
         let t_chunks = u32::from_le_bytes(buf[5..9].try_into().unwrap());
         let o_len = u32::from_le_bytes(buf[9..13].try_into().unwrap());
         total_chunks = Some(t_chunks);
         out_len = o_len as usize;
         println!("[CLIENT] RESP_META received: expecting {} chunks, {} bytes", t_chunks, o_len);
         if t_chunks == 0 {
             return Ok((bytes_in, out_len));
         }
         break;
     }
     // else ignore
 }


 // Then, gather RESP_CHUNKs until we reach total_chunks
 let need = total_chunks.unwrap();
 while got_chunks < need {
     let (n, from) = sock.recv_from(&mut buf)?;
     if from != target {
         continue;
     }
     bytes_in += n;
     if n >= 1 + 4 + 4 && buf[0] == RESP_CHUNK {
         let rid = u32::from_le_bytes(buf[1..5].try_into().unwrap());
         if rid != expect_req_id {
             continue;
         }
         // seq = u32::from_le_bytes(buf[5..9].try_into().unwrap());
         // If you want to reassemble, place payload by seq offset here.
         got_chunks += 1;
        
         // Progress every 1000 chunks
         if got_chunks % 1000 == 0 {
             println!("[CLIENT] Received {}/{} response chunks ({:.1}%)",
                      got_chunks, need,
                      (got_chunks as f64 * 100.0) / need as f64);
         }
     }
 }
 println!("[CLIENT] All {} response chunks received", need);
 Ok((bytes_in, out_len))
}


/// Parse peers "ip:port,ip:port"
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


/// Parse allow "alice:3,bob:2"
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


/// Request ID uniqueness: pack (proc_idx, seq) into u32: [16 bits proc][16 bits seq]
fn make_req_id(proc_idx: u16, seq: u16) -> u32 {
 ((proc_idx as u32) << 16) | (seq as u32)
}


fn now_unix_ms() -> u128 {
 SystemTime::now()
     .duration_since(UNIX_EPOCH)
     .unwrap()
     .as_millis()
}










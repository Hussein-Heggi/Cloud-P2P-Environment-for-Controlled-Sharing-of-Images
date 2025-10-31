use anyhow::{Context, Result};
use clap::Parser;
use rand::{rngs::StdRng, Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use std::net::UdpSocket;
use std::path::PathBuf;
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};
use std::thread;
use std::time::{Duration, Instant};

/// Simple UDP stress client that multicasts image requests to all servers.
#[derive(Parser, Debug)]
struct Args {
    /// Comma-separated list of server addresses IP:PORT (e.g. 10.40.61.79:8080,10.40.58.169:8081,10.40.50.93:8083)
    #[arg(short = 's', long)]
    servers: String,

    /// Path to the image to blast (FHD recommended, e.g. JPEG/PNG)
    #[arg(short = 'i', long)]
    image: PathBuf,

    /// Total number of requests to send
    #[arg(short = 'n', long, default_value_t = 10_000)]
    total: u32,

    /// Number of worker threads
    #[arg(short = 'c', long, default_value_t = 64)]
    concurrency: usize,

    /// UDP payload chunk size in bytes (<= 60_000)
    #[arg(short = 'k', long, default_value_t = 12_000)]
    chunk_size: usize,

    /// Request type string (e.g., "Encrypt", "Discover", "PeerFetch", "Stress")
    #[arg(short = 't', long, default_value = "Stress")]
    request_type: String,

    /// Optional per-request random sleep in microseconds (jitter)
    #[arg(long, default_value_t = 0)]
    jitter_us: u64,
}

#[derive(Serialize, Deserialize, Debug)]
struct RequestHeader {
    version: u16,          // 1
    request_type: String,  // user-chosen
    req_id: u64,           // unique per request
    image_len: u64,        // bytes
    chunk_size: u32,       // bytes per chunk
    total_chunks: u32,     // ceil(image_len / chunk_size)
}

#[derive(Serialize, Deserialize, Debug)]
struct ChunkFrame<'a> {
    version: u16,   // 1
    req_id: u64,
    seq_no: u32,    // 0..total_chunks-1
    total_chunks: u32,
    payload: &'a [u8], // raw bytes for this slice
}

fn main() -> Result<()> {
    let args = Args::parse();

    // Parse servers
    let servers: Vec<String> = args
        .servers
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    if servers.is_empty() {
        anyhow::bail!("No server addresses provided via --servers");
    }

    // Load image once into memory
    let img = std::fs::read(&args.image)
        .with_context(|| format!("Failed reading image {:?}", args.image))?;
    if img.is_empty() {
        anyhow::bail!("Image file is empty");
    }

    // Compute chunking
    let chunk_sz = args.chunk_size.min(60_000).max(1024); // keep under UDP limits
    let total_chunks = ((img.len() + chunk_sz - 1) / chunk_sz) as u32;

    // Pre-build header bytes (JSON) template sans req_id (we’ll fill dynamic fields per send)
    let version: u16 = 1;

    // Shared stuff
    let img_arc = Arc::new(img);
    let servers_arc = Arc::new(servers);
    let sent_reqs = Arc::new(AtomicU64::new(0));
    let total = args.total;
    let concurrency = args.concurrency.max(1);
    let request_type = Arc::new(args.request_type);
    let jitter_us = args.jitter_us;

    // Bind an ephemeral UDP socket per worker (faster on Linux)
    let start = Instant::now();

    // Work distributor: atomic counter
    let next_id = Arc::new(AtomicU64::new(0));

    crossbeam_utils::thread::scope(|scope| {
        for _ in 0..concurrency {
            let img = Arc::clone(&img_arc);
            let servers = Arc::clone(&servers_arc);
            let sent_reqs = Arc::clone(&sent_reqs);
            let request_type = Arc::clone(&request_type);
            let next_id = Arc::clone(&next_id);

            scope.spawn(move |_| {
                // Each worker gets its own socket
                let sock = UdpSocket::bind("0.0.0.0:0").expect("bind UDP");
                sock.set_nonblocking(false).ok();

                // Jitter RNG (per-thread)
                let mut rng = StdRng::from_entropy();

                loop {
                    let idx = next_id.fetch_add(1, Ordering::Relaxed);
                    if idx as u32 >= total {
                        break;
                    }

                    let req_id = 1_000_000_000_000 + idx; // unique-ish

                    // Build per-request header
                    let hdr = RequestHeader {
                        version,
                        request_type: request_type.clone(),
                        req_id,
                        image_len: img.len() as u64,
                        chunk_size: chunk_sz as u32,
                        total_chunks,
                    };
                    let hdr_bytes = serde_json::to_vec(&hdr).expect("serialize header");

                    // Send to all servers (multicast fan-out)
                    for target in servers.iter() {
                        // 1) header
                        let _ = sock.send_to(&hdr_bytes, target);

                        // 2) chunks
                        let mut offset = 0usize;
                        for seq in 0..total_chunks {
                            let end = (offset + chunk_sz).min(img.len());
                            let frame = ChunkFrame {
                                version,
                                req_id,
                                seq_no: seq,
                                total_chunks,
                                payload: &img[offset..end],
                            };
                            // We serialize with a small wrapper object to keep the server-side JSON decoding simple
                            // (payload is base64 within JSON). If you want raw bytes, switch to bincode on both sides.
                            let frame_bytes = serde_json::to_vec(&frame).expect("serialize chunk");
                            let _ = sock.send_to(&frame_bytes, target);
                            offset = end;
                        }
                    }

                    sent_reqs.fetch_add(1, Ordering::Relaxed);

                    if jitter_us > 0 {
                        let j = rng.gen_range(0..=jitter_us) as u64;
                        std::thread::sleep(Duration::from_micros(j));
                    }
                }
            });
        }
    }).unwrap();

    let elapsed = start.elapsed();
    let total_sent = sent_reqs.load(Ordering::Relaxed);
    let rps = (total_sent as f64) / elapsed.as_secs_f64();

    println!(
        "Done. Sent {} requests (header + {} chunks each) to {} servers in {:.2?} ≈ {:.0} req/s",
        total_sent,
        total_chunks,
        servers_arc.len(),
        elapsed,
        rps
    );

    Ok(())
}

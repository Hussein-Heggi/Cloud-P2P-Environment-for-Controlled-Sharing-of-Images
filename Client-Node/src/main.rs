use std::env;
use std::fs;
use std::io;
use std::net::UdpSocket;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Clone)]
struct Config {
    servers: Vec<String>,
    image_path: PathBuf,
    total: u32,
    concurrency: usize,
    chunk_size: usize,
    request_type: String,
    mime: String,
    jitter_us: u64,
    throttle_us: u64,
}

fn usage_and_exit() -> ! {
    eprintln!(
        "Usage:
  client --servers IP:PORT[,IP:PORT...] --image PATH
         [--total N] [--concurrency N] [--chunk-size BYTES]
         [--request-type STR] [--mime STR] [--jitter-us N] [--throttle-us N]

Example:
  client --servers 10.0.0.10:8080,10.0.0.11:8080 \\
         --image ./fhd.jpg --total 10000 --concurrency 64 \\
         --chunk-size 1200 --request-type Stress --mime image/jpeg \\
         --throttle-us 100"
    );
    std::process::exit(2);
}

fn get_flag(args: &[String], name: &str) -> Option<String> {
    for i in 0..args.len() {
        if args[i] == name {
            if i + 1 < args.len() {
                return Some(args[i + 1].clone());
            } else {
                return None;
            }
        } else if let Some(eq) = args[i].strip_prefix(&(name.to_string() + "=")) {
            return Some(eq.to_string());
        }
    }
    None
}

fn parse_args() -> io::Result<Config> {
    let args: Vec<String> = env::args().collect();

    if args.len() == 1 || args.iter().any(|a| a == "-h" || a == "--help") {
        usage_and_exit();
    }

    let servers_str = get_flag(&args, "--servers").unwrap_or_else(|| {
        eprintln!("Missing required --servers");
        usage_and_exit();
    });
    let servers: Vec<String> = servers_str
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if servers.is_empty() {
        eprintln!("--servers parsed to empty list");
        std::process::exit(2);
    }

    let image_str = get_flag(&args, "--image").unwrap_or_else(|| {
        eprintln!("Missing required --image");
        usage_and_exit();
    });

    let total = get_flag(&args, "--total")
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(10_000);

    let concurrency = get_flag(&args, "--concurrency")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(64)
        .max(1);

    let chunk_size = get_flag(&args, "--chunk-size")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(1200);

    let request_type = get_flag(&args, "--request-type").unwrap_or_else(|| "Stress".to_string());

    let mime = get_flag(&args, "--mime").unwrap_or_else(|| "image/jpeg".to_string());

    let jitter_us = get_flag(&args, "--jitter-us")
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);

    let throttle_us = get_flag(&args, "--throttle-us")
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(500);

    Ok(Config {
        servers,
        image_path: PathBuf::from(image_str),
        total,
        concurrency,
        chunk_size,
        request_type,
        mime,
        jitter_us,
        throttle_us,
    })
}

struct Lcg {
    state: u64,
}
impl Lcg {
    fn new(seed: u64) -> Self {
        Self { state: seed | 1 }
    }
    fn next(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1);
        self.state
    }
}

fn now_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_nanos()
}

fn main() -> io::Result<()> {
    let cfg = parse_args()?;

    let img = fs::read(&cfg.image_path)?;
    if img.is_empty() {
        eprintln!("Image file is empty: {:?}", cfg.image_path);
        std::process::exit(2);
    }

    let chunk_size = cfg.chunk_size.clamp(512, 60_000);
    let total_chunks = ((img.len() + chunk_size - 1) / chunk_size) as u32;

    let img_arc: Arc<Vec<u8>> = Arc::new(img);
    let servers_arc: Arc<Vec<String>> = Arc::new(cfg.servers.clone());
    let req_type_arc: Arc<String> = Arc::new(cfg.request_type.clone());
    let mime_arc: Arc<String> = Arc::new(cfg.mime.clone());
    let total = cfg.total;
    let concurrency = cfg.concurrency;
    let jitter_us = cfg.jitter_us;
    let throttle_us = cfg.throttle_us;

    let sent_reqs = Arc::new(AtomicU64::new(0));
    let next_id = Arc::new(AtomicU64::new(0));

    let start = Instant::now();
    let base_req_id = (now_nanos() & 0xFFFF_FFFF_FFFF_FFFF) as u64;

    println!("Starting consistent stream mode:");
    println!("  Throttle: {} µs per packet", throttle_us);
    println!("  Total requests: {}", total);
    println!("  Concurrency: {}", concurrency);
    println!("  Chunk size: {} bytes", chunk_size);
    println!("  Chunks per request: {}", total_chunks);
    println!("  Estimated duration: ~{:.1}s\n", 
        (total as f64 * (total_chunks as f64 + 1.0) * throttle_us as f64) / (1_000_000.0 * concurrency as f64));

    thread::scope(|scope| {
        for t in 0..concurrency {
            let img = Arc::clone(&img_arc);
            let servers = Arc::clone(&servers_arc);
            let req_type = Arc::clone(&req_type_arc);
            let mime = Arc::clone(&mime_arc);
            let sent_reqs = Arc::clone(&sent_reqs);
            let next_id = Arc::clone(&next_id);

            scope.spawn(move || {
                let sock = UdpSocket::bind("0.0.0.0:0").expect("bind UDP");
                let mut rng = Lcg::new(base_req_id ^ (t as u64) ^ 0x9E37_79B9);

                loop {
                    let idx = next_id.fetch_add(1, Ordering::Relaxed);
                    if idx as u32 >= total {
                        break;
                    }

                    let req_id = base_req_id.wrapping_add(idx);

                    let header = format!(
                        "{{\"version\":1,\
                          \"kind\":\"client_header\",\
                          \"request_type\":\"{rt}\",\
                          \"req_id\":{rid},\
                          \"mime\":\"{mime}\",\
                          \"image_len\":{len},\
                          \"chunk_size\":{cs},\
                          \"total_chunks\":{tc}}}",
                        rt = *req_type,
                        rid = req_id,
                        mime = *mime,
                        len = img.len(),
                        cs = chunk_size,
                        tc = total_chunks
                    );
                    let header_bytes = header.as_bytes();

                    for target in servers.iter() {
                        let _ = sock.send_to(header_bytes, target);
                        thread::sleep(Duration::from_micros(throttle_us));

                        let mut offset = 0usize;
                        for seq in 0..total_chunks {
                            let end = (offset + chunk_size).min(img.len());
                            let payload = &img[offset..end];

                            let mut frame = Vec::with_capacity(2 + 8 + 4 + 4 + payload.len());
                            frame.extend_from_slice(&1u16.to_be_bytes());
                            frame.extend_from_slice(&req_id.to_be_bytes());
                            frame.extend_from_slice(&seq.to_be_bytes());
                            frame.extend_from_slice(&total_chunks.to_be_bytes());
                            frame.extend_from_slice(payload);

                            let _ = sock.send_to(&frame, target);
                            thread::sleep(Duration::from_micros(throttle_us));
                            offset = end;
                        }
                    }

                    sent_reqs.fetch_add(1, Ordering::Relaxed);

                    if jitter_us > 0 {
                        let j = (rng.next() % (jitter_us + 1)) as u64;
                        thread::sleep(Duration::from_micros(j));
                    }
                }
            });
        }
    });

    let elapsed = start.elapsed();
    let total_sent = sent_reqs.load(Ordering::Relaxed);
    let servers_n = servers_arc.len();
    let datagrams_per_request = 1u64 + total_chunks as u64;
    let total_datagrams = total_sent * datagrams_per_request * servers_n as u64;

    println!(
        "Done. Sent {reqs} requests (header + {chunks} chunks) to {srv} servers in {dur:.2?}.
Datagrams total ≈ {dgrams}. Throughput ≈ {rps:.0} req/s.",
        reqs = total_sent,
        chunks = total_chunks,
        srv = servers_n,
        dur = elapsed,
        dgrams = total_datagrams,
        rps = (total_sent as f64) / elapsed.as_secs_f64()
    );

    Ok(())
}
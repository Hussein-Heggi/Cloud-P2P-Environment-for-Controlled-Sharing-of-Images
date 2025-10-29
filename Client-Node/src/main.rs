use anyhow::Result;
use clap::{Parser, Subcommand};
use std::{fs, net::{SocketAddr, UdpSocket}, process::Command, time::Duration};

#[derive(Parser, Debug, Clone)]
#[command(name = "client")]
struct Cli {
    /// Comma-separated peers (ip:port) — must match servers
    // #[arg(long, default_value = "10.40.61.79:8080,10.40.58.169:8081,10.40.50.93:8083")]
    #[arg(long, default_value = "10.40.58.169:8181")]
    peers: String,

    /// Number of OS processes to spawn
    #[arg(long, default_value_t = 1)]
    processes: usize,

    /// Requests per worker process (synchronous)
    #[arg(long, default_value_t = 3)]
    requests_per_process: usize,

    /// Path to image (same image across all processes)
    #[arg(long)]
    image: String,

    /// Owner field for stego meta
    #[arg(long, default_value = "owner")]
    owner: String,

    /// Allow list like "alice:3,bob:2"
    #[arg(long, default_value = "alice:3,bob:2")]
    allow: String,

    /// Sender ID (client logical id)
    #[arg(long, default_value_t = 1001)]
    sender_id: u32,

    #[command(subcommand)]
    cmd: Option<Cmd>,
}

#[derive(Subcommand, Debug, Clone)]
enum Cmd {
    Worker {
        #[arg(long)] peers: String,
        #[arg(long)] requests: usize,
        #[arg(long)] image: String,
        #[arg(long)] owner: String,
        #[arg(long)] allow: String,
        #[arg(long)] sender_id: u32,
        #[arg(long)] idx: usize,
    }
}

#[derive(serde::Serialize)]
struct Meta { owner: String, allow: Vec<AllowEnt> }
#[derive(serde::Serialize)]
struct AllowEnt { user: String, remaining_views: u32 }

fn parse_allow(s: &str) -> Vec<AllowEnt> {
    s.split(',').filter_map(|x| {
        let (u,v) = x.split_once(':')?;
        Some(AllowEnt{ user: u.to_string(), remaining_views: v.parse().ok()? })
    }).collect()
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match &cli.cmd {
        Some(Cmd::Worker{ peers, requests, image, owner, allow, sender_id, idx }) => {
            worker(peers, *requests, image, owner, allow, *sender_id, *idx)
        }
        None => {
            for i in 0..cli.processes {
                let mut child = Command::new(std::env::current_exe()?)
                    .args([
                        "worker",
                        "--peers", &cli.peers,
                        "--requests", &cli.requests_per_process.to_string(),
                        "--image", &cli.image,
                        "--owner", &cli.owner,
                        "--allow", &cli.allow,
                        "--sender-id", &cli.sender_id.to_string(),
                        "--idx", &i.to_string(),
                    ])
                    .spawn()?;
                let _ = child.wait();
            }
            Ok(())
        }
    }
}

fn worker(peers: &str, requests: usize, image: &str, owner: &str, allow: &str, sender_id: u32, idx: usize) -> Result<()> {
    let peers: Vec<SocketAddr> = peers.split(',').map(|s| s.parse().unwrap()).collect();
    let img = fs::read(image)?;
    let meta_json = serde_json::to_vec(&Meta { owner: owner.to_string(), allow: parse_allow(allow) })?;

    let sock = UdpSocket::bind("0.0.0.0:0")?;
    sock.set_read_timeout(Some(Duration::from_secs(5)))?;

    for r in 0..requests {
        let req_id = make_req_id(idx as u32, r as u32);

        let mut all = Vec::with_capacity(meta_json.len() + img.len());
        all.extend_from_slice(&meta_json);
        all.extend_from_slice(&img);

        let chunk_payload = 1200 - (1 + 4 + 4);
        let total_chunks = ((all.len() + chunk_payload - 1) / chunk_payload) as u32;

        // REQ_BCAST header
        let mut hdr = Vec::with_capacity(1+4+4+4+4+4);
        hdr.push(0u8);
        hdr.extend(req_id.to_le_bytes());
        hdr.extend(sender_id.to_le_bytes());
        hdr.extend(total_chunks.to_le_bytes());
        hdr.extend((meta_json.len() as u32).to_le_bytes());
        hdr.extend((img.len() as u32).to_le_bytes());

        for p in &peers { let _ = sock.send_to(&hdr, p); }

        // CHUNKs
        for i in 0..total_chunks {
            let start = (i as usize)*chunk_payload;
            let end   = ((i as usize + 1)*chunk_payload).min(all.len());
            let mut pkt = Vec::with_capacity(1+4+4 + (end-start));
            pkt.push(1u8);
            pkt.extend(req_id.to_le_bytes());
            pkt.extend(i.to_le_bytes());
            pkt.extend_from_slice(&all[start..end]);
            for p in &peers { let _ = sock.send_to(&pkt, p); }
        }

        // Wait for RESP_META/RESP_CHUNKs
        use std::collections::HashMap as Map;
        let (mut expected_chunks, mut got_chunks, mut out_buf) = (0u32, 0u32, Map::<u32, Vec<u8>>::new());
        loop {
            let mut buf = [0u8; 2048];
            match sock.recv_from(&mut buf) {
                Ok((n, _from)) if n > 0 => {
                    match buf[0] {
                        6 => { // RESP_META
                            let rid = u32::from_le_bytes(buf[1..5].try_into().unwrap());
                            if rid != req_id { continue; }
                            expected_chunks = u32::from_le_bytes(buf[5..9].try_into().unwrap());
                        }
                        7 => { // RESP_CHUNK
                            let rid = u32::from_le_bytes(buf[1..5].try_into().unwrap());
                            if rid != req_id { continue; }
                            let seq = u32::from_le_bytes(buf[5..9].try_into().unwrap());
                            out_buf.insert(seq, buf[9..n].to_vec());
                            got_chunks += 1;
                            if expected_chunks > 0 && got_chunks == expected_chunks {
                                let mut all = Vec::new();
                                for i in 0..expected_chunks {
                                    if let Some(c) = out_buf.remove(&i) { all.extend_from_slice(&c); }
                                }
                                println!("[proc#{idx}] request {}/{} OK ({} bytes)", r+1, requests, all.len());
                                break;
                            }
                        }
                        _ => {}
                    }
                }
                _ => {}
            }
        }
    }
    Ok(())
}

fn make_req_id(proc_idx: u32, seq: u32) -> u32 {
    (proc_idx << 16) | (seq & 0xFFFF)
}

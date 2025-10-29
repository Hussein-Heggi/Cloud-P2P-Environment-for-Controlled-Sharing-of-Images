//! UDP stego service with request/processing logs.
//! Wire (LE):
//!   REQ_META  (type=0): [u8][u32 req_id][u32 total_chunks][u32 img_bytes][u32 meta_bytes]
//!   REQ_CHUNK (type=1): [u8][u32 req_id][u32 seq][bytes...]
//!   RESP_META (type=2): [u8][u32 req_id][u32 total_chunks][u32 out_bytes]
//!   RESP_CHUNK(type=3): [u8][u32 req_id][u32 seq][bytes...]

use std::{collections::HashMap, net::SocketAddr};
use tokio::net::UdpSocket;
use tracing::{info, warn, debug, error};

use crate::{state::SharedState, config::Config, stego_service};

const MAX_DGRAM: usize = 1200;
const HDR_META: usize  = 1 + 4 + 4 + 4 + 4;
const HDR_CHUNK: usize = 1 + 4 + 4;

#[derive(Default)]
struct Reassembly {
    from: Option<SocketAddr>,
    total_chunks: u32,
    got: Vec<bool>,
    buf: Vec<Vec<u8>>,
    img_len: usize,
    meta_len: usize,
}

pub async fn run_udp_server(state: SharedState, cfg: Config) -> anyhow::Result<()> {
    // Bind on the **service** port
    let addr = cfg.udp_bind_addr();
    let sock = UdpSocket::bind(addr).await?;
    info!(%addr, "UDP listening (service)");

    let mut reqs: HashMap<u32, Reassembly> = HashMap::new();
    let mut buf = vec![0u8; 64 * 1024];

    loop {
        let (n, peer) = sock.recv_from(&mut buf).await?;
        // Simulated failure: act as if down (no logs, no traffic)
        if state.read().await.ignoring { continue; }
        if n == 0 { continue; }

        match buf[0] {
            // --------------------- REQ_META ---------------------
            0 => {
                if n < HDR_META { continue; }
                let req_id      = u32::from_le_bytes(buf[1..5].try_into().unwrap());
                let total_chunks= u32::from_le_bytes(buf[5..9].try_into().unwrap());
                let img_bytes   = u32::from_le_bytes(buf[9..13].try_into().unwrap()) as usize;
                let meta_bytes  = u32::from_le_bytes(buf[13..17].try_into().unwrap()) as usize;

                if !state.read().await.is_leader {
                    // Not leader → send lightweight redirect & log once
                    debug!(%peer, req_id, "Follower received REQ_META; sending REDIRECT");
                    let mut out = Vec::with_capacity(1 + 4 + 4 + 4 + 8);
                    out.push(2u8);
                    out.extend(req_id.to_le_bytes());
                    out.extend(0u32.to_le_bytes()); // total_chunks=0 => redirect marker
                    out.extend(0u32.to_le_bytes());
                    out.extend_from_slice(b"REDIRECT");
                    let _ = sock.send_to(&out, peer).await;
                    continue;
                }

                // Leader: create/replace reassembly state and LOG the request
                let mut r = Reassembly::default();
                r.from          = Some(peer);
                r.total_chunks  = total_chunks;
                r.got           = vec![false; total_chunks as usize];
                r.buf           = vec![Vec::new(); total_chunks as usize];
                r.img_len       = img_bytes;
                r.meta_len      = meta_bytes;

                reqs.insert(req_id, r);

                // 🔔 LOG: request accepted by leader
                info!(
                    %peer, req_id, total_chunks, img_bytes, meta_bytes,
                    "REQ_META received (request accepted by leader)"
                );
            }

            // --------------------- REQ_CHUNK --------------------
            1 => {
                if n < HDR_CHUNK { continue; }
                let req_id = u32::from_le_bytes(buf[1..5].try_into().unwrap());
                let seq    = u32::from_le_bytes(buf[5..9].try_into().unwrap()) as usize;

                if let Some(r) = reqs.get_mut(&req_id) {
                    if seq < r.buf.len() {
                        r.buf[seq].clear();
                        r.buf[seq].extend_from_slice(&buf[9..n]);
                        r.got[seq] = true;
                    }

                    // If all chunks arrived → assemble & process
                    if r.got.iter().all(|&b| b) {
                        let to = r.from.unwrap_or(peer);

                        // 🔔 LOG: all chunks received; start processing
                        info!(
                            %to, req_id, total_chunks = r.total_chunks,
                            "All chunks received; starting stego processing"
                        );

                        // Reassemble in-order: meta first, then image
                        let mut all = Vec::with_capacity(r.meta_len + r.img_len);
                        for chunk in &r.buf { all.extend_from_slice(chunk); }
                        let meta_json = &all[..r.meta_len];
                        let img_bytes = &all[r.meta_len..(r.meta_len + r.img_len)];

                        match stego_service::embed_meta_return_png(img_bytes, meta_json) {
                            Ok(out_png) => {
                                let out_chunks = ((out_png.len() + (MAX_DGRAM - (1+4+4)) - 1)
                                    / (MAX_DGRAM - (1+4+4))) as u32;

                                // 🔔 LOG: processing completed; sending response
                                info!(
                                    %to, req_id, out_bytes = out_png.len(), out_chunks,
                                    "Processing completed; sending RESP_META and chunks"
                                );

                                // RESP_META
                                let mut meta = Vec::with_capacity(1 + 4 + 4 + 4);
                                meta.push(2u8);
                                meta.extend(req_id.to_le_bytes());
                                meta.extend(out_chunks.to_le_bytes());
                                meta.extend((out_png.len() as u32).to_le_bytes());
                                let _ = sock.send_to(&meta, to).await;

                                // RESP_CHUNKs
                                for (i, chunk) in out_png.chunks(MAX_DGRAM - (1+4+4)).enumerate() {
                                    let mut pkt = Vec::with_capacity(1 + 4 + 4 + chunk.len());
                                    pkt.push(3u8);
                                    pkt.extend(req_id.to_le_bytes());
                                    pkt.extend((i as u32).to_le_bytes());
                                    pkt.extend_from_slice(chunk);
                                    let _ = sock.send_to(&pkt, to).await;
                                }
                            }
                            Err(e) => {
                                error!(%to, req_id, error=?e, "Stego processing failed");
                                // Minimal error meta (client may retry)
                                let mut meta = Vec::with_capacity(1 + 4 + 4 + 4);
                                meta.push(2u8);
                                meta.extend(req_id.to_le_bytes());
                                meta.extend(0u32.to_le_bytes());
                                meta.extend(0u32.to_le_bytes());
                                let _ = sock.send_to(&meta, to).await;
                            }
                        }

                        // Done with this request
                        reqs.remove(&req_id);
                    }
                } else {
                    // Late chunk with no REQ_META context
                    debug!(%peer, req_id, seq, "Chunk without context; ignoring");
                }
            }

            // --------------------- Unknown ----------------------
            other => {
                debug!(%peer, ty = other, "Unknown UDP packet type");
            }
        }
    }
}

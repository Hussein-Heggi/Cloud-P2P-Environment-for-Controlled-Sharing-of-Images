use std::{collections::HashMap, net::SocketAddr, time::{Duration, Instant}};
use tokio::net::UdpSocket;
use tracing::{info, error, debug};

use crate::{state::{SharedState, Row}, config::Config, stego_service};

const MAX_DGRAM: usize = 1200;

#[derive(Default)]
struct Reassembly {
    sender: Option<SocketAddr>,
    total_chunks: u32,
    meta_len: usize,
    img_len: usize,
    chunks: HashMap<u32, Vec<u8>>,
}

pub async fn run_udp_server(state: SharedState, cfg: Config) -> anyhow::Result<()> {
    let addr = cfg.udp_bind_addr();
    let sock = UdpSocket::bind(addr).await?;
    info!("Service UDP listening on {}", addr);

    let peers = Config::peer_addrs();
    let start = Instant::now();

    let mut buf = vec![0u8; 64 * 1024];
    let mut inflight: HashMap<u32, Reassembly> = HashMap::new();

    loop {
        // lease expiry / reassignment (leader only)
        if state.read().await.is_leader {
            let expired: Vec<u32> = {
                let s = state.read().await;
                let now_ms = start.elapsed().as_millis();
                s.history.rows.values()
                    .filter(|r| !r.completed && r.assigned.is_some()
                        && r.lease_deadline_ms.map(|d| d <= now_ms).unwrap_or(false))
                    .map(|r| r.req_id)
                    .collect()
            };
            for req_id in expired {
                let new_assignee = {
                    let mut s = state.write().await;
                    let idx = s.history.rr_next % peers.len();
                    s.history.rr_next = (s.history.rr_next + 1) % peers.len();
                    (idx as u32) + 1
                };
                assign_and_broadcast(&sock, &state, &peers, &start, req_id, new_assignee, 30_000).await;
            }
        }

        let (n, peer) = sock.recv_from(&mut buf).await?;
        if n == 0 { continue; }
        if state.read().await.ignoring { continue; }

        match buf[0] {
            0 => { // REQ_BCAST
                if n < 1 + 4 + 4 + 4 + 4 + 4 { continue; }
                let req_id       = u32::from_le_bytes(buf[1..5].try_into().unwrap());
                let sender_id    = u32::from_le_bytes(buf[5..9].try_into().unwrap());
                let total_chunks = u32::from_le_bytes(buf[9..13].try_into().unwrap());
                let meta_len     = u32::from_le_bytes(buf[13..17].try_into().unwrap()) as usize;
                let img_len      = u32::from_le_bytes(buf[17..21].try_into().unwrap()) as usize;

                {
                    let mut s = state.write().await;
                    s.history.rows.entry(req_id).or_insert(Row {
                        req_id, sender_id, assigned: None, completed: false,
                        lease_deadline_ms: None, version: 1
                    });
                }

                inflight.entry(req_id).or_insert_with(|| Reassembly {
                    sender: Some(peer),
                    total_chunks, meta_len, img_len,
                    chunks: HashMap::new(),
                });

                if state.read().await.is_leader {
                    let need_assign = {
                        let s = state.read().await;
                        s.history.rows.get(&req_id).map(|r| r.assigned.is_none() && !r.completed).unwrap_or(false)
                    };
                    if need_assign {
                        let new_assignee = {
                            let mut s = state.write().await;
                            let idx = s.history.rr_next % peers.len();
                            s.history.rr_next = (s.history.rr_next + 1) % peers.len();
                            (idx as u32) + 1
                        };
                        assign_and_broadcast(&sock, &state, &peers, &start, req_id, new_assignee, 30_000).await;
                    }
                }
            }
            1 => { // REQ_CHUNK
                if n < 1 + 4 + 4 { continue; }
                let req_id = u32::from_le_bytes(buf[1..5].try_into().unwrap());
                let seq    = u32::from_le_bytes(buf[5..9].try_into().unwrap());
                let chunk  = &buf[9..n];
                let ent = inflight.entry(req_id).or_default();
                ent.sender.get_or_insert(peer);
                ent.chunks.insert(seq, chunk.to_vec());

                // If I'm assigned worker and I have all chunks → process
                let am_worker = {
                    let s = state.read().await;
                    if let Some(row) = s.history.rows.get(&req_id) {
                        row.assigned == Some(s.node_id) && !row.completed
                    } else { false }
                };

                if am_worker && ent.chunks.len() as u32 == ent.total_chunks {
                    let mut all = Vec::with_capacity(ent.meta_len + ent.img_len);
                    for i in 0..ent.total_chunks {
                        if let Some(c) = ent.chunks.get(&i) { all.extend_from_slice(c); }
                        else { continue; }
                    }
                    let meta_json = &all[..ent.meta_len];
                    let img_bytes = &all[ent.meta_len..ent.meta_len + ent.img_len];

                    match stego_service::embed_meta_return_png(img_bytes, meta_json) {
                        Ok(out_png) => {
                            {
                                let mut s = state.write().await;
                                if let Some(r) = s.history.rows.get_mut(&req_id) {
                                    r.completed = true;
                                    r.version = r.version.saturating_add(1);
                                    r.lease_deadline_ms = None;
                                }
                            }
                            broadcast_done(&sock, &peers, req_id, state.read().await.node_id).await;

                            // Send back to client
                            send_png_to_client(&sock, ent.sender.unwrap_or(peer), req_id, &out_png).await;

                            inflight.remove(&req_id);
                        }
                        Err(e) => {
                            error!("stego failed: {:?}", e);
                        }
                    }
                }
            }
            2 => { // ASSIGN (from leader)
                if n < 1 + 4 + 4 + 8 { continue; }
                let req_id   = u32::from_le_bytes(buf[1..5].try_into().unwrap());
                let assigned = u32::from_le_bytes(buf[5..9].try_into().unwrap());
                let lease_ms = u64::from_le_bytes(buf[9..17].try_into().unwrap());
                let now_ms = start.elapsed().as_millis();
                let deadline = now_ms.saturating_add(lease_ms as u128);

                let mut s = state.write().await;
                let row = s.history.rows.entry(req_id).or_insert(Row{
                    req_id, sender_id: 0, assigned: None, completed: false,
                    lease_deadline_ms: None, version: 1
                });
                row.assigned = Some(assigned);
                row.completed = false;
                row.lease_deadline_ms = Some(deadline);
                row.version = row.version.saturating_add(1);
            }
            3 => { // DONE (from worker)
                if n < 1 + 4 + 4 { continue; }
                let req_id  = u32::from_le_bytes(buf[1..5].try_into().unwrap());
                let worker  = u32::from_le_bytes(buf[5..9].try_into().unwrap());
                let mut s = state.write().await;
                if let Some(r) = s.history.rows.get_mut(&req_id) {
                    r.completed = true;
                    r.version = r.version.saturating_add(1);
                    r.lease_deadline_ms = None;
                    r.assigned = Some(worker);
                }
            }
            _ => { debug!("unknown opcode {}", buf[0]); }
        }
    }
}

async fn assign_and_broadcast(
    sock: &UdpSocket,
    state: &SharedState,
    peers: &[SocketAddr],
    start: &Instant,
    req_id: u32,
    assigned: u32,
    lease_ms: u64,
) {
    {
        let mut s = state.write().await;
        if let Some(r) = s.history.rows.get_mut(&req_id) {
            r.assigned = Some(assigned);
            r.completed = false;
            r.version = r.version.saturating_add(1);
            r.lease_deadline_ms = Some(start.elapsed().as_millis().saturating_add(lease_ms as u128));
        }
    }
    let mut pkt = Vec::with_capacity(1+4+4+8);
    pkt.push(2u8);
    pkt.extend(req_id.to_le_bytes());
    pkt.extend(assigned.to_le_bytes());
    pkt.extend(lease_ms.to_le_bytes());
    for p in peers { let _ = sock.send_to(&pkt, p).await; }
}

async fn broadcast_done(sock: &UdpSocket, peers: &[SocketAddr], req_id: u32, worker_id: u32) {
    let mut pkt = Vec::with_capacity(1+4+4);
    pkt.push(3u8);
    pkt.extend(req_id.to_le_bytes());
    pkt.extend(worker_id.to_le_bytes());
    for p in peers { let _ = sock.send_to(&pkt, p).await; }
}

async fn send_png_to_client(sock: &UdpSocket, client: SocketAddr, req_id: u32, png: &[u8]) {
    // meta header
    let chunk_payload = MAX_DGRAM - (1 + 4 + 4);
    let total_chunks = ((png.len() + chunk_payload - 1) / chunk_payload) as u32;

    let mut meta = Vec::with_capacity(1+4+4+4);
    meta.push(6u8); // RESP_META
    meta.extend(req_id.to_le_bytes());
    meta.extend(total_chunks.to_le_bytes());
    meta.extend((png.len() as u32).to_le_bytes());
    let _ = sock.send_to(&meta, client).await;

    // chunks
    for (i, ch) in png.chunks(chunk_payload).enumerate() {
        let mut pkt = Vec::with_capacity(1+4+4 + ch.len());
        pkt.push(7u8); // RESP_CHUNK
        pkt.extend(req_id.to_le_bytes());
        pkt.extend((i as u32).to_le_bytes());
        pkt.extend_from_slice(ch);
        let _ = sock.send_to(&pkt, client).await;
    }
}

use crate::{
    config::Config,
    state::{Row, SharedState},
    stego_service,
};
use std::{collections::HashMap, net::SocketAddr, time::Instant};
use tokio::net::UdpSocket;
use tracing::{debug, error, info};

const MAX_DGRAM: usize = 1200;
// opcodes:
// 0 = REQ_BCAST header
// 1 = REQ_CHUNK
// 2 = RESP_META
// 3 = RESP_CHUNK
// 4 = ASSIGN (leader->all)
// 5 = DONE   (worker->all)

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

    let peers = Config::service_peer_addrs();
    let start = Instant::now();

    let mut buf = vec![0u8; 64 * 1024];
    let mut inflight: HashMap<u32, Reassembly> = HashMap::new();

    loop {
        // If I'm "down", do not receive/respond
        if state.read().await.ignoring {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            continue;
        }

        let (n, peer) = sock.recv_from(&mut buf).await?;
        if n == 0 { continue; }
        if state.read().await.ignoring { continue; }

        match buf[0] {
            0 => {
                // REQ_BCAST header: [0][req_id][sender_id][total_chunks][meta_len][img_len]
                if n < 1 + 4 + 4 + 4 + 4 + 4 { continue; }
                let req_id = u32::from_le_bytes(buf[1..5].try_into().unwrap());
                let sender_id = u32::from_le_bytes(buf[5..9].try_into().unwrap());
                let total_chunks = u32::from_le_bytes(buf[9..13].try_into().unwrap());
                let meta_len = u32::from_le_bytes(buf[13..17].try_into().unwrap()) as usize;
                let img_len = u32::from_le_bytes(buf[17..21].try_into().unwrap()) as usize;

                {
                    let mut s = state.write().await;
                    s.history.rows.entry(req_id).or_insert(Row {
                        req_id, sender_id,
                        assigned: None, completed: false,
                        lease_deadline_ms: None, version: 1,
                    });
                }

                inflight.entry(req_id).or_insert_with(|| Reassembly {
                    sender: Some(peer),
                    total_chunks,
                    meta_len,
                    img_len,
                    chunks: HashMap::new(),
                });

                // Leader assigns (round-robin) — but only among live peers
                if state.read().await.is_leader {
                    let need_assign = {
                        let s = state.read().await;
                        s.history.rows.get(&req_id)
                            .map(|r| r.assigned.is_none() && !r.completed)
                            .unwrap_or(false)
                    };
                    if need_assign {
                        // Snapshot of live peers
                        let (mut next_idx, live) = {
                            let s = state.read().await;
                            (s.history.rr_next, s.live_peers.clone())
                        };

                        // fallback: self only
                        let candidates = if live.is_empty() {
                            vec![state.read().await.node_id]
                        } else { live };

                        let pick = candidates[next_idx % candidates.len()];
                        next_idx = (next_idx + 1) % candidates.len();

                        {
                            let mut s = state.write().await;
                            s.history.rr_next = next_idx;
                        }

                        assign_and_broadcast(&sock, &state, &peers, &start, req_id, pick, 30_000).await;
                    }
                }
            }
            1 => {
                // REQ_CHUNK: [1][req_id][seq][bytes...]
                if n < 1 + 4 + 4 { continue; }
                let req_id = u32::from_le_bytes(buf[1..5].try_into().unwrap());
                let seq = u32::from_le_bytes(buf[5..9].try_into().unwrap());
                let payload = &buf[9..n];

                let ent = inflight.entry(req_id).or_default();
                ent.sender.get_or_insert(peer);
                ent.chunks.insert(seq, payload.to_vec());

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
                        if let Some(c) = ent.chunks.get(&i) {
                            all.extend_from_slice(c);
                        } else { continue; }
                    }
                    let meta_json = &all[..ent.meta_len];
                    let img_bytes = &all[ent.meta_len..ent.meta_len + ent.img_len];

                    match stego_service::embed_meta_return_png(img_bytes, meta_json) {
                        Ok(out_png) => {
                            if state.read().await.ignoring { continue; } // if I fail mid-send, drop
                            send_png_to_client(&sock, ent.sender.unwrap_or(peer), req_id, &out_png).await;

                            // Mark done + notify peers
                            {
                                let mut s = state.write().await;
                                if let Some(r) = s.history.rows.get_mut(&req_id) {
                                    r.completed = true;
                                    r.version = r.version.saturating_add(1);
                                    r.lease_deadline_ms = None;
                                }
                            }
                            broadcast_done(&sock, &peers, req_id, state.read().await.node_id).await;

                            inflight.remove(&req_id);
                        }
                        Err(e) => {
                            error!("stego failed: {:?}", e);
                        }
                    }
                }
            }
            4 => {
                // ASSIGN: [4][req_id][assigned][lease_ms(u64)]
                if n < 1 + 4 + 4 + 8 { continue; }
                let req_id = u32::from_le_bytes(buf[1..5].try_into().unwrap());
                let assigned = u32::from_le_bytes(buf[5..9].try_into().unwrap());
                let lease_ms = u64::from_le_bytes(buf[9..17].try_into().unwrap());
                let now_ms = start.elapsed().as_millis();
                let deadline = now_ms.saturating_add(lease_ms as u128);

                let mut s = state.write().await;
                let row = s.history.rows.entry(req_id).or_insert(Row {
                    req_id, sender_id: 0,
                    assigned: None, completed: false,
                    lease_deadline_ms: None, version: 1,
                });
                row.assigned = Some(assigned);
                row.completed = false;
                row.lease_deadline_ms = Some(deadline);
                row.version = row.version.saturating_add(1);
            }
            5 => {
                // DONE: [5][req_id][worker_id]
                if n < 1 + 4 + 4 { continue; }
                let req_id = u32::from_le_bytes(buf[1..5].try_into().unwrap());
                let worker = u32::from_le_bytes(buf[5..9].try_into().unwrap());
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

async fn send_png_to_client(sock: &UdpSocket, to: SocketAddr, req_id: u32, out_png: &[u8]) {
    let chunk_payload = MAX_DGRAM - (1 + 4 + 4);
    let total_chunks = ((out_png.len() + chunk_payload - 1) / chunk_payload) as u32;

    let mut meta = Vec::with_capacity(1 + 4 + 4 + 4);
    meta.push(2u8); // RESP_META
    meta.extend(req_id.to_le_bytes());
    meta.extend(total_chunks.to_le_bytes());
    meta.extend((out_png.len() as u32).to_le_bytes());
    let _ = sock.send_to(&meta, to).await;

    for (i, ch) in out_png.chunks(chunk_payload).enumerate() {
        let mut pkt = Vec::with_capacity(1 + 4 + 4 + ch.len());
        pkt.push(3u8); // RESP_CHUNK
        pkt.extend(req_id.to_le_bytes());
        pkt.extend((i as u32).to_le_bytes());
        pkt.extend_from_slice(ch);
        let _ = sock.send_to(&pkt, to).await;
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
    let mut pkt = Vec::with_capacity(1 + 4 + 4 + 8);
    pkt.push(4u8); // ASSIGN
    pkt.extend(req_id.to_le_bytes());
    pkt.extend(assigned.to_le_bytes());
    pkt.extend(lease_ms.to_le_bytes());
    for p in peers {
        let _ = sock.send_to(&pkt, p).await;
    }
}

async fn broadcast_done(sock: &UdpSocket, peers: &[SocketAddr], req_id: u32, worker_id: u32) {
    let mut pkt = Vec::with_capacity(1 + 4 + 4);
    pkt.push(5u8); // DONE
    pkt.extend(req_id.to_le_bytes());
    pkt.extend(worker_id.to_le_bytes());
    for p in peers {
        let _ = sock.send_to(&pkt, p).await;
    }
}

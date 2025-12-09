use crate::{config::Config, epoch, state::{SharedState, LoadInfo}};
use std::{
    net::{IpAddr, Ipv4Addr},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use sysinfo::System;
use tokio::net::UdpSocket;
use tokio::time::sleep;
use tracing::{debug, info, warn};

/// ASSIGN (type=6): [u8 ty=6][u32 leader_id][u32 lease_ms][u32 ipv4_be]
const ASSIGN: u8 = 6;

/// LOAD_REPORT (type=7): [u8 ty=7][u32 server_id][f32 load_score]
const LOAD_REPORT: u8 = 7;

#[inline]
fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}

pub async fn run_assignment_channels(
    state: SharedState,
    cfg: Config,
    sys: Arc<tokio::sync::Mutex<System>>,
) -> anyhow::Result<()> {
    // Bind our assignment listener
    let bind_addr = cfg
        .assignment_bind_addr()
        .expect("assignment_bind not configured");
    let sock = Arc::new(UdpSocket::bind(bind_addr).await?);
    println!("Assignment port bound: {}", bind_addr);
    info!(%bind_addr, "Assignment port bound");

    // Receiver (handles both ASSIGN and LOAD_REPORT)
    {
        let st = state.clone();
        let sock_rx = sock.clone();
        let cfg_rx = cfg.clone();
        tokio::spawn(async move { recv_assign_loop(st, sock_rx, cfg_rx).await });
    }

    // Load broadcaster (all servers) - TIER 1 CRITICAL
    {
        let st = state.clone();
        let sock_load = sock.clone();
        let cfg_load = cfg.clone();
        let sys_load = sys.clone();
        tokio::spawn(async move {
            broadcast_load_loop(st, sock_load, cfg_load, sys_load).await
        });
    }

    // Periodic ASSIGN broadcaster (leader only) - TIER 1 CRITICAL
    {
        let st = state.clone();
        let sock_tx = sock.clone();
        let cfg_assign = cfg.clone();
        tokio::spawn(async move { broadcast_assign_loop(st, sock_tx, cfg_assign).await });
    }

    // FORWARD_REQUEST receiver (handles forwarded requests with large data)
    {
        let st = state.clone();
        let sock_fwd = sock.clone();
        let cfg_fwd = cfg.clone();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 65536]; // Large buffer for forward requests
            loop {
                match sock_fwd.recv_from(&mut buf).await {
                    Ok((n, _from)) => {
                        if n >= 1 && buf[0] == crate::history::FORWARD_REQUEST {
                            crate::history::handle_forward_request(
                                st.clone(),
                                sock_fwd.clone(),
                                cfg_fwd.clone(),
                                &buf[..n],
                            ).await;
                        }
                    }
                    Err(_) => {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
            }
        });
    }

    Ok(())
}

async fn recv_assign_loop(state: SharedState, sock: Arc<UdpSocket>, cfg: Config) {
    let mut buf = [0u8; 256];
    let mut last_log = Instant::now() - Duration::from_secs(12);

    loop {
        if state.read().await.ignoring {
            sleep(Duration::from_millis(50)).await;
            continue;
        }

        match sock.recv_from(&mut buf).await {
            Ok((n, from)) => {
                if n < 1 {
                    continue;
                }

                match buf[0] {
                    ASSIGN => {
                        if n < 1 + 4 + 4 + 4 {
                            continue;
                        }
                        let leader_id = u32::from_le_bytes(buf[1..5].try_into().unwrap());
                        let lease_ms = u32::from_le_bytes(buf[5..9].try_into().unwrap());
                        let ipv4_be = u32::from_be_bytes(buf[9..13].try_into().unwrap());
                        let ip = IpAddr::V4(Ipv4Addr::from(ipv4_be));
                        let deadline_ms = now_ms() + lease_ms as u128;

                        // CRITICAL: Detect executor failover (check executor_ip, NOT leader!)
                        // If I WAS the current executor but NOT anymore, close all client connections
                        let (was_executor, is_new_executor) = {
                            let s = state.read().await;
                            let my_ip = cfg.service_bind_addr().map(|a| a.ip());

                            let was_exec = if let (Some(my_ip), Some(old_exec_ip)) = (my_ip, &s.executor_ip) {
                                my_ip == *old_exec_ip
                            } else {
                                false
                            };

                            let is_new_exec = if let Some(my_ip) = my_ip {
                                my_ip == ip
                            } else {
                                false
                            };

                            (was_exec, is_new_exec)
                        };

                        // Update executor info
                        {
                            let mut s = state.write().await;
                            s.executor_ip = Some(ip);
                            s.executor_lease_deadline_ms = Some(deadline_ms);
                        }

                        // If I was the current system executor but NOT anymore, close all client connections
                        // Clients will use REQUEST_EXECUTOR broadcast to find new executor
                        if was_executor && !is_new_executor {
                            println!("[EXECUTOR-FAILOVER] No longer the current system executor - closing all client connections");
                            info!("Executor failover detected - closing client connections");

                            // Close all client TCP connections
                            let connections_to_close = {
                                let mut s = state.write().await;
                                let connections = s.client_connections.clone();
                                s.client_connections.clear(); // Clear the map
                                connections
                            };

                            println!("[EXECUTOR-FAILOVER] Closing {} client connections", connections_to_close.len());

                            for (username, stream_mutex) in connections_to_close {
                                use tokio::io::AsyncWriteExt;
                                let mut stream = stream_mutex.lock().await;
                                let _ = stream.shutdown().await; // Gracefully close TCP connection
                                println!("[EXECUTOR-FAILOVER] Closed connection to {}", username);
                            }

                            println!("[EXECUTOR-FAILOVER] All client connections closed. Clients will rediscover new executor via REQUEST_EXECUTOR");
                        }

                        // Log at most once every ~12 seconds
                        if last_log.elapsed() >= Duration::from_secs(12) {
                            println!(
                                "ASSIGN received from {} | leader_id={} executor_ip={} lease_ms={}",
                                from, leader_id, ip, lease_ms
                            );
                            debug!(%from, leader_id, ?ip, lease_ms, "ASSIGN received");
                            last_log = Instant::now();
                        }
                    }
                    LOAD_REPORT => {
                        if n < 1 + 4 + 4 {
                            continue;
                        }
                        let server_id = u32::from_le_bytes(buf[1..5].try_into().unwrap());
                        let load_score = f32::from_le_bytes(buf[5..9].try_into().unwrap());

                        {
                            let mut s = state.write().await;
                            s.load_reports.insert(
                                server_id,
                                LoadInfo {
                                    server_id,
                                    load_score,
                                    timestamp_ms: now_ms(),
                                },
                            );
                        }

                        debug!(server_id, load_score, "LOAD_REPORT received");
                    }
                    crate::history::HISTORY_UPDATE => {
                        crate::history::handle_history_update(state.clone(), &buf[..n]).await;
                    }
                    crate::history::TABLE_SYNC => {
                        // Only leader processes TABLE_SYNC
                        let is_leader = { state.read().await.is_leader };
                        if is_leader {
                            crate::history::handle_table_sync(state.clone(), &buf[..n]).await;
                        }
                    }
                    crate::history::TABLE_UPDATE => {
                        crate::history::handle_table_update(state.clone(), &buf[..n]).await;
                    }
                    _ => {
                        // Unknown message type
                    }
                }
            }
            Err(e) => {
                eprintln!("assignment recv error: {}", e);
                warn!(error=%e, "assignment recv error");
                sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

/// Calculate this server's current load score (CPU + RAM only)
fn calculate_own_load(sys: &System) -> f32 {
    // CPU utilization (0.0-1.0)
    let cpu_util = sys.global_cpu_info().cpu_usage() / 100.0;

    // RAM utilization (0.0-1.0)
    let ram_util = sys.used_memory() as f32 / sys.total_memory() as f32;

    // Weighted load score: 75% CPU + 25% RAM
    let load = Config::LOAD_CPU_WEIGHT * cpu_util + Config::LOAD_RAM_WEIGHT * ram_util;

    load.clamp(0.0, 1.0)
}

/// TIER 1 CRITICAL: Epoch-aligned load broadcasting
/// All servers broadcast their load at synchronized intervals
async fn broadcast_load_loop(
    state: SharedState,
    sock: Arc<UdpSocket>,
    cfg: Config,
    sys: Arc<tokio::sync::Mutex<System>>,
) {
    let mut last_log = Instant::now() - Duration::from_secs(12);

    loop {
        // EPOCH-ALIGNED SLEEP (Tier 1: 5ms precision)
        epoch::sleep_until_next_aligned_tick_t1(cfg.load_broadcast_every_ms, "load_broadcast").await;

        // Don't broadcast if in failure mode
        let (ignoring, node_id) = {
            let s = state.read().await;
            (s.ignoring, s.node_id)
        };

        if ignoring {
            continue;
        }

        // Update system stats and calculate load
        let load_score = {
            let mut sys_lock = sys.lock().await;
            sys_lock.refresh_cpu();
            sys_lock.refresh_memory();
            calculate_own_load(&sys_lock)
        };

        // Build LOAD_REPORT: [7][server_id][load_score]
        let mut pkt = Vec::with_capacity(1 + 4 + 4);
        pkt.push(LOAD_REPORT);
        pkt.extend(node_id.to_le_bytes());
        pkt.extend(load_score.to_le_bytes());

        // Broadcast to all assignment peers (including self for new leader bootstrap)
        for peer in cfg.assignment_peer_addrs() {
            let _ = sock.send_to(&pkt, peer).await;
        }

        // Log at most once every ~12 seconds
        if last_log.elapsed() >= Duration::from_secs(12) {
            let epoch_offset = epoch::epoch_offset_ms();
            println!(
                "[LOAD] node_id={} load={:.3} epoch_offset={}ms",
                node_id, load_score, epoch_offset
            );
            debug!(node_id, load_score, epoch_offset, "LOAD broadcast sent (epoch-aligned)");
            last_log = Instant::now();
        }
    }
}

/// Select best executor from available load reports
fn select_best_executor(
    state: &crate::state::ServerState,
    cfg: &Config,
) -> Option<u32> {
    let now = now_ms();
    let mut candidates: Vec<(u32, f32)> = Vec::new();

    // Collect valid (non-stale) load reports
    for (server_id, load_info) in &state.load_reports {
        let age_ms = now.saturating_sub(load_info.timestamp_ms);
        
        // Skip stale data
        if age_ms > Config::LOAD_STALE_TIMEOUT_MS {
            continue;
        }

        // Skip self if ignoring
        if *server_id == state.node_id && state.ignoring {
            continue;
        }

        candidates.push((*server_id, load_info.load_score));
    }

    if candidates.is_empty() {
        // No valid load data - fallback to node 1
        return Some(1);
    }

    // Sort by load (ascending)
    candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

    let best = candidates[0];

    // Apply hysteresis: only switch if new executor is significantly better
    if let Some(current_exec_id) = state.current_executor_id {
        // Find current executor in candidates
        if let Some(current) = candidates.iter().find(|(id, _)| *id == current_exec_id) {
            let improvement = current.1 - best.1;
            if improvement < cfg.executor_switch_threshold {
                // Not enough improvement, keep current
                return Some(current_exec_id);
            }
        }
        // Current executor not in valid candidates (failed/stale), must switch
    }

    Some(best.0)
}

/// TIER 1 CRITICAL: Epoch-aligned assignment broadcasting
/// Leader broadcasts executor assignments at synchronized intervals
async fn broadcast_assign_loop(state: SharedState, sock: Arc<UdpSocket>, cfg: Config) {
    let mut last_log = Instant::now() - Duration::from_secs(12);

    loop {
        // EPOCH-ALIGNED SLEEP (Tier 1: 5ms precision)
        epoch::sleep_until_next_aligned_tick_t1(cfg.assign_broadcast_every_ms, "assign_broadcast").await;

        // Only broadcast if I'm leader
        let is_leader = { state.read().await.is_leader };
        if !is_leader || state.read().await.ignoring {
            continue;
        }

        // Select best executor based on load
        let (executor_id, leader_id) = {
            let mut s = state.write().await;
            
            // Prune stale load reports
            let now = now_ms();
            s.load_reports.retain(|_, info| {
                now.saturating_sub(info.timestamp_ms) <= Config::LOAD_STALE_TIMEOUT_MS
            });

            let executor_id = select_best_executor(&s, &cfg).unwrap_or(1);
            s.current_executor_id = Some(executor_id);
            
            (executor_id, s.node_id)
        };

        // Get executor IP
        let exec_ip = match cfg.node_id_to_ip(executor_id) {
            Some(ip) => ip,
            None => {
                eprintln!("Failed to map executor_id {} to IP", executor_id);
                warn!(executor_id, "Failed to map executor_id to IP");
                continue;
            }
        };

        let lease_ms = cfg.assign_lease_ms;

        let ipv4_be_bytes = match exec_ip {
            IpAddr::V4(v4) => u32::from(v4).to_be_bytes(),
            _ => {
                eprintln!("ASSIGN currently supports IPv4 only");
                warn!("ASSIGN currently supports IPv4 only");
                continue;
            }
        };

        // Build ASSIGN: [6][leader_id][lease_ms][ipv4_be]
        let mut pkt = Vec::with_capacity(1 + 4 + 4 + 4);
        pkt.push(ASSIGN);
        pkt.extend(leader_id.to_le_bytes());
        pkt.extend(lease_ms.to_le_bytes());
        pkt.extend(ipv4_be_bytes);

        // Send to all assignment peers
        for peer in cfg.assignment_peer_addrs() {
            let _ = sock.send_to(&pkt, peer).await;
        }

        // Keep my own state fresh locally
        {
            let mut s = state.write().await;
            s.executor_ip = Some(exec_ip);
            s.executor_lease_deadline_ms = Some(now_ms() + lease_ms as u128);
        }

        // Log with load information at most once every ~12 seconds
        if last_log.elapsed() >= Duration::from_secs(12) {
            let load_info: Vec<String> = {
                let s = state.read().await;
                let mut info: Vec<(u32, f32)> = s.load_reports
                    .iter()
                    .map(|(id, load)| (*id, load.load_score))
                    .collect();
                info.sort_by_key(|(id, _)| *id);
                info.iter()
                    .map(|(id, load)| {
                        if *id == executor_id {
                            format!("node_{}={:.3}*", id, load)
                        } else {
                            format!("node_{}={:.3}", id, load)
                        }
                    })
                    .collect()
            };

            let epoch_offset = epoch::epoch_offset_ms();
            println!(
                "[ASSIGN] executor=node_{} lease={}ms epoch_offset={}ms | loads: {}",
                executor_id,
                lease_ms,
                epoch_offset,
                load_info.join(" ")
            );
            info!(
                leader_id,
                executor_id,
                ?exec_ip,
                lease_ms,
                epoch_offset,
                "ASSIGN broadcast sent with load-based selection (epoch-aligned)"
            );
            last_log = Instant::now();
        }
    }
}




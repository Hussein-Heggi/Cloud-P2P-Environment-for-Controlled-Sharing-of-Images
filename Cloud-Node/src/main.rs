use clap::Parser;
use std::sync::Arc;
use sysinfo::System;
use tokio::sync::{watch, RwLock};
use tokio::time::Duration;
use tracing::info;

mod assignment;
mod config;
mod election;
mod epoch;
mod failure;
mod history;
mod state;
mod stego_service;
mod udp;

use crate::state::ServerState;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Structured logs + println! side-by-side
    tracing_subscriber::fmt()
        .with_target(true)
        .with_level(true)
        .with_ansi(true)
        .compact()
        .init();

    // Parse config
    let cfg = config::Config::parse();
    cfg.validate();

    // Initialize global epoch FIRST - all servers will align to nearest second boundary
    epoch::init_epoch();
    println!("[MAIN] Epoch initialized, starting server node_id={}", cfg.node_id);

    // Shared state
    let state: state::SharedState = Arc::new(RwLock::new(ServerState::new(cfg.node_id)));

    // System monitor for CPU/RAM tracking
    let sys = Arc::new(tokio::sync::Mutex::new(System::new_all()));

    let (leader_tx, leader_rx) = watch::channel::<u32>(0);

    // Election
    {
        let st = state.clone();
        let cfg2 = cfg.clone();
        let tx = leader_tx.clone();
        tokio::spawn(async move {
            election::run_election_loop(st, cfg2, tx).await;
        });
    }

    // Failure simulation (stays independent per your requirements)
    {
        let st = state.clone();
        let cfg2 = cfg.clone();
        tokio::spawn(async move {
            failure::run_failure_simulation(st, cfg2).await;
        });
    }

    // Client-facing UDP server
    {
        let st = state.clone();
        let cfg2 = cfg.clone();
        tokio::spawn(async move {
            let _ = udp::run_udp_server(st, cfg2).await;
        });
    }

    // Track leader changes
    {
        let st = state.clone();
        tokio::spawn(async move {
            election::handle_leader_changes(st, leader_rx).await;
        });
    }

    // Assignment channels (with load balancing) - TIER 1 CRITICAL
    {
        let st = state.clone();
        let cfg2 = cfg.clone();
        let sys2 = sys.clone();
        tokio::spawn(async move {
            let _ = assignment::run_assignment_channels(st, cfg2, sys2).await;
        });
    }

    // System stats refresher (ensures CPU averages are accurate) - TIER 1
    {
        let sys_refresh = sys.clone();
        tokio::spawn(async move {
            loop {
                // EPOCH-ALIGNED SLEEP (Tier 1: 5ms precision)
                epoch::sleep_until_next_aligned_tick_t1(500, "sys_stats_refresh").await;
                
                let mut s = sys_refresh.lock().await;
                s.refresh_cpu();
                s.refresh_memory();
            }
        });
    }

    // History table cleanup (every 5 seconds) - TIER 2
    {
        let st = state.clone();
        let cfg2 = cfg.clone();
        tokio::spawn(async move {
            history::run_cleanup_task(st, cfg2).await;
        });
    }

    // History table sync to leader (every 12 seconds, non-leader nodes) - TIER 2
    {
        let st = state.clone();
        let cfg2 = cfg.clone();
        tokio::spawn(async move {
            // Create a temporary socket for sending sync messages
            if let Ok(temp_sock) = tokio::net::UdpSocket::bind("0.0.0.0:0").await {
                history::run_sync_to_leader_task(st, Arc::new(temp_sock), cfg2).await;
            }
        });
    }

    // History table sync by leader (every 15 seconds, leader only) - TIER 2
    {
        let st = state.clone();
        let cfg2 = cfg.clone();
        tokio::spawn(async move {
            // Create a temporary socket for broadcasting
            if let Ok(temp_sock) = tokio::net::UdpSocket::bind("0.0.0.0:0").await {
                history::run_leader_sync_task(st, Arc::new(temp_sock), cfg2).await;
            }
        });
    }

    // Periodic metrics printer (every 10s) - TIER 2
    {
        let st = state.clone();
        tokio::spawn(async move {
            loop {
                // EPOCH-ALIGNED SLEEP (Tier 2: 100ms precision)
                epoch::sleep_until_next_aligned_tick_t2(10000, "metrics_printer").await;
                
                let (rec, srv) = {
                    let s = st.read().await;
                    (s.requests_received, s.requests_served)
                };
                let epoch_offset = epoch::epoch_offset_ms();
                println!(
                    "[METRICS] requests_received={} requests_served={} epoch_offset={}ms",
                    rec, srv, epoch_offset
                );
                info!(
                    requests_received = rec,
                    requests_served = srv,
                    epoch_offset,
                    "metrics (epoch-aligned)"
                );
            }
        });
    }

    // Periodic history table printer (every 10s) - TIER 2
    {
        let st = state.clone();
        tokio::spawn(async move {
            loop {
                // EPOCH-ALIGNED SLEEP (Tier 2: 100ms precision)
                epoch::sleep_until_next_aligned_tick_t2(10000, "history_printer").await;
                
                let s = st.read().await;
                let epoch_offset = epoch::epoch_offset_ms();
                
                println!("[HISTORY] Table has {} entries (epoch_offset={}ms):", s.history.len(), epoch_offset);
                if s.history.is_empty() {
                    println!("  (empty)");
                } else {
                    let mut entries: Vec<_> = s.history.iter().collect();
                    entries.sort_by_key(|(req_id, _)| *req_id);
                    
                    for (req_id, record) in entries {
                        let path_display = record.path_to_output_image
                            .as_ref()
                            .map(|p| p.as_str())
                            .unwrap_or("(no local path)");
                        println!(
                            "  req_id={} executor={} path={} timestamp={}",
                            req_id, record.executor_node, path_display, record.timestamp
                        );
                    }
                }
            }
        });
    }

    println!("Server up. Press Ctrl-C to exit.");
    tokio::signal::ctrl_c().await?;
    Ok(())
}



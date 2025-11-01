use clap::Parser;
use std::sync::Arc;
use sysinfo::System;
use tokio::sync::{watch, RwLock};
use tokio::time::{interval, Duration};
use tracing::info;

mod assignment;
mod config;
mod election;
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

    // Failure simulation
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

    // Assignment channels (with load balancing)
    {
        let st = state.clone();
        let cfg2 = cfg.clone();
        let sys2 = sys.clone();
        tokio::spawn(async move {
            let _ = assignment::run_assignment_channels(st, cfg2, sys2).await;
        });
    }

    // System stats refresher (ensures CPU averages are accurate)
    {
        let sys_refresh = sys.clone();
        tokio::spawn(async move {
            let mut tick = interval(Duration::from_millis(500));
            loop {
                tick.tick().await;
                let mut s = sys_refresh.lock().await;
                s.refresh_cpu();
                s.refresh_memory();
            }
        });
    }

    // History table cleanup (every 5 seconds)
    {
        let st = state.clone();
        let cfg2 = cfg.clone();
        tokio::spawn(async move {
            history::run_cleanup_task(st, cfg2).await;
        });
    }

    // History table sync to leader (every 12 seconds, non-leader nodes)
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

    // History table sync by leader (every 15 seconds, leader only)
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

    // Periodic metrics printer (every 10s)
    {
        let st = state.clone();
        tokio::spawn(async move {
            let mut tick = interval(Duration::from_secs(10));
            loop {
                tick.tick().await;
                let (rec, srv) = {
                    let s = st.read().await;
                    (s.requests_received, s.requests_served)
                };
                println!(
                    "[METRICS] requests_received={} requests_served={}",
                    rec, srv
                );
                info!(
                    requests_received = rec,
                    requests_served = srv,
                    "metrics"
                );
            }
        });
    }

    println!("Server up. Press Ctrl-C to exit.");
    tokio::signal::ctrl_c().await?;
    Ok(())
}
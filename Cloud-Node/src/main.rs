use clap::Parser;
use std::sync::Arc;
use tokio::sync::{watch, RwLock};

mod config;
mod state;
mod election;
mod udp;
mod failure;
mod stego_service;
mod assignment;

use crate::state::ServerState;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Enable structured logs alongside println! output
    tracing_subscriber::fmt()
        .with_target(true)
        .with_level(true)
        .with_ansi(true)
        .compact() // nice single-line format with timestamps
        .init();

    // Parse config
    let cfg = config::Config::parse();
    cfg.validate();

    // Shared state
    let state: state::SharedState = Arc::new(RwLock::new(ServerState::new(cfg.node_id)));

    let (leader_tx, leader_rx) = watch::channel::<u32>(0);

    // Election (UNCHANGED logic)
    {
        let st = state.clone();
        let cfg2 = cfg.clone();
        let tx = leader_tx.clone();
        tokio::spawn(async move { election::run_election_loop(st, cfg2, tx).await; });
    }

    // Failure simulation
    {
        let st = state.clone();
        let cfg2 = cfg.clone();
        tokio::spawn(async move { failure::run_failure_simulation(st, cfg2).await; });
    }

    // Client-facing UDP server
    {
        let st = state.clone();
        let cfg2 = cfg.clone();
        tokio::spawn(async move { let _ = udp::run_udp_server(st, cfg2).await; });
    }

    // Track leader changes
    {
        let st = state.clone();
        tokio::spawn(async move { election::handle_leader_changes(st, leader_rx).await; });
    }

    // Assignment channels (leader broadcast + all receive)
    {
        let st = state.clone();
        let cfg2 = cfg.clone();
        tokio::spawn(async move { let _ = assignment::run_assignment_channels(st, cfg2).await; });
    }

    println!("Server up. Press Ctrl-C to exit.");
    tokio::signal::ctrl_c().await?;
    Ok(())
}

use clap::Parser;
use std::sync::Arc;
use tokio::sync::{watch, RwLock};
use tracing::info;

mod config;
mod state;
mod election;
mod udp;
mod failure;
mod stego_service;

use crate::state::ServerState;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let cfg = config::Config::parse();
    cfg.validate();
    info!(?cfg, "server config");

    let state = Arc::new(RwLock::new(ServerState::new(cfg.node_id)));

    // Leader change notifier
    let (leader_tx, leader_rx) = watch::channel::<u32>(0);

    // Election
    {
        let st = state.clone();
        let cfg2 = cfg.clone();
        let tx = leader_tx.clone();
        tokio::spawn(async move { election::run_election_loop(st, cfg2, tx).await; });
    }

    // Failure simulation (toggle "ignoring" on/off)
    {
        let st = state.clone();
        let cfg2 = cfg.clone();
        tokio::spawn(async move { failure::run_failure_simulation(st, cfg2).await; });
    }

    // UDP service
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

    tokio::signal::ctrl_c().await?;
    Ok(())
}

use std::sync::Arc;
use tokio::sync::{RwLock, watch};
use tracing::info;

mod config;
mod state;
mod election;
mod udp;
mod failure;
mod reconcile;
mod stego_service;

use crate::state::ServerState;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let cfg = config::Config::parse();
    cfg.validate();
    info!(?cfg, "server config");

    let state = Arc::new(RwLock::new(ServerState::new(cfg.node_id)));
    let (leader_tx, leader_rx) = watch::channel::<u32>(0);

    {
        let st = state.clone();
        let cfg2 = cfg.clone();
        tokio::spawn(async move { election::run_election_loop(st, cfg2, leader_tx).await; });
    }

    {
        let st = state.clone();
        let cfg2 = cfg.clone();
        tokio::spawn(async move { failure::run_failure_simulation(st, cfg2).await; });
    }

    {
        let st = state.clone();
        let cfg2 = cfg.clone();
        tokio::spawn(async move { let _ = udp::run_udp_server(st, cfg2).await; });
    }

    {
        let st = state.clone();
        tokio::spawn(async move { election::handle_leader_changes(st, leader_rx).await; });
    }

    futures::future::pending::<()>().await
}

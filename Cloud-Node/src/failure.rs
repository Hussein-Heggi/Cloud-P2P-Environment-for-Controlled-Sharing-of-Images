use crate::{config::Config, state::SharedState};
use std::time::Duration;
use tokio::time::sleep;
use tracing::warn;

/// Simulates OS-like failure: while "down" the node is completely silent.
pub async fn run_failure_simulation(state: SharedState, cfg: Config) {
    loop {
        sleep(Duration::from_secs(cfg.fail_every_secs)).await;

        {
            let mut s = state.write().await;
            s.ignoring = true;
        }
        warn!("Simulating FAILURE: node is DOWN (silent)");

        sleep(Duration::from_secs(cfg.fail_duration_secs)).await;

        {
            let mut s = state.write().await;
            s.ignoring = false;
        }
        warn!("Node is UP again");
    }
}

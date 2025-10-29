use std::time::Duration;
use tokio::time::sleep;
use tracing::warn;

use crate::{state::SharedState, config::Config, election};

pub async fn run_failure_simulation(state: SharedState, cfg: Config) {
    loop {
        sleep(Duration::from_secs(cfg.fail_every_secs)).await;
        {
            let mut s = state.write().await;
            s.ignoring = true;
            warn!("Simulating failure: ignoring traffic");
        }
        sleep(Duration::from_secs(cfg.fail_duration_secs)).await;
        {
            let mut s = state.write().await;
            s.ignoring = false;
            warn!("Revived from simulated failure");
        }
        election::reconcile_state_after_revive().await;
    }
}

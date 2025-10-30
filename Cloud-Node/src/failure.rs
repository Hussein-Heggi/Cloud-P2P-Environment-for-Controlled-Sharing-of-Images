use std::time::Duration;
use crate::{config::Config, state::SharedState};

/// Re-enabled failure injection:
/// - Every `fail_every_secs`, mark the node as "ignoring" (drops external work)
///   for `fail_duration_secs`, then restore.
/// - Set both flags > 0 to activate; keep them 0 if you don't want failures yet.
pub async fn run_failure_simulation(state: SharedState, cfg: Config) {
    // Run forever; if misconfigured, just sleep to avoid busy loop.
    if cfg.fail_every_secs == 0 || cfg.fail_duration_secs == 0 {
        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    }

    loop {
        // Wait until the next failure window
        tokio::time::sleep(Duration::from_secs(cfg.fail_every_secs)).await;

        {
            let mut s = state.write().await;
            if !s.ignoring {
                s.ignoring = true;
                println!(
                    "[FAILURE] Entering failure window for {}s (node_id={})",
                    cfg.fail_duration_secs, s.node_id
                );
            }
        }

        // Keep failing for the configured duration
        tokio::time::sleep(Duration::from_secs(cfg.fail_duration_secs)).await;

        {
            let mut s = state.write().await;
            if s.ignoring {
                s.ignoring = false;
                println!("[FAILURE] Exiting failure window (node_id={})", s.node_id);
            }
        }
    }
}

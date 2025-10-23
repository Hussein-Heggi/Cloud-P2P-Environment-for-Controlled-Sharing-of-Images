mod message;
mod network;
mod node;

use anyhow::{Context, Result};
use log::info;
use node::{Config, Node};
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logger
    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("info")
    ).init();

    let args: Vec<String> = env::args().collect();
    
    if args.len() < 2 {
        eprintln!("╔═══════════════════════════════════════════════════════════╗");
        eprintln!("║ Modified Bully Algorithm - Leader Election System        ║");
        eprintln!("╚═══════════════════════════════════════════════════════════╝");
        eprintln!();
        eprintln!("Usage: {} <node_id> [config_file]", args[0]);
        eprintln!();
        eprintln!("Arguments:");
        eprintln!("  <node_id>       Your node ID from config.json (e.g., 0, 1, 2)");
        eprintln!("  [config_file]   Path to config file (default: config.json)");
        eprintln!();
        eprintln!("Example:");
        eprintln!("  {} 0              # Start as Node 0", args[0]);
        eprintln!("  {} 1 config.json  # Start as Node 1", args[0]);
        eprintln!();
        eprintln!("Algorithm:");
        eprintln!("  - Highest ID is leader");
        eprintln!("  - Leader tracks successor (next highest alive node)");
        eprintln!("  - Fast failover: successor takes over immediately");
        eprintln!("  - No full election needed on single failure");
        eprintln!();
        std::process::exit(1);
    }

    let node_id: u32 = args[1].parse()
        .context("Node ID must be a number (e.g., 0, 1, 2)")?;
    
    let config_path = args.get(2)
        .map(|s| s.as_str())
        .unwrap_or("config.json");

    info!("╔═══════════════════════════════════════════════════════════╗");
    info!("║ Modified Bully Algorithm Node                             ║");
    info!("╠═══════════════════════════════════════════════════════════╣");
    info!("║ Config: {}                                                ║", config_path);
    info!("║ Node ID: {}                                               ║", node_id);
    info!("╚═══════════════════════════════════════════════════════════╝");

    // Load configuration
    let config = Config::from_file(config_path)?;

    // Create and run node
    let node = Node::new(node_id, config)?;
    node.run().await?;

    Ok(())
}
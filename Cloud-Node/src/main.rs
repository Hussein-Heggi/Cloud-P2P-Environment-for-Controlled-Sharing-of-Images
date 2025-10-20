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
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .init();

    let args: Vec<String> = env::args().collect();
    
    // Usage: cloud-p2p <node_id> [config_file]
    if args.len() < 2 {
        eprintln!("Usage: {} <node_id> [config_file]", args[0]);
        eprintln!("Example: {} 0 config.json", args[0]);
        std::process::exit(1);
    }

    let node_id: u32 = args[1].parse()
        .context("Node ID must be a number (e.g., 0, 1, 2)")?;
    
    let config_path = args.get(2)
        .map(|s| s.as_str())
        .unwrap_or("config.json");

    info!("Loading configuration from {}", config_path);

    // Load configuration
    let config = Config::from_file(config_path)?;

    info!("Starting node {} in cluster of {} nodes", 
          node_id, 
          config.nodes.len());

    // Create and run node
    let node = Node::new(node_id, config)?;
    node.run().await?;

    Ok(())
}
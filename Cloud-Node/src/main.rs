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
    
    // Usage: cloud-p2p <physical_id> [config_file]
    if args.len() < 2 {
        eprintln!("╔═══════════════════════════════════════════════════════════╗");
        eprintln!("║ Cloud P2P - Dynamic Leader Election System              ║");
        eprintln!("╚═══════════════════════════════════════════════════════════╝");
        eprintln!();
        eprintln!("Usage: {} <physical_id> [config_file]", args[0]);
        eprintln!();
        eprintln!("Arguments:");
        eprintln!("  <physical_id>   Your physical ID from config.json (0, 1, or 2)");
        eprintln!("  [config_file]   Path to config file (default: config.json)");
        eprintln!();
        eprintln!("Example:");
        eprintln!("  {} 0              # Start as Physical ID 0", args[0]);
        eprintln!("  {} 1 config.json  # Start as Physical ID 1", args[0]);
        eprintln!();
        eprintln!("Notes:");
        eprintln!("  - Physical ID is STATIC (from config, never changes)");
        eprintln!("  - Logical PID is DYNAMIC (assigned by network)");
        eprintln!("  - Logical PIDs: 2=Leader, 1=Next Leader, 0=Follower");
        eprintln!();
        std::process::exit(1);
    }

    let physical_id: u32 = args[1].parse()
        .context("Physical ID must be a number (e.g., 0, 1, 2)")?;
    
    let config_path = args.get(2)
        .map(|s| s.as_str())
        .unwrap_or("config.json");

    info!("╔═══════════════════════════════════════════════════════════╗");
    info!("║ Cloud P2P Node Starting                                   ║");
    info!("╠═══════════════════════════════════════════════════════════╣");
    info!("║ Configuration File: {}                                    ║", config_path);
    info!("║ Physical ID: {}                                           ║", physical_id);
    info!("║ Logical PID: TBD (will be assigned by network)            ║");
    info!("╚═══════════════════════════════════════════════════════════╝");

    // Load configuration
    let config = Config::from_file(config_path)?;

    // Create and run node
    let node = Node::new(physical_id, config)?;
    node.run().await?;

    Ok(())
}
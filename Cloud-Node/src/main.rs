// src/main.rs
mod node;

use crate::node::{Node, NodeConfig, Peer};
use anyhow::{anyhow, Result};
use clap::Parser;
use serde::Deserialize;
use std::{fs, net::SocketAddr, path::PathBuf};
use tracing::info;

#[derive(Parser, Debug)]
struct Args {
    /// This node's ID (e.g., A, B, C)
    #[clap(long)]
    id: String,

    /// Path to cluster JSON file describing all nodes (id + addr)
    #[clap(long, default_value = "cluster.json")]
    cluster: PathBuf,

    /// Bind address for this node (often 0.0.0.0:PORT). If omitted, we'll infer from cluster port.
    #[clap(long)]
    bind: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Cluster {
    nodes: Vec<ClusterNode>,
}

#[derive(Debug, Deserialize)]
struct ClusterNode {
    id: String,
    /// Advertised address others should use to reach this node, e.g. "172.23.23.143:5001"
    addr: String,
}

fn init_logging() {
    use tracing_subscriber::{fmt, prelude::*, EnvFilter};
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::registry().with(filter).with(fmt::layer()).init();
}

fn build_config(args: &Args, cluster: Cluster) -> Result<NodeConfig> {
    // find me
    let me = cluster
        .nodes
        .iter()
        .find(|n| n.id == args.id)
        .ok_or_else(|| anyhow!("id {} not found in cluster file", args.id))?;

    // bind: if provided via flag, use it; else bind to 0.0.0.0:<same_port_as_my_addr>
    let bind_sock: SocketAddr = if let Some(b) = &args.bind {
        b.parse()?
    } else {
        // extract port from my advertised addr
        let my_addr: SocketAddr = me.addr.parse()?;
        let port = my_addr.port();
        format!("0.0.0.0:{}", port).parse()?
    };

    // peers = everyone except me
    let peers: Vec<Peer> = cluster
        .nodes
        .into_iter()
        .filter(|n| n.id != me.id)
        .map(|n| Peer {
            id: n.id,
            addr: n.addr.parse().expect("invalid peer addr"),
        })
        .collect();

    Ok(NodeConfig {
        id: me.id.clone(),
        bind: bind_sock,
        peers,
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    init_logging();
    let args = Args::parse();

    // load cluster.json
    let text = fs::read_to_string(&args.cluster)?;
    let cluster: Cluster = serde_json::from_str(&text)?;
    let cfg = build_config(&args, cluster)?;

    info!(?cfg, "built node config");
    Node::new(cfg).run().await?;
    Ok(())
}

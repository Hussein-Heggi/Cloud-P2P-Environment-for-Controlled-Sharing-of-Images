// src/node.rs
use anyhow::Result;
use std::{net::SocketAddr, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    task, time,
};
use tracing::{info, warn};

#[derive(Debug, Clone)]
pub struct Peer {
    pub id: String,
    pub addr: SocketAddr, // ip:port of the peer (reachable address)
}

#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub id: String,          // my stable ID (A, B, C, ...)
    pub bind: SocketAddr,    // where I listen (often 0.0.0.0:port)
    pub peers: Vec<Peer>,    // all other nodes
}

pub struct Node {
    cfg: NodeConfig,
}

impl Node {
    pub fn new(cfg: NodeConfig) -> Self {
        Self { cfg }
    }

    /// Start the node: accept incoming, and dial only "higher-ID" peers to avoid duplicate links.
    pub async fn run(self) -> Result<()> {
        let my_id = self.cfg.id.clone();

        // 1) Listener
        let listener = TcpListener::bind(self.cfg.bind).await?;
        info!(id=%self.cfg.id, bind=%self.cfg.bind, "node starting; listening");

        let id_for_accept = self.cfg.id.clone();
        task::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((sock, addr)) => {
                        info!(?addr, "incoming connection");
                        let my_id = id_for_accept.clone();
                        task::spawn(async move {
                            if let Err(e) = handle_link(sock, my_id, "<incoming>".into()).await {
                                warn!(error=?e, "incoming link ended");
                            }
                        });
                    }
                    Err(e) => warn!(error=?e, "accept failed"),
                }
            }
        });

        // 2) Outgoing dials: only to peers with lexicographically greater IDs
        for p in self.cfg.peers.into_iter().filter(|p| p.id > my_id) {
            let my_id = my_id.clone();
            task::spawn(async move {
                loop {
                    match TcpStream::connect(p.addr).await {
                        Ok(stream) => {
                            info!(peer=%p.id, addr=%p.addr, "connected (outgoing)");
                            if let Err(e) = handle_link(stream, my_id.clone(), p.id.clone()).await {
                                warn!(peer=%p.id, error=?e, "link ended; retry soon");
                                time::sleep(Duration::from_secs(2)).await;
                            }
                        }
                        Err(e) => {
                            warn!(peer=%p.id, error=?e, "connect failed; retry in 2s");
                            time::sleep(Duration::from_secs(2)).await;
                        }
                    }
                }
            });
        }

        // keep alive forever
        futures::future::pending::<()>().await;
        // Ok(())
    }
}

async fn handle_link(mut stream: TcpStream, my_id: String, peer_label: String) -> Result<()> {
    // say hello from *both* sides (symmetry)
    stream
        .write_all(format!("HELLO from {}\n", my_id).as_bytes())
        .await?;

    // read newline-delimited messages
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    loop {
        let n = reader.read_line(&mut line).await?;
        if n == 0 {
            info!(peer=%peer_label, "connection closed");
            break;
        }
        let msg = line.trim_end_matches(['\r', '\n']);
        info!(me=%my_id, from=%peer_label, %msg, "received line");
        line.clear();
    }
    Ok(())
}

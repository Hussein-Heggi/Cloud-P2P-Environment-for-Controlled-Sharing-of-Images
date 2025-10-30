use crate::{config::Config, state::SharedState};
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::net::UdpSocket;
use tokio::time::{interval, sleep};
use tracing::{debug, info, warn};

/// ASSIGN (type=6): [u8 ty=6][u32 leader_id][u32 lease_ms][u32 ipv4_be]
const ASSIGN: u8 = 6;

#[inline]
fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}

pub async fn run_assignment_channels(state: SharedState, cfg: Config) -> anyhow::Result<()> {
    // Bind our assignment listener
    let bind_addr: SocketAddr = cfg
        .assignment_bind_addr()
        .expect("assignment_bind not configured");
    let sock = Arc::new(UdpSocket::bind(bind_addr).await?);
    println!("Assignment port bound: {}", bind_addr);
    info!(%bind_addr, "Assignment port bound");

    // Receiver
    {
        let st = state.clone();
        let sock_rx = sock.clone();
        tokio::spawn(async move { recv_assign_loop(st, sock_rx).await; });
    }

    // Periodic broadcaster (leader only)
    {
        let st = state.clone();
        let sock_tx = sock.clone();
        tokio::spawn(async move { broadcast_assign_loop(st, sock_tx, cfg).await; });
    }

    Ok(())
}

async fn recv_assign_loop(state: SharedState, sock: Arc<UdpSocket>) {
    let mut buf = [0u8; 256];
    // Throttle terminal/tracing logs to ~every 12s
    let mut last_log = Instant::now() - Duration::from_secs(12);

    loop {
        if state.read().await.ignoring {
            sleep(Duration::from_millis(50)).await;
            continue;
        }

        match sock.recv_from(&mut buf).await {
            Ok((n, from)) => {
                if n < 1 + 4 + 4 + 4 || buf[0] != ASSIGN {
                    continue;
                }
                let leader_id = u32::from_le_bytes(buf[1..5].try_into().unwrap());
                let lease_ms = u32::from_le_bytes(buf[5..9].try_into().unwrap());
                let ipv4_be = u32::from_be_bytes(buf[9..13].try_into().unwrap());
                let ip = IpAddr::V4(Ipv4Addr::from(ipv4_be));
                let deadline_ms = now_ms() + lease_ms as u128;

                {
                    let mut s = state.write().await;
                    s.executor_ip = Some(ip);
                    s.executor_lease_deadline_ms = Some(deadline_ms);
                }

                // Log at most once every ~12 seconds
                if last_log.elapsed() >= Duration::from_secs(12) {
                    println!(
                        "ASSIGN received from {} | leader_id={} executor_ip={} lease_ms={}",
                        from, leader_id, ip, lease_ms
                    );
                    debug!(%from, leader_id, ?ip, lease_ms, "ASSIGN received; executor updated");
                    last_log = Instant::now();
                }
            }
            Err(e) => {
                eprintln!("assignment recv error: {}", e);
                warn!(error=%e, "assignment recv error");
                sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

async fn broadcast_assign_loop(state: SharedState, sock: Arc<UdpSocket>, cfg: Config) {
    let mut tick = interval(Duration::from_millis(cfg.assign_broadcast_every_ms));
    // Throttle terminal/tracing logs to ~every 12s
    let mut last_log = Instant::now() - Duration::from_secs(12);

    loop {
        tick.tick().await;

        // Only broadcast if I'm leader
        let is_leader = { state.read().await.is_leader };
        if !is_leader || state.read().await.ignoring {
            continue;
        }

        // Phase-1: static executor IP (port is fixed globally on client side)
        let exec_ip = cfg.static_executor_ip();
        let leader_id = { state.read().await.node_id };
        let lease_ms = cfg.assign_lease_ms;

        let ipv4_be_bytes = match exec_ip {
            IpAddr::V4(v4) => u32::from(v4).to_be_bytes(),
            _ => {
                eprintln!("ASSIGN currently supports IPv4 only");
                warn!("ASSIGN currently supports IPv4 only");
                continue;
            }
        };

        // Build ASSIGN: [6][leader_id][lease_ms][ipv4_be]
        let mut pkt = Vec::with_capacity(1 + 4 + 4 + 4);
        pkt.push(ASSIGN);
        pkt.extend(leader_id.to_le_bytes());
        pkt.extend(lease_ms.to_le_bytes());
        pkt.extend(ipv4_be_bytes);

        // Send to all assignment peers
        for peer in cfg.assignment_peer_addrs() {
            let _ = sock.send_to(&pkt, peer).await;
        }

        // Keep my own lease fresh locally
        {
            let mut s = state.write().await;
            s.executor_ip = Some(exec_ip);
            s.executor_lease_deadline_ms = Some(now_ms() + lease_ms as u128);
        }

        // Log at most once every ~12 seconds
        if last_log.elapsed() >= Duration::from_secs(12) {
            println!(
                "ASSIGN broadcast sent | leader_id={} executor_ip={} lease_ms={}",
                leader_id, exec_ip, lease_ms
            );
            info!(leader_id, ?exec_ip, lease_ms, "ASSIGN broadcast sent");
            last_log = Instant::now();
        }
    }
}
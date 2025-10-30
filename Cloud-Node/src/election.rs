use crate::{config::Config, state::SharedState};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::{
    net::UdpSocket,
    sync::{watch, RwLock},
    time::{interval, sleep},
};
use tracing::{debug, info, warn};

const LEADER_TIMEOUT_SECS: u64 = 5;
const PEER_EXPIRY_SECS: u64 = 6;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
enum Message {
    Discovery { sender_id: u32, timestamp: u64 },
    LeaderAnnounce { leader_id: u32, timestamp: u64 },
    Election { sender_id: u32, timestamp: u64 },
    ElectionOk { sender_id: u32, timestamp: u64 },
    Coordinator { leader_id: u32, timestamp: u64 },
    Heartbeat { leader_id: u32, successor_id: Option<u32>, timestamp: u64 },
    HeartbeatAck { sender_id: u32, timestamp: u64 },
}

#[derive(Debug, Clone, PartialEq)]
enum NodeState {
    Follower,
    Candidate,
    Leader,
}

#[inline]
fn now_ts() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

pub async fn run_election_loop(
    shared: SharedState,
    cfg: Config,
    leader_tx: watch::Sender<u32>,
) {
    let node_id = cfg.node_id;
    let my_addr: SocketAddr = cfg
    .election_bind_addr()
    .expect("election_bind_addr() is None; check --elect-bind or Config::election_peers()");
    let sock = Arc::new(
        UdpSocket::bind(my_addr)
            .await
            .expect("bind election socket"),
    );
    info!(%my_addr, node=%node_id, "Election socket bound");

    // node_id -> election socket addr (derived from +100 scheme in Config)
    let peers: Arc<HashMap<u32, SocketAddr>> = Arc::new(
        Config::election_peer_addrs()
            .into_iter()
            .enumerate()
            .map(|(i, a)| ((i as u32) + 1, a))
            .collect(),
    );

    // Local election state
    let state = Arc::new(RwLock::new(NodeState::Follower));
    let current_leader = Arc::new(RwLock::new(None::<u32>));
    let last_heartbeat = Arc::new(RwLock::new(SystemTime::now()));
    let election_in_progress = Arc::new(RwLock::new(false));
    let active_nodes = Arc::new(RwLock::new(HashMap::<u32, SystemTime>::new()));
    let successor_hint = Arc::new(RwLock::new(None::<u32>));

    // ===== Listener =====
    {
        let sock_c = Arc::clone(&sock);
        let peers_c = Arc::clone(&peers);
        let state_c = Arc::clone(&state);
        let current_leader_c = Arc::clone(&current_leader);
        let last_heartbeat_c = Arc::clone(&last_heartbeat);
        let election_in_progress_c = Arc::clone(&election_in_progress);
        let active_nodes_c = Arc::clone(&active_nodes);
        let successor_hint_c = Arc::clone(&successor_hint);
        let shared_c = Arc::clone(&shared);
        let leader_tx_c = leader_tx.clone();
        let node_id_c = node_id;

        tokio::spawn(async move {
            let mut buf = [0u8; 4096];
            loop {
                if shared_c.read().await.ignoring {
                    sleep(Duration::from_millis(100)).await;
                    continue;
                }
                match sock_c.recv_from(&mut buf).await {
                    Ok((len, from)) => {
                        if shared_c.read().await.ignoring {
                            continue;
                        }
                        if let Ok(msg) = serde_json::from_slice::<Message>(&buf[..len]) {
                            match msg {
                                Message::Discovery { sender_id, .. } => {
                                    active_nodes_c
                                        .write()
                                        .await
                                        .insert(sender_id, SystemTime::now());
                                    debug!(node=%node_id_c, from=%sender_id, "Discovery");

                                    if *state_c.read().await == NodeState::Leader {
                                        if let Some(addr) = peers_c.get(&sender_id) {
                                            if shared_c.read().await.ignoring {
                                                continue;
                                            }
                                            let m = Message::LeaderAnnounce {
                                                leader_id: current_leader_c.read().await.unwrap_or(0),
                                                timestamp: now_ts(),
                                            };
                                            if let Ok(data) = serde_json::to_vec(&m) {
                                                let _ = sock_c.send_to(&data, addr).await;
                                            }
                                        }
                                    }
                                }
                                Message::LeaderAnnounce { leader_id, .. } => {
                                    *current_leader_c.write().await = Some(leader_id);
                                    *state_c.write().await = NodeState::Follower;
                                    *last_heartbeat_c.write().await = SystemTime::now();
                                    info!(node=%node_id_c, leader=%leader_id, "Leader announced; following");
                                }
                                Message::Election { sender_id, .. } => {
                                    active_nodes_c
                                        .write()
                                        .await
                                        .insert(sender_id, SystemTime::now());
                                    info!(node=%node_id_c, from=%sender_id, "Election received");
                                    if sender_id < node_id_c {
                                        if let Some(addr) = peers_c.get(&sender_id) {
                                            if !shared_c.read().await.ignoring {
                                                let ok = Message::ElectionOk {
                                                    sender_id: node_id_c,
                                                    timestamp: now_ts(),
                                                };
                                                if let Ok(data) = serde_json::to_vec(&ok) {
                                                    let _ = sock_c.send_to(&data, addr).await;
                                                }
                                            }
                                        }
                                        if !*election_in_progress_c.read().await {
                                            start_election(
                                                &sock_c,
                                                node_id_c,
                                                &peers_c,
                                                &state_c,
                                                &current_leader_c,
                                                &election_in_progress_c,
                                                &shared_c,
                                                &leader_tx_c,
                                            )
                                            .await;
                                        }
                                    }
                                }
                                Message::ElectionOk { sender_id, .. } => {
                                    info!(node=%node_id_c, higher=%sender_id, "ElectionOk (higher node alive)");
                                }
                                Message::Coordinator { leader_id, .. } => {
                                    *current_leader_c.write().await = Some(leader_id);
                                    *state_c.write().await = NodeState::Follower;
                                    *last_heartbeat_c.write().await = SystemTime::now();
                                    *election_in_progress_c.write().await = false;
                                    info!(node=%node_id_c, new_leader=%leader_id, "Coordinator received; following");
                                }
                                Message::Heartbeat { leader_id, successor_id, .. } => {
                                    *successor_hint_c.write().await = successor_id;
                                    if *current_leader_c.read().await == Some(leader_id) {
                                        *last_heartbeat_c.write().await = SystemTime::now();
                                    } else {
                                        *current_leader_c.write().await = Some(leader_id);
                                        *last_heartbeat_c.write().await = SystemTime::now();
                                        *state_c.write().await = if node_id_c == leader_id {
                                            NodeState::Leader
                                        } else {
                                            NodeState::Follower
                                        };
                                    }
                                    active_nodes_c
                                        .write()
                                        .await
                                        .insert(leader_id, SystemTime::now());
                                    debug!(node=%node_id_c, from_leader=%leader_id, succ=?successor_id, "Heartbeat");
                                }
                                Message::HeartbeatAck { .. } => {}
                            }
                        } else {
                            warn!(node=%node_id_c, from=%from, "Election: bad packet");
                        }
                    }
                    Err(e) => {
                        warn!(node=%node_id_c, error=?e, "Election recv_from error");
                    }
                }
            }
        });
    }

    // ===== Discovery (startup + periodic) =====
    {
        let sock_c = Arc::clone(&sock);
        let peers_c = Arc::clone(&peers);
        let state_c = Arc::clone(&state);
        let current_leader_c = Arc::clone(&current_leader);
        let election_in_progress_c = Arc::clone(&election_in_progress);
        let shared_c = Arc::clone(&shared);
        let leader_tx_c = leader_tx.clone();
        let node_id_c = node_id;

        tokio::spawn(async move {
            // initial discovery sequence (first join)
            sleep(Duration::from_millis(300)).await;
            info!(node=%node_id_c, "Sending initial discovery burst");
            send_discovery(&sock_c, node_id_c, &peers_c, &shared_c).await;

            sleep(Duration::from_secs(2)).await;
            if !shared_c.read().await.ignoring
                && current_leader_c.read().await.is_none()
                && !*election_in_progress_c.read().await
            {
                warn!(node=%node_id_c, "No leader after startup delay; starting election");
                start_election(
                    &sock_c,
                    node_id_c,
                    &peers_c,
                    &state_c,
                    &current_leader_c,
                    &election_in_progress_c,
                    &shared_c,
                    &leader_tx_c,
                )
                .await;
            }

            // periodic discovery pings
            loop {
                sleep(Duration::from_secs(5)).await;
                if !shared_c.read().await.ignoring {
                    debug!(node=%node_id_c, "Periodic discovery");
                    send_discovery(&sock_c, node_id_c, &peers_c, &shared_c).await;
                }
            }
        });
    }

    // ===== Heartbeats (leaders only) =====
    {
        let sock_c = Arc::clone(&sock);
        let state_c = Arc::clone(&state);
        let shared_c = Arc::clone(&shared);
        let active_nodes_c = Arc::clone(&active_nodes);
        let current_leader_c = Arc::clone(&current_leader);
        let node_id_c = node_id;

        tokio::spawn(async move {
            let mut tick = interval(Duration::from_secs(2));
            loop {
                tick.tick().await;
                if shared_c.read().await.ignoring {
                    continue;
                }
                if *state_c.read().await == NodeState::Leader {
                    // prune stale peers
                    {
                        let mut act = active_nodes_c.write().await;
                        let now = SystemTime::now();
                        act.retain(|peer_id, last| {
                            if *peer_id == node_id_c {
                                return true;
                            }
                            now.duration_since(*last)
                                .unwrap_or(Duration::from_secs(0))
                                < Duration::from_secs(PEER_EXPIRY_SECS)
                        });
                    }
                    let succ = {
                        let act = active_nodes_c.read().await;
                        calculate_successor(node_id_c, &act)
                    };

                    let hb = Message::Heartbeat {
                        leader_id: node_id_c,
                        successor_id: succ,
                        timestamp: now_ts(),
                    };
                    if let Ok(data) = serde_json::to_vec(&hb) {
                        for (id, addr) in Config::election_peer_addrs()
                            .into_iter()
                            .enumerate()
                            .map(|(i, a)| ((i as u32) + 1, a))
                        {
                            if id != node_id_c && !shared_c.read().await.ignoring {
                                let _ = sock_c.send_to(&data, addr).await;
                            }
                        }
                        debug!(leader=%node_id_c, succ=?succ, "Heartbeat broadcast");
                    }

                    *current_leader_c.write().await = Some(node_id_c);
                }
            }
        });
    }

    // ===== Monitor (1s) & Status (5s) with REVIVAL REJOIN =====
    {
        // monitor loop
        {
            let sock_c = Arc::clone(&sock);
            let peers_c = Arc::clone(&peers);
            let state_c = Arc::clone(&state);
            let current_leader_c = Arc::clone(&current_leader);
            let election_in_progress_c = Arc::clone(&election_in_progress);
            let last_heartbeat_c = Arc::clone(&last_heartbeat);
            let active_nodes_c = Arc::clone(&active_nodes);
            let shared_c = Arc::clone(&shared);
            let leader_tx_c = leader_tx.clone();
            let node_id_c = node_id;

            tokio::spawn(async move {
                let mut tick = interval(Duration::from_secs(1));
                loop {
                    tick.tick().await;

                    // publish live set (include self if up)
                    {
                        let now = SystemTime::now();
                        let mut live: Vec<u32> = Vec::new();
                        if !shared_c.read().await.ignoring {
                            live.push(node_id_c);
                        }
                        for (peer_id, last) in active_nodes_c.read().await.iter() {
                            if *peer_id == node_id_c {
                                continue;
                            }
                            if now
                                .duration_since(*last)
                                .unwrap_or_default()
                                < Duration::from_secs(PEER_EXPIRY_SECS)
                            {
                                live.push(*peer_id);
                            }
                        }
                        let mut s = shared_c.write().await;
                        s.live_peers = live;
                    }

                    // follower timeout → election
                    if *state_c.read().await != NodeState::Leader && !shared_c.read().await.ignoring {
                        let elapsed = SystemTime::now()
                            .duration_since(*last_heartbeat_c.read().await)
                            .unwrap_or(Duration::ZERO);
                        if elapsed > Duration::from_secs(LEADER_TIMEOUT_SECS) {
                            warn!(
                                node=%node_id_c,
                                "Leader heartbeat timeout (> {}s); triggering election",
                                LEADER_TIMEOUT_SECS
                            );
                            if !*election_in_progress_c.read().await {
                                start_election(
                                    &sock_c,
                                    node_id_c,
                                    &peers_c,
                                    &state_c,
                                    &current_leader_c,
                                    &election_in_progress_c,
                                    &shared_c,
                                    &leader_tx_c,
                                )
                                .await;
                            }
                        }
                    }
                }
            });
        }

        // status reporter (5s) + **REVIVAL REJOIN** logic
        {
            let sock_c = Arc::clone(&sock);
            let peers_c = Arc::clone(&peers);
            let state_c = Arc::clone(&state);
            let current_leader_c = Arc::clone(&current_leader);
            let last_heartbeat_c = Arc::clone(&last_heartbeat);
            let active_nodes_c = Arc::clone(&active_nodes);
            let successor_hint_c = Arc::clone(&successor_hint);
            let election_in_progress_c = Arc::clone(&election_in_progress);
            let shared_c = Arc::clone(&shared);
            let leader_tx_c = leader_tx.clone();
            let node_id_c = node_id;

            tokio::spawn(async move {
                let mut every = interval(Duration::from_secs(5));
                let mut was_ignoring = false;

                loop {
                    every.tick().await;

                    // suppress logs while failing
                    if shared_c.read().await.ignoring {
                        was_ignoring = true;
                        continue;
                    }

                    // ==== REVIVAL → restart as first join ====
                    if was_ignoring {
                        // 1) reset local election state
                        *state_c.write().await = NodeState::Follower;
                        *current_leader_c.write().await = None;
                        active_nodes_c.write().await.clear();
                        *successor_hint_c.write().await = None;
                        *last_heartbeat_c.write().await = SystemTime::now();
                        was_ignoring = false;

                        info!(node=%node_id_c, "Revived: restarting join sequence");
                        // 2) discovery burst
                        send_discovery(&sock_c, node_id_c, &peers_c, &shared_c).await;

                        // 3) wait 2s; if still no leader → start election
                        sleep(Duration::from_secs(2)).await;
                        if !shared_c.read().await.ignoring
                            && current_leader_c.read().await.is_none()
                            && !*election_in_progress_c.read().await
                        {
                            warn!(node=%node_id_c, "Post-revival: no leader; starting election");
                            start_election(
                                &sock_c,
                                node_id_c,
                                &peers_c,
                                &state_c,
                                &current_leader_c,
                                &election_in_progress_c,
                                &shared_c,
                                &leader_tx_c,
                            )
                            .await;
                        }
                        // continue to next tick to print status after the above steps
                        continue;
                    }

                    // ==== Normal status printing (when up) ====
                    let elapsed = SystemTime::now()
                        .duration_since(*last_heartbeat_c.read().await)
                        .unwrap_or(Duration::ZERO)
                        .as_secs_f64();

                    let st = state_c.read().await.clone();
                    let leader = *current_leader_c.read().await;

                    if st == NodeState::Leader {
                        let act = active_nodes_c.read().await;
                        let computed_succ = calculate_successor(node_id_c, &act);
                        let active_count = act.len() + 1; // include self
                        let live: Vec<u32> = act.keys().copied().collect(); // peers only
                        drop(act);
                        info!(
                            "Node {}: Leader, leader={leader:?}, successor={computed_succ:?}, active={}, followers={:?}, Δhb={:.1}s",
                            node_id_c, active_count, live, elapsed
                        );
                    } else {
                        let hint = *successor_hint_c.read().await;
                        info!(
                            "Node {}: Follower, leader={leader:?}, successor_hint={hint:?}, Δhb={:.1}s",
                            node_id_c, elapsed
                        );
                    }
                }
            });
        }
    }
}

fn calculate_successor(
    exclude_id: u32,
    active_nodes: &HashMap<u32, SystemTime>,
) -> Option<u32> {
    let mut ids: Vec<u32> = active_nodes.keys().copied().collect();
    ids.sort_unstable();
    ids.dedup();
    ids.into_iter().rev().find(|&id| id != exclude_id)
}

async fn send_discovery(
    sock: &UdpSocket,
    my_id: u32,
    peers: &Arc<HashMap<u32, SocketAddr>>,
    shared: &SharedState,
) {
    if shared.read().await.ignoring {
        return;
    }
    let msg = Message::Discovery {
        sender_id: my_id,
        timestamp: now_ts(),
    };
    if let Ok(data) = serde_json::to_vec(&msg) {
        for (id, addr) in peers.iter() {
            if *id != my_id && !shared.read().await.ignoring {
                let _ = sock.send_to(&data, addr).await;
            }
        }
    }
    debug!(node=%my_id, "Discovery broadcast sent");
}

async fn start_election(
    sock: &UdpSocket,
    my_id: u32,
    peers: &Arc<HashMap<u32, SocketAddr>>,
    state: &Arc<RwLock<NodeState>>,
    current_leader: &Arc<RwLock<Option<u32>>>,
    election_in_progress: &Arc<RwLock<bool>>,
    shared: &SharedState,
    leader_tx: &watch::Sender<u32>,
) {
    if shared.read().await.ignoring {
        return;
    }
    *election_in_progress.write().await = true;
    *state.write().await = NodeState::Candidate;

    info!(node=%my_id, "Starting election (bully)");

    // Bully: ping higher nodes
    let higher: Vec<_> = peers.keys().filter(|&&k| k > my_id).cloned().collect();
    if higher.is_empty() {
        become_leader(sock, my_id, peers, state, current_leader, shared, leader_tx).await;
        *election_in_progress.write().await = false;
        return;
    }

    let msg = Message::Election {
        sender_id: my_id,
        timestamp: now_ts(),
    };
    if let Ok(data) = serde_json::to_vec(&msg) {
        for id in &higher {
            if let Some(addr) = peers.get(id) {
                if !shared.read().await.ignoring {
                    let _ = sock.send_to(&data, addr).await;
                }
            }
        }
        debug!(node=%my_id, higher=?higher, "Election ping to higher nodes");
    }

    // Wait a bit; if nobody responds and no Coordinator arrives → assume leadership
    sleep(Duration::from_millis(1200)).await;

    if *state.read().await == NodeState::Candidate && !shared.read().await.ignoring {
        info!(node=%my_id, "No higher node claimed leadership; becoming leader");
        become_leader(sock, my_id, peers, state, current_leader, shared, leader_tx).await;
    }

    *election_in_progress.write().await = false;
}

async fn become_leader(
    sock: &UdpSocket,
    my_id: u32,
    peers: &Arc<HashMap<u32, SocketAddr>>,
    state: &Arc<RwLock<NodeState>>,
    current_leader: &Arc<RwLock<Option<u32>>>,
    shared: &SharedState,
    leader_tx: &watch::Sender<u32>,
) {
    if shared.read().await.ignoring {
        return;
    }
    *state.write().await = NodeState::Leader;
    *current_leader.write().await = Some(my_id);
    {
        let mut s = shared.write().await;
        s.leader_id = my_id;
        s.is_leader = true;
    }
    info!(leader=%my_id, "Node became leader");
    let _ = leader_tx.send(my_id);

    // Announce Coordinator
    let msg = Message::Coordinator {
        leader_id: my_id,
        timestamp: now_ts(),
    };
    if let Ok(data) = serde_json::to_vec(&msg) {
        for (id, addr) in peers.iter() {
            if *id != my_id && !shared.read().await.ignoring {
                let _ = sock.send_to(&data, addr).await;
            }
        }
        debug!(leader=%my_id, "Coordinator broadcast");
    }
}

pub async fn handle_leader_changes(shared: SharedState, mut rx: watch::Receiver<u32>) {
    while rx.changed().await.is_ok() {
        let new_leader = *rx.borrow();
        let mut s = shared.write().await;
        s.leader_id = new_leader;
        s.is_leader = s.node_id == new_leader;
        info!(leader=%new_leader, is_leader=%s.is_leader, node=%s.node_id, "leader changed");
    }
}

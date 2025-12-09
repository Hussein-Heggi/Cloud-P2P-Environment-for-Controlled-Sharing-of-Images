use clap::Parser;
use std::sync::Arc;
use sysinfo::System;
use tokio::sync::{watch, RwLock};
use tokio::time::Duration;
use tracing::info;

mod assignment;
mod client_protocol;
mod config;
mod election;
mod epoch;
mod executor_leader;
mod failure;
mod firebase;
mod history;
mod state;
mod stego_service;
mod tcp_client;
mod udp;
mod owner_image;
mod access_map_storage;

use crate::state::ServerState;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Structured logs + println! side-by-side
    tracing_subscriber::fmt()
        .with_target(true)
        .with_level(true)
        .with_ansi(true)
        .compact()
        .init();

    // Parse config
    let cfg = config::Config::parse();
    cfg.validate();

    // Initialize global epoch FIRST - all servers will align to nearest second boundary
    epoch::init_epoch();
    println!("[MAIN] Epoch initialized, starting server node_id={}", cfg.node_id);

    // Shared state
    let state: state::SharedState = Arc::new(RwLock::new(ServerState::new(cfg.node_id)));

    // Initialize Firebase connection
    println!("[MAIN] Initializing Firebase connection...");
    match firebase::init_firestore().await {
        Ok(db) => {
            println!("[MAIN] ✅ Firebase connected successfully");
            info!("Firebase Firestore connected");

            // Store Firebase connection in state
            {
                let mut s = state.write().await;
                s.firestore_db = Some(db);
            }

            // Load initial DOS data from Firebase
            let s = state.read().await;
            if let Some(db) = &s.firestore_db {
                println!("[MAIN] Loading initial DOS data from Firebase...");

                match firebase::read_all_clients(db).await {
                    Ok(clients) => {
                        drop(s);
                        let mut s = state.write().await;
                        s.dos_clients = clients;
                        println!("[MAIN] ✅ Loaded {} clients from Firebase", s.dos_clients.len());
                    }
                    Err(e) => {
                        println!("[MAIN] ⚠️  Failed to load clients from Firebase: {}", e);
                    }
                }

                let s = state.read().await;
                if let Some(db) = &s.firestore_db {
                    match firebase::read_all_access(db).await {
                        Ok(access) => {
                            drop(s);
                            let mut s = state.write().await;
                            s.dos_access = access;
                            println!("[MAIN] ✅ Loaded {} access records from Firebase", s.dos_access.len());
                        }
                        Err(e) => {
                            println!("[MAIN] ⚠️  Failed to load access records from Firebase: {}", e);
                        }
                    }
                }
            } else {
                drop(s);
            }
        }
        Err(e) => {
            println!("[MAIN] ⚠️  Firebase connection failed: {}", e);
            println!("[MAIN] Server will continue without Firebase (degraded mode)");
            info!(error=%e, "Firebase connection failed - running without Firebase");
        }
    }

    // System recovery: Verify which clients are still alive after server restart
    // Only runs on current executor (check executor_ip, NOT leader)
    {
        let st = state.clone();
        let cfg_clone = cfg.clone();
        tokio::spawn(async move {
            // Wait 10 seconds for system to stabilize (executor assignment, etc.)
            tokio::time::sleep(Duration::from_secs(10)).await;

            println!("[RECOVERY] Starting system recovery check...");

            // Check if I'm the current executor
            let (is_executor, db_opt) = {
                let s = st.read().await;
                let my_ip = match cfg_clone.service_bind_addr() {
                    Some(addr) => addr.ip(),
                    None => {
                        eprintln!("[RECOVERY] No service bind address configured, skipping recovery");
                        return;
                    }
                };

                let is_exec = if let (Some(exec_ip), Some(deadline)) = (&s.executor_ip, s.executor_lease_deadline_ms) {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis();
                    exec_ip == &my_ip && now <= deadline
                } else {
                    false
                };

                (is_exec, s.firestore_db.clone())
            };

            if !is_executor {
                println!("[RECOVERY] Not current executor, skipping recovery");
                return;
            }

            let db = match db_opt {
                Some(d) => d,
                None => {
                    eprintln!("[RECOVERY] Firebase not connected, skipping recovery");
                    return;
                }
            };

            println!("[RECOVERY] I am executor, performing client liveness check...");

            // Read all clients from Firebase (NO local cache)
            let firebase_clients = match firebase::read_all_clients(&db).await {
                Ok(clients) => clients,
                Err(e) => {
                    eprintln!("[RECOVERY] Failed to read clients from Firebase: {}", e);
                    return;
                }
            };

            println!("[RECOVERY] Found {} clients in Firebase", firebase_clients.len());

            // For each client marked online, send LIFE_CHECK via TCP
            for (username, client) in firebase_clients {
                if !client.online {
                    continue; // Skip offline clients
                }

                println!("[RECOVERY] Checking {} ({}:{})", username, client.client_ip, client.client_port);

                // Send LIFE_CHECK via TCP to client's P2P port
                let is_alive = match tcp_client::send_life_check(&client.client_ip, client.client_port, &username).await {
                    Ok(alive) => alive,
                    Err(e) => {
                        eprintln!("[RECOVERY] Error checking {}: {}", username, e);
                        false
                    }
                };

                if !is_alive {
                    println!("[RECOVERY] ❌ {} is not responding, marking offline", username);

                    // Mark as offline in Firebase
                    let mut updated_client = client.clone();
                    updated_client.online = false;
                    updated_client.last_seen = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64;

                    if let Err(e) = firebase::write_client(&db, &updated_client).await {
                        eprintln!("[RECOVERY] Failed to update {} in Firebase: {}", username, e);
                    }
                } else {
                    println!("[RECOVERY] ✅ {} is alive", username);
                }
            }

            println!("[RECOVERY] System recovery completed");
        });
    }

    // Stale client cleanup: Remove clients offline for > 2 minutes
    // Only runs on leader (leader writes to Firebase)
    {
        let st = state.clone();
        let cfg_clone = cfg.clone();
        tokio::spawn(async move {
            // Wait for system to stabilize
            tokio::time::sleep(Duration::from_secs(15)).await;

            loop {
                // Run every 60 seconds
                tokio::time::sleep(Duration::from_secs(60)).await;

                // Check if I'm the leader (leader writes to Firebase, NOT executor)
                let (is_leader, db_opt) = {
                    let s = st.read().await;
                    (s.is_leader, s.firestore_db.clone())
                };

                if !is_leader {
                    continue; // Only leader cleans up stale clients
                }

                let db = match db_opt {
                    Some(d) => d,
                    None => {
                        eprintln!("[CLEANUP] Firebase not connected, skipping cleanup");
                        continue;
                    }
                };

                println!("[CLEANUP] Starting stale client cleanup (leader task)...");

                // Read all clients from Firebase
                let firebase_clients = match firebase::read_all_clients(&db).await {
                    Ok(clients) => clients,
                    Err(e) => {
                        eprintln!("[CLEANUP] Failed to read clients from Firebase: {}", e);
                        continue;
                    }
                };

                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;

                let stale_threshold_ms = 2 * 60 * 1000; // 2 minutes in milliseconds

                // Find stale clients (offline for > 2 minutes)
                let mut stale_count = 0;
                for (username, client) in firebase_clients {
                    if !client.online {
                        let offline_duration_ms = now_ms.saturating_sub(client.last_seen);
                        if offline_duration_ms > stale_threshold_ms {
                            println!("[CLEANUP] Removing stale client {} (offline for {}ms)", username, offline_duration_ms);

                            // Delete from Firebase
                            if let Err(e) = firebase::delete_client(&db, &username).await {
                                eprintln!("[CLEANUP] Failed to delete {}: {}", username, e);
                            } else {
                                println!("[CLEANUP] ✅ Deleted stale client {}", username);
                                stale_count += 1;
                            }
                        }
                    }
                }

                if stale_count > 0 {
                    println!("[CLEANUP] ✅ Removed {} stale clients", stale_count);
                } else {
                    println!("[CLEANUP] No stale clients to remove");
                }
            }
        });
    }

    // System monitor for CPU/RAM tracking
    let sys = Arc::new(tokio::sync::Mutex::new(System::new_all()));

    let (leader_tx, leader_rx) = watch::channel::<u32>(0);

    // Election
    {
        let st = state.clone();
        let cfg2 = cfg.clone();
        let tx = leader_tx.clone();
        tokio::spawn(async move {
            election::run_election_loop(st, cfg2, tx).await;
        });
    }

    // Failure simulation (stays independent per your requirements)
    {
        let st = state.clone();
        let cfg2 = cfg.clone();
        tokio::spawn(async move {
            failure::run_failure_simulation(st, cfg2).await;
        });
    }

    // Client-facing UDP server (DEPRECATED - keeping for backward compatibility)
    {
        let st = state.clone();
        let cfg2 = cfg.clone();
        tokio::spawn(async move {
            let _ = udp::run_udp_server(st, cfg2).await;
        });
    }

    // Client-facing TCP server (NEW - primary client interface)
    {
        let st = state.clone();
        let cfg2 = cfg.clone();
        tokio::spawn(async move {
            loop {
                if let Err(e) = tcp_client::run_tcp_client_server(st.clone(), cfg2.clone()).await {
                    println!("[TCP-CLIENT] ⚠️  Server error: {} - restarting in 5s", e);
                    info!(error=%e, "TCP client server error - will restart");
                }
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        });
    }

    // Track leader changes
    {
        let st = state.clone();
        tokio::spawn(async move {
            election::handle_leader_changes(st, leader_rx).await;
        });
    }

    // Assignment channels (with load balancing) - TIER 1 CRITICAL
    {
        let st = state.clone();
        let cfg2 = cfg.clone();
        let sys2 = sys.clone();
        tokio::spawn(async move {
            let _ = assignment::run_assignment_channels(st, cfg2, sys2).await;
        });
    }

    // System stats refresher (ensures CPU averages are accurate) - TIER 1
    {
        let sys_refresh = sys.clone();
        tokio::spawn(async move {
            loop {
                // EPOCH-ALIGNED SLEEP (Tier 1: 5ms precision)
                epoch::sleep_until_next_aligned_tick_t1(500, "sys_stats_refresh").await;
                
                let mut s = sys_refresh.lock().await;
                s.refresh_cpu();
                s.refresh_memory();
            }
        });
    }

    // History table cleanup (every 5 seconds) - TIER 2
    {
        let st = state.clone();
        let cfg2 = cfg.clone();
        tokio::spawn(async move {
            history::run_cleanup_task(st, cfg2).await;
        });
    }

    // History table sync to leader (every 12 seconds, non-leader nodes) - TIER 2
    {
        let st = state.clone();
        let cfg2 = cfg.clone();
        tokio::spawn(async move {
            // Create a temporary socket for sending sync messages
            if let Ok(temp_sock) = tokio::net::UdpSocket::bind("0.0.0.0:0").await {
                history::run_sync_to_leader_task(st, Arc::new(temp_sock), cfg2).await;
            }
        });
    }

    // History table sync by leader (every 15 seconds, leader only) - TIER 2
    {
        let st = state.clone();
        let cfg2 = cfg.clone();
        tokio::spawn(async move {
            // Create a temporary socket for broadcasting
            if let Ok(temp_sock) = tokio::net::UdpSocket::bind("0.0.0.0:0").await {
                history::run_leader_sync_task(st, Arc::new(temp_sock), cfg2).await;
            }
        });
    }

    // ============================================================================
    // NEW PROTOCOL TASKS (Firebase + Executor-Leader + Client Management)
    // ============================================================================

    // Firebase periodic read + broadcast (reads from Firebase every 5s and broadcasts to clients)
    // CRITICAL: Servers do NOT cache DOS locally - Firebase is the ONLY persistent storage
    {
        let st = state.clone();
        let cfg_clone = cfg.clone();
        tokio::spawn(async move {
            loop {
                // Wait until Firebase is connected
                let db = {
                    let s = st.read().await;
                    s.firestore_db.clone()
                };

                if let Some(db) = db {
                    println!("[FIREBASE-BROADCAST] Starting periodic read + broadcast (NO local caching)...");

                    // Run listener (will run forever unless error)
                    if let Err(e) = firebase::listen_dos_changes(db, st.clone(), cfg_clone.clone()).await {
                        println!("[FIREBASE-BROADCAST] ⚠️  Listener error: {} - restarting in 10s", e);
                        info!(error=%e, "Firebase listener error - will restart");
                    }
                }

                // Wait before retry
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        });
    }

    // Executor-Leader communication channel (leader only)
    {
        let st = state.clone();
        let cfg2 = cfg.clone();
        tokio::spawn(async move {
            loop {
                println!("[EXECUTOR-LEADER] Starting executor-leader channel...");

                if let Err(e) = executor_leader::run_executor_leader_channel(st.clone(), cfg2.clone()).await {
                    println!("[EXECUTOR-LEADER] ⚠️  Channel error: {} - restarting in 5s", e);
                    info!(error=%e, "Executor-leader channel error - will restart");
                }

                // Wait before retry
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        });
    }

    // Firebase cleanup: Delete expired access records (every hour)
    {
        let st = state.clone();
        tokio::spawn(async move {
            loop {
                // Wait 1 hour
                tokio::time::sleep(Duration::from_secs(3600)).await;

                // Only leader runs cleanup
                let (is_leader, db) = {
                    let s = st.read().await;
                    (s.is_leader, s.firestore_db.clone())
                };

                if is_leader {
                    if let Some(db) = db {
                        println!("[FIREBASE-CLEANUP] Running cleanup of expired access records...");

                        if let Err(e) = firebase::cleanup_expired_access(&db, st.clone()).await {
                            println!("[FIREBASE-CLEANUP] ⚠️  Cleanup error: {}", e);
                            info!(error=%e, "Firebase cleanup error");
                        } else {
                            println!("[FIREBASE-CLEANUP] ✅ Cleanup completed");
                        }
                    }
                }
            }
        });
    }

    // Periodic metrics printer (every 10s) - TIER 2
    {
        let st = state.clone();
        tokio::spawn(async move {
            loop {
                // EPOCH-ALIGNED SLEEP (Tier 2: 100ms precision)
                epoch::sleep_until_next_aligned_tick_t2(10000, "metrics_printer").await;
                
                let (rec, srv) = {
                    let s = st.read().await;
                    (s.requests_received, s.requests_served)
                };
                let epoch_offset = epoch::epoch_offset_ms();
                println!(
                    "[METRICS] requests_received={} requests_served={} epoch_offset={}ms",
                    rec, srv, epoch_offset
                );
                info!(
                    requests_received = rec,
                    requests_served = srv,
                    epoch_offset,
                    "metrics (epoch-aligned)"
                );
            }
        });
    }

    // Periodic history table printer (every 10s) - TIER 2
    {
        let st = state.clone();
        tokio::spawn(async move {
            loop {
                // EPOCH-ALIGNED SLEEP (Tier 2: 100ms precision)
                epoch::sleep_until_next_aligned_tick_t2(10000, "history_printer").await;
                
                let s = st.read().await;
                let epoch_offset = epoch::epoch_offset_ms();
                
                println!("[HISTORY] Table has {} entries (epoch_offset={}ms):", s.history.len(), epoch_offset);
                if s.history.is_empty() {
                    println!("  (empty)");
                } else {
                    let mut entries: Vec<_> = s.history.iter().collect();
                    entries.sort_by_key(|(req_id, _)| *req_id);
                    
                    for (req_id, record) in entries {
                        let path_display = record.path_to_output_image
                            .as_ref()
                            .map(|p| p.as_str())
                            .unwrap_or("(no local path)");
                        println!(
                            "  req_id={} executor={} path={} timestamp={}",
                            req_id, record.executor_node, path_display, record.timestamp
                        );
                    }
                }
            }
        });
    }

    println!("Server up. Press Ctrl-C to exit.");
    tokio::signal::ctrl_c().await?;
    Ok(())
}

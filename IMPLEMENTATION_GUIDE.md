# Implementation Guide: Client-Server Revamp

## ✅ Completed So Far

### Server Foundation (DONE)
1. ✅ **Firebase dependencies** added to `Cloud-Node/Cargo.toml`
2. ✅ **firebase.rs** created with DosClient/DosAccess structures and all Firebase operations
3. ✅ **config.rs** updated with executor-leader ports (8380/8381/8383)
4. ✅ **state.rs** extended with DOS fields and pending_requests
5. ✅ **executor_leader.rs** created with leader-only Firebase writes
6. ✅ **client_protocol.rs** skeleton created with all message type constants
7. ✅ **Stego/src/lib.rs** updated with new Meta structure
8. ✅ **main.rs** updated with new module declarations

---

## 🚧 Remaining Implementation Tasks

### PRIORITY 1: Complete Server-Side Implementation

#### Task 1: Complete `client_protocol.rs` Handlers

**File**: `Cloud-Node/src/client_protocol.rs`

**What's Missing**: Full implementation of handler functions (currently just skeletons)

**Functions to Implement**:

```rust
// 1. APPROVE_VIEW handler - Most critical
pub async fn handle_approve_view(
    state: SharedState,
    cfg: &Config,
    sock: &UdpSocket,
    owner_addr: SocketAddr,
    data: &[u8],
) -> Result<()> {
    // Parse: req_id, granted_views
    // Lookup pending_request
    // Wait for owner to upload images (OWNER_IMAGE_META + OWNER_IMAGE_CHUNK)
    // Generate image_uuid (use uuid::Uuid::new_v4())
    // Create new Meta: { owner, viewer, image_name, remaining_views, image_uuid }
    // Call stego_service::embed_meta_return_png()
    // Send to leader: EXEC_ADD_ACCESS
    // Send APPROVED + IMAGE_CHUNKs to viewer
    // Remove from pending_requests
}

// 2. DENY_VIEW handler
pub async fn handle_deny_view(
    state: SharedState,
    sock: &UdpSocket,
    owner_addr: SocketAddr,
    data: &[u8],
) -> Result<()> {
    // Parse: req_id
    // Lookup pending_request
    // Send REJECTED to viewer
    // Remove from pending_requests
}

// 3. OWNER_IMAGE_META handler
pub async fn handle_owner_image_meta(
    state: SharedState,
    data: &[u8],
) -> Result<()> {
    // Parse: req_id, true_chunks, true_bytes, cover_chunks, cover_bytes
    // Store in pending_request context
}

// 4. OWNER_IMAGE_CHUNK handler
pub async fn handle_owner_image_chunk(
    state: SharedState,
    data: &[u8],
) -> Result<()> {
    // Parse: req_id, image_type (0=true, 1=cover), seq, chunk_data
    // Assemble chunks in pending_request context
    // When all chunks received, signal ready for embedding
}

// 5. ADJUST_REQUEST handler
pub async fn handle_adjust_request(
    state: SharedState,
    sock: &UdpSocket,
    viewer_addr: SocketAddr,
    data: &[u8],
) -> Result<()> {
    // Parse: req_id, viewer_name, owner_name, image_name, current_views, requested_views
    // Check executor, owner online
    // Create pending_request
    // Send ADJUST_NOTIFICATION to owner
}

// 6. APPROVE_ADJUST handler
pub async fn handle_approve_adjust(
    state: SharedState,
    cfg: &Config,
    sock: &UdpSocket,
    owner_addr: SocketAddr,
    data: &[u8],
) -> Result<()> {
    // Parse: req_id, new_views
    // Lookup access_id in dos_access
    // Update granted_views via EXEC_UPDATE_ACCESS to leader
    // Re-encrypt image with new metadata
    // Send ADJUSTED_VIEWS + new IMAGE_CHUNKs to viewer
}

// 7. REVOKE_REQUEST handler
pub async fn handle_revoke_request(
    state: SharedState,
    cfg: &Config,
    sock: &UdpSocket,
    owner_addr: SocketAddr,
    data: &[u8],
) -> Result<()> {
    // Parse: req_id, viewer_name, image_name
    // Find access_id in dos_access
    // Send EXEC_REVOKE_ACCESS to leader
    // Send DELETE_IMAGE to viewer
    // Send REVOKED confirmation to owner
}

// 8. SYNC_USAGE handler
pub async fn handle_sync_usage(
    state: SharedState,
    cfg: &Config,
    sock: &UdpSocket,
    client_addr: SocketAddr,
    data: &[u8],
) -> Result<()> {
    // Parse: req_id, image_uuid, consumed_offline
    // Find access record by image_uuid
    // Check if revoked
    // If revoked: send REVOKED + DELETE_IMAGE
    // If valid: update consumed_views via EXEC_UPDATE_ACCESS
    // Send SYNC_ACK
}

// 9. REQUEST_VIEW_PERMISSION handler
pub async fn handle_request_view_permission(
    state: SharedState,
    sock: &UdpSocket,
    client_addr: SocketAddr,
    data: &[u8],
) -> Result<()> {
    // Parse: req_id, image_uuid or (owner, viewer, image_name)
    // Check dos_access for record
    // If revoked or consumed >= granted: VIEW_PERMISSION_DENIED
    // Else: VIEW_PERMISSION_GRANTED + increment consumed_views
}
```

**Implementation Tips**:
- Reuse chunking logic from existing `udp.rs` (lines 1036-1342)
- Follow the sticky executor pattern for request ownership
- Use `executor_leader::send_to_leader()` for Firebase writes
- Store chunk assembly state in `PendingRequest` (extend struct as needed)

---

#### Task 2: Modify `udp.rs` to Route New Messages

**File**: `Cloud-Node/src/udp.rs`

**Location**: In `receiver_task()` function, around line 450-550

**Add routing**:

```rust
async fn receiver_task(...) {
    loop {
        let (n, from) = sock.recv_from(&mut buf).await?;
        let msg_type = buf[0];

        match msg_type {
            // NEW PROTOCOL MESSAGES
            client_protocol::REQ => {
                client_protocol::handle_req(state.clone(), &cfg, &sock, from, &buf[1..n]).await?;
            }
            client_protocol::JOIN => {
                client_protocol::handle_join(state.clone(), &cfg, &sock, from, &buf[1..n]).await?;
            }
            client_protocol::CLIENT_PING => {
                client_protocol::handle_client_ping(state.clone(), &sock, from, &buf[1..n]).await?;
            }
            client_protocol::VIEW_REQUEST => {
                client_protocol::handle_view_request(state.clone(), &cfg, &sock, from, &buf[1..n]).await?;
            }
            client_protocol::APPROVE_VIEW => {
                client_protocol::handle_approve_view(state.clone(), &cfg, &sock, from, &buf[1..n]).await?;
            }
            client_protocol::DENY_VIEW => {
                client_protocol::handle_deny_view(state.clone(), &sock, from, &buf[1..n]).await?;
            }
            client_protocol::OWNER_IMAGE_META => {
                client_protocol::handle_owner_image_meta(state.clone(), &buf[1..n]).await?;
            }
            client_protocol::OWNER_IMAGE_CHUNK => {
                client_protocol::handle_owner_image_chunk(state.clone(), &buf[1..n]).await?;
            }
            // ... add all other message types ...

            // EXISTING PROTOCOL (keep for backward compatibility)
            SELECT => { /* existing logic */ }
            REQ_META => { /* existing logic */ }
            REQ_CHUNK => { /* existing logic */ }

            _ => {}
        }
    }
}
```

---

#### Task 3: Add New Tasks to `main.rs`

**File**: `Cloud-Node/src/main.rs`

**Location**: After existing task spawns, around line 120

**Add**:

```rust
// Firebase initialization
let firestore_db = match firebase::init_firestore().await {
    Ok(db) => {
        info!("Firebase connected successfully");
        Some(db)
    }
    Err(e) => {
        warn!("Firebase init failed: {}, continuing with local only", e);
        None
    }
};
state.write().await.firestore_db = firestore_db.clone();

// Firebase real-time listener (all nodes)
if let Some(db) = firestore_db {
    let state_clone = state.clone();
    tokio::spawn(async move {
        if let Err(e) = firebase::listen_dos_changes(db, state_clone).await {
            error!("Firebase listener error: {}", e);
        }
    });
}

// Executor-Leader channel (all nodes, only leader processes)
{
    let state_clone = state.clone();
    let cfg_clone = cfg.clone();
    tokio::spawn(async move {
        if let Err(e) = executor_leader::run_executor_leader_channel(state_clone, cfg_clone).await {
            error!("Executor-leader channel error: {}", e);
        }
    });
}

// Periodic: Cleanup expired access records (leader only, every 1 hour)
{
    let state_clone = state.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(3600)).await;

            let s = state_clone.read().await;
            let is_leader = s.is_leader;
            let db = s.firestore_db.clone();
            drop(s);

            if is_leader {
                if let Some(database) = db {
                    if let Err(e) = firebase::cleanup_expired_access(&database, state_clone.clone()).await {
                        error!("Cleanup error: {}", e);
                    }
                }
            }
        }
    });
}

// Periodic: Check client online status (executor only, every 30s)
{
    let state_clone = state.clone();
    let cfg_clone = cfg.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(30)).await;

            let now = client_protocol::now_ms();
            let timeout_threshold = now - 45_000; // 45 seconds

            let mut s = state_clone.write().await;
            let my_ip = cfg_clone.node_id_to_ip(cfg_clone.node_id).unwrap();
            let is_exec = client_protocol::is_executor(&*s, my_ip);

            if is_exec {
                let mut offline_clients = Vec::new();

                for (name, client) in &mut s.dos_clients {
                    if client.online && client.last_seen < timeout_threshold {
                        client.online = false;
                        offline_clients.push(name.clone());
                    }
                }

                drop(s);

                // Send DELETE_CLIENT to leader for offline clients
                for name in offline_clients {
                    let mut msg = Vec::new();
                    msg.extend((name.len() as u16).to_le_bytes());
                    msg.extend(name.as_bytes());

                    if let Err(e) = executor_leader::send_to_leader(
                        &cfg_clone,
                        executor_leader::EXEC_DELETE_CLIENT,
                        &msg
                    ).await {
                        error!("Failed to delete client {}: {}", name, e);
                    }
                }
            }
        }
    });
}
```

---

### PRIORITY 2: Client-Side Implementation

This is a **complete rewrite** of the client. The old client is a pure sender; the new client is a UDP listener daemon.

#### Task 4: Client Dependencies

**File**: `Client-Node/Cargo.toml`

**Add**:

```toml
uuid = { version = "1", features = ["v4", "serde"] }
```

---

#### Task 5: Create Client Modules

**Create these new files**:

1. **`Client-Node/src/local_state.rs`** - Track offline view counts

```rust
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalState {
    pub images: HashMap<String, ImageState>,  // path -> state
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImageState {
    pub image_uuid: String,
    pub owner: String,
    pub image_name: String,
    pub remaining_views: u32,
    pub consumed_offline: u32,
    pub needs_sync: bool,
}

pub fn load_local_state() -> anyhow::Result<LocalState> {
    let path = "./client_local_state.json";
    if !Path::new(path).exists() {
        return Ok(LocalState {
            images: HashMap::new(),
        });
    }
    let data = fs::read_to_string(path)?;
    Ok(serde_json::from_str(&data)?)
}

pub fn save_local_state(state: &LocalState) -> anyhow::Result<()> {
    let path = "./client_local_state.json";
    let json = serde_json::to_string_pretty(state)?;
    fs::write(path, json)?;
    Ok(())
}
```

2. **`Client-Node/src/dos_c.rs`** - DOS certificate management

```rust
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DosC {
    pub version: u32,
    pub clients: HashMap<String, ClientEntry>,  // username -> images
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientEntry {
    pub client_name: String,
    pub images: Vec<String>,
}

pub fn load_dos_c(path: &str) -> anyhow::Result<DosC> {
    if !Path::new(path).exists() {
        return Ok(DosC {
            version: 0,
            clients: HashMap::new(),
        });
    }
    let data = fs::read_to_string(path)?;
    Ok(serde_json::from_str(&data)?)
}

pub fn save_dos_c(path: &str, dos_c: &DosC) -> anyhow::Result<()> {
    let json = serde_json::to_string_pretty(dos_c)?;
    fs::write(path, json)?;
    Ok(())
}

pub fn dos_c_contains_image(dos_c: &DosC, owner: &str, image_name: &str) -> bool {
    dos_c.clients
        .get(owner)
        .map_or(false, |entry| entry.images.iter().any(|i| i == image_name))
}
```

3. **`Client-Node/src/operations.rs`** - Implement view/adjust/revoke operations

4. **`Client-Node/src/listener.rs`** - UDP listener daemon

5. **`Client-Node/src/sync.rs`** - Offline usage sync logic

---

#### Task 6: Rewrite `Client-Node/src/main.rs`

**New CLI structure**:

```rust
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "client-node")]
struct Cli {
    /// Username
    #[arg(long)]
    username: String,

    /// Server peers (comma-separated)
    #[arg(long, default_value = "10.40.61.79:8180,10.40.58.169:8181,10.40.63.10:8183")]
    server_peers: String,

    /// UDP listen port (0 = random)
    #[arg(long, default_value_t = 0)]
    listen_port: u16,

    /// Directory with images to share
    #[arg(long, default_value = "./my_images")]
    images_dir: String,

    /// Cover image path
    #[arg(long, default_value = "./cover.png")]
    cover_image: String,

    /// DOS-C cache file
    #[arg(long, default_value = "./dos_c.json")]
    dos_c_file: String,

    /// Auto-approve all requests
    #[arg(long, default_value_t = true)]
    auto_accept: bool,

    /// Ping interval (seconds)
    #[arg(long, default_value_t = 10)]
    ping_interval_secs: u64,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run listener daemon
    Listen,

    /// Request to view an image
    View {
        #[arg(long)]
        owner: String,
        #[arg(long)]
        image_name: String,
        #[arg(long)]
        requested_views: u32,
    },

    /// Request to adjust view count
    Adjust {
        #[arg(long)]
        owner: String,
        #[arg(long)]
        image_name: String,
        #[arg(long)]
        new_views: u32,
    },

    /// Revoke access
    Revoke {
        #[arg(long)]
        viewer: String,
        #[arg(long)]
        image_name: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Listen => listener::run_listener_mode(cli).await?,
        Commands::View { owner, image_name, requested_views } => {
            operations::execute_view_request(&cli, owner, image_name, *requested_views).await?;
        }
        Commands::Adjust { owner, image_name, new_views } => {
            operations::execute_adjust_request(&cli, owner, image_name, *new_views).await?;
        }
        Commands::Revoke { viewer, image_name } => {
            operations::execute_revoke_request(&cli, viewer, image_name).await?;
        }
    }

    Ok(())
}
```

**Listener mode implementation** in `listener.rs`:

```rust
pub async fn run_listener_mode(cli: Cli) -> Result<()> {
    // Bind UDP listener
    let sock = UdpSocket::bind(format!("0.0.0.0:{}", cli.listen_port)).await?;
    let actual_port = sock.local_addr()?.port();
    println!("Listening on port {}", actual_port);

    // Load DOS-C
    let mut dos_c = dos_c::load_dos_c(&cli.dos_c_file)?;

    // Send JOIN
    send_join(&sock, &cli.server_peers, &cli.username, actual_port, &cli.images_dir).await?;

    // Spawn ping task
    tokio::spawn(ping_loop(sock.clone(), cli.server_peers.clone(), cli.username.clone(), cli.ping_interval_secs));

    // Spawn sync task
    tokio::spawn(sync::sync_offline_usage(sock.clone(), cli.username.clone()));

    // Main listener loop
    loop {
        let mut buf = vec![0u8; 65536];
        let (n, from) = sock.recv_from(&mut buf).await?;

        let msg_type = buf[0];
        match msg_type {
            VIEW_NOTIFICATION => handle_view_notification(&cli, &sock, from, &buf[1..n]).await?,
            ADJUST_NOTIFICATION => handle_adjust_notification(&cli, &sock, from, &buf[1..n]).await?,
            DOS_UPDATE => handle_dos_update(&mut dos_c, &buf[1..n], &cli.dos_c_file).await?,
            DELETE_IMAGE => handle_delete_image(&buf[1..n]).await?,
            SERVER_PONG => handle_pong(&dos_c, &buf[1..n]).await?,
            _ => {}
        }
    }
}
```

---

### PRIORITY 3: Testing

#### Test Scenario 1: Client Startup

```bash
# Terminal 1: Start server nodes
cd Cloud-Node
cargo build --release

./target/release/server --node-id 1
./target/release/server --node-id 2
./target/release/server --node-id 3

# Terminal 2: Start Alice (owner)
cd Client-Node
cargo build --release
mkdir -p my_images
cp ../test_images/vacation.png my_images/
cp ../test_images/cover.png ./

./target/release/client-node --username alice listen

# Terminal 3: Start Bob (owner)
./target/release/client-node --username bob --listen-port 9001 listen

# Verify: Check server logs for JOIN messages and DOS-C updates
```

#### Test Scenario 2: View Request

```bash
# Terminal 4: Charlie requests to view Alice's image
./target/release/client-node --username charlie view \
  --owner alice \
  --image-name vacation.png \
  --requested-views 5

# Expected flow:
# 1. Charlie sends REQ → ACCEPT
# 2. Charlie sends VIEW_REQUEST
# 3. Server sends VIEW_NOTIFICATION to Alice
# 4. Alice auto-approves (sends APPROVE_VIEW + uploads images)
# 5. Server embeds steganography
# 6. Server sends APPROVED + IMAGE_CHUNKs to Charlie
# 7. Charlie saves encrypted image and metadata
```

#### Test Scenario 3: Offline Viewing

```bash
# Charlie views image offline (disconnect from network)
# Local view counter decrements
# needs_sync flag set

# Reconnect to network
# Auto-sync on next interaction
# Server receives SYNC_USAGE
# consumed_views updated in DOS-S
```

#### Test Scenario 4: Revoke

```bash
# Alice revokes Charlie's access
./target/release/client-node --username alice revoke \
  --viewer charlie \
  --image-name vacation.png

# Expected:
# 1. Server marks access as revoked in DOS-S
# 2. Server sends DELETE_IMAGE to Charlie
# 3. Charlie deletes local encrypted image
```

---

## 📋 Implementation Checklist

### Server (Cloud-Node)
- [x] Add Firebase dependencies
- [x] Create firebase.rs
- [x] Add executor-leader ports to config.rs
- [x] Extend ServerState
- [x] Create executor_leader.rs
- [x] Create client_protocol.rs skeleton
- [ ] **Complete all handler functions in client_protocol.rs**
- [ ] **Modify udp.rs to route new messages**
- [ ] **Add new tasks to main.rs (Firebase, executor-leader, ping checker)**
- [ ] Test server compiles
- [ ] Test Firebase connection
- [ ] Test executor-leader channel

### Stego Library
- [x] Update Meta structure
- [ ] Test embedding/extraction with new metadata

### Client (Client-Node)
- [ ] Add uuid dependency
- [ ] Create local_state.rs
- [ ] Create dos_c.rs
- [ ] Create operations.rs
- [ ] Create listener.rs
- [ ] Create sync.rs
- [ ] Rewrite main.rs with new CLI
- [ ] Test client compiles
- [ ] Test JOIN operation
- [ ] Test VIEW operation
- [ ] Test offline viewing
- [ ] Test sync after offline
- [ ] Test ADJUST operation
- [ ] Test REVOKE operation

### Integration Testing
- [ ] Test full flow: JOIN → VIEW_REQUEST → APPROVE → offline viewing → sync
- [ ] Test leader failover (server-server should still work)
- [ ] Test DOS-C consistency
- [ ] Test concurrent requests
- [ ] Test revoke + sync race condition

---

## ⚠️ Important Notes

1. **Firebase Credentials**: The file `firebase-admin.json` is already present with project `dist-proj-25`

2. **Backward Compatibility**: Old protocol messages (SELECT, REQ_META, etc.) are kept in udp.rs for transition period

3. **Web UI**: Intentionally left broken for now (will be fixed in future iteration)

4. **Security**: This is demo-level security (client-side view count enforcement). For production, implement server-side permission checks before every view.

5. **Testing Order**: Test server changes first before client rewrite

6. **Error Handling**: All handlers should use `tracing::error!()` for errors and not panic

7. **Chunking**: Reuse existing chunk size (1200 bytes) and assembly logic from udp.rs

---

## 🎯 Next Steps

1. **Start with server**: Complete `client_protocol.rs` handler implementations
2. **Test server**: Ensure server compiles and Firebase connects
3. **Client rewrite**: Systematic implementation following the structure above
4. **Integration test**: Full end-to-end testing

The foundation is complete. The remaining work is primarily implementing the handler logic and client operations following the patterns established in the existing code.

---

## 📞 Questions?

If you encounter issues:
1. Check Firebase connection with `firebase::init_firestore()` test
2. Verify message types match between client and server
3. Use `tracing::debug!()` liberally for debugging
4. Test each operation individually before integration

Good luck with the implementation! The architecture is sound and well-structured.

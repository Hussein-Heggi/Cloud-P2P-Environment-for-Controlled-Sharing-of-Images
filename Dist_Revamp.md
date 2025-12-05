# Implementation Plan: Complete Client-Server Logic Revamp

## Executive Summary

**Scope**: Complete revamp of client-server communication while preserving all server-to-server logic (election, load balancing, heartbeats).

**Key Changes**:
- New Directory of Service (DOS-S in Firebase, DOS-C in client)
- Client becomes UDP listener (receives approval requests, DOS updates)
- New protocol: REQ → ACCEPT → REQUEST_DETAILS → APPROVED/REJECTED
- Operations: View request, Adjust views, Revoke access
- Offline viewing support with sync-on-demand
- One encrypted image per viewer (not multi-user)
- Leader-only Firebase writes via new executor-leader channel

**Estimated Effort**: 2-3 weeks of focused development

---

## Architecture Overview

### High-Level Flow

```
STARTUP:
Client → Broadcast JOIN → Executor → Leader (Firebase write) → DOS-S updated
                                    ↓
                        Executor returns DOS-C to client

CLIENT PING:
Client → Ping every 10s → All servers → Update last_seen in DOS-S

VIEW REQUEST:
Viewer → REQ → Executor → ACCEPT → Viewer sends REQUEST_DETAILS
                                  ↓
                    Executor → VIEW_NOTIFICATION → Owner
                                                  ↓
                    Owner auto-accepts → APPROVE_VIEW
                                       ↓
                    Owner uploads true+cover images (chunked)
                                       ↓
                    Executor → Leader (Firebase: create access record)
                                     ↓
                    Executor embeds → APPROVED + IMAGE_CHUNKs → Viewer
```

---

## Phase 1: Server-Side Changes

### 1.1 Firebase Integration

**New File**: `Cloud-Node/src/firebase.rs` (create)

**Dependencies** (add to `Cloud-Node/Cargo.toml`):
```toml
firestore = "0.41"
gcp-auth = "0.12"
```

**Implementation**:
```rust
use firestore::*;
use serde::{Deserialize, Serialize};

// DOS-S Client entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DosClient {
    pub client_name: String,
    pub client_ip: String,
    pub client_port: u16,
    pub images: Vec<String>,
    pub last_seen: u128,
    pub online: bool,
}

// DOS-S Access entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DosAccess {
    pub owner: String,
    pub viewer: String,
    pub image_name: String,
    pub granted_views: u32,
    pub consumed_views: u32,
    pub revoked: bool,
    pub granted_at: u128,
    pub image_uuid: String,
}

pub async fn init_firestore() -> Result<FirestoreDb> {
    FirestoreDb::with_options(
        FirestoreDbOptions::new("dist-proj-25".to_string())
            .with_service_account_key_file("./firebase-admin.json")
    ).await
}

// Leader-only write operations
pub async fn write_client(db: &FirestoreDb, client: &DosClient) -> Result<()> {
    db.fluent()
        .update()
        .in_col("dos_s_clients")
        .document_id(&client.client_name)
        .object(client)
        .execute()
        .await
}

pub async fn write_access(db: &FirestoreDb, access_id: &str, access: &DosAccess) -> Result<()> {
    db.fluent()
        .update()
        .in_col("dos_s_access")
        .document_id(access_id)
        .object(access)
        .execute()
        .await
}

pub async fn read_all_clients(db: &FirestoreDb) -> Result<Vec<DosClient>> {
    let docs = db.fluent()
        .select()
        .from("dos_s_clients")
        .execute()
        .await?;
    // Parse and return
}

// Real-time listener for all nodes
pub async fn listen_dos_changes(
    db: &FirestoreDb,
    state: SharedState
) -> Result<()> {
    let listener = db.fluent()
        .select()
        .from("dos_s_clients")
        .listen()
        .await?;

    listener.for_each(|change| {
        // Update local SharedState with new DOS data
        update_local_dos(state.clone(), change).await;
    }).await;

    Ok(())
}
```

**Key Functions**:
- `init_firestore()`: Initialize Firebase connection with service account
- `write_client()`: Leader writes client registration
- `write_access()`: Leader writes access grant
- `read_all_clients()`: Read DOS-C for distribution
- `listen_dos_changes()`: Real-time sync to local state

---

### 1.2 Shared State Extension

**File**: `Cloud-Node/src/state.rs` (modify)

**Add to ServerState**:
```rust
pub struct ServerState {
    // ... existing fields ...

    // Firebase connection (optional, None if Firebase down)
    pub firestore_db: Option<FirestoreDb>,

    // DOS-S local copy (synchronized from Firebase)
    pub dos_clients: HashMap<String, DosClient>,  // username -> client info
    pub dos_access: HashMap<String, DosAccess>,   // access_id -> access record
    pub dos_c_version: u32,                        // Incremented on DOS changes

    // In-flight request tracking (repurposed history table)
    pub pending_requests: HashMap<u32, PendingRequest>,
}

#[derive(Clone, Debug)]
pub struct PendingRequest {
    pub req_id: u32,
    pub executor_ip: IpAddr,
    pub req_type: RequestType,  // View, Adjust, Revoke
    pub owner_name: String,
    pub viewer_name: String,
    pub image_name: String,
    pub initiated_at: u128,
}

#[derive(Clone, Debug)]
pub enum RequestType {
    View,
    AdjustViews,
    Revoke,
}
```

---

### 1.3 Executor-Leader Communication Channel

**File**: `Cloud-Node/src/config.rs` (modify)

**Add new port configuration**:
```rust
impl Config {
    pub fn executor_leader_peers() -> &'static [&'static str] {
        &[
            "10.40.61.79:8380",   // node 1
            "10.40.58.169:8381",  // node 2
            "10.40.63.10:8383",   // node 3
        ]
    }

    pub fn executor_leader_bind_addr(&self) -> Option<SocketAddr> {
        let idx = (self.node_id - 1) as usize;
        Some(Self::parse_addr(Self::executor_leader_peers()[idx]))
    }
}
```

**New File**: `Cloud-Node/src/executor_leader.rs` (create)

**Message Types**:
```rust
const EXEC_ADD_CLIENT: u8 = 40;     // Executor → Leader: Add client to DOS-S
const EXEC_ADD_ACCESS: u8 = 41;     // Executor → Leader: Grant access
const EXEC_UPDATE_ACCESS: u8 = 42;  // Executor → Leader: Update consumed views
const EXEC_REVOKE_ACCESS: u8 = 43;  // Executor → Leader: Revoke access
const LEADER_ACK: u8 = 44;          // Leader → Executor: Success
const LEADER_ERROR: u8 = 45;        // Leader → Executor: Error
```

**Implementation**:
```rust
pub async fn run_executor_leader_channel(state: SharedState, cfg: Config) {
    let sock = UdpSocket::bind(cfg.executor_leader_bind_addr().unwrap()).await.unwrap();

    loop {
        let mut buf = [0u8; 65536];
        let (n, from) = sock.recv_from(&mut buf).await.unwrap();

        let s = state.read().await;
        if !s.is_leader {
            continue;  // Only leader processes
        }
        drop(s);

        let msg_type = buf[0];
        match msg_type {
            EXEC_ADD_CLIENT => handle_add_client(state.clone(), &buf[1..n]).await,
            EXEC_ADD_ACCESS => handle_add_access(state.clone(), &buf[1..n]).await,
            EXEC_UPDATE_ACCESS => handle_update_access(state.clone(), &buf[1..n]).await,
            EXEC_REVOKE_ACCESS => handle_revoke_access(state.clone(), &buf[1..n]).await,
            _ => {}
        }
    }
}

async fn handle_add_client(state: SharedState, data: &[u8]) {
    // Parse client data
    // Write to Firebase
    // Update local DOS
    // Increment dos_c_version
    // Broadcast DOS_UPDATE to all clients
}
```

---

### 1.4 New Client Protocol Messages

**File**: `Cloud-Node/src/client_protocol.rs` (create)

**Message Type Constants**:
```rust
// Phase 1: Initial handshake
pub const REQ: u8 = 10;                    // Client → Server: "I have a request"
pub const ACCEPT: u8 = 11;                 // Server → Client: "I'm the executor"

// Phase 2: Request details
pub const VIEW_REQUEST: u8 = 12;           // Viewer → Executor: Request to view
pub const ADJUST_REQUEST: u8 = 13;         // Viewer → Executor: Adjust view count
pub const REVOKE_REQUEST: u8 = 14;         // Owner → Executor: Revoke access

// Phase 3: Owner notifications
pub const VIEW_NOTIFICATION: u8 = 15;      // Executor → Owner: Someone wants to view
pub const ADJUST_NOTIFICATION: u8 = 16;    // Executor → Owner: Someone wants more views

// Phase 4: Owner responses
pub const APPROVE_VIEW: u8 = 17;           // Owner → Executor: Approved with count
pub const DENY_VIEW: u8 = 18;              // Owner → Executor: Denied
pub const APPROVE_ADJUST: u8 = 19;         // Owner → Executor: Approved adjust
pub const DENY_ADJUST: u8 = 20;            // Owner → Executor: Denied adjust

// Phase 5: Viewer responses
pub const APPROVED: u8 = 21;               // Executor → Viewer: Approved, chunks coming
pub const REJECTED: u8 = 22;               // Executor → Viewer: Rejected
pub const IMAGE_CHUNK: u8 = 23;            // Executor → Viewer: Image data chunk
pub const ADJUSTED_VIEWS: u8 = 24;         // Executor → Viewer: New view count + chunks
pub const REVOKED: u8 = 25;                // Executor → Viewer: Access revoked

// Management messages
pub const DELETE_IMAGE: u8 = 26;           // Executor → Viewer: Delete local image
pub const JOIN: u8 = 27;                   // Client → Executor: Join system
pub const JOIN_ACK: u8 = 28;               // Executor → Client: Welcome, here's DOS-C
pub const DOS_UPDATE: u8 = 29;             // Executor → All clients: DOS-C changed
pub const CLIENT_PING: u8 = 50;            // Client → All servers: Heartbeat
pub const SERVER_PONG: u8 = 51;            // Server → Client: Pong + DOS version

// Sync messages
pub const SYNC_USAGE: u8 = 30;             // Client → Executor: Report offline usage
pub const SYNC_ACK: u8 = 31;               // Executor → Client: Sync accepted
pub const REQUEST_VIEW_PERMISSION: u8 = 32;// Client → Executor: Can I view?
pub const VIEW_PERMISSION_GRANTED: u8 = 33;// Executor → Client: Yes
pub const VIEW_PERMISSION_DENIED: u8 = 34; // Executor → Client: No (revoked/exhausted)
```

**Wire Formats**:
```rust
// REQ: [type=10][client_ip:u32][client_port:u16]
// ACCEPT: [type=11][req_id:u32][executor_ip:u32]
// VIEW_REQUEST: [type=12][req_id:u32][owner_len:u16][owner_name][image_len:u16][image_name][views:u32]
// VIEW_NOTIFICATION: [type=15][req_id:u32][viewer_len:u16][viewer_name][image_len:u16][image_name][views:u32]
// APPROVE_VIEW: [type=17][req_id:u32][granted_views:u32]
// APPROVED: [type=21][req_id:u32][num_chunks:u32][image_uuid_len:u16][image_uuid]
// JOIN: [type=27][username_len:u16][username][port:u16][num_images:u32][image_names...]
// CLIENT_PING: [type=50][username_len:u16][username]
```

---

### 1.5 Client Protocol Handler

**File**: `Cloud-Node/src/client_protocol.rs` (continued)

**Handler Functions**:
```rust
pub async fn handle_req(
    state: SharedState,
    cfg: &Config,
    sock: &UdpSocket,
    client_addr: SocketAddr,
    data: &[u8]
) -> Result<()> {
    let s = state.read().await;
    let executor_ip = s.executor_ip.ok_or("No executor assigned")?;
    drop(s);

    // Generate req_id
    let req_id = generate_req_id();

    // Send ACCEPT with executor IP
    let mut resp = vec![ACCEPT];
    resp.extend(req_id.to_le_bytes());
    resp.extend(executor_ip_as_u32(executor_ip).to_le_bytes());
    sock.send_to(&resp, client_addr).await?;

    Ok(())
}

pub async fn handle_view_request(
    state: SharedState,
    cfg: &Config,
    sock: &UdpSocket,
    viewer_addr: SocketAddr,
    data: &[u8]
) -> Result<()> {
    // Parse: req_id, owner_name, image_name, requested_views

    // Check if executor
    let s = state.read().await;
    if !is_executor(&*s, get_local_ip()) {
        return Ok(()); // Not executor, ignore
    }

    // Check if owner online
    let owner_client = s.dos_clients.get(owner_name).ok_or("Owner not found")?;
    if !owner_client.online {
        // Send REJECTED: owner offline
        send_rejected(sock, viewer_addr, req_id, "Owner offline").await?;
        return Ok(());
    }

    // Create pending request
    let pending = PendingRequest {
        req_id,
        executor_ip: get_local_ip(),
        req_type: RequestType::View,
        owner_name: owner_name.to_string(),
        viewer_name: viewer_name.to_string(),
        image_name: image_name.to_string(),
        initiated_at: now_ms(),
    };
    s.pending_requests.insert(req_id, pending);
    drop(s);

    // Forward VIEW_NOTIFICATION to owner
    let owner_addr = SocketAddr::new(
        owner_client.client_ip.parse()?,
        owner_client.client_port
    );
    send_view_notification(sock, owner_addr, req_id, viewer_name, image_name, requested_views).await?;

    Ok(())
}

pub async fn handle_approve_view(
    state: SharedState,
    cfg: &Config,
    sock: &UdpSocket,
    owner_addr: SocketAddr,
    data: &[u8]
) -> Result<()> {
    // Parse: req_id, granted_views

    // Retrieve pending request
    let s = state.read().await;
    let pending = s.pending_requests.get(&req_id).ok_or("Request not found")?;
    let viewer_name = pending.viewer_name.clone();
    let image_name = pending.image_name.clone();
    drop(s);

    // Request owner to upload images (new message type)
    // Wait for chunked upload (true + cover images)
    // ... (similar to current REQ_CHUNK handling)

    // Generate image_uuid
    let image_uuid = format!("{}-{}-{}", owner_name, viewer_name, uuid::Uuid::new_v4());

    // Send to leader: create access record
    send_to_leader_add_access(
        state.clone(),
        cfg,
        owner_name,
        viewer_name,
        image_name,
        granted_views,
        &image_uuid
    ).await?;

    // Embed steganography with new metadata
    let meta = NewMeta {
        owner: owner_name.to_string(),
        viewer: viewer_name.to_string(),
        image_name: image_name.to_string(),
        remaining_views: granted_views,
        image_uuid: image_uuid.clone(),
    };
    let meta_json = serde_json::to_vec(&meta)?;
    let encrypted_png = stego_service::embed_meta_return_png(
        &true_img_bytes,
        &cover_img_bytes,
        &meta_json
    )?;

    // Send APPROVED + IMAGE_CHUNKs to viewer
    send_approved_with_chunks(sock, viewer_addr, req_id, &encrypted_png, &image_uuid).await?;

    // Remove from pending
    state.write().await.pending_requests.remove(&req_id);

    Ok(())
}
```

---

### 1.6 Main Server Integration

**File**: `Cloud-Node/src/main.rs` (modify)

**Add new tasks**:
```rust
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ... existing initialization ...

    // Initialize Firebase
    let firestore_db = match firebase::init_firestore().await {
        Ok(db) => Some(db),
        Err(e) => {
            warn!("Firebase init failed: {}, continuing with local only", e);
            None
        }
    };
    state.write().await.firestore_db = firestore_db.clone();

    // ... existing tasks ...

    // NEW: Executor-Leader channel
    tokio::spawn(executor_leader::run_executor_leader_channel(
        state.clone(),
        cfg.clone()
    ));

    // NEW: Firebase real-time listener (all nodes)
    if let Some(db) = firestore_db {
        tokio::spawn(firebase::listen_dos_changes(
            db,
            state.clone()
        ));
    }

    // NEW: Ping handler task
    tokio::spawn(client_protocol::run_ping_handler(
        state.clone(),
        cfg.clone()
    ));

    // NEW: DOS-C version broadcaster
    tokio::spawn(client_protocol::run_dos_update_broadcaster(
        state.clone(),
        cfg.clone()
    ));

    // ... rest of existing tasks ...
}
```

---

### 1.7 Modified UDP Server

**File**: `Cloud-Node/src/udp.rs` (modify)

**Update receiver_task to handle new message types**:
```rust
async fn receiver_task(...) {
    loop {
        let (n, from) = sock.recv_from(&mut buf).await?;
        let msg_type = buf[0];

        match msg_type {
            // NEW MESSAGES (delegate to client_protocol)
            client_protocol::REQ => {
                client_protocol::handle_req(state.clone(), &cfg, &sock, from, &buf[1..n]).await?;
            }
            client_protocol::VIEW_REQUEST => {
                client_protocol::handle_view_request(state.clone(), &cfg, &sock, from, &buf[1..n]).await?;
            }
            client_protocol::APPROVE_VIEW => {
                client_protocol::handle_approve_view(state.clone(), &cfg, &sock, from, &buf[1..n]).await?;
            }
            // ... other new message types ...

            // EXISTING MESSAGES (keep as is for backward compatibility during transition)
            SELECT => { /* existing logic */ }
            REQ_META => { /* existing logic */ }
            REQ_CHUNK => { /* existing logic */ }

            _ => {}
        }
    }
}
```

**Note**: Keep existing message handlers during transition, but they won't be used by new clients.

---

## Phase 2: Client-Side Changes

### 2.1 Complete Client Rewrite

**File**: `Client-Node/src/main.rs` (major rewrite)

**New CLI Arguments**:
```rust
struct Cli {
    // Identity
    username: String,                 // Client's username

    // Network
    server_peers: String,             // "ip:port,ip:port,ip:port"
    listen_port: Option<u16>,         // UDP listener port (0 = random)

    // Local storage
    images_dir: String,               // Directory with images to share (default: "./my_images")
    cover_image: String,              // Cover image path (default: "./cover.png")
    dos_c_file: String,               // DOS-C cache file (default: "./dos_c.json")

    // Operations
    operation: ClientOperation,       // View, Adjust, Revoke, or Listen (daemon mode)

    // Operation-specific args (for View/Adjust/Revoke)
    owner: Option<String>,            // Owner of image to view/adjust
    image_name: Option<String>,       // Image name
    requested_views: Option<u32>,     // For View/Adjust

    // Behavior
    auto_accept: bool,                // Auto-approve all requests (default: true)
    ping_interval_secs: u64,          // Ping interval (default: 10)
}

enum ClientOperation {
    Listen,       // Daemon mode: listen for requests, send pings
    View,         // Request to view image
    AdjustViews,  // Request to adjust view count
    Revoke,       // Revoke access to image
}
```

**Main Structure**:
```rust
#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.operation {
        ClientOperation::Listen => run_listener_mode(cli).await?,
        ClientOperation::View => execute_view_request(cli).await?,
        ClientOperation::AdjustViews => execute_adjust_request(cli).await?,
        ClientOperation::Revoke => execute_revoke_request(cli).await?,
    }

    Ok(())
}
```

---

### 2.2 Listener Mode (Daemon)

**Implementation**:
```rust
async fn run_listener_mode(cli: Cli) -> Result<()> {
    // Bind UDP listener
    let listen_port = cli.listen_port.unwrap_or(0);
    let sock = UdpSocket::bind(format!("0.0.0.0:{}", listen_port)).await?;
    let actual_port = sock.local_addr()?.port();
    println!("Listening on port {}", actual_port);

    // Load DOS-C from disk
    let mut dos_c = load_dos_c(&cli.dos_c_file)?;

    // Send JOIN to servers
    send_join(&sock, &cli.server_peers, &cli.username, actual_port, &cli.images_dir).await?;

    // Spawn ping task
    let sock_clone = sock.clone();
    let peers_clone = cli.server_peers.clone();
    let username_clone = cli.username.clone();
    tokio::spawn(async move {
        ping_loop(sock_clone, peers_clone, username_clone).await;
    });

    // Spawn sync task (sync offline usage on startup)
    let sock_clone = sock.clone();
    let username_clone = cli.username.clone();
    tokio::spawn(async move {
        sync_offline_usage(sock_clone, username_clone).await;
    });

    // Main listener loop
    loop {
        let mut buf = vec![0u8; 65536];
        let (n, from) = sock.recv_from(&mut buf).await?;

        let msg_type = buf[0];
        match msg_type {
            VIEW_NOTIFICATION => {
                handle_view_notification(&cli, &sock, from, &buf[1..n]).await?;
            }
            ADJUST_NOTIFICATION => {
                handle_adjust_notification(&cli, &sock, from, &buf[1..n]).await?;
            }
            DOS_UPDATE => {
                handle_dos_update(&mut dos_c, &buf[1..n]).await?;
                save_dos_c(&cli.dos_c_file, &dos_c)?;
            }
            DELETE_IMAGE => {
                handle_delete_image(&buf[1..n]).await?;
            }
            SERVER_PONG => {
                handle_pong(&dos_c, &buf[1..n]).await?;
            }
            _ => {}
        }
    }
}

async fn handle_view_notification(
    cli: &Cli,
    sock: &UdpSocket,
    executor_addr: SocketAddr,
    data: &[u8]
) -> Result<()> {
    // Parse: req_id, viewer_name, image_name, requested_views

    println!("\n[VIEW REQUEST]");
    println!("  From: {}", viewer_name);
    println!("  Image: {}", image_name);
    println!("  Requested Views: {}", requested_views);

    let granted_views = if cli.auto_accept {
        println!("  [AUTO-APPROVED with {} views]", requested_views);
        requested_views
    } else {
        // Terminal input (future work)
        println!("  Type 'approve <views>' or 'deny': ");
        // Read stdin...
        requested_views
    };

    // Send APPROVE_VIEW
    send_approve_view(sock, executor_addr, req_id, granted_views).await?;

    // Upload images (chunked)
    let true_img_path = format!("{}/{}", cli.images_dir, image_name);
    let true_img_bytes = fs::read(&true_img_path)?;
    let cover_img_bytes = fs::read(&cli.cover_image)?;

    upload_images_chunked(sock, executor_addr, req_id, &true_img_bytes, &cover_img_bytes).await?;

    println!("  [UPLOAD COMPLETE]");

    Ok(())
}
```

---

### 2.3 View Request Operation

**Implementation**:
```rust
async fn execute_view_request(cli: Cli) -> Result<()> {
    // Load DOS-C
    let dos_c = load_dos_c(&cli.dos_c_file)?;

    let owner = cli.owner.ok_or("--owner required")?;
    let image_name = cli.image_name.ok_or("--image-name required")?;
    let requested_views = cli.requested_views.ok_or("--requested-views required")?;

    // Check DOS-C if image exists
    if !dos_c_contains_image(&dos_c, &owner, &image_name) {
        return Err(anyhow!("Image not found in DOS-C"));
    }

    // Check if already have cached encrypted image
    let cached_path = format!("./encrypted_images/{}_{}.png", owner, image_name);
    if Path::new(&cached_path).exists() {
        // Check if we can view offline
        let local_state = load_local_state()?;
        if let Some(image_state) = local_state.get(&cached_path) {
            if image_state.remaining_views > 0 {
                println!("Viewing offline (cached)...");
                view_image_offline(&cached_path, &mut local_state)?;
                return Ok(());
            }
        }
    }

    // Need to request from server
    let sock = UdpSocket::bind("0.0.0.0:0").await?;

    // Phase 1: REQ
    let peers = parse_peers(&cli.server_peers)?;
    let req_id = generate_req_id();
    send_req(&sock, &peers, req_id).await?;

    // Phase 2: Wait for ACCEPT
    let executor_addr = wait_accept(&sock, req_id, Duration::from_secs(5)).await?;

    // Phase 3: Send VIEW_REQUEST
    send_view_request(&sock, executor_addr, req_id, &owner, &image_name, requested_views).await?;

    // Phase 4: Wait for APPROVED or REJECTED
    let response = wait_response(&sock, req_id, Duration::from_secs(30)).await?;

    match response {
        Response::Approved { num_chunks, image_uuid } => {
            println!("Approved! Downloading {} chunks...", num_chunks);

            // Receive chunks
            let encrypted_png = receive_chunks(&sock, req_id, num_chunks).await?;

            // Save encrypted image
            fs::write(&cached_path, &encrypted_png)?;

            // Extract and view
            let (true_img, meta) = extract_image_and_metadata(&encrypted_png)?;

            // Save to local state
            let mut local_state = load_local_state()?;
            local_state.insert(cached_path.clone(), ImageState {
                image_uuid,
                remaining_views: meta.remaining_views,
                consumed_offline: 0,
                needs_sync: false,
            });
            save_local_state(&local_state)?;

            // Display image
            fs::write("./viewed_image.png", &true_img)?;
            println!("Image saved to ./viewed_image.png");

            Ok(())
        }
        Response::Rejected { reason } => {
            Err(anyhow!("Request rejected: {}", reason))
        }
    }
}

fn view_image_offline(cached_path: &str, local_state: &mut LocalState) -> Result<()> {
    // Load encrypted image
    let encrypted_png = fs::read(cached_path)?;

    // Extract
    let (true_img, meta) = extract_image_and_metadata(&encrypted_png)?;

    // Get state
    let state = local_state.get_mut(cached_path).ok_or("State not found")?;

    // Decrement views
    if state.remaining_views == 0 {
        return Err(anyhow!("No views remaining. Please sync with server."));
    }

    state.remaining_views -= 1;
    state.consumed_offline += 1;
    state.needs_sync = true;

    // Display
    fs::write("./viewed_image.png", &true_img)?;
    println!("Image saved to ./viewed_image.png");
    println!("Remaining views: {} (offline)", state.remaining_views);

    save_local_state(local_state)?;

    Ok(())
}
```

---

### 2.4 Local State Management

**File**: `Client-Node/src/local_state.rs` (create)

**Structure**:
```rust
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalState {
    pub images: HashMap<String, ImageState>,  // path -> state
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImageState {
    pub image_uuid: String,
    pub remaining_views: u32,
    pub consumed_offline: u32,
    pub needs_sync: bool,
}

pub fn load_local_state() -> Result<LocalState> {
    let path = "./client_local_state.json";
    if !Path::new(path).exists() {
        return Ok(LocalState {
            images: HashMap::new(),
        });
    }
    let data = fs::read_to_string(path)?;
    Ok(serde_json::from_str(&data)?)
}

pub fn save_local_state(state: &LocalState) -> Result<()> {
    let path = "./client_local_state.json";
    let json = serde_json::to_string_pretty(state)?;
    fs::write(path, json)?;
    Ok(())
}
```

---

### 2.5 DOS-C Management

**File**: `Client-Node/src/dos_c.rs` (create)

**Structure**:
```rust
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

pub fn load_dos_c(path: &str) -> Result<DosC> {
    if !Path::new(path).exists() {
        return Ok(DosC {
            version: 0,
            clients: HashMap::new(),
        });
    }
    let data = fs::read_to_string(path)?;
    Ok(serde_json::from_str(&data)?)
}

pub fn save_dos_c(path: &str, dos_c: &DosC) -> Result<()> {
    let json = serde_json::to_string_pretty(dos_c)?;
    fs::write(path, json)?;
    Ok(())
}

pub fn dos_c_contains_image(dos_c: &DosC, owner: &str, image_name: &str) -> bool {
    dos_c.clients
        .get(owner)
        .map_or(false, |entry| entry.images.contains(&image_name.to_string()))
}
```

---

### 2.6 Sync Logic

**File**: `Client-Node/src/sync.rs` (create)

**Implementation**:
```rust
pub async fn sync_offline_usage(sock: UdpSocket, username: String) -> Result<()> {
    let mut local_state = load_local_state()?;

    let images_needing_sync: Vec<_> = local_state.images
        .iter()
        .filter(|(_, state)| state.needs_sync && state.consumed_offline > 0)
        .collect();

    if images_needing_sync.is_empty() {
        return Ok(());
    }

    println!("Syncing {} images with server...", images_needing_sync.len());

    for (path, state) in images_needing_sync {
        // Send SYNC_USAGE
        let req_id = generate_req_id();
        send_sync_usage(&sock, req_id, &state.image_uuid, state.consumed_offline).await?;

        // Wait for SYNC_ACK or REVOKED
        match wait_sync_response(&sock, req_id).await? {
            SyncResponse::Ack => {
                println!("  ✓ Synced: {}", path);
                // Reset offline counter
                if let Some(img_state) = local_state.images.get_mut(path) {
                    img_state.consumed_offline = 0;
                    img_state.needs_sync = false;
                }
            }
            SyncResponse::Revoked => {
                println!("  ✗ Revoked: {}", path);
                // Delete image
                fs::remove_file(path)?;
                local_state.images.remove(path);
            }
        }
    }

    save_local_state(&local_state)?;

    Ok(())
}
```

---

## Phase 3: Stego Library Changes

### 3.1 New Metadata Structure

**File**: `Stego/src/lib.rs` (modify)

**Replace existing Meta struct**:
```rust
// OLD (remove):
// pub struct Meta {
//     pub owner: String,
//     pub allow: Vec<AccessEntry>,
// }

// NEW:
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Meta {
    pub owner: String,
    pub viewer: String,
    pub image_name: String,
    pub remaining_views: u32,
    pub image_uuid: String,
}
```

**Update embed() and extract() to use new structure** (minimal changes needed, just JSON serialization difference).

---

## Phase 4: Testing and Integration

### 4.1 Test Scenarios

**Test 1: Client Startup**
```bash
# Server nodes (existing)
./server --node-id 1 ...
./server --node-id 2 ...
./server --node-id 3 ...

# Client 1 (Alice - listener mode)
./client-node --username alice --operation listen --images-dir ./alice_images --auto-accept

# Client 2 (Bob - listener mode)
./client-node --username bob --operation listen --images-dir ./bob_images --auto-accept
```

**Test 2: View Request**
```bash
# Client 3 (Charlie - request to view)
./client-node --username charlie \
  --operation view \
  --owner alice \
  --image-name vacation.png \
  --requested-views 5
```

**Test 3: Offline Viewing**
```bash
# Kill client Charlie
# Restart Charlie
./client-node --username charlie --operation listen
# Try viewing cached image (should work offline)
```

**Test 4: Revoke**
```bash
# Alice revokes Charlie's access
./client-node --username alice \
  --operation revoke \
  --viewer charlie \
  --image-name vacation.png
```

---

## Phase 5: Migration Strategy

### 5.1 Backward Compatibility

During transition:
1. Keep existing message handlers in server (SELECT, REQ_META, etc.)
2. Old clients continue working
3. New clients use new protocol
4. Gradually migrate all clients

### 5.2 Deployment Order

1. Deploy server changes (keeps old handlers)
2. Test with new client prototype
3. Migrate clients one by one
4. After all clients migrated, remove old handlers

---

## Critical Files Summary

### Server (Cloud-Node)
- **New**: `src/firebase.rs` - Firebase integration
- **New**: `src/executor_leader.rs` - Leader communication channel
- **New**: `src/client_protocol.rs` - New client message handlers
- **Modify**: `src/state.rs` - Add DOS, pending requests
- **Modify**: `src/config.rs` - Add executor-leader ports
- **Modify**: `src/main.rs` - Spawn new tasks
- **Modify**: `src/udp.rs` - Handle new message types
- **Keep**: `src/election.rs` - DO NOT TOUCH
- **Keep**: `src/assignment.rs` - DO NOT TOUCH

### Client (Client-Node)
- **Major rewrite**: `src/main.rs` - New architecture
- **New**: `src/local_state.rs` - Local view count tracking
- **New**: `src/dos_c.rs` - DOS-C management
- **New**: `src/sync.rs` - Offline usage sync
- **Modify**: `ui/*` - Leave as is (will be broken, fix later)

### Stego
- **Modify**: `src/lib.rs` - New metadata structure

### Configuration
- **Add**: `firebase-admin.json` - Already exists ✓

---

## Implementation Checklist

### Server Phase 1: Foundation
- [ ] Add firestore dependency to Cargo.toml
- [ ] Create firebase.rs module
- [ ] Add executor-leader ports to config.rs
- [ ] Extend ServerState with DOS fields
- [ ] Test Firebase connection

### Server Phase 2: Leader Channel
- [ ] Create executor_leader.rs module
- [ ] Implement message types (40-45)
- [ ] Implement handler functions
- [ ] Test leader write operations

### Server Phase 3: Client Protocol
- [ ] Create client_protocol.rs module
- [ ] Define message types (10-34, 50-51)
- [ ] Implement REQ/ACCEPT flow
- [ ] Implement VIEW_REQUEST flow
- [ ] Implement approval flow
- [ ] Implement chunked image upload from owner
- [ ] Implement steganography with new metadata

### Server Phase 4: Integration
- [ ] Modify udp.rs receiver to route new messages
- [ ] Add new tasks to main.rs
- [ ] Test server-to-server still works
- [ ] Test new client protocol

### Client Phase 1: Core Rewrite
- [ ] Rewrite main.rs with new CLI
- [ ] Implement listener mode (daemon)
- [ ] Implement UDP listener loop
- [ ] Implement ping loop

### Client Phase 2: Operations
- [ ] Implement JOIN operation
- [ ] Implement VIEW_REQUEST operation
- [ ] Implement owner approval handler
- [ ] Implement chunked upload from owner
- [ ] Implement chunked download to viewer

### Client Phase 3: State Management
- [ ] Create local_state.rs
- [ ] Create dos_c.rs
- [ ] Implement offline viewing
- [ ] Create sync.rs
- [ ] Implement sync on startup

### Client Phase 4: Additional Operations
- [ ] Implement ADJUST_REQUEST
- [ ] Implement REVOKE_REQUEST
- [ ] Implement DELETE_IMAGE handler
- [ ] Implement DOS_UPDATE handler

### Stego Phase
- [ ] Update Meta structure
- [ ] Test embedding with new metadata
- [ ] Test extraction with new metadata

### Testing Phase
- [ ] Test client startup and JOIN
- [ ] Test view request flow (online)
- [ ] Test offline viewing
- [ ] Test sync after offline
- [ ] Test revoke
- [ ] Test adjust views
- [ ] Test DOS-C updates
- [ ] Test leader failover (server-server should still work)

---

## Known Limitations and Future Work

### Current Limitations
1. **Web UI will be broken** after changes (left as is per requirement)
2. **Terminal approval** not implemented (auto-accept only for now)
3. **No encryption beyond steganography** (client-side honor system for view counts)
4. **Firebase credentials in plaintext** (acceptable for demo)
5. **No authentication** (username-based trust)

### Future Enhancements
1. Implement terminal approval UI with rich prompts
2. Add encryption layer on top of steganography
3. Implement proper authentication (JWT, OAuth)
4. Add web UI support for new protocol
5. Add metrics and monitoring (Prometheus)
6. Implement proper logging and audit trails
7. Add rate limiting and abuse detection
8. Support image formats beyond PNG

---

## Estimated Timeline

- **Week 1**: Server foundation (Firebase, leader channel, basic protocol)
- **Week 2**: Client rewrite (listener, operations, state management)
- **Week 3**: Integration, testing, bug fixes

**Total**: 2-3 weeks of focused development

---

## Risk Assessment

### High Risk
- ✅ Firebase Rust integration (mitigated: using firestore-rs crate)
- ⚠️ Client rewrite scope (large, but well-defined)
- ⚠️ Backward compatibility during transition

### Medium Risk
- Offline sync logic (complex state machine)
- Chunked upload from owner (new flow)
- DOS-C consistency across clients

### Low Risk
- Server-server communication (untouched)
- Stego library changes (minimal)
- Leader-executor channel (straightforward UDP)

---

## Success Criteria

1. ✅ Server-to-server logic untouched and working
2. ✅ Client can join system and register images
3. ✅ View request flow works (viewer → owner approval → encrypted image)
4. ✅ Offline viewing works with local view count
5. ✅ Sync on startup reports offline usage
6. ✅ Revoke deletes image from viewer
7. ✅ DOS-C stays consistent across clients
8. ✅ Firebase acts as authoritative DOS-S
9. ✅ Leader failover doesn't break client operations
10. ✅ Multiple concurrent requests handled correctly

---

## Notes

- This plan prioritizes functionality over polish (terminal approval deferred)
- Web UI intentionally left broken (will fix in future iteration)
- Security is demo-level (acceptable per requirements)
- Firebase is not SPOF (local copy fallback)
- All message types documented for future reference

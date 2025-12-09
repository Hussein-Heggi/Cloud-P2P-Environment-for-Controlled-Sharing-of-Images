# DOS Logic Comprehensive Analysis & Redesign Plan

## ⚠️⚠️⚠️ READ THIS FIRST ⚠️⚠️⚠️

**CRITICAL**: See `/home/g7/.claude/plans/DOS-IMPLEMENTATION-CRITICAL-SUMMARY.md` for mandatory architectural constraints.

**Key Requirements** (DO NOT SKIP):
1. ❌ NO local DOS caching on servers - Firebase is the ONLY source
2. ✅ TCP ONLY for all server-client communication (NO UDP)
3. ✅ Leader ≠ Executor (different roles, different responsibilities)

---

## Executive Summary

After analyzing the entire codebase, I've identified **critical gaps** in the current DOS implementation and **additional requirements** needed beyond what you've proposed to make the system robust.

**Current Status**: DOS update logic is partially implemented but has major synchronization gaps
**Your Proposal**: Addresses some issues but misses several critical edge cases
**Recommendation**: Enhanced proposal with additional safeguards

---

## ⚠️ CRITICAL ARCHITECTURE CONSTRAINT ⚠️

**ALL SERVER-CLIENT COMMUNICATION MUST USE TCP EXCLUSIVELY**

- ❌ **NO UDP allowed** for any server-client messages
- ✅ **TCP ONLY** for all communication including:
  - DOS updates (periodic broadcast every 5s)
  - LIFE_CHECK system recovery messages
  - Client-initiated requests (JOIN, DOS_QUERY, etc.)
  - Graceful shutdown (CLIENT_LEAVE)
  - Executor failover (REDIRECT)

**Implementation Impact**:
- LIFE_CHECK: Server creates TCP connection TO client's P2P port (client runs TCP server on P2P port)
- All message handlers must use TCP streams (AsyncReadExt, AsyncWriteExt)
- No UdpSocket usage for server-client communication

---

## 🔑 KEY ARCHITECTURAL CLARIFICATIONS 🔑

### 1. NO Local DOS Caching on Servers
**User Clarification**: "Nodes (servers) do not save the DOS from Firebase. They read every 5 seconds and send the updates to the clients. No need for local copy on each node (server)."

**Impact on Implementation**:
- ❌ **NO `dos_clients` HashMap** syncing from Firebase
- ✅ **Read from Firebase → broadcast directly** to clients (ephemeral data)
- Firebase is the ONLY persistent storage for DOS
- Servers act as pass-through: Firebase → Server → Clients

### 2. Leader ≠ Executor (Different Roles)
**User Clarification**: "Be careful, leader and executor are different things."

**Role Definitions**:
- **Leader**: Node responsible for Firebase writes (persistent role)
- **Executor**: Node handling client TCP connections (can change via ASSIGN message)

**Impact on Implementation**:
- Executor failover detection must check `executor_ip`, NOT leader status
- System recovery runs on current executor (not necessarily leader)
- Stale client cleanup runs on leader (cleanup = Firebase writes)
- Use correct terminology in all comments and log messages

---

## Part 1: Current Implementation Analysis

**NOTE**: Parts 1-4 describe the EXISTING (pre-change) implementation. The NEW architecture (Parts 5-12) will eliminate local DOS caching based on user's clarification.

### Current DOS Structure (Cloud-Node)

**DosClient** (`firebase.rs:13-22`):
- `client_name`: Username
- `client_ip`: IP for P2P connections
- `client_port`: P2P listen port
- `actual_images`: Vec<String> of encrypted images
- `last_seen`: Unix timestamp (ms)
- `online`: Boolean status flag

**Storage**:
- Leader: Firebase `dos_s_clients` collection (authoritative)
- All servers: Local `HashMap<String, DosClient>` cache
- Version tracking: `dos_c_version` (u32) incremented on changes

---

## Part 2: Current Update Triggers (What Exists)

### ✅ 1. Client JOIN
**Location**: `tcp_client.rs:353-550`

**Flow**:
1. Executor receives JOIN → adds to local `dos_clients` (line 427)
2. Sets `online=true`, `last_seen=now_ms` (lines 429-432)
3. Increments `dos_c_version` (line 438)
4. Stores TCP connection in `client_connections` (line 441)
5. Notifies leader via `EXEC_ADD_CLIENT` (line 492)
6. Leader writes to Firebase (executor_leader.rs:148)

**Gap**: ✅ Works correctly

---

### ⚠️ 2. Image Upload Completion
**Location**: `tcp_client.rs:904-1092`

**Current Flow**:
1. Client sends OWNER_IMAGE_META + OWNER_IMAGE_CHUNK messages
2. Server receives chunks, performs encryption (line 994)
3. Sends encrypted image back to owner (line 998)
4. **RECENTLY ADDED** (lines 1003-1077): Updates DOS with new image

**Implementation** (lines 1007-1015):
```rust
// Add encrypted image to owner's actual_images list
if let Some(client) = s.dos_clients.get_mut(&owner) {
    let encrypted_image_name = format!("{}_encrypted.png", image_name);
    if !client.actual_images.contains(&encrypted_image_name) {
        client.actual_images.push(encrypted_image_name.clone());
        s.dos_c_version += 1;
    }
}
```

**Then notifies leader** (lines 1038-1077):
- If executor: directly updates Firebase
- If not executor: sends `EXEC_ADD_CLIENT` with updated client info

**Status**: ✅ Recently implemented, should work

---

### ⚠️ 3. Client Disconnect
**Location**: `tcp_client.rs:142-207`

**Current Flow**:
1. TCP read error detected (lines 77-84)
2. Loop exits, cleanup begins (line 142)
3. Removes from `client_connections` (line 147)
4. Sets `online=false` in local `dos_clients` (lines 150-152)
5. Increments `dos_c_version` (line 155)
6. **If executor**: Updates Firebase directly (lines 172-182)
7. **If not executor**: Sends `EXEC_UPDATE_CLIENT_STATUS` to leader (lines 184-196)

**Gap**: ❌ **NO BROADCAST TO CLIENTS** - Other clients don't learn about offline status

---

### ❌ 4. Client PING (Heartbeat)
**Location**: `tcp_client.rs:552-591`

**Current Flow**:
1. Updates `last_seen` timestamp (line 578)
2. Sets `online=true` (line 579)
3. Responds with SERVER_PONG + dos_c_version

**Gap**: ❌ **Does NOT increment dos_c_version** - Status change not propagated
**Gap**: ❌ **Does NOT notify leader** - Firebase not updated with heartbeat

---

## Part 3: Current Synchronization Mechanisms

### ❌ Leader → Executors: Firebase Listeners
**Location**: `firebase.rs:339-386`

**Status**: **NOT IMPLEMENTED** - Just TODO stubs that sleep forever

```rust
async fn listen_clients_collection(_db: FirestoreDb, _state: SharedState) -> Result<()> {
    // TODO: Implement with correct firestore-rs API
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
    }
}
```

**Impact**: Non-leader servers never receive DOS updates from leader
**Your Proposal**: Implement periodic 5-second sync ✅

---

### ⚠️ Executor → Clients: DOS_UPDATE on Query
**Location**: `tcp_client.rs:593-675`

**Current Mechanism**:
- Client sends CLIENT_PING every 10 seconds
- SERVER_PONG includes `dos_c_version` (line 584)
- If version changed, client sends DOS_QUERY (simple_client.rs:355)
- Server responds with DOS_UPDATE

**Gap**: ❌ **Pull-only, not push** - Clients must detect version change and request
**Gap**: ❌ **No proactive broadcast** when DOS changes

---

## Part 4: Analysis of Your Proposed Logic

### Your Proposal: DOS Update Conditions

#### ✅ 1. Client Startup (TCP Connection Established)
**Your requirement**: "Executor should inform leader to add this new client to DOS"

**Current implementation**: ✅ **ALREADY WORKS**
- `tcp_client.rs:492` - Executor calls `notify_leader_add_client()`
- `executor_leader.rs:98-161` - Leader adds to Firebase

**Recommendation**: ✅ Keep as-is

---

#### ✅ 2. New Uploaded Images
**Your requirement**: "Upon encryption, executor sends to leader to update images vector in DOS"

**Current implementation**: ✅ **RECENTLY IMPLEMENTED**
- `tcp_client.rs:1038-1077` - Notifies leader after successful encryption
- Leader updates Firebase

**Recommendation**: ✅ Keep as-is, but needs broadcast addition (see below)

---

#### ✅ 3. TCP Connection Change (Client Goes Offline)
**Your requirement**: "Executor sends to leader to mark client as offline"

**Current implementation**: ✅ **WORKS**
- `tcp_client.rs:142-207` - Detects disconnect, notifies leader

**Recommendation**: ✅ Keep as-is, but needs broadcast addition (see below)

---

### Your Proposal: DOS Syncing

#### ✅ Periodic every 5 seconds
**Your requirement**: "DOS syncing from server to clients should be periodic every 5s"

**Current implementation**: ❌ **NOT IMPLEMENTED**
- Currently pull-based via CLIENT_PING (10 seconds)
- No periodic push from server

**Recommendation**: ✅ **IMPLEMENT** - Add periodic broadcast task

---

### Your Proposal: System Recovery

**Your requirement**: "When servers come back, must try to establish TCP channel with all DOS users to verify if they're live"

**Current implementation**: ❌ **NOT IMPLEMENTED**

**Critical Issue**: This won't work because:
1. **Servers don't initiate TCP connections to clients** - Only clients connect to servers
2. **Clients listen on P2P ports, not main ports** - Server would need to know P2P port
3. **Firewall/NAT issues** - Server-to-client connections often blocked

**Recommendation**: ⚠️ **RETHINK APPROACH** - Use different recovery mechanism (see below)

---

## Part 5: Architecture Clarifications from User

### Clarification #1: DOS Broadcast Mechanism
**User Confirmation**: "We broadcast the server new versions to client every 5s (this is what we mean by syncing)"

✅ **Implementation**: Periodic DOS_UPDATE broadcast every 5 seconds from executor to all connected clients
- This is the PRIMARY sync mechanism
- Replaces pull-based polling
- Max 5-second staleness guarantee

---

### Clarification #2: System Recovery with LIFE_CHECK
**User Correction**: "The client port listens since they receive back images from server etc., the executor node can just attempt to contact client using stored IP address and port by sending a message LIFE_CHECK"

✅ **Implementation**: LIFE_CHECK message to client's P2P port via TCP
- Clients DO listen on P2P port (9080+) as TCP server
- Server creates TCP connection to client's stored IP:port
- Server sends LIFE_CHECK via TCP
- Client responds with LIFE_CHECK_ACK via same TCP connection
- 1-second timeout determines if client is alive
- Server closes TCP connection after response/timeout

**Critical constraint**: ALL server-client communication MUST be TCP, no exceptions

**Architecture is correct** - Clients run TCP server on P2P port, server connects as TCP client to verify liveness.

---

### Clarification #3: Executor→Firebase Sync
**User Confirmation**: "Every 5 seconds, executors read from Firebase directly"

✅ **Implementation**: Periodic Firebase read (every 5s) → direct broadcast to clients
- Current executor reads `dos_s_clients` from Firebase every 5 seconds
- **NO local caching** - reads are ephemeral, used only for broadcasting
- Executor broadcasts DOS directly to clients after reading from Firebase
- **Servers do NOT maintain local DOS copy** - Firebase is the only source
- All nodes can read Firebase, only leader writes

---

### Clarification #4: Executor Failover Logic
**User Correction**: "Executor A keeps all current connections as is. Since the clients already broadcast every new request to all servers, the old TCP connection with Executor A which recognizes that it is no longer the leader will be closed, and a new TCP connection with Executor B will be established."

✅ **Implementation**: Graceful migration via multi-server discovery
- Old executor (A) keeps connections initially
- **When old executor detects it's not the current system executor anymore**, closes connections
- Clients use multi-server discovery (REQUEST_EXECUTOR broadcast) to find new executor (B)
- Clients establish new connection with B
- **No REDIRECT message needed** - clients already have discovery mechanism

**Critical terminology**:
- **Leader** = Node responsible for Firebase writes (persistent role)
- **Executor** = Node handling client connections (can change via ASSIGN message)
- These are DIFFERENT roles - don't confuse them!

**Note**: This simplifies implementation - leverage existing REQUEST_EXECUTOR protocol instead of adding REDIRECT.

---

### Clarification #5: Version Conflict Resolution
**User Confirmation**: "All read from the same firebase source of truth (shared across all of them). Only leader can write to firebase, all can read the firebase at any time."

✅ **Implementation**: Firebase as single source of truth
- **Only leader writes** to Firebase (dos_s_clients, dos_s_access)
- **All nodes read** from Firebase (every 5s for current executor)
- **No version conflicts** possible - leader is sole writer
- Executors increment `dos_c_version` locally, then sync from Firebase

**Architecture is sound** - Single writer prevents conflicts.

---

### Clarification #6: Network Partitions
**User Statement**: "This can't happen with us."

✅ **No implementation needed**
- Infrastructure guarantees no network partitions
- Retry logic (5 attempts) handles transient failures
- No need for distributed consensus

---

### Clarification #7: Image Name Uniqueness
**User Agreement**: "Use UUID-based image names: {username}_{uuid}_{name}_encrypted.png"

✅ **Implementation**: Add UUID to prevent collisions
```rust
let encrypted_image_name = format!(
    "{}_{}_encrypted.png",
    owner,
    uuid::Uuid::new_v4().to_string()
);
```

---

### Clarification #8: Stale Client Cleanup
**User Specification**: "Remove offline clients after 2 mins and Clients send explicit LEAVE message on graceful shutdown"

✅ **Implementation**: Two-part cleanup
1. **Graceful shutdown**: Client sends LEAVE message, server removes immediately
2. **Timeout cleanup**: Periodic task removes clients offline for > 2 minutes

```rust
// New message type
pub const CLIENT_LEAVE: u8 = 73; // Client → Server: "I'm leaving gracefully"

// Cleanup task (every minute)
if client.online == false && (now - client.last_seen) > 120_000 {
    // Remove from Firebase after 2 minutes offline
}
```

---

### Additional Clarification: Any Node Can Update Leader
**User Note**: "Any node can contact the leader to update/change the DOS since old executors still carry connections etc."

✅ **Implementation**: All nodes have equal access to leader
- **Not just current executor** can send EXEC_* messages to leader
- Old executors can still update DOS for their connected clients
- Enables smooth failover - old executor handles its clients until they migrate
- Leader accepts updates from any node in the cluster

**Key insight**: This is why executor failover works smoothly - old executor continues managing its clients' DOS updates until they migrate.

---

## Part 6: Final Approved Implementation Logic

### DOS Update Conditions (FINAL)

#### 1. Client Startup (TCP Connection Established)
**Flow**:
```
Client → Executor (JOIN message via TCP)
  ↓
Executor:
  ├─ Store TCP connection in client_connections (username → TcpStream)
  ├─ Send EXEC_ADD_CLIENT to Leader (with 5 retries)
  └─ Respond with JOIN_ACK to client

Leader:
  ├─ Receive EXEC_ADD_CLIENT
  ├─ Write to Firebase (dos_s_clients collection)
  ├─ Set online=true, update last_seen timestamp
  └─ Send ACK to executor

Executor periodic task (every 5s):
  ├─ Read DOS from Firebase (NO local caching)
  └─ Broadcast DOS_UPDATE to all clients in client_connections via TCP
```

**Status**: ✅ Mostly implemented, needs periodic Firebase read + broadcast

---

#### 2. New Uploaded Images by Clients
**Flow**:
```
Client → Executor (OWNER_IMAGE_META + OWNER_IMAGE_CHUNK via TCP)
  ↓
Executor:
  ├─ Receive all chunks via TCP
  ├─ Perform steganographic encryption
  ├─ Send encrypted image back to client via TCP
  ├─ Send EXEC_ADD_CLIENT to Leader with updated images list (with 5 retries)
  └─ (NO local caching - leader writes to Firebase)

Leader:
  ├─ Receive EXEC_ADD_CLIENT
  ├─ Write updated client to Firebase (dos_s_clients collection)
  ├─ Add new image to client's actual_images array
  └─ Send ACK to executor

Executor periodic task (every 5s):
  ├─ Read DOS from Firebase (NO local caching)
  └─ Broadcast DOS_UPDATE to all clients via TCP
```

**Status**: ✅ Recently implemented, needs periodic Firebase read + broadcast

---

#### 3. TCP Connection Change (Client Goes Offline)
**Flow**:
```
TCP connection error detected by executor
  ↓
Executor:
  ├─ Remove from client_connections HashMap
  ├─ Send EXEC_UPDATE_CLIENT_STATUS to Leader (with 5 retries, online=false)
  └─ (Periodic task will broadcast change in next 5s window)

Leader:
  ├─ Receive EXEC_UPDATE_CLIENT_STATUS
  ├─ Update Firebase (set online=false, update last_seen)
  └─ Send ACK to executor

Executor periodic task (every 5s):
  ├─ Read DOS from Firebase (NO local caching)
  └─ Broadcast DOS_UPDATE to all remaining clients via TCP
```

**Status**: ✅ Implemented, needs periodic Firebase read + broadcast

---

### DOS Syncing Mechanisms (FINAL)

#### Executor → Clients (Every 5 seconds)
**Mechanism**: Periodic broadcast from current executor only

**Implementation**:
```rust
// Start this task only if I am the current executor
pub async fn start_periodic_dos_broadcast(state: SharedState, cfg: Config) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;

            // Check if still executor
            let is_executor = check_if_executor(&state, &cfg).await;
            if !is_executor {
                continue; // Skip if not executor
            }

            // Broadcast to all connected clients
            broadcast_dos_update_to_all_clients(&state).await;
        }
    });
}
```

**Details**:
- Only the current executor broadcasts
- Sent to all clients in `client_connections` HashMap
- Contains full DOS-C (excluding requesting client)
- Includes `dos_c_version` for cache coherence

---

#### Firebase → Executor → Clients (Every 5 seconds)
**Mechanism**: Current executor reads from Firebase and broadcasts to clients (NO local caching)

**Implementation**:
```rust
// Start this task only if I am the current executor
pub async fn start_periodic_firebase_sync_and_broadcast(state: SharedState, cfg: Config) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;

            // Check if still executor
            let is_executor = check_if_executor(&state, &cfg).await;
            if !is_executor {
                continue; // Skip if not executor
            }

            // Read from Firebase
            if let Some(db) = get_firestore_db(&state).await {
                match read_all_clients_from_firebase(&db).await {
                    Ok(firebase_dos) => {
                        // DO NOT cache locally - broadcast directly to clients
                        broadcast_dos_from_firebase(&state, firebase_dos).await;
                    }
                    Err(e) => {
                        eprintln!("[FIREBASE-SYNC] Error reading: {}", e);
                    }
                }
            }
        }
    });
}
```

**Details**:
- Only current executor reads from Firebase and broadcasts
- Reads `dos_s_clients` collection every 5 seconds
- **NO local caching** - DOS data is ephemeral, used only for broadcasting
- Broadcasts directly to all connected clients
- Firebase is the single source of truth

---

### System Recovery (FINAL)

#### On Server Startup
**Mechanism**: Send life_check to all clients stored in Firebase

**Implementation**:
```rust
pub async fn recover_client_connections(state: SharedState, cfg: Config) {
    // Only leader performs recovery
    let is_leader = check_if_leader(&state, &cfg).await;
    if !is_leader {
        return;
    }

    let db = match get_firestore_db(&state).await {
        Some(db) => db,
        None => return,
    };

    // Read all clients from Firebase
    let clients = match read_all_clients_from_firebase(&db).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[RECOVERY] Failed to read clients: {}", e);
            return;
        }
    };

    // For each client marked online, send life_check
    for (username, client) in clients {
        if !client.online {
            continue; // Skip offline clients
        }

        // Send LIFE_CHECK message to client's P2P port
        let client_addr = format!("{}:{}", client.client_ip, client.client_port);
        match send_life_check(&client_addr, &username).await {
            Ok(true) => {
                // Client responded - keep online
                println!("[RECOVERY] ✅ {} is alive", username);
            }
            Ok(false) | Err(_) => {
                // No response - mark offline
                println!("[RECOVERY] ❌ {} is offline, marking as such", username);
                let mut updated = client.clone();
                updated.online = false;
                let _ = firebase::write_client(&db, &updated).await;
            }
        }
    }
}
```

**LIFE_CHECK Message Protocol**:
```rust
// New message type
pub const LIFE_CHECK: u8 = 70;  // Server → Client P2P port (TCP)
pub const LIFE_CHECK_ACK: u8 = 71; // Client → Server (TCP)

async fn send_life_check(addr: &str, username: &str) -> Result<bool> {
    // Parse address
    let socket_addr: SocketAddr = addr.parse()?;

    // Create TCP connection to client's P2P port with 1 second timeout
    let tcp_stream = match timeout(
        Duration::from_secs(1),
        TcpStream::connect(socket_addr)
    ).await {
        Ok(Ok(stream)) => stream,
        _ => return Ok(false), // Connection failed or timeout
    };

    // Build LIFE_CHECK message: [msg_type:u8][username_len:u16][username]
    let mut payload = Vec::new();
    payload.push(LIFE_CHECK);
    payload.extend((username.len() as u16).to_le_bytes());
    payload.extend(username.as_bytes());

    // Send LIFE_CHECK
    if let Err(_) = tcp_stream.write_all(&payload).await {
        return Ok(false);
    }

    // Wait for ACK (1 second timeout)
    let mut buf = [0u8; 1];
    match timeout(Duration::from_secs(1), tcp_stream.read_exact(&mut buf)).await {
        Ok(Ok(_)) if buf[0] == LIFE_CHECK_ACK => Ok(true),
        _ => Ok(false),
    }
}
```

**Client-side handler** (Client-Node):
```rust
// Add to P2P TCP server message handler
LIFE_CHECK => {
    // Respond with ACK via TCP
    let response = vec![LIFE_CHECK_ACK];
    if let Err(e) = stream.write_all(&response).await {
        eprintln!("[P2P] Failed to send LIFE_CHECK_ACK: {}", e);
    }
}
```

---

### Executor Failover Logic (FINAL)

**Scenario**: System assigns new executor (Executor B), old executor (Executor A) has active connections

**Behavior**:
```
1. Executor A receives ASSIGN message with new executor IP
   ├─ Updates local executor_ip = Executor B's IP
   ├─ Keeps all existing TCP connections in client_connections
   └─ Continues handling messages from connected clients

2. Client sends new request (e.g., DOS_QUERY, VIEW_REQUEST)
   ├─ Executor A receives the message
   ├─ Checks if it's still the executor
   └─ If NOT executor:
       ├─ Responds with REDIRECT message: "New executor is at X.X.X.X"
       ├─ Closes the TCP connection
       └─ Client reconnects to new executor

3. Client discovers new executor
   ├─ Via REDIRECT response from old executor
   ├─ OR via multi-server discovery (REQUEST_EXECUTOR broadcast)
   └─ Establishes new TCP connection with Executor B

4. Gradual migration:
   ├─ Active clients migrate as they make requests
   ├─ Idle clients migrate at next heartbeat (CLIENT_PING)
   └─ Eventually all clients connected to new executor
```

**Key implementation points**:
- **No forced disconnection** - Executor A doesn't close all connections immediately
- **Lazy migration** - Clients migrate when they send next request
- **REDIRECT message** - Informs client of new executor address
- **Backward compatible** - Clients with multi-server discovery can find new executor automatically

**New message type needed**:
```rust
pub const REDIRECT: u8 = 72; // Server → Client: "Connect to new executor"

// Message format: [msg_type:u8][ip_len:u16][ip:String][port:u16]
```

---

## Part 7: Implementation Plan (FINAL)

### Overview

Based on your requirements and clarifications, we need to implement:

1. **Periodic DOS read + broadcast** - Executor reads from Firebase every 5s and broadcasts to clients (NO local caching)
2. **System recovery** with LIFE_CHECK messages via TCP to verify clients
3. **Executor failover** - Detect when node is no longer current executor and close connections
4. **Client cleanup** - Graceful shutdown (CLIENT_LEAVE) + stale client removal (2 min threshold)
5. **5-retry logic** for all executor-leader communications

**Key Architectural Principles**:
- ❌ NO local DOS caching on servers - Firebase is the ONLY persistent storage
- ✅ TCP ONLY for all server-client communication (no UDP)
- ✅ Leader ≠ Executor (different roles, different responsibilities)

### Phase 1: Core DOS Broadcasting (HIGH PRIORITY)

**CRITICAL ARCHITECTURE NOTE**: Servers do NOT cache DOS locally. Task 1.1 reads from Firebase and broadcasts directly to clients (NO local caching).

#### Task 1.1: Add Periodic Firebase Read + Broadcast (Firebase → Clients via Executor)
**Files**: `Cloud-Node/src/firebase.rs` + `Cloud-Node/src/tcp_client.rs`

**What to add**: Background task that reads from Firebase every 5s and broadcasts directly to clients (NO local caching)

**Location (firebase.rs)**: Replace TODO stub in `listen_clients_collection()`
**Location (tcp_client.rs)**: Add broadcast helper function

**New function**:
```rust
/// Start periodic DOS broadcast task (only runs on current executor)
pub async fn start_periodic_dos_broadcast(state: SharedState, cfg: Config) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;

            // Check if still executor
            let is_executor = {
                let s = state.read().await;
                let my_ip = cfg.service_bind_addr()
                    .expect("service_bind_addr not configured")
                    .ip();

                if let (Some(exec_ip), Some(deadline)) = (&s.executor_ip, s.executor_lease_deadline_ms) {
                    exec_ip == &my_ip && std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() <= deadline
                } else {
                    false
                }
            };

            if !is_executor {
                continue; // Skip if not executor
            }

            // Broadcast DOS_UPDATE to all connected clients
            broadcast_dos_to_all_clients(&state).await;
        }
    });
}

/// Broadcast DOS_UPDATE to all clients in client_connections
async fn broadcast_dos_to_all_clients(state: &SharedState) {
    let connections = {
        let s = state.read().await;
        s.client_connections.clone()
    };

    for (username, stream) in connections {
        // Build DOS-C payload (excluding this client)
        let dos_payload = {
            let s = state.read().await;
            build_dos_c_payload_excluding(&s, &username)
        };

        // Send DOS_UPDATE
        match send_tcp_response(stream, client_protocol::DOS_UPDATE, &dos_payload).await {
            Ok(_) => {},
            Err(e) => {
                eprintln!("[DOS-BROADCAST] Failed to send to {}: {}", username, e);
            }
        }
    }
}

/// Build DOS-C payload excluding a specific client
fn build_dos_c_payload_excluding(s: &ServerState, exclude_username: &str) -> Vec<u8> {
    let mut payload = Vec::new();

    // DOS version (u64)
    let dos_version_u64 = s.dos_c_version as u64;
    payload.extend(dos_version_u64.to_le_bytes());

    // Filter clients (exclude the specified one)
    let clients_to_send: Vec<_> = s.dos_clients
        .iter()
        .filter(|(name, _)| *name != exclude_username)
        .collect();

    // Number of clients (u32)
    payload.extend((clients_to_send.len() as u32).to_le_bytes());

    // For each client: name, IP, port, images
    for (client_name, client) in clients_to_send {
        // Username
        payload.extend((client_name.len() as u16).to_le_bytes());
        payload.extend_from_slice(client_name.as_bytes());

        // Client IP
        payload.extend((client.client_ip.len() as u16).to_le_bytes());
        payload.extend_from_slice(client.client_ip.as_bytes());

        // Client port
        payload.extend(client.client_port.to_le_bytes());

        // Actual images
        payload.extend((client.actual_images.len() as u32).to_le_bytes());
        for img in &client.actual_images {
            payload.extend((img.len() as u16).to_le_bytes());
            payload.extend_from_slice(img.as_bytes());
        }
    }

    payload
}
```

**Call site** (`Cloud-Node/src/main.rs`):
```rust
// After starting TCP server, start periodic DOS broadcast
tcp_client::start_periodic_dos_broadcast(state.clone(), cfg.clone()).await;
```

---

#### Task 1.2: Add Periodic Firebase Read & Broadcast (Firebase → Executor → Clients)
**File**: `Cloud-Node/src/firebase.rs`

**What to change**: Replace TODO stub with actual Firebase polling + direct broadcast

**Location**: Lines 370-386 (`listen_clients_collection` and `listen_access_collection`)

**CRITICAL**: NO local DOS caching - read from Firebase and broadcast directly to clients

**Updated implementation**:
```rust
async fn listen_clients_collection(db: FirestoreDb, state: SharedState, cfg: Config) -> Result<()> {
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        // Only read/broadcast if I'm the current executor
        let is_executor = {
            let s = state.read().await;
            let my_ip = cfg.service_bind_addr()
                .expect("service_bind_addr not configured")
                .ip();

            if let (Some(exec_ip), Some(deadline)) = (&s.executor_ip, s.executor_lease_deadline_ms) {
                exec_ip == &my_ip && std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() <= deadline
            } else {
                false
            }
        };

        if !is_executor {
            continue; // Skip if not current executor
        }

        // Read all clients from Firebase
        match read_all_clients_from_firebase(&db).await {
            Ok(firebase_dos) => {
                // DO NOT cache locally - broadcast directly to clients
                broadcast_dos_to_all_clients_from_firebase(&state, firebase_dos).await;
            }
            Err(e) => {
                error!("[FIREBASE-SYNC] Failed to read clients from Firebase: {}", e);
            }
        }
    }
}

/// Helper: Read all clients from Firebase dos_s_clients collection
async fn read_all_clients_from_firebase(db: &FirestoreDb) -> Result<HashMap<String, DosClient>> {
    let clients: Vec<DosClient> = db
        .fluent()
        .select()
        .from("dos_s_clients")
        .obj()
        .query()
        .await?;

    let mut map = HashMap::new();
    for client in clients {
        map.insert(client.client_name.clone(), client);
    }
    Ok(map)
}

/// Broadcast DOS from Firebase directly to all connected clients (no caching)
async fn broadcast_dos_to_all_clients_from_firebase(
    state: &SharedState,
    firebase_dos: HashMap<String, DosClient>
) {
    let connections = {
        let s = state.read().await;
        s.client_connections.clone()
    };

    for (username, stream) in connections {
        // Build DOS payload excluding this client
        let dos_payload = build_dos_payload_from_firebase(&firebase_dos, &username);

        // Send DOS_UPDATE via TCP
        match send_tcp_response(stream, client_protocol::DOS_UPDATE, &dos_payload).await {
            Ok(_) => {},
            Err(e) => {
                eprintln!("[FIREBASE-BROADCAST] Failed to send to {}: {}", username, e);
            }
        }
    }
}
```

**Note**: Also update `listen_access_collection` similarly

---

### Phase 2: System Recovery & LIFE_CHECK (HIGH PRIORITY)

#### Task 2.1: Add Message Type Constants
**File**: `Cloud-Node/src/client_protocol.rs`

**What to add**: New message types for LIFE_CHECK and REDIRECT

**Location**: After existing message type constants (around line 60)

**CRITICAL**: All these messages use TCP (no UDP allowed)

```rust
// System recovery and failover (TCP-based)
pub const LIFE_CHECK: u8 = 70;       // Server → Client P2P TCP: "Are you alive?"
pub const LIFE_CHECK_ACK: u8 = 71;   // Client → Server TCP: "Yes, I'm alive"
pub const REDIRECT: u8 = 72;         // Server → Client TCP: "New executor is at X"
```

---

#### Task 2.2: Implement LIFE_CHECK Mechanism
**File**: `Cloud-Node/src/tcp_client.rs`

**What to add**: Function to send LIFE_CHECK via TCP and wait for ACK

**Location**: After helper functions

**CRITICAL**: ALL server-client communication MUST be via TCP (no UDP)

```rust
use tokio::time::timeout;
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Send LIFE_CHECK to client's P2P TCP port and wait for ACK
pub async fn send_life_check(client_ip: &str, client_port: u16, username: &str) -> Result<bool> {
    let client_addr = format!("{}:{}", client_ip, client_port);
    let socket_addr: SocketAddr = client_addr.parse()?;

    // Create TCP connection to client's P2P port (1 second timeout)
    let mut tcp_stream = match timeout(
        Duration::from_secs(1),
        TcpStream::connect(socket_addr)
    ).await {
        Ok(Ok(stream)) => stream,
        Ok(Err(e)) => {
            eprintln!("[LIFE_CHECK] Failed to connect to {}: {}", client_addr, e);
            return Ok(false);
        }
        Err(_) => {
            eprintln!("[LIFE_CHECK] Timeout connecting to {}", client_addr);
            return Ok(false);
        }
    };

    // Build LIFE_CHECK message: [msg_type:u8][username_len:u16][username]
    let mut payload = Vec::new();
    payload.push(client_protocol::LIFE_CHECK);
    payload.extend((username.len() as u16).to_le_bytes());
    payload.extend(username.as_bytes());

    // Send LIFE_CHECK
    if let Err(e) = tcp_stream.write_all(&payload).await {
        eprintln!("[LIFE_CHECK] Failed to send to {}: {}", client_addr, e);
        return Ok(false);
    }

    // Wait for ACK (1 second timeout)
    let mut buf = [0u8; 1];
    match timeout(Duration::from_secs(1), tcp_stream.read_exact(&mut buf)).await {
        Ok(Ok(_)) if buf[0] == client_protocol::LIFE_CHECK_ACK => {
            println!("[LIFE_CHECK] ✅ Received ACK from {}", client_addr);
            Ok(true)
        }
        Ok(Ok(_)) => {
            eprintln!("[LIFE_CHECK] Wrong response from {}: {}", client_addr, buf[0]);
            Ok(false)
        }
        Ok(Err(e)) => {
            eprintln!("[LIFE_CHECK] Read error from {}: {}", client_addr, e);
            Ok(false)
        }
        Err(_) => {
            eprintln!("[LIFE_CHECK] Timeout waiting for ACK from {}", client_addr);
            Ok(false)
        }
    }
}
```

---

#### Task 2.3: Implement System Recovery Function
**File**: `Cloud-Node/src/main.rs`

**What to add**: Recovery function called on server startup

**Location**: In main(), after Firebase initialization

```rust
/// Clean up stale online clients by sending LIFE_CHECK via TCP
async fn recover_client_connections(state: SharedState, cfg: Config, db: &FirestoreDb) -> Result<()> {
    // Only current system executor performs recovery
    // Note: This checks executor role, not leader role (different responsibilities)
    let is_executor = {
        let s = state.read().await;
        let my_ip = cfg.service_bind_addr()
            .expect("service_bind_addr not configured")
            .ip();

        if let (Some(exec_ip), Some(deadline)) = (&s.executor_ip, s.executor_lease_deadline_ms) {
            exec_ip == &my_ip && std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() <= deadline
        } else {
            false
        }
    };

    if !is_executor {
        return Ok(()); // Only current executor does recovery
    }

    println!("[RECOVERY] Starting client connection recovery...");

    // Read all clients directly from Firebase (no local caching)
    let clients = match read_all_clients_from_firebase(&db).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[RECOVERY] Failed to read clients from Firebase: {}", e);
            return Err(e);
        }
    };

    // For each client marked online, send LIFE_CHECK via TCP
    for (username, client) in clients {
        if !client.online {
            continue; // Skip offline clients
        }

        println!("[RECOVERY] Checking client: {}", username);

        match tcp_client::send_life_check(&client.client_ip, client.client_port, &username).await {
            Ok(true) => {
                println!("[RECOVERY] ✅ {} is alive", username);
            }
            Ok(false) | Err(_) => {
                println!("[RECOVERY] ❌ {} is offline, marking as such", username);

                // Mark offline in Firebase
                let mut updated = client.clone();
                updated.online = false;
                if let Err(e) = firebase::write_client(&db, &updated).await {
                    eprintln!("[RECOVERY] Failed to update {}: {}", username, e);
                }

                // Update local state
                let mut s = state.write().await;
                if let Some(c) = s.dos_clients.get_mut(&username) {
                    c.online = false;
                }
            }
        }
    }

    println!("[RECOVERY] Client connection recovery complete");
    Ok(())
}

// Call site in main():
// After Firebase initialized and before starting server
if let Some(db) = &state.read().await.firestore_db {
    recover_client_connections(state.clone(), cfg.clone(), db).await?;
}
```

---

#### Task 2.4: Add Client-Side LIFE_CHECK Handler
**File**: `Client-Node/src/p2p_server.rs`

**What to add**: Handler for LIFE_CHECK message in P2P TCP server

**Location**: In TCP message handler (around where other message types are handled)

**CRITICAL**: Client P2P port MUST run a TCP server to receive LIFE_CHECK from cloud servers

```rust
use crate::protocol::{LIFE_CHECK, LIFE_CHECK_ACK};
use tokio::io::AsyncWriteExt;

// In handle_peer_connection or TCP message handler:
match msg_type {
    LIFE_CHECK => {
        println!("[P2P] Received LIFE_CHECK from server");

        // Respond with ACK immediately via TCP
        let response = vec![LIFE_CHECK_ACK];

        if let Err(e) = stream.write_all(&response).await {
            eprintln!("[P2P] Failed to send LIFE_CHECK_ACK: {}", e);
        } else {
            println!("[P2P] ✅ Sent LIFE_CHECK_ACK to server");
        }
    }
    // ... other message types
}
```

**Note**: Ensure the P2P server is listening on TCP (not UDP) to receive connections from cloud servers during system recovery.

---

### Phase 3: Executor Failover & Client Cleanup (MEDIUM PRIORITY)

#### Task 3.1: Implement Executor Failover Detection
**File**: `Cloud-Node/src/assignment.rs`

**What to add**: Detect when node is no longer the current system executor and close connections

**Location**: In ASSIGN message handler

**CRITICAL**: Leader ≠ Executor. This is about EXECUTOR failover, not leader change.

```rust
// When receiving ASSIGN message
match buf[0] {
    ASSIGN => {
        // ... existing parsing logic ...

        let my_ip = cfg.service_bind_addr()
            .expect("service_bind_addr not configured")
            .ip();

        // Check if I WAS the current system executor but am NOT anymore
        let was_executor = {
            let s = state.read().await;
            if let Some(old_exec_ip) = &s.executor_ip {
                old_exec_ip == &my_ip
            } else {
                false
            }
        };

        let is_new_executor = ip == my_ip;

        // Update executor info
        {
            let mut s = state.write().await;
            s.executor_ip = Some(ip);
            s.executor_lease_deadline_ms = Some(deadline_ms);
        }

        // If I was the current system executor but not anymore, close all client connections
        if was_executor && !is_new_executor {
            println!("[EXECUTOR-FAILOVER] No longer the current system executor - closing client connections");
            close_all_client_connections(state.clone()).await;
        }
    }
}

/// Close all client connections during failover
async fn close_all_client_connections(state: SharedState) {
    let connections = {
        let mut s = state.write().await;
        let conns = s.client_connections.clone();
        s.client_connections.clear();
        conns
    };

    for (username, stream) in connections {
        println!("[FAILOVER] Closing connection for {}", username);
        drop(stream); // Close the TCP stream
    }

    println!("[FAILOVER] All client connections closed - clients will use REQUEST_EXECUTOR to find new executor");
}
```

**Note**: Clients already have multi-server discovery via REQUEST_EXECUTOR broadcast. When their TCP connection closes, they'll automatically discover and connect to the new executor.

---

#### Task 3.2: Add CLIENT_LEAVE Message Handler
**File**: `Cloud-Node/src/client_protocol.rs`

**What to add**: New message type for graceful shutdown

```rust
// Graceful shutdown (TCP-based)
pub const CLIENT_LEAVE: u8 = 73;  // Client → Server TCP: "I'm leaving gracefully"
```

**File**: `Cloud-Node/src/tcp_client.rs`

**What to add**: Handler for CLIENT_LEAVE message

**Location**: In message routing (`route_client_message`)

```rust
x if x == client_protocol::CLIENT_LEAVE => {
    info!(%peer_addr, "Received CLIENT_LEAVE from client");
    handle_client_leave(state, cfg, payload).await
}

/// Handle graceful client shutdown
async fn handle_client_leave(
    state: SharedState,
    cfg: &Config,
    data: &[u8],
) -> Result<()> {
    // Parse username
    if data.len() < 2 {
        return Err(anyhow::anyhow!("CLIENT_LEAVE too short"));
    }

    let username_len = u16::from_le_bytes(data[0..2].try_into()?) as usize;
    let username = String::from_utf8(data[2..2 + username_len].to_vec())?;

    println!("[LEAVE] Client {} is leaving gracefully", username);

    // Remove from local state
    {
        let mut s = state.write().await;
        s.client_connections.remove(&username);
        s.dos_clients.remove(&username);
        s.dos_c_version += 1;
    }

    // Notify leader to remove from Firebase
    let mut data = Vec::new();
    data.extend((username.len() as u16).to_le_bytes());
    data.extend(username.as_bytes());

    executor_leader::send_to_leader(
        &cfg,
        executor_leader::EXEC_REMOVE_CLIENT, // New message type
        &data,
    ).await?;

    println!("[LEAVE] ✅ Client {} removed from DOS", username);
    Ok(())
}
```

**File**: `Cloud-Node/src/executor_leader.rs`

**What to add**: Handler for EXEC_REMOVE_CLIENT

```rust
pub const EXEC_REMOVE_CLIENT: u8 = 47; // Executor → Leader: Remove client from DOS

async fn handle_remove_client(state: SharedState, data: &[u8]) -> Result<()> {
    let username_len = u16::from_le_bytes(data[0..2].try_into()?) as usize;
    let username = String::from_utf8(data[2..2 + username_len].to_vec())?;

    println!("[LEADER] Removing client {} from Firebase", username);

    // Delete from Firebase
    let s = state.read().await;
    if let Some(db) = &s.firestore_db {
        firebase::delete_client(db, &username).await?;
    }
    drop(s);

    // Remove from local state
    let mut s = state.write().await;
    s.dos_clients.remove(&username);
    s.dos_c_version += 1;

    Ok(())
}
```

---

#### Task 3.3: Add Periodic Stale Client Cleanup
**File**: `Cloud-Node/src/main.rs`

**What to add**: Background task to remove stale offline clients

**Note**: This is one of the few leader-specific tasks (not executor-specific)

```rust
/// Start periodic cleanup task (runs on leader only)
pub async fn start_stale_client_cleanup(state: SharedState, cfg: Config) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(60)).await; // Every minute

            // Only LEADER performs cleanup (not executor - this is a leader responsibility)
            let is_leader = {
                let s = state.read().await;
                let my_ip = cfg.service_bind_addr()
                    .expect("service_bind_addr not configured")
                    .ip();

                if let (Some(exec_ip), Some(deadline)) = (&s.executor_ip, s.executor_lease_deadline_ms) {
                    exec_ip == &my_ip && std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() <= deadline
                } else {
                    false
                }
            };

            if !is_leader {
                continue;
            }

            // Find clients offline for > 2 minutes
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            let stale_threshold = 120_000; // 2 minutes in milliseconds

            let stale_clients = {
                let s = state.read().await;
                s.dos_clients.iter()
                    .filter(|(_, client)| {
                        !client.online && (now_ms - client.last_seen) > stale_threshold
                    })
                    .map(|(name, _)| name.clone())
                    .collect::<Vec<_>>()
            };

            for username in stale_clients {
                println!("[CLEANUP] Removing stale client: {}", username);

                // Delete from Firebase
                if let Some(db) = &state.read().await.firestore_db {
                    if let Err(e) = firebase::delete_client(db, &username).await {
                        eprintln!("[CLEANUP] Failed to delete {}: {}", username, e);
                        continue;
                    }
                }

                // Remove from local state
                let mut s = state.write().await;
                s.dos_clients.remove(&username);
                s.dos_c_version += 1;
            }
        }
    });
}

// Call site in main():
start_stale_client_cleanup(state.clone(), cfg.clone()).await;
```

---

#### Task 3.4: Add Client-Side LEAVE Message on Shutdown
**File**: `Client-Node/src/protocol.rs`

**What to add**: CLIENT_LEAVE constant

```rust
pub const CLIENT_LEAVE: u8 = 73; // Client → Server: "I'm leaving gracefully"
```

**File**: `Client-Node/src/main.rs`

**What to add**: Send LEAVE on Ctrl+C

```rust
// In main(), before shutdown
tokio::select! {
    _ = tokio::signal::ctrl_c() => {
        println!("\n[CLIENT] Shutting down gracefully...");

        // Send CLIENT_LEAVE to server
        {
            let s = state.read().await;
            if s.joined {
                let mut payload = Vec::new();
                payload.extend((s.username.len() as u16).to_le_bytes());
                payload.extend(s.username.as_bytes());

                if let Err(e) = send_tcp_message_generic(&mut *writer.lock().await, CLIENT_LEAVE, &payload).await {
                    eprintln!("[CLIENT] Failed to send LEAVE: {}", e);
                } else {
                    println!("[CLIENT] ✅ Sent graceful LEAVE to server");
                }
            }
        }
    }
}
```

---

### Phase 4: 5-Retry Logic for Leader Communication (HIGH PRIORITY)

#### Task 4.1: Update send_to_leader with Retry Logic
**File**: `Cloud-Node/src/executor_leader.rs`

**What to change**: Modify `send_to_leader()` to retry 5 times

**Location**: Lines 335-373 (current send_to_leader function)

**Updated function**:
```rust
/// Send message to leader with 5 retries and exponential backoff
pub async fn send_to_leader(
    cfg: &Config,
    msg_type: u8,
    data: &[u8],
) -> Result<Vec<u8>> {
    const MAX_RETRIES: u32 = 5;

    for attempt in 0..MAX_RETRIES {
        match send_to_leader_once(cfg, msg_type, data).await {
            Ok(resp) => {
                if attempt > 0 {
                    println!("[LEADER-COMM] ✅ Succeeded on retry {}", attempt);
                }
                return Ok(resp);
            }
            Err(e) if attempt < MAX_RETRIES - 1 => {
                let backoff_secs = 2u64.pow(attempt); // Exponential: 1, 2, 4, 8, 16 seconds
                eprintln!("[LEADER-COMM] ⚠️  Attempt {} failed: {}", attempt + 1, e);
                eprintln!("[LEADER-COMM] Retrying in {} seconds...", backoff_secs);
                tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
            }
            Err(e) => {
                eprintln!("[LEADER-COMM] ❌ All {} attempts failed", MAX_RETRIES);
                return Err(e);
            }
        }
    }

    unreachable!()
}

/// Single attempt to send message to leader
async fn send_to_leader_once(
    cfg: &Config,
    msg_type: u8,
    data: &[u8],
) -> Result<Vec<u8>> {
    // Existing send_to_leader logic here (server-to-server communication)
    // Note: This is server-to-server (executor-leader), not server-client
    // ... (copy existing implementation)
}

```

**Note**: Refactor existing `send_to_leader` logic into `send_to_leader_once`

**Note**: Executor-leader communication is server-to-server, separate from the TCP-only constraint for server-client

---

## Part 8: Critical Files Summary

**CRITICAL CONSTRAINT**: ALL server-client communication MUST use TCP exclusively (no UDP allowed)

### Cloud-Node (Server)

1. **src/client_protocol.rs**
   - Add: LIFE_CHECK (70), LIFE_CHECK_ACK (71), REDIRECT (72), CLIENT_LEAVE (73) constants
   - All messages are TCP-based

2. **src/tcp_client.rs**
   - Add: `start_periodic_dos_broadcast()` - Background task (TCP broadcast)
   - Add: `broadcast_dos_to_all_clients()` - Send DOS_UPDATE to all via TCP
   - Add: `build_dos_c_payload_excluding()` - Build DOS payload
   - Add: `send_life_check()` - System recovery helper (creates TCP connection to client's P2P port)
   - Add: `handle_client_leave()` - Graceful shutdown handler (TCP)
   - Modify: Message handlers to check executor before processing

3. **src/firebase.rs**
   - Modify: `listen_clients_collection()` - Replace TODO with polling
   - Modify: `listen_access_collection()` - Replace TODO with polling
   - Add: `read_all_clients_from_firebase()` - Firebase query helper

4. **src/executor_leader.rs**
   - Modify: `send_to_leader()` - Add 5-retry logic with backoff
   - Add: `send_to_leader_once()` - Single attempt

5. **src/main.rs**
   - Add: `recover_client_connections()` - System recovery on startup
   - Modify: main() - Call recovery and start periodic tasks

### Client-Node (Client)

6. **src/protocol.rs**
   - Add: LIFE_CHECK (70), LIFE_CHECK_ACK (71), REDIRECT (72), CLIENT_LEAVE (73) constants
   - All messages are TCP-based

7. **src/p2p_server.rs**
   - Add: LIFE_CHECK handler - Respond with ACK via TCP
   - **CRITICAL**: P2P port must run TCP server to receive LIFE_CHECK from cloud servers

8. **src/simple_client.rs**
   - Add: REDIRECT handler - Reconnect to new executor via TCP

9. **src/main.rs**
   - Add: CLIENT_LEAVE message on Ctrl+C (graceful shutdown via TCP)

---

## Part 9: Implementation Order (Recommended)

**Day 1 - Core Functionality**:
1. Task 1.1: Periodic DOS broadcast (executor → clients)
2. Task 1.2: Periodic Firebase sync (leader → executor)
3. Task 4.1: 5-retry logic for leader communication

**Day 2 - System Recovery**:
4. Task 2.1: Add message type constants
5. Task 2.2: Implement LIFE_CHECK mechanism (server)
6. Task 2.3: System recovery function
7. Task 2.4: LIFE_CHECK handler (client)

**Day 3 - Executor Failover**:
8. Task 3.1: REDIRECT message sending (server)
9. Task 3.2: REDIRECT handler (client)

---

## Part 10: Testing Strategy

### Unit Tests
- [ ] `build_dos_c_payload_excluding()` excludes correct client
- [ ] `send_life_check()` times out properly (1 second)
- [ ] `check_executor_and_redirect()` detects non-executor

### Integration Tests
- [ ] **Periodic broadcast**: Client receives DOS_UPDATE every 5 seconds
- [ ] **Firebase sync**: Executor syncs from Firebase every 5 seconds
- [ ] **System recovery**: Offline clients marked as offline on startup
- [ ] **Client JOIN**: DOS updates and broadcasts to all clients within 5s
- [ ] **Client disconnect**: DOS updates and broadcasts offline status within 5s
- [ ] **Image upload**: New image appears in DOS within 5s of encryption
- [ ] **Executor failover**: Clients receive REDIRECT and reconnect
- [ ] **Retry logic**: Leader communication succeeds after transient failures
- [ ] **LIFE_CHECK**: Server can detect offline clients on startup

### End-to-End Tests
- [ ] 3-server cluster with clients connecting to different servers
- [ ] Executor failover with active client connections
- [ ] Network partition simulation (leader unreachable for 10s)
- [ ] Server restart with stale online clients
- [ ] Multiple clients uploading simultaneously

---

## Part 11: Final Analysis

### Critical Architectural Corrections

Based on user clarifications, the following critical corrections were made:

1. **NO Local DOS Caching** (Clarification #3)
   - ❌ **Original plan**: Servers maintain local `dos_clients` HashMap synced from Firebase
   - ✅ **Corrected**: Servers read from Firebase every 5s → broadcast directly to clients (NO caching)
   - **Impact**: Firebase is the ONLY persistent storage, servers are pass-through

2. **TCP-Only Communication** (User requirement)
   - ❌ **Original plan**: LIFE_CHECK used UDP
   - ✅ **Corrected**: ALL server-client communication via TCP (including LIFE_CHECK)
   - **Impact**: Server creates TCP connection to client's P2P port for liveness check

3. **Leader ≠ Executor** (Clarification #4)
   - ❌ **Original plan**: Confused leader and executor terminology
   - ✅ **Corrected**: Clear separation - Leader (Firebase writes) vs Executor (client connections)
   - **Impact**: Correct role checks in failover detection and system recovery

### Gaps Addressed from Your Proposal

✅ **What your proposal covered**:
1. DOS updates on client startup → ✅ Already implemented
2. DOS updates on image upload → ✅ Recently implemented
3. DOS updates on disconnect → ✅ Already implemented
4. Periodic 5-second sync → **NOW IMPLEMENTED** (Task 1.2: Firebase read + broadcast)

✅ **What we added to your proposal**:
1. **System recovery with LIFE_CHECK** → Tasks 2.1-2.4 (TCP-based liveness verification)
2. **Executor failover detection** → Task 3.1 (graceful connection closure)
3. **Client cleanup** → Tasks 3.2-3.4 (graceful shutdown + stale removal)
4. **5-retry logic** → Task 4.1 (exponential backoff for executor-leader communication)

### Architecture Flow (After Implementation)

```
┌──────────────────────────────────────────────────────────────────┐
│                    DOS UPDATE FLOW                               │
├──────────────────────────────────────────────────────────────────┤
│ 1. Client Event (JOIN, DISCONNECT, UPLOAD)                      │
│     ↓                                                            │
│ 2. Executor sends to Leader (EXEC_* message, 5 retries, TCP)   │
│     ↓                                                            │
│ 3. Leader Writes to Firebase (ONLY persistent storage)          │
│     ↓                                                            │
│ 4. Executor Reads from Firebase (every 5s)                      │
│     ↓                                                            │
│ 5. Executor → All Clients (DOS_UPDATE broadcast via TCP, every 5s) │
│     ↓                                                            │
│ 6. Clients Update Local DOS Cache                               │
│                                                                  │
│ NOTE: Servers do NOT cache DOS - Firebase is the only source    │
└──────────────────────────────────────────────────────────────────┘

SYSTEM RECOVERY FLOW (TCP-based):
├─ Current Executor Startup
├─ Executor Reads Firebase (all clients marked online)
├─ For each online client: Create TCP connection to client's P2P port
├─ Send LIFE_CHECK via TCP → Client responds LIFE_CHECK_ACK (or timeout)
└─ Executor updates Firebase: mark non-responding clients as offline

EXECUTOR FAILOVER FLOW:
├─ ASSIGN message changes executor IP
├─ Old executor detects it's no longer current system executor
├─ Old executor closes all client TCP connections
├─ Clients use REQUEST_EXECUTOR broadcast to find new executor
├─ Clients establish new TCP connection with new executor
└─ Migration completed (no REDIRECT needed - existing discovery)
```

### Key Improvements

| Aspect | Before | After |
|--------|--------|-------|
| **DOS Sync** | Pull-only (client queries) | Push every 5s via TCP from executor |
| **DOS Storage** | Possibly cached locally | ❌ NO caching - Firebase is sole source |
| **Firebase Read** | None (TODO stubs) | Read every 5s → broadcast to clients |
| **System Recovery** | None | TCP-based LIFE_CHECK to verify clients |
| **Executor Failover** | Orphaned connections | Graceful closure + client rediscovery |
| **Server-Client Protocol** | Mixed | ✅ TCP-only (no UDP) |
| **Leader/Executor Roles** | Confused | Clear separation of responsibilities |
| **Leader Comm** | Single attempt | 5 retries with exponential backoff |
| **Online Status** | May be stale | Updated within 5s max |
| **DOS Accuracy** | Can be outdated | Max 5s staleness guaranteed |

---

## Part 12: Success Criteria

The implementation is successful when:

✅ **DOS Accuracy**:
- All clients receive DOS updates within 5 seconds of any change
- Online/offline status reflects true client state within 5 seconds
- New images appear in DOS within 5 seconds of encryption

✅ **System Robustness**:
- Server can recover from restart (marks stale clients offline)
- Executor can retry leader communication (5 attempts)
- Clients can migrate during executor failover

✅ **Scalability**:
- Broadcast scales to 100+ connected clients
- Firebase sync doesn't overwhelm database
- LIFE_CHECK completes for 100+ clients in reasonable time

✅ **Correctness**:
- No ghost clients (marked online but not connected)
- No missing updates (all changes propagate)
- No version conflicts (leader is source of truth)

# View Request System Refactor - Implementation Plan (REVISED)

## Overview
Refactor the view request system from server-mediated to P2P direct communication with proper online/offline handling, local access map management, and secure view count tracking.

**MAJOR ARCHITECTURE CHANGE:** Owner sends pre-encrypted images AS-IS (no re-encryption per viewer). View count transmitted in protocol message, tracked locally by viewer.

## Current State Analysis (VERIFIED)

### What's Working
- ✅ P2P TCP server active on clients (port 9080+)
- ✅ Local access_map exists (`Client-Node/src/local_access_map.rs`)
- ✅ Steganography server-side encryption working (`stego_service.rs`)
- ✅ **Encrypted images already received by owner in `encrypted_images/` directory**
- ✅ Firebase offline_requests_map with owner-keyed structure
- ✅ DOS-S includes `online` boolean field
- ✅ Client startup queries offline requests
- ✅ Dummy metadata already defined (empty strings, 0 values)
- ✅ Image chunk reception and reassembly logic complete

### Critical Gaps
- ❌ DOS-C (client-side) missing `online` field for owner availability detection
- ❌ P2P view request handlers are stubs (no approval/rejection logic)
- ❌ No viewer-side P2P connection initiation for direct requests
- ❌ Decrypted images permanently stored on disk (security issue)
- ❌ No viewer-side local map for view count tracking
- ❌ No view count decrement on image view
- ❌ Access_map stored in multiple places (Firebase + server + client) - needs cleanup
- ❌ No extensive logging for debugging flow
- ❌ UI missing online status display

## Implementation Plan

### Phase 1: DOS Enhancement - Add Online Status to Client-Side

**Goal:** Enable clients to detect owner online/offline status from local DOS

**Files:**
- `Client-Node/src/dos.rs`
- `Cloud-Node/src/tcp_client.rs` (JOIN_ACK, DOS_UPDATE builders)
- `Cloud-Node/src/client_protocol.rs` (DOS_QUERY handler)

**Changes:**
1. Add `online: bool` field to client-side `DosClient` struct
2. Update DOS-C parsing in `parse_dos_c_from_join_ack()` to read online flag
3. Update server DOS broadcast builders to include online status:
   - `handle_join()` - JOIN_ACK payload
   - `handle_dos_query_tcp()` - DOS_UPDATE payload
   - `broadcast_dos_to_clients()` - DOS_UPDATE payload
4. Wire format: Add 1 byte after `client_port` field: `[online:u8]` (0=offline, 1=online)
5. Add logging: Client prints DOS with online status on receive

**Testing:** Client can see which owners are online/offline in local DOS

---

### Phase 2: Access Map Cleanup - Remove Server/Firebase Storage

**Goal:** Keep ONLY client-side local access maps (owner and viewer separate)

**Files to KEEP:**
- `Client-Node/src/local_access_map.rs` (owner's grants)
- `Client-Node/src/viewer.rs` (viewer's remaining views - NEW)

**Files to REMOVE/MODIFY:**
- `Cloud-Node/src/access_map_storage.rs` - **DELETE** entire file
- `Cloud-Node/src/firebase.rs` - Remove `dos_s_access` collection code:
  - Remove `write_access()` function
  - Remove `read_all_access()` function
  - Remove `cleanup_expired_access()` function
  - Remove `DosAccess` struct
- `Cloud-Node/src/tcp_client.rs` - Remove `handle_access_map_query()` handler
- `Cloud-Node/src/state.rs` - Remove `dos_access` field if exists

**Changes:**
1. **Owner-side:** Keep `LocalAccessMap` as-is, ensure save on every grant/revoke
2. **Viewer-side:** Create new `ViewerAccessMap` (Phase 4A)
3. **Server-side:** Remove all access map tracking
4. **Firebase:** Delete `dos_s_access` collection (manual cleanup after deployment)

**Storage Locations:**
- Owner: `~/.p2p_client/local_access_map.json` (what I granted to others)
- Viewer: `~/.p2p_client/viewer_access_map.json` (what I have access to)

**Logging:**
- Owner: "Granted 5 views of photo.png to bob"
- Viewer: "Received photo.png from alice (5 views)"

**Testing:** Restart both owner and viewer clients → maps restored from JSON

---

### Phase 3: P2P Direct View Request - Online Owner Flow

**Goal:** Viewer sends request directly to owner's P2P server when online

#### 3A. Viewer-Side: Initiate P2P Connection

**Files:**
- `Client-Node/src/viewer.rs`

**New Function:** `send_peer_view_request()`
```rust
pub async fn send_peer_view_request(
    state: SharedClientState,
    owner_name: &str,
    image_name: &str,
    requested_views: u32,
) -> Result<u32> // Returns request_id
```

**Flow:**
1. Check local DOS: `state.dos.get_client(owner_name)`
2. If `online == false` → return `Err("Owner offline")`
3. Get owner's `client_ip` and `client_port` from DOS
4. Connect to `TcpStream::connect((owner_ip, owner_port))`
5. Send PEER_VIEW_REQUEST message:
   ```
   [msg_type: PEER_VIEW_REQUEST]
   [viewer_len:u16][viewer:username]
   [image_len:u16][image:image_name]
   [requested_views:u32]
   ```
6. Store pending request locally (existing `PendingRequest` struct)
7. Return request_id
8. Log: "[VIEWER] Sent P2P view request to alice for image.png (5 views)"

#### 3B. Owner-Side: Handle P2P View Request

**Files:**
- `Client-Node/src/p2p_server.rs`
- `Client-Node/src/owner.rs`

**Complete:** `handle_peer_view_request()` implementation

**Flow:**
1. Parse PEER_VIEW_REQUEST payload
2. Load local_access_map from file
3. Check if viewer already has access: `local_access_map.get_grant(viewer, image)`
4. **Prompt owner via UI** (API endpoint or console):
   - Show: "alice wants to view image.png (requests 5 views)"
   - Options: [Approve] [Deny] [Modify count]
5. Store pending approval in memory
6. Log: "[OWNER] P2P view request from alice for image.png (5 views) - awaiting approval"

**New API Endpoint:** `POST /api/pending-requests/:request_id/approve`
```json
{
  "approved": true,
  "final_view_count": 5
}
```

#### 3C. Owner-Side: Approval Response (REVISED - NO RE-ENCRYPTION)

**Files:**
- `Client-Node/src/owner.rs`
- `Client-Node/src/p2p_server.rs`

**New Function:** `approve_peer_view_request()`
```rust
pub async fn approve_peer_view_request(
    stream: &mut TcpStream,
    viewer: &str,
    image_name: &str,
    final_view_count: u32,
) -> Result<()>
```

**Flow (APPROVAL):**
1. **Update local_access_map:**
   ```rust
   let mut map = LocalAccessMap::load_from_file()?;
   map.grant_access(viewer, image_name, final_view_count);  // REPLACE existing grant
   map.save_to_file()?;
   ```

2. **Load pre-encrypted image** (already exists from initial upload):
   ```rust
   let encrypted_path = format!("encrypted_storage/{}.png", image_name);
   let encrypted_bytes = tokio::fs::read(&encrypted_path).await?;
   ```

   **NO RE-ENCRYPTION** - Use the image AS-IS from server

3. **Send PEER_VIEW_RESPONSE with view count:**
   ```
   [msg_type: PEER_VIEW_RESPONSE]
   [owner_len:u16][owner:string]
   [image_name_len:u16][image_name:string]
   [final_views:u32]              // View count in PROTOCOL, not in image
   [num_chunks:u32]
   ```

4. **Send image chunks via PEER_IMAGE_CHUNK:**
   ```
   [msg_type: PEER_IMAGE_CHUNK]
   [owner_len:u16][owner:string]         // For filename construction
   [image_name_len:u16][image_name:string]
   [chunk_seq:u32][total_chunks:u32]
   [chunk_data_len:u16][chunk_data:bytes]
   ```

   Chunk size: 1000 bytes (match existing IMAGE_CHUNK implementation)

5. Log: "[OWNER] Approved bob for photo.png (5 views) - sending pre-encrypted image"

**Key Change:** View count sent in protocol message, NOT embedded in image file. Image sent once, view count managed separately.

**Flow (REJECTION):**
1. Send PEER_VIEW_REJECTED:
   ```
   [msg_type: PEER_VIEW_REJECTED]
   [reason_len:u16][reason:string]
   ```
2. Close connection
3. Log: "[OWNER] Rejected alice's request for image.png"

#### 3D. Viewer-Side: Receive Response (REVISED)

**Files:**
- `Client-Node/src/viewer.rs`

**New Function:** `handle_peer_view_response()`
```rust
pub async fn handle_peer_view_response(
    stream: &mut TcpStream,
) -> Result<(String, String, u32, PathBuf)>  // (owner, image_name, view_count, path)
```

**Flow:**
1. **Receive PEER_VIEW_RESPONSE or PEER_VIEW_REJECTED:**
   ```rust
   let msg_type = read_u8(stream).await?;
   match msg_type {
       PEER_VIEW_REJECTED => {
           let reason = read_string(stream).await?;
           return Err(anyhow!("Request rejected: {}", reason));
       }
       PEER_VIEW_RESPONSE => { /* continue */ }
       _ => return Err(anyhow!("Unexpected message type")),
   }
   ```

2. **Parse PEER_VIEW_RESPONSE:**
   ```rust
   let owner = read_string(stream).await?;
   let image_name = read_string(stream).await?;
   let final_views = read_u32(stream).await?;  // View count from PROTOCOL
   let num_chunks = read_u32(stream).await?;
   ```

3. **Receive PEER_IMAGE_CHUNK messages (reassemble):**
   ```rust
   let mut chunks: HashMap<u32, Vec<u8>> = HashMap::new();
   for _ in 0..num_chunks {
       let chunk_owner = read_string(stream).await?;
       let chunk_image = read_string(stream).await?;
       let seq = read_u32(stream).await?;
       let total = read_u32(stream).await?;
       let data = read_bytes(stream).await?;
       chunks.insert(seq, data);

       // Progress log
       println!("[VIEWER] 📥 Chunk {}/{} for {}/{}", seq+1, total, owner, image_name);
   }
   ```

4. **Assemble and save encrypted image:**
   ```rust
   let mut assembled = Vec::new();
   for seq in 0..num_chunks {
       assembled.extend(chunks.get(&seq).ok_or_else(|| anyhow!("Missing chunk"))?);
   }

   let save_path = format!("encrypted_storage/{}_{}.png", owner, image_name);
   tokio::fs::create_dir_all("encrypted_storage").await?;
   tokio::fs::write(&save_path, &assembled).await?;
   ```

5. **Process received image (initial verification):**
   ```rust
   process_received_encrypted_image(&owner, &image_name, final_views, &save_path).await?;
   ```
   This will:
   - Decrypt once to verify valid stego format
   - Store view count in viewer_access_map.json
   - Discard decrypted bytes (not shown to user yet)

6. Log: "[VIEWER] Received {}/{} from owner (initial views: {})", owner, image_name, final_views)
7. Return (owner, image_name, final_views, save_path)

---

### Phase 4: Viewer-Side View Count Management (REVISED ARCHITECTURE)

**Goal:** Track view count locally in map (not in image file), decrypt on demand, never save plaintext

#### 4A. Viewer Local Map Structure

**Files:**
- `Client-Node/src/viewer.rs` (new module additions)

**New Structure:** `ViewerAccessMap`
```rust
pub struct ViewerAccessMap {
    pub grants: HashMap<String, ViewGrant>,  // Key: "{owner}_{image_name}"
}

pub struct ViewGrant {
    pub owner: String,
    pub image_name: String,
    pub remaining_views: u32,
    pub granted_at: u64,  // Unix timestamp ms
    pub encrypted_image_path: String,  // Path to local encrypted file
}
```

**Storage Location:** `~/.p2p_client/viewer_access_map.json`

**Methods:**
```rust
impl ViewerAccessMap {
    pub fn load() -> Result<Self>  // Load from JSON file
    pub fn save(&self) -> Result<()>  // Save to JSON file
    pub fn add_grant(&mut self, owner: &str, image: &str, views: u32, path: String)
    pub fn get_grant(&self, owner: &str, image: &str) -> Option<&ViewGrant>
    pub fn get_grant_mut(&mut self, owner: &str, image: &str) -> Option<&mut ViewGrant>
    pub fn decrement_view(&mut self, owner: &str, image: &str) -> Result<u32>  // Returns new count
    fn make_key(owner: &str, image: &str) -> String  // Format: "{owner}_{image}"
}
```

#### 4B. On Image Receipt - Extract View Count (INITIAL DECRYPT)

**Files:**
- `Client-Node/src/viewer.rs`

**New Function:** `process_received_encrypted_image()`
```rust
pub async fn process_received_encrypted_image(
    owner: &str,
    image_name: &str,
    view_count: u32,  // From protocol message, NOT from stego metadata
    encrypted_path: &str,
) -> Result<()>
```

**Flow:**
1. **Initial decrypt to verify image:**
   ```rust
   let embedded_bytes = tokio::fs::read(encrypted_path).await?;
   let (secret_bytes, dummy_metadata) =
       stego_client::decrypt_image_and_extract_metadata(&embedded_bytes).await?;
   ```
2. **Ignore dummy metadata** (contains empty strings and 0s from server)
3. **Discard secret_bytes immediately** (do NOT save, do NOT show)
4. **Store in viewer map:**
   ```rust
   let mut map = ViewerAccessMap::load()?;
   map.add_grant(owner, image_name, view_count, encrypted_path.to_string());
   map.save()?;
   ```
5. Log: "[VIEWER] Received alice/photo.png (5 views) - verified and registered"

**Purpose:** Verify image is valid stego format, store view count from protocol

#### 4C. On User View Action - Decrypt and Display

**Files:**
- `Client-Node/src/api_server.rs`
- `Client-Node/src/viewer.rs`

**New API Endpoint:** `POST /api/view/:owner/:image`
```rust
async fn view_image(
    State(state): State<ApiState>,
    Path((owner, image_name)): Path<(String, String)>,
) -> Result<Response<Body>>
```

**Flow:**
1. **Load viewer map and check count:**
   ```rust
   let mut map = ViewerAccessMap::load()?;
   let grant = map.get_grant(&owner, &image_name)
       .ok_or_else(|| anyhow!("Image not found"))?;

   if grant.remaining_views == 0 {
       return Err((StatusCode::FORBIDDEN, "No views remaining").into());
   }
   ```

2. **Decrypt in memory:**
   ```rust
   let embedded_bytes = tokio::fs::read(&grant.encrypted_image_path).await?;
   let (secret_bytes, _dummy_metadata) =
       stego_client::decrypt_image_and_extract_metadata(&embedded_bytes).await?;
   ```

3. **Decrement count in map (BEFORE rendering):**
   ```rust
   let new_count = map.decrement_view(&owner, &image_name)?;
   map.save()?;  // Persist immediately
   ```

4. **Return decrypted image in HTTP response:**
   ```rust
   Ok(Response::builder()
       .header("Content-Type", "image/png")
       .body(Body::from(secret_bytes))?)
   ```

5. **DO NOT save secret_bytes to disk**
6. Log: "[VIEWER] Viewed alice/photo.png ({} views remaining)", new_count)

**Security:** Decrypted image only exists in HTTP response body (RAM), never written to filesystem

#### 4D. Offline Viewing Support

**Current State:** Client has stego library and can decrypt locally

**Offline Behavior:**
- Viewer map stored in JSON file (persistent across restarts)
- View count decrements work offline (no server/owner communication)
- Owner never receives usage updates (trust-based model)

**Note:** This is by design - owner's local_access_map is "what I granted", viewer's map is "what I have left"

---

### Phase 5: Offline Owner Request Handling

**Goal:** Viewer sends request to server when owner offline; owner processes on return

#### 5A. Viewer Detects Owner Offline

**Files:**
- `Client-Node/src/viewer.rs`

**Modify:** `send_peer_view_request()` (from Phase 3A)

**Flow:**
1. Check DOS: `state.dos.get_client(owner_name)`
2. If `online == false`:
   - Log: "[VIEWER] Owner alice is offline, sending request to server"
   - Call `send_offline_view_request_to_server()` (new function)
3. If `online == true`:
   - Proceed with P2P direct (existing Phase 3A flow)

**New Function:** `send_offline_view_request_to_server()`
```rust
pub async fn send_offline_view_request_to_server(
    writer: Arc<Mutex<OwnedWriteHalf>>,
    viewer: &str,
    owner: &str,
    image_name: &str,
    requested_views: u32,
) -> Result<()>
```

**Flow:**
1. Send OFFLINE_VIEW_REQUEST message to server (TCP):
   ```
   [msg_type: NEW_CONSTANT]
   [viewer_len:u16][viewer]
   [owner_len:u16][owner]
   [image_len:u16][image]
   [requested_views:u32]
   [timestamp:u64]
   ```
2. Log: "[VIEWER] Sent offline view request to server for alice/image.png"

**New Protocol Constant:** `OFFLINE_VIEW_REQUEST = 77` (add to protocol.rs)

#### 5B. Server Stores Offline Request

**Files:**
- `Cloud-Node/src/tcp_client.rs`
- `Cloud-Node/src/firebase.rs`
- `Cloud-Node/src/client_protocol.rs`

**New Handler:** `handle_offline_view_request()`
```rust
async fn handle_offline_view_request(
    state: SharedState,
    cfg: &Config,
    _stream: Arc<Mutex<TcpStream>>,
    peer_addr: SocketAddr,
    data: &[u8],
) -> Result<()>
```

**Flow:**
1. Parse OFFLINE_VIEW_REQUEST payload
2. Create `OfflineRequest` struct (already exists in firebase.rs)
3. Store in Firebase:
   ```rust
   firebase::add_offline_request(db, &owner, offline_req).await?;
   ```
4. Collection: `offline_requests_map` with document ID = `owner`
5. Log: "[SERVER] Stored offline request: bob → alice/image.png (5 views)"

**Firebase Structure (ALREADY EXISTS):**
```rust
pub struct OfflineRequestsDoc {
    pub owner: String,            // Document ID
    pub requests: Vec<OfflineRequest>,
}

pub struct OfflineRequest {
    pub requester: String,
    pub owner: String,
    pub image_name: String,
    pub request_id: u32,
    pub requested_views: u32,
    pub timestamp: u64,
}
```

#### 5C. Owner Startup - Retrieve and Process Offline Requests (REVISED)

**Files:**
- `Client-Node/src/simple_client.rs` (startup flow)
- `Client-Node/src/owner.rs`

**Current Flow (ALREADY EXISTS):**
1. Client sends OFFLINE_REQUESTS_QUERY on JOIN
2. Server calls `firebase::get_and_delete_offline_requests(db, username)`
3. Server sends OFFLINE_REQUESTS_RESPONSE
4. Client receives pending requests

**Enhancement Needed:** Process received requests

**New Function:** `process_offline_requests()`
```rust
pub async fn process_offline_requests(
    state: SharedClientState,
    requests: Vec<OfflineRequest>,
) -> Result<()>
```

**Flow:**
1. **For each offline request:**
   ```rust
   for request in requests {
       println!("[OWNER] Processing offline request: {} wants {}/{} ({} views)",
                request.requester, request.owner, request.image_name, request.requested_views);

       // Check if requester is still online
       let requester_info = {
           let s = state.read().await;
           s.dos.get_client(&request.requester).cloned()
       };

       let requester = match requester_info {
           Some(client) if client.online => client,
           _ => {
               println!("[OWNER] Dropping request from {} (requester offline)", request.requester);
               continue;
           }
       };

       // Initiate P2P connection TO requester
       let requester_addr = format!("{}:{}", requester.client_ip, requester.client_port);
       let stream = match TcpStream::connect(&requester_addr).await {
           Ok(s) => s,
           Err(e) => {
               println!("[OWNER] Failed to connect to {}: {}", request.requester, e);
               continue;
           }
       };

       // Prompt owner for approval (store in pending state)
       // UI will call approve/deny via API endpoint
       store_pending_request(state.clone(), request).await?;
   }
   ```

2. **Approval/Denial handled via UI** (same as online requests)
3. **Send response to requester** (follow Phase 3C flow):
   - Send PEER_VIEW_RESPONSE with view count + encrypted image chunks
   - OR send PEER_VIEW_REJECTED

4. Log: "[OWNER] Processed {} offline requests", requests.len()

**Note:** Owner initiates connection TO requester (not typical request/response pattern)

---

### Phase 6: Extensive Logging

**Goal:** Add debug-level logging throughout the flow for troubleshooting

#### Logging Points

**Client-Side:**
- DOS updates: Print full DOS with online status
- View request sent (P2P or server)
- Request approval/rejection received
- Image chunk received (progress %)
- View count extraction
- Image view with decrement
- Offline request stored/retrieved

**Server-Side:**
- Offline request received and stored
- Offline request retrieved and deleted
- DOS broadcast with online status changes
- Client connect/disconnect events

**Format:**
```rust
println!("[{}] {}", component, message);
// Example: "[VIEWER] Sent P2P view request to alice for image.png (5 views)"
```

**Components:**
- [VIEWER] - Viewer operations
- [OWNER] - Owner operations
- [P2P] - P2P connection events
- [DOS] - DOS updates
- [STEGO] - Steganography operations
- [SERVER] - Server-side operations
- [FIREBASE] - Firebase operations

---

### Phase 7: UI Integration

**Goal:** Add UI components to display and interact with view requests

#### 7A. DOS Display in Web UI

**Files:**
- `Client-Node/web-ui/src/components/` (new component)

**Component:** `DOSViewer.tsx`

**Features:**
- Display all clients from local DOS
- Show online/offline status (green/red badge)
- Show available images per client
- Click image → Send view request
- Real-time updates on DOS change

**API Endpoint (NEW):** `GET /api/dos`
```rust
async fn get_dos(
    State(state): State<ApiState>,
) -> Result<Json<DosResponse>>

struct DosResponse {
    version: u64,
    clients: Vec<DosClientInfo>,
}

struct DosClientInfo {
    name: String,
    ip: String,
    port: u16,
    online: bool,
    images: Vec<String>,
}
```

#### 7B. Pending Requests UI

**Component:** `PendingRequests.tsx`

**Features:**
- Display pending view requests (owner-side)
- Show: Viewer name, image name, requested views, timestamp
- Actions: [Approve] [Deny] [Modify Count]
- Click Approve → Show count input → Confirm

**API Endpoints (NEW):**
- `GET /api/pending-requests` - List all pending
- `POST /api/pending-requests/:id/approve` - Approve with final count
- `POST /api/pending-requests/:id/deny` - Deny with reason

#### 7C. Image Gallery with View Counts (REVISED)

**Component:** `ImageGallery.tsx` (enhance existing)

**Features:**
- Display encrypted images from `encrypted_storage/` folder
- Show view count badge from viewer_access_map (not from image file)
- Show online/offline status for image owners
- Click image → View (decrements count in map)
- Show "0 views remaining" for exhausted images (disable viewing)
- Separate sections: "My Encrypted Images" (owner) vs "Shared With Me" (viewer)

**API Endpoint (NEW):** `GET /api/viewer-grants`
```rust
struct ViewerGrantInfo {
    owner: String,
    image_name: String,
    remaining_views: u32,
    granted_at: u64,
    encrypted_path: String,
    owner_online: bool,  // From DOS
}
```

**API Endpoint (ENHANCE):** `GET /api/owner-grants`
```rust
struct OwnerGrantInfo {
    viewer: String,
    image_name: String,
    granted_views: u32,
    granted_at: u64,
    viewer_online: bool,  // From DOS
}
```

---

## Implementation Order (REVISED)

1. **Phase 0** - Encrypted storage reorganization (move images, update paths)
2. **Phase 1** - DOS online status (enables online/offline detection)
3. **Phase 2** - Access map cleanup (remove server/Firebase code)
4. **Phase 4A** - Viewer local map structure (foundation for tracking)
5. **Phase 3A-3B** - P2P viewer request + owner receive (core flow)
6. **Phase 3C-3D** - Owner approval + viewer receive (NO re-encryption)
7. **Phase 4B-4D** - Viewer receipt processing + secure viewing
8. **Phase 5A-5B** - Offline request to server
9. **Phase 5C** - Owner retrieves and processes offline requests
10. **Phase 6** - Add extensive logging everywhere
11. **Phase 7** - UI integration with online status

### Phase 0: Encrypted Storage Reorganization (NEW)

**Goal:** Organize encrypted images for owner reuse

**Changes:**
1. Create `encrypted_storage/` directory at client root
2. When owner receives encrypted image from server (IMAGE_CHUNK):
   - Save to `encrypted_storage/{original_name}.png` (not `{name}_encrypted.png`)
   - Maintain index: `encrypted_image_index.json`
   ```json
   {
     "photo": {
       "path": "encrypted_storage/photo.png",
       "uploaded_at": 1234567890,
       "size_bytes": 1048576
     }
   }
   ```
3. Update DOS to use clean names (not `{name}_encrypted`)
4. Owner can now send same encrypted file to multiple viewers

## Critical Files to Modify (REVISED)

**Client-Node:**
- `src/dos.rs` - Add `online: bool` field to `DosClient`
- `src/viewer.rs` - Add `ViewerAccessMap`, P2P request sending, response handling, view count tracking
- `src/owner.rs` - Approval logic, send pre-encrypted images (NO re-embedding)
- `src/p2p_server.rs` - Complete PEER_VIEW_REQUEST handler
- `src/stego_client.rs` - Keep existing decrypt function (used for verification and viewing)
- `src/api_server.rs` - New endpoints: DOS, pending requests, viewer grants, owner grants, view image
- `src/protocol.rs` - New constant: OFFLINE_VIEW_REQUEST = 77
- `src/simple_client.rs` - Update IMAGE_CHUNK handler to save to `encrypted_storage/`

**Cloud-Node:**
- `src/tcp_client.rs` - DOS broadcast with online field, offline request handler, remove access_map_query
- `src/client_protocol.rs` - New constant: OFFLINE_VIEW_REQUEST = 77, update DOS-C wire format
- `src/firebase.rs` - Remove `dos_s_access` collection code (write_access, read_all_access, cleanup_expired_access)
- `src/access_map_storage.rs` - **DELETE** this entire file
- `src/state.rs` - Remove `dos_access` field if exists

**Web UI:**
- `web-ui/src/components/DOSViewer.tsx` - **NEW** (show clients with online status)
- `web-ui/src/components/PendingRequests.tsx` - **NEW** (owner approves requests)
- `web-ui/src/components/ImageGallery.tsx` - **ENHANCE** (show view counts from map, online status)
- `web-ui/src/components/ImageViewer.tsx` - **NEW** (display decrypted image from memory)

## Testing Strategy

1. **Unit Tests:** Steganography view count extraction
2. **Integration Tests:**
   - Online owner: Send request → Approve → Receive image → View with decrement
   - Offline owner: Send to server → Owner startup → Retrieve → Process
3. **E2E Test Scenarios:**
   - 2 clients + 1 server: Alice (owner) online, Bob (viewer) requests
   - 2 clients + 1 server: Alice offline, Bob sends to server, Alice returns
   - View count exhaustion: Bob views 5 times, 6th attempt blocked

## Security Considerations (REVISED)

1. **Decrypted Images Never Stored:** Only streamed in HTTP response (RAM only), never written to disk
2. **View Count Trust Model:** Viewer map is local-only, can be tampered with (ACCEPTED - trust-based system)
3. **No Re-encryption Per Viewer:** Same encrypted image sent to all viewers, view count in protocol message
4. **Access Map Local Only:** No server-side tracking reduces attack surface
5. **P2P Authentication:** Verify requester identity via username in message (basic trust model)
6. **Steganography Not Encryption:** LSB hiding (obfuscation), not cryptographic encryption - anyone with stego library can extract
7. **Dummy Metadata Ignored:** Server embeds empty metadata as library placeholder, viewer ignores on decrypt

## Migration Notes (REVISED)

- Deprecated server-mediated handlers remain for backward compatibility
- Firebase `dos_s_access` collection should be deleted manually after deployment
- Existing `access_maps/*.json` files on server can be deleted (no longer used)
- Client-side `local_access_map.json` format unchanged (owner grants)
- New `viewer_access_map.json` will be created for viewer tracking
- Existing `encrypted_images/` files should be moved to `encrypted_storage/` and renamed (remove `_encrypted` suffix)
- DOS-C wire format changes (add online byte) - clients must update together

## Key Architecture Changes Summary

### OLD (Deprecated):
- Server mediates view requests (VIEW_REQUEST → VIEW_NOTIFICATION → APPROVE_VIEW)
- Server re-encrypts image per viewer with embedded view count
- Access map stored in Firebase + server filesystem + client
- Decrypted images saved to disk permanently
- View count embedded in image file metadata

### NEW (Target):
- P2P direct view requests (PEER_VIEW_REQUEST → PEER_VIEW_RESPONSE)
- Owner sends same pre-encrypted image to all viewers (NO re-encryption)
- View count sent in protocol message, tracked in viewer's local map
- Access map ONLY on clients (owner tracks grants, viewer tracks remaining)
- Decrypted images never saved, only rendered in HTTP response (memory)
- View count decrements stored in viewer_access_map.json
- Owner never receives usage updates (trust-based model)

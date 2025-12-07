# DOS-C Parsing Fix - Verification Guide

## The Bug

**Symptom**: Client shows garbage values for DOS version and num_clients:
```
[DOS] Parsing JOIN_ACK: version=8589934593 num_clients=1868824586
[CLIENT] ⚠️ Failed to parse DOS-C from JOIN_ACK: Invalid name length at client 0
```

**Root Cause**: Type mismatch between server and client

| Component | Field Type | Wire Format Expected |
|-----------|------------|---------------------|
| Server (state.rs:74) | `dos_c_version: u32` | - |
| Server (tcp_client.rs:347) | Sent as-is | **4 bytes** ❌ |
| Client (dos.rs:91) | Expects `u64` | **8 bytes** ✅ |

When server sent 4 bytes but client expected 8 bytes, all subsequent fields became misaligned, causing parsing to fail.

## The Fix

**File**: `Cloud-Node/src/tcp_client.rs:347`

**Before**:
```rust
// DOS version (u64)
payload.extend(s.dos_c_version.to_le_bytes());  // ❌ Sends 4 bytes (u32)
```

**After**:
```rust
// DOS version (u64) - Cast u32 to u64 for wire format!
let dos_version_u64 = s.dos_c_version as u64;
payload.extend(dos_version_u64.to_le_bytes());  // ✅ Sends 8 bytes (u64)
```

## Testing Steps

### 1. Rebuild Server
```bash
cd Cloud-Node
cargo build --release
cargo run --release
```

Wait for:
```
🚀 TCP client server listening on 0.0.0.0:9080
```

### 2. Start First Client
**Terminal 1**:
```bash
cd Client-Node
cargo run -- interactive bob 10.40.61.79:9080 cover.png secret.png
```

**Expected Output** (CORRECT):
```
[CLIENT] ✅ JOIN_ACK received from server
[DOS] Parsing JOIN_ACK: version=1 num_clients=1     ← CORRECT VALUES!
[DOS] Parsed client: bob (online=true, 2 images)
[DOS] Successfully parsed 1 clients from JOIN_ACK
[CLIENT] ✅ Successfully joined server!
```

**Old Output** (WRONG):
```
[DOS] Parsing JOIN_ACK: version=8589934593 num_clients=1868824586  ← GARBAGE!
[CLIENT] ⚠️ Failed to parse DOS-C from JOIN_ACK: Invalid name length
```

### 3. Start Web UI
**Terminal 2**:
```bash
cd Client-Node/web-ui
npm run dev
```

Open http://localhost:3000
- Enter username: `bob`
- Click "Connect to Network"

**Expected**: 
- Status bar shows "Connected" ✅
- DOS table shows "bob" with 2 images ✅
- Images: "cover.png", "secret.png" ✅

### 4. Test Multiple Clients
**Terminal 3**:
```bash
cd Client-Node
cargo run -- interactive alice 10.40.61.79:9080 image1.jpg image2.jpg
```

**Expected in bob's web UI**:
- DOS table should update automatically
- Should see BOTH "bob" and "alice"
- Each with their respective images

**Terminal 4** (alice's web UI):
```bash
cd Client-Node/web-ui
npm run dev
# In a different browser profile or incognito window
```

Open http://localhost:3000
- Username: `alice`
- Should see both "alice" and "bob" in DOS table

### 5. Verify Request Flow
**In bob's web UI** (Viewer):
1. Go to "Viewer Mode" tab
2. Click on "image1.jpg" under alice
3. Enter 5 views, click "Send Request"
4. Check "My Requests" - should show "pending"

**In alice's web UI** (Owner):
1. Go to "Owner Mode" tab
2. Should see notification: "bob requests image1.jpg"
3. Click "Approve"

**In bob's web UI** (Viewer):
1. Request status changes to "approved"
2. Click "View Downloads"
3. Should see the downloaded embedded image

## Success Criteria

- [ ] Client logs show correct version and num_clients
- [ ] No "Failed to parse DOS-C" errors
- [ ] DOS table in web UI populates with all clients
- [ ] Multiple clients can see each other
- [ ] Viewer can request images from owner
- [ ] Owner sees notifications and can approve/deny
- [ ] Downloads work correctly

## Debugging

If issues persist, check:

1. **Server logs**: Look for `[HANDLE_JOIN_TCP] Building DOS-C: version=X, num_clients=Y`
   - version should be 1, 2, 3... (incrementing)
   - num_clients should match actual connected clients

2. **Client logs**: Look for `[DOS] Parsing JOIN_ACK: version=X num_clients=Y`
   - Should match server's values
   - No error messages after this line

3. **Network inspection**: Use Wireshark on port 9080 if needed

## Related Files

- Server state: `Cloud-Node/src/state.rs:74` (dos_c_version definition)
- Server send: `Cloud-Node/src/tcp_client.rs:341-395` (JOIN_ACK handler)
- Client parse: `Client-Node/src/dos.rs:83-201` (parse_dos_c_from_join_ack)

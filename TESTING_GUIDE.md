# Cloud-P2P Multi-Machine Testing Guide

This guide explains how to test the complete Cloud-P2P image sharing system across multiple machines.

## System Architecture

```
┌──────────────────┐         ┌──────────────────┐         ┌──────────────────┐
│   Machine A      │         │   Machine B      │         │   Server         │
│   (Owner)        │         │   (Viewer)       │         │   (Cloud-Node)   │
│                  │         │                  │         │                  │
│  ┌────────────┐  │         │  ┌────────────┐  │         │  ┌────────────┐  │
│  │ Web UI     │◄─┼─┐       │  │ Web UI     │◄─┼─┐       │  │            │  │
│  │ :3000      │  │ │       │  │ :3000      │  │ │       │  │   Server   │  │
│  └────────────┘  │ │       │  └────────────┘  │ │       │  │   :9080    │  │
│        ▲         │ │       │        ▲         │ │       │  │   TCP      │  │
│        │ HTTP    │ │       │        │ HTTP    │ │       │  └──────▲─────┘  │
│  ┌─────▼──────┐  │ │       │  ┌─────▼──────┐  │ │       │         │        │
│  │ API Server │  │ │       │  │ API Server │  │ │       │         │        │
│  │ :3001      │  │ │       │  │ :3001      │  │ │       │         │        │
│  └─────▲──────┘  │ │       │  └─────▲──────┘  │ │       │         │        │
│        │         │ │       │        │         │ │       │         │        │
│  ┌─────▼──────┐  │ │       │  ┌─────▼──────┐  │ │       │         │        │
│  │TCP Client  │◄─┼─┼───────┼──┼───TCP────────┼─┼───────┼─────────┘        │
│  │(Rust)      │  │ │       │  │   Client    │  │       │                  │
│  └────────────┘  │ │       │  │   (Rust)    │  │       │                  │
│                  │ │       │  └────────────┘  │ │       │                  │
└──────────────────┘ │       └──────────────────┘ │       └──────────────────┘
                     │                            │
                     └────── Local Network ───────┘
                            (10.40.61.x)
```

## Prerequisites

### On All Machines:
- Rust toolchain installed
- Node.js 18+ and npm installed
- Network connectivity on the same LAN

### On Server Machine:
- Port 9080 (TCP) open and accessible
- Port 9081 (UDP) for server-to-server communication

### On Client Machines:
- Port 3000 (HTTP) for web UI (local only)
- Port 3001 (HTTP) for API server (local only)

## Setup Instructions

### 1. Server Setup (Cloud-Node)

On the server machine (e.g., 10.40.61.79):

```bash
cd Cloud-Node
cargo build --release

# Run the server
cargo run --release

# Or use the compiled binary
./target/release/server
```

Expected output:
```
[SERVER] Starting Cloud-Node server...
[SERVER] TCP listening on 0.0.0.0:9080
[SERVER] UDP listening on 0.0.0.0:9081
[SERVER] Firebase initialized
[SERVER] Stego service ready
```

### 2. Client Setup (Owner - Machine A)

#### Terminal 1: Start Rust Client
```bash
cd Client-Node
cargo run -- interactive alice 10.40.61.79:9080 secret.jpg confidential.png
```

Expected output:
```
=== CLIENT INTERACTIVE MODE (TCP + HTTP API) ===
Username: alice
Server: 10.40.61.79:9080
Client port: 8000
Images: ["secret.jpg", "confidential.png"]

[CLIENT] ✅ Successfully joined server!

✅ Client is running!
📱 HTTP API: http://localhost:3001
🌐 Web UI: http://localhost:3000 (run 'npm run dev' in Client-Node/web-ui/)

Press Ctrl+C to exit.
```

#### Terminal 2: Start Web UI
```bash
cd Client-Node/web-ui
npm run dev
```

Expected output:
```
  VITE v5.0.8  ready in 329 ms

  ➜  Local:   http://localhost:3000/
  ➜  Network: use --host to expose
```

#### Browser: Open Web UI
1. Navigate to http://localhost:3000
2. Enter username: `alice`
3. Server address should already be filled: `10.40.61.79:9080`
4. Click "Connect to Network"
5. You should be redirected to the Dashboard

### 3. Client Setup (Viewer - Machine B)

#### Terminal 1: Start Rust Client
```bash
cd Client-Node
cargo run -- interactive bob 10.40.61.79:9080
```

Expected output:
```
=== CLIENT INTERACTIVE MODE (TCP + HTTP API) ===
Username: bob
Server: 10.40.61.79:9080
Client port: 8000
Images: []

[CLIENT] ✅ Successfully joined server!

✅ Client is running!
📱 HTTP API: http://localhost:3001
🌐 Web UI: http://localhost:3000 (run 'npm run dev' in Client-Node/web-ui/)
```

#### Terminal 2: Start Web UI
```bash
cd Client-Node/web-ui
npm run dev
```

#### Browser: Open Web UI
1. Navigate to http://localhost:3000
2. Enter username: `bob`
3. Server address: `10.40.61.79:9080`
4. Click "Connect to Network"

## Testing Scenarios

### Scenario 1: Basic Viewer Request Flow

**Objective**: Bob requests to view Alice's image

**Steps**:

1. **On Bob's Machine (Viewer)**:
   - Go to Dashboard → Viewer Mode tab
   - You should see Alice in the DOS-C table with her images
   - Click on `secret.jpg`
   - Modal appears: enter number of views (e.g., 5)
   - Click "Send Request"
   - Check "My Requests" panel - status should be "pending"

2. **On Alice's Machine (Owner)**:
   - Go to Dashboard → Owner Mode tab
   - You should see a notification: "bob requests secret.jpg"
   - Shows: Views requested: 5, Request ID: 1
   - Click "Approve"

3. **On Bob's Machine (Viewer)**:
   - Check "My Requests" panel - status changes to "approved"
   - Click "View Downloads" button
   - You should see the embedded image
   - Click "Extract True Image"
   - The extracted (true) image appears
   - Metadata shows remaining views, UUID, etc.

### Scenario 2: Multiple Requests

**Objective**: Test multiple viewers requesting same image

**Setup**: 3 machines - Alice (owner), Bob (viewer1), Charlie (viewer2)

**Steps**:

1. Both Bob and Charlie send requests for `secret.jpg`
2. Alice sees 2 pending requests
3. Alice approves Bob, denies Charlie
4. Bob can download and extract
5. Charlie sees "rejected" status

### Scenario 3: Owner Denial Flow

**Objective**: Owner denies a request

**Steps**:

1. Bob requests `confidential.png` from Alice
2. Alice sees notification
3. Alice clicks "Deny"
4. Bob sees status change to "rejected"
5. No download occurs

### Scenario 4: DOS Synchronization

**Objective**: Verify directory updates propagate

**Steps**:

1. Start with Alice and Bob connected
2. Charlie joins the network with images
3. Both Alice and Bob should see Charlie appear in DOS-C within 5 seconds
4. Verify Charlie's images are visible
5. Charlie goes offline (Ctrl+C)
6. Alice and Bob should see Charlie's status change to "Offline"

### Scenario 5: Steganography Verification

**Objective**: Verify true image is hidden in cover image

**Setup**: Alice needs prepared images:
- `true_image.png` - the secret image to hide
- `cover_image.png` - the cover image (must be larger than true image)
- `metadata.json` - metadata file

**Steps**:

1. Alice uploads using the upload command:
```bash
cargo run -- upload alice 10.40.61.79:9080 mysecret true_image.png cover_image.png metadata.json
```

2. Bob requests `mysecret` via web UI
3. Alice approves
4. Bob downloads embedded image
5. Visually, embedded image should look identical to cover image
6. Bob extracts true image
7. Extracted image should match the original true_image.png

## Troubleshooting

### Client Can't Connect to Server

**Symptoms**: "Connection refused" or "JOIN failed"

**Solutions**:
1. Verify server is running: `netstat -tuln | grep 9080`
2. Check firewall allows TCP port 9080
3. Ping server IP: `ping 10.40.61.79`
4. Verify server IP address is correct

### Web UI Shows "Disconnected"

**Symptoms**: Red dot in status bar, "Disconnected"

**Solutions**:
1. Ensure Rust client is running in interactive mode
2. Check API server is on port 3001: `netstat -tuln | grep 3001`
3. Check browser console for errors (F12)
4. Verify Vite proxy configuration

### DOS-C is Empty

**Symptoms**: No users visible in Viewer Mode

**Solutions**:
1. Ensure other clients have successfully joined
2. Check server logs for JOIN messages
3. Wait 5 seconds for DOS update propagation
4. Refresh browser page

### Image Upload Fails

**Symptoms**: "Upload failed" or stego errors

**Solutions**:
1. Verify cover image is larger than true image
2. Check image formats (PNG, JPEG supported)
3. Ensure metadata.json is valid JSON
4. Check server logs for stego service errors

### Extraction Fails

**Symptoms**: "Failed to extract" or corrupted image

**Solutions**:
1. Verify embedded PNG downloaded correctly
2. Check file permissions on downloads/ directory
3. Check Rust client logs for stego extraction errors
4. Ensure Stego library is properly linked

## Directory Structure

```
Cloud-P2P-Environment-for-Controlled-Sharing-of-Images/
├── Cloud-Node/              # Server
│   ├── src/
│   ├── Cargo.toml
│   └── received/            # Uploaded images stored here
├── Client-Node/             # Rust client
│   ├── src/
│   │   ├── main.rs
│   │   ├── simple_client.rs
│   │   ├── dos.rs
│   │   ├── viewer.rs
│   │   ├── owner.rs
│   │   ├── extraction.rs
│   │   └── api_server.rs
│   ├── web-ui/              # React web UI
│   │   ├── src/
│   │   ├── package.json
│   │   └── README.md
│   ├── downloads/           # Downloaded images
│   └── Cargo.toml
└── Stego/                   # Steganography library
    ├── src/
    └── Cargo.toml
```

## Logs and Debugging

### Server Logs
Watch server logs for connection events:
```bash
cd Cloud-Node
cargo run | tee server.log
```

### Client Logs
Client logs show protocol messages:
```bash
cd Client-Node
cargo run -- interactive alice 10.40.61.79:9080 | tee client.log
```

### Browser Console
Open browser DevTools (F12) to see:
- Network requests to API
- React Query cache updates
- JavaScript errors

## Performance Expectations

- **JOIN latency**: < 100ms
- **DOS propagation**: < 5 seconds
- **Image download**: Depends on size (1MB ~ 2-5 seconds)
- **Extraction**: < 1 second for typical images
- **Web UI polling**: Every 2-5 seconds

## Known Limitations (MVP)

- No offline viewing support
- View count not enforced (future: SYNC_USAGE)
- No permission adjustments after approval
- No revocation mechanism
- No local state persistence
- Single server (no federation)
- No WebSocket for real-time updates

## Success Criteria Checklist

- [ ] Server starts without errors
- [ ] Multiple clients can join simultaneously
- [ ] DOS-C shows all connected users
- [ ] Viewer can request images
- [ ] Owner receives notifications in real-time
- [ ] Approve/Deny works correctly
- [ ] Image downloads successfully
- [ ] Extraction reveals true image
- [ ] Metadata is correctly parsed
- [ ] Web UI updates automatically
- [ ] Multiple requests handled independently

## Next Steps After MVP

1. Implement SYNC_USAGE for view count enforcement
2. Add permission adjustment (ADJUST_REQUEST flow)
3. Add revocation (REVOKE_REQUEST flow)
4. Implement local state persistence
5. Add WebSocket for real-time notifications
6. Implement DELETE_IMAGE handling
7. Add unit tests and integration tests
8. Security audit of steganography implementation
9. Performance optimization (image compression, caching)
10. Production deployment guide

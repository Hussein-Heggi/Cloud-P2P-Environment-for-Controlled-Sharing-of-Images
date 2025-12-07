# Cloud-P2P Quick Start Guide

Get up and running with the Cloud-P2P image sharing system in 5 minutes.

## One-Time Setup

### 1. Install Dependencies

```bash
# Rust (if not already installed)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Node.js and npm (if not already installed)
# Visit https://nodejs.org/ or use your package manager

# Install web UI dependencies
cd Client-Node/web-ui
npm install
cd ../..
```

### 2. Build the Project

```bash
# Build server
cd Cloud-Node
cargo build --release
cd ..

# Build client
cd Client-Node
cargo build
cd ..
```

## Running the System

### Start Server (Machine 1 or localhost)

```bash
cd Cloud-Node
cargo run --release
```

Leave this terminal running. Note the IP address shown (e.g., `10.40.61.79:9080`).

### Start Client (Machine 2 or localhost)

**Terminal 1** - Rust Client:
```bash
cd Client-Node
cargo run -- interactive alice 10.40.61.79:9080 image1.jpg image2.png
```

Replace `10.40.61.79:9080` with your server's address.

**Terminal 2** - Web UI:
```bash
cd Client-Node/web-ui
npm run dev
```

**Browser** - Open http://localhost:3000
- Username: `alice`
- Server: `10.40.61.79:9080`
- Click "Connect to Network"

## Testing with Multiple Clients

Repeat the client steps on another machine or in different terminal windows with different usernames:
- Machine A: `alice` (owner with images)
- Machine B: `bob` (viewer, no images)

### Quick Test Workflow:

1. **Bob** (viewer): Browse DOS-C table, click on Alice's image, request 5 views
2. **Alice** (owner): See notification, click "Approve"
3. **Bob** (viewer): Go to Downloads, click "Extract True Image"

## Useful Commands

### Check Server Status
```bash
netstat -tuln | grep 9080  # Should show LISTEN
```

### Check Client API Server
```bash
netstat -tuln | grep 3001  # Should show LISTEN
```

### View Logs
```bash
# Server logs
cd Cloud-Node && cargo run 2>&1 | tee server.log

# Client logs
cd Client-Node && cargo run -- interactive alice SERVER:PORT 2>&1 | tee client.log
```

## Troubleshooting

### "Connection refused"
- Ensure server is running
- Check firewall allows port 9080
- Verify server IP address

### "Web UI shows Disconnected"
- Ensure Rust client is running in `interactive` mode
- Check http://localhost:3001/api/status in browser

### "No images in DOS-C"
- Ensure other clients have joined with images
- Wait 5 seconds for DOS update
- Refresh browser

## File Locations

- **Server received images**: `Cloud-Node/received/`
- **Client downloads**: `Client-Node/downloads/`
- **Web UI**: `Client-Node/web-ui/`

## Next Steps

- Read [TESTING_GUIDE.md](TESTING_GUIDE.md) for detailed multi-machine testing scenarios
- Read [Client-Node/web-ui/README.md](Client-Node/web-ui/README.md) for Web UI documentation
- Check the plan file at `~/.claude/plans/` for implementation details

## Common Use Cases

### Owner Uploads Steganographic Image
```bash
cd Client-Node
cargo run -- upload alice SERVER:PORT secret true.png cover.png meta.json
```

### Viewer Requests and Extracts
```bash
# Start interactive mode
cargo run -- interactive bob SERVER:PORT

# Then use Web UI:
# 1. Request image from owner
# 2. Wait for approval
# 3. Download appears automatically
# 4. Click "Extract True Image"
```

### Owner Approves Request
```bash
# Use Web UI:
# Dashboard → Owner Mode → See pending requests → Approve/Deny
```

## Architecture Overview

```
Web Browser (port 3000)
    ↓ HTTP
Vite Dev Server
    ↓ Proxy
Rust API Server (port 3001)
    ↓ HTTP/JSON
Rust TCP Client
    ↓ TCP
Cloud-Node Server (port 9080)
```

## Support

- For detailed testing: See [TESTING_GUIDE.md](TESTING_GUIDE.md)
- For Web UI help: See [Client-Node/web-ui/README.md](Client-Node/web-ui/README.md)
- For protocol details: See source code in `Client-Node/src/protocol.rs`

# Cloud-P2P Web UI

Modern React web interface for the Cloud-P2P distributed image sharing system.

## Features

- **Viewer Mode**: Browse available images (DOS-C), request access, track request status
- **Owner Mode**: Approve/deny incoming view requests
- **Downloads**: View embedded images, extract true images, inspect metadata
- **Real-time Updates**: Automatic polling for DOS, requests, and notifications

## Prerequisites

- Node.js 18+ and npm
- Rust client running in interactive mode

## Setup

1. Install dependencies:
```bash
npm install
```

2. Start the development server:
```bash
npm run dev
```

The web UI will be available at http://localhost:3000

## Usage

### Starting the System

1. **Start Cloud-Node Server** (on server machine):
```bash
cd Cloud-Node
cargo run
```

2. **Start Rust Client in Interactive Mode** (on client machine):
```bash
cd Client-Node
cargo run -- interactive <username> <server_ip:port> [images...]

# Example:
cargo run -- interactive alice 10.40.61.79:9080 sunset.jpg mountain.png
```

3. **Start Web UI** (same machine as Rust client):
```bash
cd Client-Node/web-ui
npm run dev
```

4. **Open Browser**:
Navigate to http://localhost:3000

### Workflow

#### As a Viewer:
1. Go to "Viewer Mode" tab
2. Browse available images in the DOS-C table
3. Click on an image name to request it
4. Specify number of views and submit
5. Wait for owner approval (check "My Requests" panel)
6. Once approved, go to "Downloads" page
7. Click "Extract True Image" to reveal the hidden image

#### As an Owner:
1. Go to "Owner Mode" tab
2. View incoming requests in "Pending View Requests"
3. Click "Approve" or "Deny" for each request
4. Approved images will be automatically embedded and sent

## Architecture

```
┌─────────────┐     HTTP (3000→3001)     ┌──────────────┐     TCP (9080)     ┌─────────────┐
│  React UI   │ ◄────────────────────────► │ Rust Client  │ ◄──────────────────► │ Cloud-Node  │
│  (Vite)     │                            │ (API Server) │                     │  (Server)   │
└─────────────┘                            └──────────────┘                     └─────────────┘
```

- **React UI** (port 3000): User interface
- **Rust Client API** (port 3001): HTTP API exposing TCP client functionality
- **Cloud-Node** (port 9080): Central server coordinating P2P network

## API Endpoints

All endpoints are proxied through Vite from port 3000 to 3001:

- `GET /api/status` - Client connection status
- `GET /api/dos` - Directory of Service (available users/images)
- `POST /api/request-view` - Request to view an image
- `GET /api/requests` - Viewer's pending requests
- `GET /api/notifications` - Owner's pending approvals
- `POST /api/approve/:id` - Approve a request
- `POST /api/deny/:id` - Deny a request
- `GET /api/downloads` - List downloaded images
- `POST /api/extract/:filename` - Extract true image from embedded PNG

## Development

### Build for Production
```bash
npm run build
```

### Preview Production Build
```bash
npm run preview
```

## Troubleshooting

### "Cannot connect to API"
- Ensure Rust client is running with `cargo run -- interactive ...`
- Check that API server is listening on port 3001
- Verify Vite proxy configuration in `vite.config.ts`

### "No images showing"
- Check that other clients have joined the network with images
- Verify DOS-C is populated (check Rust client logs)
- Ensure server is running and reachable

### "Extract fails"
- Verify embedded PNG was downloaded successfully
- Check Rust client logs for stego extraction errors
- Ensure `downloads/` directory exists

## Technology Stack

- **React 18**: UI framework
- **TypeScript**: Type safety
- **Vite**: Build tool and dev server
- **TailwindCSS**: Utility-first CSS
- **Axios**: HTTP client
- **React Query**: State management and data fetching
- **React Router**: Client-side routing

# Cloud-P2P Client Web UI

Modern React+TypeScript web interface for the Cloud-P2P distributed image sharing system.

## Features

- **Dashboard** with Owner and Viewer modes
  - **Viewer Mode**: Browse DOS, request images, track request status
  - **Owner Mode**: Approve/deny incoming view requests
- **Downloads Page**: View embedded images, extract true images, view metadata
- **Real-time Updates**: Auto-refresh every 2-5 seconds
- **Responsive Design**: TailwindCSS styling

## Setup

### Prerequisites

- Node.js 18+ and npm
- Rust client backend running on port 3001

### Installation

```bash
cd Client-Node/web-ui
npm install
```

### Development

```bash
npm run dev
```

This starts the development server on http://localhost:3000

The Vite dev server proxies API requests to http://localhost:3001 (Rust backend).

### Build for Production

```bash
npm run build
```

Output will be in `dist/` directory.

## Usage

### Starting the Full System

1. **Start the Cloud-Node server** (on server machine):
   ```bash
   cd Cloud-Node
   cargo run --release -- --node-id 1 --udp-bind 10.40.61.79:8000 --tcp-bind 10.40.61.79:9080 --elect-bind 10.40.61.79:8010
   ```

2. **Start the Rust client backend** (on client machine):
   ```bash
   cd Client-Node
   # Run in interactive mode with HTTP API server
   cargo run --release -- interactive <username> <server_ip>:9080
   ```

   This will:
   - Connect to the Cloud-Node server
   - Start HTTP API server on port 3001
   - Enable web UI access

3. **Start the web UI** (on client machine):
   ```bash
   cd Client-Node/web-ui
   npm run dev
   ```

4. **Open browser**: http://localhost:3000

### Workflow

**As a Viewer:**
1. Go to Dashboard → Viewer Mode
2. Browse the DOS table to see available users and images
3. Click on an image to request it
4. Enter number of views and send request
5. Wait for owner approval (check "My Requests" section)
6. Once approved, go to Downloads page
7. Click "Extract True Image" to reveal the hidden image

**As an Owner:**
1. Go to Dashboard → Owner Mode
2. Wait for view request notifications
3. Click "Approve" or "Deny" for each request
4. Approved images will be sent to the viewer

## API Endpoints Used

- `GET /api/status` - Client connection status
- `GET /api/dos` - Directory of Services
- `POST /api/request-view` - Request to view an image
- `GET /api/requests` - Viewer's pending requests
- `GET /api/notifications` - Owner's pending notifications
- `POST /api/approve/:id` - Approve request
- `POST /api/deny/:id` - Deny request
- `GET /api/downloads` - List downloads
- `POST /api/extract/:filename` - Extract true image

## Technology Stack

- **React 18** - UI framework
- **TypeScript** - Type safety
- **Vite** - Build tool & dev server
- **React Router** - Navigation
- **Axios** - HTTP client
- **TailwindCSS** - Styling

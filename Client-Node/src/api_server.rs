//! HTTP API Server for Web UI
//! Provides REST endpoints for the React frontend to interact with the client

use anyhow::Result;
use axum::{
    extract::{Path, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tower_http::cors::{Any, CorsLayer};
use tower_http::services::ServeDir;

use crate::dos::DosClient;
use crate::simple_client::SharedClientState;
use crate::viewer::RequestStatus;
use crate::owner::PendingViewRequest;

/// Shared state for HTTP API
#[derive(Clone)]
pub struct ApiState {
    pub client_state: SharedClientState,
    pub writer: Arc<tokio::sync::Mutex<tokio::net::tcp::OwnedWriteHalf>>,
}

// ============================================================================
// API Request/Response Types
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct JoinRequest {
    pub username: String,
    pub server_addr: String,
    pub images: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct JoinResponse {
    pub success: bool,
    pub dos_c_version: u64,
    pub message: String,
}

#[derive(Debug, Serialize)]
pub struct DosResponse {
    pub users: Vec<DosClientInfo>,
    pub version: u64,
}

#[derive(Debug, Serialize)]
pub struct DosClientInfo {
    pub name: String,
    pub images: Vec<String>,
    // Removed: ip, port, online, last_seen (not in minimal DOS-C v2.0)
}

#[derive(Debug, Deserialize)]
pub struct ViewRequestPayload {
    pub owner: String,
    pub image_name: String,
    pub requested_views: u32,
}

#[derive(Debug, Serialize)]
pub struct ViewRequestResponse {
    pub request_id: u32,
    pub status: String,
}

#[derive(Debug, Serialize)]
pub struct PendingRequestInfo {
    pub request_id: u32,
    pub owner: String,
    pub image: String,
    pub status: String,
    pub timestamp: u64,
}

#[derive(Debug, Serialize)]
pub struct PendingViewNotification {
    pub request_id: u32,
    pub viewer: String,
    pub image: String,
    pub requested_views: u32,
    pub timestamp: u64,
}

#[derive(Debug, Serialize)]
pub struct StatusResponse {
    pub connected: bool,
    pub username: String,
    pub dos_version: u32,
    pub server_addr: String,
}

#[derive(Debug, Serialize)]
pub struct DownloadInfo {
    pub owner: String,
    pub image_name: String,
    pub embedded_path: String,
    pub extracted_path: Option<String>,
    pub metadata: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ExtractionResponse {
    pub true_image_path: String,
    pub metadata: String,
}

#[derive(Debug, Serialize)]
pub struct ApproveResponse {
    pub success: bool,
    pub message: String,
}

// ============================================================================
// API Endpoints
// ============================================================================

/// GET /api/status - Get client status
async fn get_status(State(api_state): State<ApiState>) -> Json<StatusResponse> {
    let s = api_state.client_state.read().await;

    Json(StatusResponse {
        connected: s.joined,
        username: s.username.clone(),
        dos_version: s.dos_version,
        server_addr: s.server_addr.to_string(),
    })
}

/// GET /api/dos - Get full DOS-C
async fn get_dos(State(api_state): State<ApiState>) -> Json<DosResponse> {
    let s = api_state.client_state.read().await;

    println!("[API DEBUG] /api/dos called");
    println!("[API DEBUG] DOS state has {} clients", s.dos.get_all_clients().len());

    let all_clients = s.dos.get_all_clients();
    for (name, client) in all_clients.iter() {
        println!("[API DEBUG] Client in DOS: {} ({} images)",
                 name, client.images.len());
    }

    let users: Vec<DosClientInfo> = all_clients
        .iter()
        .map(|(_, client)| DosClientInfo {
            name: client.client_name.clone(),
            images: client.images.clone(),
        })
        .collect();

    println!("[API DEBUG] Returning {} users in response (MINIMAL DOS-C v2.0)", users.len());
    for user in &users {
        println!("[API DEBUG] Response user: {} ({} images)",
                 user.name, user.images.len());
    }

    Json(DosResponse {
        users,
        version: s.dos.get_version(),
    })
}

/// POST /api/request-view - Request to view an image
async fn request_view(
    State(api_state): State<ApiState>,
    Json(payload): Json<ViewRequestPayload>,
) -> Result<Json<ViewRequestResponse>, (StatusCode, String)> {
    // Clone payload fields before moving
    let owner = payload.owner.clone();
    let image_name = payload.image_name.clone();
    let requested_views = payload.requested_views;

    // Send request using smart routing (P2P if online, server-mediated if offline)
    let request_id = crate::viewer::request_image_access(
        api_state.client_state.clone(),
        api_state.writer.clone(),
        &payload.owner,
        &payload.image_name,
        payload.requested_views,
    )
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Store in client state
    {
        let mut s = api_state.client_state.write().await;
        s.my_requests.insert(
            request_id,
            crate::viewer::PendingRequest {
                request_id,
                owner,
                image_name,
                requested_views,
                status: RequestStatus::Pending,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            },
        );
    }

    Ok(Json(ViewRequestResponse {
        request_id,
        status: "pending".to_string(),
    }))
}

/// GET /api/requests - Get viewer's pending requests
async fn get_requests(State(api_state): State<ApiState>) -> Json<Vec<PendingRequestInfo>> {
    let s = api_state.client_state.read().await;

    let requests: Vec<PendingRequestInfo> = s.my_requests
        .values()
        .map(|req| PendingRequestInfo {
            request_id: req.request_id,
            owner: req.owner.clone(),
            image: req.image_name.clone(),
            status: match req.status {
                RequestStatus::Pending => "pending".to_string(),
                RequestStatus::Approved => "approved".to_string(),
                RequestStatus::Rejected => "rejected".to_string(),
            },
            timestamp: req.timestamp,
        })
        .collect();

    Json(requests)
}

/// GET /api/notifications - Get owner's pending view requests
async fn get_notifications(State(api_state): State<ApiState>) -> Json<Vec<PendingViewNotification>> {
    let s = api_state.client_state.read().await;

    let notifications: Vec<PendingViewNotification> = s.pending_view_requests
        .values()
        .map(|req| PendingViewNotification {
            request_id: req.request_id,
            viewer: req.viewer.clone(),
            image: req.image_name.clone(),
            requested_views: req.requested_views,
            timestamp: req.timestamp,
        })
        .collect();

    Json(notifications)
}

/// POST /api/approve/:request_id - Approve a view request (P2P)
async fn approve_request(
    State(api_state): State<ApiState>,
    Path(request_id): Path<u32>,
) -> Result<Json<ApproveResponse>, (StatusCode, String)> {
    // Use P2P approval (sends image directly to viewer)
    crate::owner::approve_peer_view_request(
        api_state.client_state.clone(),
        request_id,
    )
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(ApproveResponse {
        success: true,
        message: format!("Request {} approved via P2P", request_id),
    }))
}

/// POST /api/deny/:request_id - Deny a view request
async fn deny_request(
    State(api_state): State<ApiState>,
    Path(request_id): Path<u32>,
) -> Result<Json<ApproveResponse>, (StatusCode, String)> {
    crate::owner::deny_request(
        api_state.client_state.clone(),
        api_state.writer.clone(),
        request_id,
    )
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(ApproveResponse {
        success: true,
        message: format!("Request {} denied", request_id),
    }))
}

/// GET /api/downloads - List downloaded images
async fn get_downloads(State(api_state): State<ApiState>) -> Json<Vec<DownloadInfo>> {
    let s = api_state.client_state.read().await;

    let downloads: Vec<DownloadInfo> = s.downloads
        .iter()
        .map(|d| DownloadInfo {
            owner: d.owner.clone(),
            image_name: d.image_name.clone(),
            embedded_path: d.embedded_path.display().to_string(),
            extracted_path: d.extracted_path.as_ref().map(|p| p.display().to_string()),
            metadata: d.metadata.clone(),
        })
        .collect();

    Json(downloads)
}

/// POST /api/extract/:filename - Extract true image from embedded PNG
async fn extract_image(
    State(api_state): State<ApiState>,
    Path(filename): Path<String>,
) -> Result<Json<ExtractionResponse>, (StatusCode, String)> {
    use std::path::PathBuf;

    let embedded_path = PathBuf::from("downloads").join(&filename);

    let (true_image_path, metadata_json) = crate::extraction::extract_true_image(&embedded_path)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Update downloads list
    {
        let mut s = api_state.client_state.write().await;
        if let Some(download) = s.downloads.iter_mut().find(|d| {
            d.embedded_path.file_name() == Some(std::ffi::OsStr::new(&filename))
        }) {
            download.extracted_path = Some(true_image_path.clone());
            download.metadata = Some(metadata_json.clone());
        }
    }

    Ok(Json(ExtractionResponse {
        true_image_path: true_image_path.display().to_string(),
        metadata: metadata_json,
    }))
}

// ============================================================================
// Phase 4C: Secure Image Viewing with Count Decrement
// ============================================================================

/// GET /api/view/:owner/:image_name - View an encrypted image (with view count decrement)
/// CRITICAL: Decrypts in memory, decrements count, returns image, NEVER saves decrypted version
async fn view_image(
    State(api_state): State<ApiState>,
    Path((owner, image_name)): Path<(String, String)>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    use crate::viewer::ViewerAccessMap;
    use std::path::PathBuf;

    println!("[VIEW] 🔍 Request to view: owner={}, image={}", owner, image_name);

    // Load ViewerAccessMap
    let map_path = ViewerAccessMap::default_path()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to get map path: {}", e)))?;

    let mut viewer_map = ViewerAccessMap::load_from_file(&map_path)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to load viewer map: {}", e)))?;

    // Check remaining views
    let remaining_views = viewer_map.get_remaining_views(&owner, &image_name);
    println!("[VIEW] Remaining views: {}", remaining_views);

    if remaining_views == 0 {
        return Err((
            StatusCode::FORBIDDEN,
            format!("No remaining views for image '{}' from owner '{}'", image_name, owner)
        ));
    }

    // Get encrypted image path from grant
    let grant = viewer_map.get_grant(&owner, &image_name)
        .ok_or_else(|| (
            StatusCode::NOT_FOUND,
            format!("No access grant found for image '{}' from owner '{}'", image_name, owner)
        ))?;

    let encrypted_path = PathBuf::from(&grant.encrypted_path);

    println!("[VIEW] 📦 Reading encrypted image from: {}", encrypted_path.display());

    // Check if encrypted file exists
    if !tokio::fs::metadata(&encrypted_path).await.is_ok() {
        return Err((
            StatusCode::NOT_FOUND,
            format!("Encrypted image not found at: {}", encrypted_path.display())
        ));
    }

    // Read encrypted PNG
    let encrypted_bytes = tokio::fs::read(&encrypted_path)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to read encrypted image: {}", e)))?;

    println!("[VIEW] 📦 Loaded encrypted image: {} bytes", encrypted_bytes.len());

    // Load image and extract true image using stego
    println!("[VIEW] 🔓 Decrypting image in memory...");

    let embedded_img = image::load_from_memory(&encrypted_bytes)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to load image: {}", e)))?;

    let (true_img, metadata) = stego::extract(&embedded_img)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to extract from stego: {}", e)))?;

    println!("[VIEW] ✅ Decrypted successfully!");
    println!("[VIEW] Metadata: owner={}, image={}", metadata.owner, metadata.image_name);

    // Decrement view count BEFORE returning image
    if !viewer_map.decrement_view(&owner, &image_name) {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to decrement view count".to_string()
        ));
    }

    // Save updated ViewerAccessMap
    viewer_map.save_to_file(&map_path)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to save viewer map: {}", e)))?;

    let new_remaining = viewer_map.get_remaining_views(&owner, &image_name);
    println!("[VIEW] 📉 View count decremented: {} → {} remaining", remaining_views, new_remaining);

    // AUTO-CLEANUP: Delete if views = 0
    if new_remaining == 0 {
        println!("[VIEW] Views exhausted, triggering auto-cleanup");

        // Remove from map
        viewer_map.remove_grant(&owner, &image_name);

        // Delete file
        let file_path = PathBuf::from(&encrypted_path);
        if let Err(e) = tokio::fs::remove_file(&file_path).await {
            println!("[VIEW] ⚠ Failed to delete {}: {}", file_path.display(), e);
        } else {
            println!("[VIEW] ✓ Deleted {}", file_path.display());
        }

        // Save updated map (grant removed)
        viewer_map.save_to_file(&map_path)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to save viewer map: {}", e)))?;
    }

    // Convert image to PNG bytes in memory
    let mut png_bytes: Vec<u8> = Vec::new();
    true_img.write_to(&mut std::io::Cursor::new(&mut png_bytes), image::ImageOutputFormat::Png)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to encode PNG: {}", e)))?;

    println!("[VIEW] 🖼️  Returning decrypted image ({} bytes, {} views remaining)", png_bytes.len(), new_remaining);

    // Return image as PNG with appropriate headers
    use axum::response::Response;
    use axum::body::Body;
    use axum::http::header::{CONTENT_TYPE, CACHE_CONTROL};

    let response = Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "image/png")
        .header("X-Remaining-Views", new_remaining.to_string())
        .header(CACHE_CONTROL, "no-store, must-revalidate")
        .body(Body::from(png_bytes))
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to build response: {}", e)))?;

    Ok(response)
}

/// GET /api/viewer-access-map - Get viewer's access map (for UI display)
async fn get_viewer_access_map(
    State(api_state): State<ApiState>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    use crate::viewer::ViewerAccessMap;

    let map_path = ViewerAccessMap::default_path()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to get map path: {}", e)))?;

    let viewer_map = ViewerAccessMap::load_from_file(&map_path)
        .await
        .unwrap_or_else(|_| ViewerAccessMap::new());

    // Convert to JSON for API response
    let available_grants: Vec<serde_json::Value> = viewer_map.list_available()
        .iter()
        .map(|grant| serde_json::json!({
            "owner": grant.owner,
            "image_name": grant.image_name,
            "remaining_views": grant.remaining_views,
            "received_at": grant.received_at,
            "encrypted_path": grant.encrypted_path,
        }))
        .collect();

    Ok(Json(serde_json::json!({
        "grants": available_grants,
        "total_available": available_grants.len(),
    })))
}

// ============================================================================
// Phase 3: Adjust and Revoke API Endpoints
// ============================================================================

/// POST /api/adjust-request/:owner/:image - Viewer requests view count adjustment
#[derive(Debug, serde::Deserialize)]
struct AdjustRequestPayload {
    requested_views: u32,
}

#[derive(Debug, serde::Serialize)]
struct AdjustRequestResponse {
    success: bool,
    request_id: u32,
}

async fn request_adjust_views(
    State(api_state): State<ApiState>,
    Path((owner, image_name)): Path<(String, String)>,
    Json(payload): Json<AdjustRequestPayload>,
) -> Result<Json<AdjustRequestResponse>, (StatusCode, String)> {
    // Use smart routing (P2P if online, server-mediated if offline/TCP fails)
    let request_id = crate::viewer::request_adjust_views(
        api_state.client_state.clone(),
        api_state.writer.clone(),
        &owner,
        &image_name,
        payload.requested_views,
    ).await.map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Store in pending requests
    {
        let mut s = api_state.client_state.write().await;
        s.pending_adjust_requests.insert(request_id, crate::simple_client::PendingAdjustRequest {
            request_id,
            owner,
            image_name,
            requested_views: payload.requested_views,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        });
    }

    Ok(Json(AdjustRequestResponse {
        success: true,
        request_id
    }))
}

/// GET /api/local-access-map - Get owner's granted access
#[derive(Debug, serde::Serialize)]
struct LocalAccessMapResponse {
    grants: Vec<AccessGrantInfo>,
}

#[derive(Debug, serde::Serialize)]
struct AccessGrantInfo {
    viewer: String,
    image_name: String,
    granted_views: u32,
    granted_at: u64,
}

async fn get_local_access_map(
    State(api_state): State<ApiState>,
) -> Json<LocalAccessMapResponse> {
    let s = api_state.client_state.read().await;

    let grants: Vec<AccessGrantInfo> = s.local_access_map.grants.values()
        .map(|g| AccessGrantInfo {
            viewer: g.viewer.clone(),
            image_name: g.image_name.clone(),
            granted_views: g.granted_views,
            granted_at: g.granted_at,
        })
        .collect();

    Json(LocalAccessMapResponse { grants })
}

/// POST /api/owner/adjust/:viewer/:image - Owner adjusts view count
#[derive(Debug, serde::Deserialize)]
struct OwnerAdjustPayload {
    new_views: u32,
}

#[derive(Debug, serde::Serialize)]
struct SuccessResponse {
    success: bool,
}

async fn owner_adjust_views(
    State(api_state): State<ApiState>,
    Path((viewer, image_name)): Path<(String, String)>,
    Json(payload): Json<OwnerAdjustPayload>,
) -> Result<Json<SuccessResponse>, (StatusCode, String)> {
    // Validation: new_views must be > 0
    if payload.new_views == 0 {
        return Err((StatusCode::BAD_REQUEST,
                   "View count must be > 0. Use revoke endpoint instead.".to_string()));
    }

    // Use smart routing (P2P if online, server-mediated if offline/TCP fails)
    crate::owner::adjust_viewer_views(
        api_state.client_state.clone(),
        api_state.writer.clone(),
        &viewer,
        &image_name,
        payload.new_views,
    ).await.map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(SuccessResponse { success: true }))
}

/// POST /api/owner/revoke/:viewer/:image - Owner revokes access
async fn owner_revoke_access(
    State(api_state): State<ApiState>,
    Path((viewer, image_name)): Path<(String, String)>,
) -> Result<Json<SuccessResponse>, (StatusCode, String)> {
    // Use smart routing (P2P if online, server-mediated if offline/TCP fails)
    crate::owner::revoke_access(
        api_state.client_state.clone(),
        api_state.writer.clone(),
        &viewer,
        &image_name,
    ).await.map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(SuccessResponse { success: true }))
}

/// GET /api/incoming-adjust-requests - Get owner's incoming adjust requests
#[derive(Debug, serde::Serialize)]
struct IncomingAdjustRequestInfo {
    request_id: u32,
    viewer: String,
    image_name: String,
    requested_views: u32,
    current_views: u32,
    timestamp: u64,
}

#[derive(Debug, serde::Serialize)]
struct IncomingAdjustRequestsResponse {
    requests: Vec<IncomingAdjustRequestInfo>,
}

async fn get_incoming_adjust_requests(
    State(api_state): State<ApiState>,
) -> Json<IncomingAdjustRequestsResponse> {
    let s = api_state.client_state.read().await;

    let requests: Vec<IncomingAdjustRequestInfo> = s.incoming_adjust_requests.values()
        .map(|r| IncomingAdjustRequestInfo {
            request_id: r.request_id,
            viewer: r.viewer.clone(),
            image_name: r.image_name.clone(),
            requested_views: r.requested_views,
            current_views: r.current_views,
            timestamp: r.timestamp,
        })
        .collect();

    Json(IncomingAdjustRequestsResponse { requests })
}

/// POST /api/owner/approve-adjust/:request_id - Owner approves adjust request
#[derive(Debug, serde::Deserialize)]
struct ApproveAdjustPayload {
    approved_views: u32,
}

async fn approve_adjust_request_endpoint(
    State(api_state): State<ApiState>,
    Path(request_id): Path<u32>,
    Json(payload): Json<ApproveAdjustPayload>,
) -> Result<Json<SuccessResponse>, (StatusCode, String)> {
    crate::owner::approve_adjust_request(
        api_state.client_state.clone(),
        request_id,
        payload.approved_views,
    ).await.map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(SuccessResponse { success: true }))
}

/// POST /api/owner/reject-adjust/:request_id - Owner rejects adjust request
#[derive(Debug, serde::Deserialize)]
struct RejectAdjustPayload {
    reason: String,
}

async fn reject_adjust_request_endpoint(
    State(api_state): State<ApiState>,
    Path(request_id): Path<u32>,
    Json(payload): Json<RejectAdjustPayload>,
) -> Result<Json<SuccessResponse>, (StatusCode, String)> {
    crate::owner::reject_adjust_request(
        api_state.client_state.clone(),
        request_id,
        &payload.reason,
    ).await.map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(SuccessResponse { success: true }))
}

// ============================================================================
// Server Setup
// ============================================================================

/// Run HTTP API server
pub async fn run_api_server(
    client_state: SharedClientState,
    writer: Arc<tokio::sync::Mutex<tokio::net::tcp::OwnedWriteHalf>>,
    port: u16,
) -> Result<()> {
    let api_state = ApiState {
        client_state,
        writer,
    };

    // CORS configuration for local development
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    // Build router
    let app = Router::new()
        // Status
        .route("/api/status", get(get_status))
        // DOS queries
        .route("/api/dos", get(get_dos))
        // Viewer operations
        .route("/api/request-view", post(request_view))
        .route("/api/requests", get(get_requests))
        .route("/api/downloads", get(get_downloads))
        .route("/api/extract/:filename", post(extract_image))
        // Phase 4C: Secure viewing with count decrement
        .route("/api/view/:owner/:image_name", get(view_image))
        .route("/api/viewer-access-map", get(get_viewer_access_map))
        // Owner operations
        .route("/api/notifications", get(get_notifications))
        .route("/api/approve/:request_id", post(approve_request))
        .route("/api/deny/:request_id", post(deny_request))
        // Adjust and Revoke operations
        .route("/api/adjust-request/:owner/:image", post(request_adjust_views))
        .route("/api/local-access-map", get(get_local_access_map))
        .route("/api/incoming-adjust-requests", get(get_incoming_adjust_requests))
        .route("/api/owner/approve-adjust/:request_id", post(approve_adjust_request_endpoint))
        .route("/api/owner/reject-adjust/:request_id", post(reject_adjust_request_endpoint))
        .route("/api/owner/adjust/:viewer/:image", post(owner_adjust_views))
        .route("/api/owner/revoke/:viewer/:image", post(owner_revoke_access))
        // Serve static files (images)
        .nest_service("/downloads", ServeDir::new("downloads"))
        .layer(cors)
        .with_state(api_state);

    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    println!("🚀 HTTP API server listening on http://{}", addr);
    println!("📱 Web UI should connect to: http://localhost:{}", port);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

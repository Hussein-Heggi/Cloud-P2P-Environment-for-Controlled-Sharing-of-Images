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

    // Send VIEW_REQUEST via TCP
    let request_id = crate::viewer::send_view_request(
        api_state.client_state.clone(),
        api_state.writer.clone(),
        payload.owner,
        payload.image_name,
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

/// POST /api/approve/:request_id - Approve a view request
async fn approve_request(
    State(api_state): State<ApiState>,
    Path(request_id): Path<u32>,
) -> Result<Json<ApproveResponse>, (StatusCode, String)> {
    crate::owner::approve_request(
        api_state.client_state.clone(),
        api_state.writer.clone(),
        request_id,
    )
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(ApproveResponse {
        success: true,
        message: format!("Request {} approved", request_id),
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
        // Owner operations
        .route("/api/notifications", get(get_notifications))
        .route("/api/approve/:request_id", post(approve_request))
        .route("/api/deny/:request_id", post(deny_request))
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

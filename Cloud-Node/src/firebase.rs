use anyhow::{Context, Result};
use firestore::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use tracing::{debug, info, warn, error};

use crate::state::SharedState;

/// DOS-S Client entry - represents a registered client in the system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DosClient {
    pub client_name: String,
    pub client_ip: String,
    pub client_port: u16,
    /// Cover image (first image from JOIN) - used for steganography, NOT sent in DOS-C
    pub cover_image: Option<String>,
    /// Actual images (remaining images from JOIN) - shareable, sent in DOS-C
    pub actual_images: Vec<String>,
    pub last_seen: u64, // use u64 for Firestore compatibility
    pub online: bool,
}

/// DOS-S Access entry - represents granted access to an image
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DosAccess {
    pub owner: String,
    pub viewer: String,
    pub image_name: String,
    pub granted_views: u32,
    pub consumed_views: u32,
    pub revoked: bool,
    pub granted_at: u64, // use u64 for Firestore compatibility
    pub image_uuid: String,
}

/// Initialize Firebase connection
pub async fn init_firestore() -> Result<FirestoreDb> {
    info!("Initializing Firestore connection...");

    // Prefer explicit service account credentials if available
    // 1) GOOGLE_APPLICATION_CREDENTIALS env var
    // 2) firebase-admin.json in CWD
    // 3) ../firebase-admin.json (when running from Cloud-Node dir)
    let mut key_path: Option<PathBuf> = None;

    if let Ok(env_path) = std::env::var("GOOGLE_APPLICATION_CREDENTIALS") {
        let candidate = PathBuf::from(env_path);
        if candidate.exists() {
            key_path = Some(candidate);
        } else {
            warn!(
                "GOOGLE_APPLICATION_CREDENTIALS is set but file not found at {:?}, falling back",
                candidate
            );
        }
    }

    if key_path.is_none() {
        let local = PathBuf::from("firebase-admin.json");
        if local.exists() {
            key_path = Some(local);
        } else {
            let parent = PathBuf::from("../firebase-admin.json");
            if parent.exists() {
                key_path = Some(parent);
            }
        }
    }

    let project_id = "dist-proj-25";

    let db = if let Some(service_account) = key_path {
        info!(?service_account, "Using Firebase service account key file");
        FirestoreDb::with_options_service_account_key_file(
            FirestoreDbOptions::new(project_id.to_string()),
            service_account.clone(),
        )
        .await
        .with_context(|| {
            format!(
                "Failed to initialize Firestore with credentials at {:?}",
                service_account
            )
        })?
    } else {
        info!("No service account file found; falling back to default Google auth chain");
        FirestoreDb::new(project_id)
            .await
            .context("Failed to initialize Firestore")?
    };

    info!("Firestore connection established");
    Ok(db)
}

/// Leader-only: Write client to Firebase
pub async fn write_client(db: &FirestoreDb, client: &DosClient) -> Result<()> {
    debug!("Writing client {} to Firebase", client.client_name);

    // Try update first; if doc does not exist, fallback to insert.
    match db
        .fluent()
        .update()
        .in_col("dos_s_clients")
        .document_id(&client.client_name)
        .object(client)
        .execute::<()>()
        .await
    {
        Ok(_) => {
            debug!("Client {} written successfully (update)", client.client_name);
        }
        Err(e) => {
            warn!(
                "Update failed for client {} (likely missing doc), retrying with insert: {}",
                client.client_name, e
            );
            db.fluent()
                .insert()
                .into("dos_s_clients")
                .document_id(&client.client_name)
                .object(client)
                .execute::<()>()
                .await
                .context("Failed to insert client to Firebase")?;
            debug!("Client {} written successfully (insert)", client.client_name);
        }
    }

    Ok(())
}

/// Leader-only: Delete client from Firebase
pub async fn delete_client(db: &FirestoreDb, client_name: &str) -> Result<()> {
    debug!("Deleting client {} from Firebase", client_name);

    db.fluent()
        .delete()
        .from("dos_s_clients")
        .document_id(client_name)
        .execute()
        .await
        .context("Failed to delete client from Firebase")?;

    debug!("Client {} deleted successfully", client_name);
    Ok(())
}

/// Leader-only: Write access record to Firebase
pub async fn write_access(db: &FirestoreDb, access_id: &str, access: &DosAccess) -> Result<()> {
    debug!("Writing access {} to Firebase", access_id);

    // Try update first; if doc does not exist, fallback to insert.
    match db
        .fluent()
        .update()
        .in_col("dos_s_access")
        .document_id(access_id)
        .object(access)
        .execute::<()>()
        .await
    {
        Ok(_) => {
            debug!("Access {} written successfully (update)", access_id);
        }
        Err(e) => {
            warn!(
                "Update failed for access {} (likely missing doc), retrying with insert: {}",
                access_id, e
            );
            db.fluent()
                .insert()
                .into("dos_s_access")
                .document_id(access_id)
                .object(access)
                .execute::<()>()
                .await
                .context("Failed to insert access to Firebase")?;
            debug!("Access {} written successfully (insert)", access_id);
        }
    }

    Ok(())
}

/// Leader-only: Delete access record from Firebase
pub async fn delete_access(db: &FirestoreDb, access_id: &str) -> Result<()> {
    debug!("Deleting access {} from Firebase", access_id);

    db.fluent()
        .delete()
        .from("dos_s_access")
        .document_id(access_id)
        .execute()
        .await
        .context("Failed to delete access from Firebase")?;

    debug!("Access {} deleted successfully", access_id);
    Ok(())
}

/// Read all clients from Firebase (for DOS-C construction)
pub async fn read_all_clients(db: &FirestoreDb) -> Result<HashMap<String, DosClient>> {
    debug!("Reading all clients from Firebase");

    let docs: Vec<DosClient> = db
        .fluent()
        .select()
        .from("dos_s_clients")
        .obj()
        .query()
        .await
        .context("Failed to read clients from Firebase")?;

    let mut clients = HashMap::new();
    for client in docs {
        clients.insert(client.client_name.clone(), client);
    }

    debug!("Read {} clients from Firebase", clients.len());
    Ok(clients)
}

/// Read all access records from Firebase
pub async fn read_all_access(db: &FirestoreDb) -> Result<HashMap<String, DosAccess>> {
    debug!("Reading all access records from Firebase");

    let docs: Vec<(String, DosAccess)> = db
        .fluent()
        .select()
        .from("dos_s_access")
        .obj()
        .query()
        .await
        .context("Failed to read access records from Firebase")?;

    let mut access_map = HashMap::new();
    for (id, access) in docs {
        access_map.insert(id, access);
    }

    debug!("Read {} access records from Firebase", access_map.len());
    Ok(access_map)
}

/// Real-time listener for DOS-S changes (all nodes)
/// This function spawns a background task that listens for Firebase changes
/// and updates the local SharedState accordingly
pub async fn listen_dos_changes(
    db: FirestoreDb,
    state: SharedState,
) -> Result<()> {
    info!("Starting Firebase real-time listener...");

    // Spawn listener for dos_s_clients collection
    let db_clone = db.clone();
    let state_clone = state.clone();
    tokio::spawn(async move {
        if let Err(e) = listen_clients_collection(db_clone, state_clone).await {
            error!("Clients listener error: {}", e);
        }
    });

    // Spawn listener for dos_s_access collection
    let db_clone = db.clone();
    let state_clone = state.clone();
    tokio::spawn(async move {
        if let Err(e) = listen_access_collection(db_clone, state_clone).await {
            error!("Access listener error: {}", e);
        }
    });

    info!("Firebase listeners started");
    Ok(())
}

async fn listen_clients_collection(_db: FirestoreDb, _state: SharedState) -> Result<()> {
    // TODO: Implement with correct firestore-rs API
    // The firestore-rs 0.41 API for listeners needs to be researched
    // For now, periodic polling will handle sync
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
    }
}

async fn listen_access_collection(_db: FirestoreDb, _state: SharedState) -> Result<()> {
    // TODO: Implement with correct firestore-rs API
    // The firestore-rs 0.41 API for listeners needs to be researched
    // For now, periodic polling will handle sync
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
    }
}

/// Cleanup expired access records (called periodically)
/// Deletes access records older than 5 hours where consumed >= granted or revoked
pub async fn cleanup_expired_access(db: &FirestoreDb, state: SharedState) -> Result<()> {
    let now: u128 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis();

    let five_hours_ms: u128 = 5 * 60 * 60 * 1000; // 5 hours in milliseconds

    let s = state.read().await;
    let mut to_delete = Vec::new();

    for (access_id, access) in &s.dos_access {
        let age = now.saturating_sub(access.granted_at as u128);
        let should_delete = age > five_hours_ms &&
            (access.consumed_views >= access.granted_views || access.revoked);

        if should_delete {
            to_delete.push(access_id.clone());
        }
    }
    drop(s);

    for access_id in to_delete {
        if let Err(e) = delete_access(db, &access_id).await {
            warn!("Failed to delete expired access {}: {}", access_id, e);
        } else {
            // Remove from local state
            state.write().await.dos_access.remove(&access_id);
            info!("Cleaned up expired access: {}", access_id);
        }
    }

    Ok(())
}

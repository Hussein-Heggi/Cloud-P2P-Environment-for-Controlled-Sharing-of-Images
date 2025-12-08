//! Server-side Access Map Filesystem Storage
//! Stores client access maps at access_maps/{username}.json

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Server access map structure (stored per client)
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ServerAccessMap {
    /// Map of viewer_image_key → AccessGrant
    /// Key format: "{viewer}_{image_name}"
    pub grants: HashMap<String, AccessGrant>,
}

/// Access grant entry (server copy)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessGrant {
    pub viewer: String,
    pub image_name: String,
    pub granted_views: u32,
    pub granted_at: u64, // Unix timestamp in milliseconds
}

impl ServerAccessMap {
    /// Create a new empty access map
    pub fn new() -> Self {
        Self {
            grants: HashMap::new(),
        }
    }

    /// Add or update a grant
    pub fn set_grant(&mut self, grant: AccessGrant) {
        let key = Self::make_key(&grant.viewer, &grant.image_name);
        self.grants.insert(key, grant);
    }

    /// Remove a grant
    pub fn remove_grant(&mut self, viewer: &str, image_name: &str) -> bool {
        let key = Self::make_key(viewer, image_name);
        self.grants.remove(&key).is_some()
    }

    /// Get a grant
    pub fn get_grant(&self, viewer: &str, image_name: &str) -> Option<&AccessGrant> {
        let key = Self::make_key(viewer, image_name);
        self.grants.get(&key)
    }

    /// Make key for HashMap lookup
    fn make_key(viewer: &str, image: &str) -> String {
        format!("{}_{}", viewer, image)
    }
}

/// Save client's access map to filesystem
/// Path: access_maps/{username}.json
pub async fn save_access_map(username: &str, map: &ServerAccessMap) -> Result<()> {
    let path = get_access_map_path(username)?;

    // Create parent directory if it doesn't exist
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await
            .with_context(|| format!("Failed to create directory: {:?}", parent))?;
    }

    let json = serde_json::to_string_pretty(map)
        .context("Failed to serialize access map")?;

    tokio::fs::write(&path, json).await
        .with_context(|| format!("Failed to write access map to {:?}", path))?;

    println!("[ACCESS_MAP_STORAGE] Saved {} grants for {} to {:?}",
             map.grants.len(), username, path);
    Ok(())
}

/// Load client's access map from filesystem
/// Returns empty map if file doesn't exist
pub async fn load_access_map(username: &str) -> Result<ServerAccessMap> {
    let path = get_access_map_path(username)?;

    if !path.exists() {
        println!("[ACCESS_MAP_STORAGE] No existing access map for {}, creating new",
                 username);
        return Ok(ServerAccessMap::new());
    }

    let json = tokio::fs::read_to_string(&path).await
        .with_context(|| format!("Failed to read access map from {:?}", path))?;

    let map: ServerAccessMap = serde_json::from_str(&json)
        .context("Failed to deserialize access map")?;

    println!("[ACCESS_MAP_STORAGE] Loaded {} grants for {} from {:?}",
             map.grants.len(), username, path);
    Ok(map)
}

/// Delete client's access map file
pub async fn delete_access_map(username: &str) -> Result<()> {
    let path = get_access_map_path(username)?;

    if path.exists() {
        tokio::fs::remove_file(&path).await
            .with_context(|| format!("Failed to delete access map at {:?}", path))?;
        println!("[ACCESS_MAP_STORAGE] Deleted access map for {}", username);
    }

    Ok(())
}

/// Get path to access map file for a username
/// Returns: access_maps/{username}.json
fn get_access_map_path(username: &str) -> Result<PathBuf> {
    // Use current directory / access_maps / {username}.json
    let mut path = std::env::current_dir()
        .context("Failed to get current directory")?;
    path.push("access_maps");
    path.push(format!("{}.json", username));
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_access_map_operations() {
        let mut map = ServerAccessMap::new();

        let grant1 = AccessGrant {
            viewer: "alice".to_string(),
            image_name: "photo.png".to_string(),
            granted_views: 3,
            granted_at: 1234567890,
        };

        map.set_grant(grant1.clone());

        assert!(map.get_grant("alice", "photo.png").is_some());
        assert!(map.get_grant("bob", "photo.png").is_none());

        let removed = map.remove_grant("alice", "photo.png");
        assert!(removed);
        assert!(map.get_grant("alice", "photo.png").is_none());

        let removed_again = map.remove_grant("alice", "photo.png");
        assert!(!removed_again);
    }

    #[test]
    fn test_serialization() {
        let mut map = ServerAccessMap::new();

        map.set_grant(AccessGrant {
            viewer: "alice".to_string(),
            image_name: "photo.png".to_string(),
            granted_views: 3,
            granted_at: 1234567890,
        });

        map.set_grant(AccessGrant {
            viewer: "bob".to_string(),
            image_name: "secret.png".to_string(),
            granted_views: 5,
            granted_at: 1234567891,
        });

        let json = serde_json::to_string(&map).unwrap();
        let deserialized: ServerAccessMap = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.grants.len(), 2);
        assert!(deserialized.get_grant("alice", "photo.png").is_some());
        assert!(deserialized.get_grant("bob", "secret.png").is_some());
    }
}

//! Local Access Map - Owner-side permissions tracking
//! Stores access grants in ~/.p2p_client/local_access_map.json

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Local access map storing granted permissions (owner-side)
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LocalAccessMap {
    /// Map of viewer_image_key → AccessGrant
    /// Key format: "{viewer}_{image_name}"
    pub grants: HashMap<String, AccessGrant>,
}

/// Access grant entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessGrant {
    pub viewer: String,
    pub image_name: String,
    pub granted_views: u32,
    pub granted_at: u64, // Unix timestamp in milliseconds
}

impl LocalAccessMap {
    /// Create a new empty access map
    pub fn new() -> Self {
        Self {
            grants: HashMap::new(),
        }
    }

    /// Grant access to a viewer for an image
    pub fn grant_access(&mut self, viewer: &str, image: &str, views: u32) {
        let key = Self::make_key(viewer, image);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let grant = AccessGrant {
            viewer: viewer.to_string(),
            image_name: image.to_string(),
            granted_views: views,
            granted_at: now,
        };

        self.grants.insert(key, grant);
        println!("[ACCESS_MAP] Granted {} views of '{}' to {}", views, image, viewer);
    }

    /// Revoke access for a viewer to an image
    pub fn revoke_access(&mut self, viewer: &str, image: &str) -> bool {
        let key = Self::make_key(viewer, image);
        if self.grants.remove(&key).is_some() {
            println!("[ACCESS_MAP] Revoked access to '{}' from {}", image, viewer);
            true
        } else {
            println!("[ACCESS_MAP] No access to revoke for {} on '{}'", viewer, image);
            false
        }
    }

    /// Adjust view count for an existing grant
    pub fn adjust_views(&mut self, viewer: &str, image: &str, new_views: u32) -> bool {
        let key = Self::make_key(viewer, image);
        if let Some(grant) = self.grants.get_mut(&key) {
            let old_views = grant.granted_views;
            grant.granted_views = new_views;
            println!("[ACCESS_MAP] Adjusted views for {} on '{}': {} → {}",
                     viewer, image, old_views, new_views);
            true
        } else {
            println!("[ACCESS_MAP] Cannot adjust views: no grant found for {} on '{}'",
                     viewer, image);
            false
        }
    }

    /// Check if a viewer has access to an image
    pub fn has_access(&self, viewer: &str, image: &str) -> bool {
        let key = Self::make_key(viewer, image);
        self.grants.contains_key(&key)
    }

    /// Get grant details if exists
    pub fn get_grant(&self, viewer: &str, image: &str) -> Option<&AccessGrant> {
        let key = Self::make_key(viewer, image);
        self.grants.get(&key)
    }

    /// Get all grants for a specific viewer
    pub fn get_viewer_grants(&self, viewer: &str) -> Vec<&AccessGrant> {
        self.grants
            .values()
            .filter(|g| g.viewer == viewer)
            .collect()
    }

    /// Get all grants for a specific image
    pub fn get_image_grants(&self, image: &str) -> Vec<&AccessGrant> {
        self.grants
            .values()
            .filter(|g| g.image_name == image)
            .collect()
    }

    /// Save access map to file
    pub fn save_to_file(&self, path: &Path) -> Result<()> {
        // Create parent directory if it doesn't exist
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create directory: {:?}", parent))?;
        }

        let json = serde_json::to_string_pretty(self)
            .context("Failed to serialize access map")?;

        std::fs::write(path, json)
            .with_context(|| format!("Failed to write access map to {:?}", path))?;

        println!("[ACCESS_MAP] Saved {} grants to {:?}", self.grants.len(), path);
        Ok(())
    }

    /// Load access map from file
    pub fn load_from_file(path: &Path) -> Result<Self> {
        if !path.exists() {
            println!("[ACCESS_MAP] No existing access map at {:?}, creating new", path);
            return Ok(Self::new());
        }

        let json = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read access map from {:?}", path))?;

        let map: Self = serde_json::from_str(&json)
            .context("Failed to deserialize access map")?;

        println!("[ACCESS_MAP] Loaded {} grants from {:?}", map.grants.len(), path);
        Ok(map)
    }

    /// Get default storage path for access map
    /// Returns ~/.p2p_client/local_access_map.json
    pub fn default_path() -> Result<PathBuf> {
        let home = std::env::var("HOME")
            .or_else(|_| std::env::var("USERPROFILE"))
            .context("Failed to get home directory")?;

        let mut path = PathBuf::from(home);
        path.push(".p2p_client");
        path.push("local_access_map.json");

        Ok(path)
    }

    /// Make key for HashMap lookup
    fn make_key(viewer: &str, image: &str) -> String {
        format!("{}_{}", viewer, image)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_grant_and_check_access() {
        let mut map = LocalAccessMap::new();

        assert!(!map.has_access("alice", "photo.png"));

        map.grant_access("alice", "photo.png", 3);

        assert!(map.has_access("alice", "photo.png"));
        assert!(!map.has_access("bob", "photo.png"));
        assert!(!map.has_access("alice", "other.png"));
    }

    #[test]
    fn test_revoke_access() {
        let mut map = LocalAccessMap::new();

        map.grant_access("alice", "photo.png", 3);
        assert!(map.has_access("alice", "photo.png"));

        let revoked = map.revoke_access("alice", "photo.png");
        assert!(revoked);
        assert!(!map.has_access("alice", "photo.png"));

        // Revoking again should return false
        let revoked_again = map.revoke_access("alice", "photo.png");
        assert!(!revoked_again);
    }

    #[test]
    fn test_adjust_views() {
        let mut map = LocalAccessMap::new();

        map.grant_access("alice", "photo.png", 3);

        let adjusted = map.adjust_views("alice", "photo.png", 5);
        assert!(adjusted);

        let grant = map.get_grant("alice", "photo.png").unwrap();
        assert_eq!(grant.granted_views, 5);

        // Adjusting non-existent grant should fail
        let adjusted_missing = map.adjust_views("bob", "photo.png", 5);
        assert!(!adjusted_missing);
    }

    #[test]
    fn test_get_grants() {
        let mut map = LocalAccessMap::new();

        map.grant_access("alice", "photo1.png", 3);
        map.grant_access("alice", "photo2.png", 5);
        map.grant_access("bob", "photo1.png", 2);

        let alice_grants = map.get_viewer_grants("alice");
        assert_eq!(alice_grants.len(), 2);

        let photo1_grants = map.get_image_grants("photo1.png");
        assert_eq!(photo1_grants.len(), 2);
    }

    #[test]
    fn test_serialization() {
        let mut map = LocalAccessMap::new();
        map.grant_access("alice", "photo.png", 3);
        map.grant_access("bob", "secret.png", 5);

        let json = serde_json::to_string(&map).unwrap();
        let deserialized: LocalAccessMap = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.grants.len(), 2);
        assert!(deserialized.has_access("alice", "photo.png"));
        assert!(deserialized.has_access("bob", "secret.png"));
    }
}

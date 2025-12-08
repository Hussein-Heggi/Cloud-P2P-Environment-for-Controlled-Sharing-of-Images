//! Client-Side Steganography Service
//! Provides encryption and decryption of images with embedded metadata for P2P transfers

use anyhow::{Context, Result};
use image::DynamicImage;
use serde::{Deserialize, Serialize};
use std::io::Cursor;
use std::path::Path;

/// View metadata embedded in steganographic images
/// This is embedded with the actual image inside the cover image
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewMetadata {
    pub owner: String,
    pub viewer: String,
    pub image_name: String,
    pub remaining_views: u32,
    pub image_uuid: String,
}

impl From<ViewMetadata> for stego::Meta {
    fn from(vm: ViewMetadata) -> Self {
        stego::Meta {
            owner: vm.owner,
            viewer: vm.viewer,
            image_name: vm.image_name,
            remaining_views: vm.remaining_views,
            image_uuid: vm.image_uuid,
        }
    }
}

impl From<stego::Meta> for ViewMetadata {
    fn from(m: stego::Meta) -> Self {
        ViewMetadata {
            owner: m.owner,
            viewer: m.viewer,
            image_name: m.image_name,
            remaining_views: m.remaining_views,
            image_uuid: m.image_uuid,
        }
    }
}

/// Encrypt actual image with metadata using cover image
/// Used by owner to prepare image for P2P transfer
///
/// # Arguments
/// * `actual_image_path` - Path to the actual image (secret)
/// * `cover_image_path` - Path to the cover image (carrier)
/// * `metadata` - View metadata to embed
///
/// # Returns
/// * PNG bytes of the steganographic image (cover with embedded actual + metadata)
pub async fn encrypt_image_with_metadata(
    actual_image_path: &str,
    cover_image_path: &str,
    metadata: ViewMetadata,
) -> Result<Vec<u8>> {
    // Read actual image
    let actual_img_bytes = tokio::fs::read(actual_image_path)
        .await
        .with_context(|| format!("Failed to read actual image: {}", actual_image_path))?;

    // Read cover image
    let cover_img_bytes = tokio::fs::read(cover_image_path)
        .await
        .with_context(|| format!("Failed to read cover image: {}", cover_image_path))?;

    // Convert ViewMetadata to stego::Meta and serialize to JSON
    let stego_meta: stego::Meta = metadata.clone().into();
    let meta_json = serde_json::to_vec(&stego_meta)
        .context("Failed to serialize metadata")?;

    // Embed using stego library
    let stego_bytes = stego::embed_to_png_bytes(&actual_img_bytes, &cover_img_bytes, &meta_json)
        .map_err(|e| anyhow::anyhow!("Steganography embedding failed: {}", e))?;

    println!("[STEGO_CLIENT] Encrypted image: {} bytes → {} bytes (stego)",
             actual_img_bytes.len(), stego_bytes.len());
    println!("[STEGO_CLIENT] Embedded metadata: owner={}, viewer={}, views={}",
             metadata.owner, metadata.viewer, metadata.remaining_views);

    Ok(stego_bytes)
}

/// Decrypt and extract metadata from embedded image
/// Used by viewer to retrieve actual image from steganographic cover
///
/// # Arguments
/// * `embedded_image_bytes` - PNG bytes of steganographic image
///
/// # Returns
/// * Tuple of (actual_image_bytes, metadata)
pub async fn decrypt_image_and_extract_metadata(
    embedded_image_bytes: &[u8],
) -> Result<(Vec<u8>, ViewMetadata)> {
    // Load steganographic image
    let stego_img = image::load_from_memory(embedded_image_bytes)
        .context("Failed to load steganographic image")?;

    // Extract actual image and metadata
    let (actual_img, meta) = stego::extract(&stego_img)
        .map_err(|e| anyhow::anyhow!("Steganography extraction failed: {}", e))?;

    // Convert actual image to PNG bytes
    let mut actual_img_bytes = Vec::new();
    let mut cursor = Cursor::new(&mut actual_img_bytes);
    actual_img.write_to(&mut cursor, image::ImageFormat::Png)
        .context("Failed to encode actual image to PNG")?;

    println!("[STEGO_CLIENT] Decrypted image: {} bytes (stego) → {} bytes (actual)",
             embedded_image_bytes.len(), actual_img_bytes.len());
    println!("[STEGO_CLIENT] Extracted metadata: owner={}, viewer={}, views={}",
             meta.owner, meta.viewer, meta.remaining_views);

    // Convert from stego::Meta to ViewMetadata
    let view_metadata: ViewMetadata = meta.into();

    Ok((actual_img_bytes, view_metadata))
}

/// Decrement view count in metadata
/// Returns updated metadata with decremented count
///
/// # Arguments
/// * `metadata` - Current metadata
///
/// # Returns
/// * Updated metadata with views decremented by 1
///
/// # Errors
/// * Returns error if no views remaining
pub fn decrement_view_count(mut metadata: ViewMetadata) -> Result<ViewMetadata> {
    if metadata.remaining_views == 0 {
        return Err(anyhow::anyhow!("No views remaining"));
    }

    metadata.remaining_views -= 1;

    println!("[STEGO_CLIENT] Decremented view count: {} views remaining",
             metadata.remaining_views);

    Ok(metadata)
}

/// Helper: Embed image bytes directly (no file I/O)
/// Used for in-memory encryption operations
pub fn embed_meta_return_png(
    actual_img_bytes: &[u8],
    cover_img_bytes: &[u8],
    metadata: &ViewMetadata,
) -> Result<Vec<u8>> {
    // Convert ViewMetadata to stego::Meta and serialize to JSON
    let stego_meta: stego::Meta = metadata.clone().into();
    let meta_json = serde_json::to_vec(&stego_meta)
        .context("Failed to serialize metadata")?;

    let stego_bytes = stego::embed_to_png_bytes(actual_img_bytes, cover_img_bytes, &meta_json)
        .map_err(|e| anyhow::anyhow!("Steganography embedding failed: {}", e))?;

    Ok(stego_bytes)
}

/// Helper: Extract from image bytes directly (no file I/O)
/// Used for in-memory decryption operations
pub fn extract_meta_from_png(embedded_png: &[u8]) -> Result<(Vec<u8>, ViewMetadata)> {
    let stego_img = image::load_from_memory(embedded_png)
        .context("Failed to load steganographic image")?;

    let (actual_img, meta) = stego::extract(&stego_img)
        .map_err(|e| anyhow::anyhow!("Steganography extraction failed: {}", e))?;

    // Convert actual image to PNG bytes
    let mut actual_img_bytes = Vec::new();
    let mut cursor = Cursor::new(&mut actual_img_bytes);
    actual_img.write_to(&mut cursor, image::ImageFormat::Png)
        .context("Failed to encode actual image to PNG")?;

    // Convert from stego::Meta to ViewMetadata
    let view_metadata: ViewMetadata = meta.into();

    Ok((actual_img_bytes, view_metadata))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_view_metadata_serialization() {
        let meta = ViewMetadata {
            owner: "alice".to_string(),
            viewer: "bob".to_string(),
            image_name: "secret.png".to_string(),
            remaining_views: 3,
            image_uuid: "uuid-123".to_string(),
        };

        let json = serde_json::to_vec(&meta).unwrap();
        let deserialized: ViewMetadata = serde_json::from_slice(&json).unwrap();

        assert_eq!(deserialized.owner, "alice");
        assert_eq!(deserialized.remaining_views, 3);
    }

    #[test]
    fn test_decrement_view_count() {
        let meta = ViewMetadata {
            owner: "alice".to_string(),
            viewer: "bob".to_string(),
            image_name: "secret.png".to_string(),
            remaining_views: 3,
            image_uuid: "uuid-123".to_string(),
        };

        let updated = decrement_view_count(meta).unwrap();
        assert_eq!(updated.remaining_views, 2);

        let mut meta_zero = ViewMetadata {
            owner: "alice".to_string(),
            viewer: "bob".to_string(),
            image_name: "secret.png".to_string(),
            remaining_views: 0,
            image_uuid: "uuid-123".to_string(),
        };

        let result = decrement_view_count(meta_zero);
        assert!(result.is_err());
    }
}

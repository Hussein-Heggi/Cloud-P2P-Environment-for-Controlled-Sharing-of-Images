//! Client-side steganography extraction
//! Extracts the true image and metadata from embedded PNG files

use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use tokio::fs;

/// Extract true image from embedded steganographic PNG
/// Returns (true_image_path, metadata_json)
pub async fn extract_true_image(embedded_path: &Path) -> Result<(PathBuf, String)> {
    println!("[EXTRACTION] Extracting from: {}", embedded_path.display());

    // Read embedded PNG file
    let embedded_bytes = fs::read(embedded_path).await
        .context("Failed to read embedded PNG")?;

    // Load image
    let embedded_img = image::load_from_memory(&embedded_bytes)
        .context("Failed to load embedded image")?;

    // Extract using stego library
    let (true_img, metadata) = stego::extract(&embedded_img)
        .context("Failed to extract true image from steganographic PNG")?;

    println!("[EXTRACTION] ✅ Extracted successfully!");
    println!("[EXTRACTION] Metadata: owner={}, viewer={}, image={}, remaining_views={}",
        metadata.owner, metadata.viewer, metadata.image_name, metadata.remaining_views);

    // Serialize metadata to JSON
    let metadata_json = serde_json::to_string_pretty(&metadata)
        .context("Failed to serialize metadata")?;

    // Generate true image filename
    let true_image_filename = format!("{}_{}_{}_true.png",
        metadata.owner,
        metadata.viewer,
        metadata.image_name.replace(".png", "").replace(".jpg", "")
    );

    // Save true image to downloads directory
    let downloads_dir = embedded_path.parent().unwrap_or(Path::new("downloads"));
    let true_image_path = downloads_dir.join(&true_image_filename);

    save_image(&true_img, &true_image_path).await?;

    println!("[EXTRACTION] True image saved to: {}", true_image_path.display());

    Ok((true_image_path, metadata_json))
}

/// Save DynamicImage to PNG file
async fn save_image(img: &image::DynamicImage, path: &Path) -> Result<()> {
    // Save synchronously in a blocking task
    let img_clone = img.clone();
    let path_clone = path.to_path_buf();

    tokio::task::spawn_blocking(move || {
        img_clone.save(&path_clone)
            .context("Failed to save image")
    })
    .await??;

    Ok(())
}

/// Parse metadata JSON string into structured data
pub fn parse_metadata(metadata_json: &str) -> Result<stego::Meta> {
    let meta: stego::Meta = serde_json::from_str(metadata_json)
        .context("Failed to parse metadata JSON")?;
    Ok(meta)
}

/// Save metadata JSON to file
pub async fn save_metadata(metadata_json: &str, embedded_path: &Path) -> Result<PathBuf> {
    let metadata_filename = format!("{}_metadata.json",
        embedded_path.file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown")
    );

    let downloads_dir = embedded_path.parent().unwrap_or(Path::new("downloads"));
    let metadata_path = downloads_dir.join(&metadata_filename);

    fs::write(&metadata_path, metadata_json).await
        .context("Failed to write metadata file")?;

    println!("[EXTRACTION] Metadata saved to: {}", metadata_path.display());

    Ok(metadata_path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_metadata() {
        let json = r#"{
            "owner": "alice",
            "viewer": "bob",
            "image_name": "secret.png",
            "remaining_views": 5,
            "image_uuid": "test-uuid-123"
        }"#;

        let meta = parse_metadata(json).unwrap();
        assert_eq!(meta.owner, "alice");
        assert_eq!(meta.viewer, "bob");
        assert_eq!(meta.remaining_views, 5);
    }
}

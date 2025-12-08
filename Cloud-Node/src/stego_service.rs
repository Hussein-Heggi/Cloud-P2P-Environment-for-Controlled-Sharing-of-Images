//! DEPRECATED: Server-side Steganography Service
//!
//! This module is legacy code from the server-mediated architecture.
//! In the new P2P architecture, clients perform steganographic encryption locally
//! using the client-side stego_client module before P2P transfer.
//!
//! This service remains for backwards compatibility only.

use anyhow::{Result, anyhow};

/// DEPRECATED: Use client-side stego_client for P2P architecture
/// Embeds true image into cover image
/// NOTE: The stego library requires a Meta struct for API compatibility.
/// For owner uploads (no client metadata), we provide minimal empty structure.
pub fn embed_meta_return_png(
    true_img_bytes: &[u8],
    cover_img_bytes: &[u8],
    meta_json: &[u8]
) -> Result<Vec<u8>> {
    // Library API requires Meta struct with all fields
    // For owner uploads (no client metadata), use minimal valid Meta JSON
    // This is NOT server-generated metadata - just a library API requirement
    let metadata_to_use = if meta_json.is_empty() {
        println!("[STEGO] Client sent 0 bytes metadata - using minimal library placeholder");
        // Minimal valid Meta struct that stego library expects
        br#"{"owner":"","viewer":"","image_name":"","remaining_views":0,"image_uuid":""}"# as &[u8]
    } else {
        meta_json
    };

    match stego::embed_to_png_bytes(true_img_bytes, cover_img_bytes, metadata_to_use) {
        Ok(v) => Ok(v),
        Err(e) => Err(anyhow!(e)),
    }
}


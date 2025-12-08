//! DEPRECATED: Server-side Steganography Service
//!
//! This module is legacy code from the server-mediated architecture.
//! In the new P2P architecture, clients perform steganographic encryption locally
//! using the client-side stego_client module before P2P transfer.
//!
//! This service remains for backwards compatibility only.

use anyhow::{Result, anyhow};

/// DEPRECATED: Use client-side stego_client for P2P architecture
pub fn embed_meta_return_png(
    true_img_bytes: &[u8],
    cover_img_bytes: &[u8],
    meta_json: &[u8]
) -> Result<Vec<u8>> {
    match stego::embed_to_png_bytes(true_img_bytes, cover_img_bytes, meta_json) {
        Ok(v) => Ok(v),
        Err(e) => Err(anyhow!(e)),
    }
}


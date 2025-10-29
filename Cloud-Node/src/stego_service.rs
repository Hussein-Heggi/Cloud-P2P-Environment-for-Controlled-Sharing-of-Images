use anyhow::{Result, anyhow};

pub fn embed_meta_return_png(img_bytes: &[u8], meta_json: &[u8]) -> Result<Vec<u8>> {
    stego::embed_to_png_bytes(img_bytes, meta_json).map_err(|e| anyhow!(e))
}

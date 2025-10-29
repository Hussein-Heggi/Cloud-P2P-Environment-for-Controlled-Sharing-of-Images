use anyhow::{Result, anyhow};

pub fn embed_meta_return_png(img_bytes: &[u8], meta_json: &[u8]) -> Result<Vec<u8>> {
    match stego::embed_to_png_bytes(img_bytes, meta_json) {
        Ok(v) => Ok(v),
        Err(e) => Err(anyhow!(e)),
    }
}


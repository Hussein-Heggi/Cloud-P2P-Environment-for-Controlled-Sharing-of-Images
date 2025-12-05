use anyhow::{Result, anyhow};

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


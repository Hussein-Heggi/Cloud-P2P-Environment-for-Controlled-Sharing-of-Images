use std::io::Cursor;

use image::{DynamicImage, ImageFormat, RgbaImage};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StegoError {
    #[error("image error: {0}")]
    Img(#[from] image::ImageError),
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("payload too large to embed")]
    TooLarge,
    #[error("no payload found")]
    NoPayload,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessEntry {
    pub user: String,
    pub remaining_views: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Meta {
    pub owner: String,
    pub allow: Vec<AccessEntry>,
}

const MAX_PAYLOAD_BYTES: usize = 64 * 1024;
const MAGIC: &[u8; 4] = b"SP2P";

/// LSB embed: MAGIC + len(u32 LE) + JSON(meta) into lowest bit of RGBA channels.
pub fn embed(img: DynamicImage, meta: &Meta) -> Result<DynamicImage, StegoError> {
    let json = serde_json::to_vec(meta)?;
    if json.len() > MAX_PAYLOAD_BYTES {
        return Err(StegoError::TooLarge);
    }

    let mut payload = Vec::with_capacity(8 + json.len());
    payload.extend_from_slice(MAGIC);
    payload.extend((json.len() as u32).to_le_bytes());
    payload.extend(json);

    let mut rgba: RgbaImage = img.to_rgba8();
    // capacity in bits (1 bit per channel)
    let cap_bits = rgba.width() as usize * rgba.height() as usize * 4;
    let need_bits = payload.len() * 8;
    if need_bits > cap_bits {
        return Err(StegoError::TooLarge);
    }

    let mut bit_i = 0usize;
    'outer: for y in 0..rgba.height() {
        for x in 0..rgba.width() {
            let p = rgba.get_pixel_mut(x, y);
            for c in 0..4 {
                if bit_i >= need_bits {
                    break 'outer;
                }
                let byte = payload[bit_i / 8];
                let bit = (byte >> (bit_i % 8)) & 1;
                p[c] = (p[c] & 0xFE) | bit;
                bit_i += 1;
            }
        }
    }

    Ok(DynamicImage::ImageRgba8(rgba))
}

pub fn extract(img: &DynamicImage) -> Result<Meta, StegoError> {
    let rgba: RgbaImage = img.to_rgba8();
    let mut bytes = Vec::new();
    let mut cur: u8 = 0;
    let mut bitpos = 0;

    for y in 0..rgba.height() {
        for x in 0..rgba.width() {
            let p = rgba.get_pixel(x, y);
            for c in 0..4 {
                let bit = p[c] & 1;
                cur |= bit << (bitpos as u8);
                bitpos += 1;
                if bitpos == 8 {
                    bytes.push(cur);
                    cur = 0;
                    bitpos = 0;

                    if bytes.len() >= 8 && &bytes[0..4] == MAGIC {
                        let len = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
                        if bytes.len() >= 8 + len {
                            let meta: Meta = serde_json::from_slice(&bytes[8..8 + len])?;
                            return Ok(meta);
                        }
                    }
                }
            }
        }
    }
    Err(StegoError::NoPayload)
}

/// Helper API used by the server: returns a PNG buffer after embedding `meta_json`.
pub fn embed_to_png_bytes(img_bytes: &[u8], meta_json: &[u8]) -> Result<Vec<u8>, StegoError> {
    let img = image::load_from_memory(img_bytes)?;
    let meta: Meta = serde_json::from_slice(meta_json)?;
    let out = embed(img, &meta)?;

    // write PNG to a Cursor<Vec<u8>> because write_to requires Write + Seek
    let mut buf: Vec<u8> = Vec::new();
    let mut cursor = Cursor::new(&mut buf);
    out.write_to(&mut cursor, ImageFormat::Png)?;
    Ok(buf)
}

use std::io::Cursor;

use image::{DynamicImage, ImageEncoder, ImageFormat, RgbaImage, codecs::png::{PngEncoder, CompressionType, FilterType}};
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
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

// Legacy structure (kept for backward compatibility during transition)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessEntry {
    pub user: String,
    pub remaining_views: u32,
}

// NEW metadata structure for revamped system
// One encrypted image per viewer (not multi-user)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Meta {
    pub owner: String,
    pub viewer: String,
    pub image_name: String,
    pub remaining_views: u32,
    pub image_uuid: String,
}

// Legacy metadata structure (for old protocol - will be deprecated)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LegacyMeta {
    pub owner: String,
    pub allow: Vec<AccessEntry>,
}

const MAGIC: &[u8; 4] = b"SP2P";

/// Compress true_image to PNG bytes with specified compression level
/// Returns PNG-encoded bytes
fn compress_image_to_png(img: &DynamicImage, compression: CompressionType) -> Result<Vec<u8>, StegoError> {
    let mut buf = Vec::new();
    let mut cursor = Cursor::new(&mut buf);

    let rgba = img.to_rgba8();
    let encoder = PngEncoder::new_with_quality(&mut cursor, compression, FilterType::Sub);
    encoder.write_image(
        &rgba,
        rgba.width(),
        rgba.height(),
        image::ColorType::Rgba8,
    )?;

    Ok(buf)
}

/// Try to compress true_image to fit within max_size
/// Tries compression levels from Fast to Best
fn compress_image_adaptive(img: &DynamicImage, max_size: usize) -> Result<Vec<u8>, StegoError> {
    // Try different compression levels
    let compression_levels = [
        CompressionType::Fast,
        CompressionType::Default,
        CompressionType::Best,
    ];

    for &compression in &compression_levels {
        let compressed = compress_image_to_png(img, compression)?;
        if compressed.len() <= max_size {
            return Ok(compressed);
        }
    }

    // If still too large after best compression, return error
    Err(StegoError::TooLarge)
}

/// Two-image LSB embedding: embeds [true_image + metadata] into cover_image
///
/// Payload format:
/// [MAGIC: 4 bytes "SP2P"]
/// [total_length: u32] - total payload size after this field
/// [true_image_length: u32]
/// [true_image_bytes: PNG compressed]
/// [metadata_json_length: u32]
/// [metadata_json: JSON bytes]
pub fn embed(
    true_img: DynamicImage,
    cover_img: DynamicImage,
    meta: &Meta,
) -> Result<DynamicImage, StegoError> {
    // Serialize metadata
    let meta_json = serde_json::to_vec(meta)?;

    // Calculate cover capacity (in bytes)
    let cover_rgba = cover_img.to_rgba8();
    let cover_capacity_bits = cover_rgba.width() as usize * cover_rgba.height() as usize * 4;
    let cover_capacity_bytes = cover_capacity_bits / 8;

    // Reserve space for header: MAGIC(4) + total_len(4) + img_len(4) + meta_len(4) = 16 bytes
    let header_size = 16;
    let available_for_payload = cover_capacity_bytes.saturating_sub(header_size);

    // Compress true_image to PNG bytes
    let max_image_size = available_for_payload.saturating_sub(meta_json.len() + 4); // -4 for meta_len field
    let true_img_bytes = compress_image_adaptive(&true_img, max_image_size)?;

    // Build complete payload
    let total_payload_len = 4 + true_img_bytes.len() + 4 + meta_json.len();
    let mut payload = Vec::with_capacity(header_size + total_payload_len);

    payload.extend_from_slice(MAGIC);
    payload.extend((total_payload_len as u32).to_le_bytes());
    payload.extend((true_img_bytes.len() as u32).to_le_bytes());
    payload.extend(&true_img_bytes);
    payload.extend((meta_json.len() as u32).to_le_bytes());
    payload.extend(&meta_json);

    // Final capacity check
    let need_bits = payload.len() * 8;
    if need_bits > cover_capacity_bits {
        return Err(StegoError::TooLarge);
    }

    // Embed payload into cover image using LSB
    let mut rgba = cover_rgba;
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

/// Extract true_image and metadata from steganographic cover image
/// Returns (true_image, metadata)
pub fn extract(img: &DynamicImage) -> Result<(DynamicImage, Meta), StegoError> {
    let rgba: RgbaImage = img.to_rgba8();
    let mut bytes = Vec::new();
    let mut cur: u8 = 0;
    let mut bitpos = 0;

    // Extract LSBs bit by bit
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

                    // Check for complete header: MAGIC + total_len
                    if bytes.len() >= 8 && &bytes[0..4] == MAGIC {
                        let total_len = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;

                        // Check if we have the complete payload
                        if bytes.len() >= 8 + total_len {
                            // Parse payload
                            let mut offset = 8;

                            // Extract true_image_length
                            if offset + 4 > bytes.len() {
                                return Err(StegoError::NoPayload);
                            }
                            let img_len = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
                            offset += 4;

                            // Extract true_image bytes
                            if offset + img_len > bytes.len() {
                                return Err(StegoError::NoPayload);
                            }
                            let img_bytes = &bytes[offset..offset + img_len];
                            offset += img_len;

                            // Extract metadata_length
                            if offset + 4 > bytes.len() {
                                return Err(StegoError::NoPayload);
                            }
                            let meta_len = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
                            offset += 4;

                            // Extract metadata JSON
                            if offset + meta_len > bytes.len() {
                                return Err(StegoError::NoPayload);
                            }
                            let meta_bytes = &bytes[offset..offset + meta_len];

                            // Decode true image from PNG bytes
                            let true_img = image::load_from_memory(img_bytes)?;

                            // Parse metadata
                            let meta: Meta = serde_json::from_slice(meta_bytes)?;

                            return Ok((true_img, meta));
                        }
                    }
                }
            }
        }
    }

    Err(StegoError::NoPayload)
}

/// Helper API used by the server: returns a steganographic PNG buffer
/// Embeds true_image + metadata into cover_image
pub fn embed_to_png_bytes(
    true_img_bytes: &[u8],
    cover_img_bytes: &[u8],
    meta_json: &[u8],
) -> Result<Vec<u8>, StegoError> {
    let true_img = image::load_from_memory(true_img_bytes)?;
    let cover_img = image::load_from_memory(cover_img_bytes)?;
    let meta: Meta = serde_json::from_slice(meta_json)?;

    let stego = embed(true_img, cover_img, &meta)?;

    // Write steganographic image to PNG bytes
    let mut buf: Vec<u8> = Vec::new();
    let mut cursor = Cursor::new(&mut buf);
    stego.write_to(&mut cursor, ImageFormat::Png)?;
    Ok(buf)
}

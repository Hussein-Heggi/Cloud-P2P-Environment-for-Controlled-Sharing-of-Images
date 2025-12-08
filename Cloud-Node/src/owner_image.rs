use std::collections::HashMap;

use crate::state::{SharedState, StoredImageAssets};

/// Initialize or reset an owner/image entry with expected sizes
pub async fn init_owner_image(
    state: SharedState,
    owner: String,
    image_name: String,
    true_size: usize,
    cover_size: usize,
    meta_size: usize,
) {
    let mut s = state.write().await;
    let entry = s
        .owner_images
        .entry(owner)
        .or_insert_with(HashMap::new)
        .entry(image_name)
        .or_insert_with(StoredImageAssets::default);
    entry.true_expected = true_size;
    entry.cover_expected = cover_size;
    entry.meta_expected = meta_size;
    entry.true_buf.clear();
    entry.cover_buf.clear();
    entry.meta_buf.clear();
    entry.true_received = 0;
    entry.cover_received = 0;
    entry.meta_received = 0;
}

/// Append chunk data to the correct buffer (kind: 0=true,1=cover,2=meta)
pub async fn append_chunk(
    state: SharedState,
    owner: &str,
    image_name: &str,
    kind: u8,
    offset: usize,
    data: &[u8],
) -> Option<StoredImageAssets> {
    let mut s = state.write().await;
    if let Some(imgs) = s.owner_images.get_mut(owner) {
        if let Some(entry) = imgs.get_mut(image_name) {
            let (target, received_counter) = match kind {
                0 => {
                    if entry.true_buf.is_empty() {
                        entry.true_buf.resize(entry.true_expected, 0);
                    }
                    (&mut entry.true_buf, &mut entry.true_received)
                }
                1 => {
                    if entry.cover_buf.is_empty() {
                        entry.cover_buf.resize(entry.cover_expected, 0);
                    }
                    (&mut entry.cover_buf, &mut entry.cover_received)
                }
                2 => {
                    if entry.meta_buf.is_empty() {
                        entry.meta_buf.resize(entry.meta_expected, 0);
                    }
                    (&mut entry.meta_buf, &mut entry.meta_received)
                }
                _ => return None,
            };
            if offset + data.len() <= target.len() {
                target[offset..offset + data.len()].copy_from_slice(data);
                *received_counter += data.len(); // Track bytes received
            }
            if entry.ready() {
                return Some(entry.clone());
            }
        }
    }
    None
}

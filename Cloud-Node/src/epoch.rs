use std::sync::OnceLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;
use tracing::debug;

/// Global epoch - initialized once at startup, aligned to nearest second
static GLOBAL_EPOCH: OnceLock<SystemTime> = OnceLock::new();

/// Initialize the global epoch aligned to the nearest second boundary.
/// This should be called once at startup in main().
/// All servers starting around the same time will align to the same second boundary.
pub fn init_epoch() {
    let now = SystemTime::now();
    let since_epoch = now.duration_since(UNIX_EPOCH).expect("Time went backwards");
    
    // Round up to nearest second
    let seconds = since_epoch.as_secs();
    let nanos = since_epoch.subsec_nanos();
    
    let aligned_epoch = if nanos == 0 {
        // Already aligned
        UNIX_EPOCH + Duration::from_secs(seconds)
    } else {
        // Round up to next second
        UNIX_EPOCH + Duration::from_secs(seconds + 1)
    };
    
    // Wait until we reach the aligned epoch
    if let Ok(wait_duration) = aligned_epoch.duration_since(now) {
        if wait_duration.as_millis() > 0 {
            std::thread::sleep(wait_duration);
            println!("[EPOCH] Aligned to nearest second boundary (waited {}ms)", wait_duration.as_millis());
        }
    }
    
    GLOBAL_EPOCH.set(aligned_epoch).expect("Epoch already initialized");
    println!("[EPOCH] Global epoch initialized at {:?}", aligned_epoch);
}

/// Get the global epoch. Panics if not initialized.
fn get_epoch() -> SystemTime {
    *GLOBAL_EPOCH.get().expect("Epoch not initialized - call init_epoch() first")
}

/// Get current offset from epoch in milliseconds
pub fn epoch_offset_ms() -> u128 {
    let now = SystemTime::now();
    let epoch = get_epoch();
    now.duration_since(epoch)
        .expect("Current time before epoch")
        .as_millis()
}

/// Tier 1: High precision (5ms granularity) for critical operations.
/// Sleeps until the next aligned tick based on the interval.
/// 
/// Example: If interval_ms=750 and we're at epoch+740ms, sleeps 10ms to reach epoch+750ms
/// 
/// # Arguments
/// * `interval_ms` - Interval in milliseconds (should be multiple of 5ms for best alignment)
/// * `task_name` - Name for debug logging
pub async fn sleep_until_next_aligned_tick_t1(interval_ms: u64, task_name: &str) {
    let current_offset = epoch_offset_ms() as u64;
    
    // Calculate the next tick aligned to interval boundaries
    let next_tick_offset = ((current_offset / interval_ms) + 1) * interval_ms;
    
    // Round to nearest 5ms for Tier 1 precision
    let next_tick_aligned = ((next_tick_offset + 2) / 5) * 5;
    
    let sleep_ms = next_tick_aligned.saturating_sub(current_offset);
    
    debug!(
        task = task_name,
        current_offset_ms = current_offset,
        next_tick_ms = next_tick_aligned,
        sleep_ms = sleep_ms,
        "T1 aligned sleep"
    );
    
    if sleep_ms > 0 {
        sleep(Duration::from_millis(sleep_ms)).await;
    }
}

/// Tier 2: Standard precision (100ms granularity) for maintenance operations.
/// Sleeps until the next aligned tick based on the interval.
/// 
/// Example: If interval_ms=10000 and we're at epoch+9850ms, sleeps 150ms to reach epoch+10000ms
/// 
/// # Arguments
/// * `interval_ms` - Interval in milliseconds (should be multiple of 100ms for best alignment)
/// * `task_name` - Name for debug logging
pub async fn sleep_until_next_aligned_tick_t2(interval_ms: u64, task_name: &str) {
    let current_offset = epoch_offset_ms() as u64;
    
    // Calculate the next tick aligned to interval boundaries
    let next_tick_offset = ((current_offset / interval_ms) + 1) * interval_ms;
    
    // Round to nearest 100ms for Tier 2 precision
    let next_tick_aligned = ((next_tick_offset + 50) / 100) * 100;
    
    let sleep_ms = next_tick_aligned.saturating_sub(current_offset);
    
    debug!(
        task = task_name,
        current_offset_ms = current_offset,
        next_tick_ms = next_tick_aligned,
        sleep_ms = sleep_ms,
        "T2 aligned sleep"
    );
    
    if sleep_ms > 0 {
        sleep(Duration::from_millis(sleep_ms)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_epoch_offset() {
        init_epoch();
        let offset1 = epoch_offset_ms();
        std::thread::sleep(Duration::from_millis(100));
        let offset2 = epoch_offset_ms();
        assert!(offset2 > offset1);
        assert!(offset2 - offset1 >= 100);
    }
}



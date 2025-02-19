use std::sync::atomic::{AtomicU32, AtomicU64};

pub struct SnowFlakeGenerator {
    machine_id: u32,
    timestamp_ms: AtomicU64,
    sequence: AtomicU32,
}

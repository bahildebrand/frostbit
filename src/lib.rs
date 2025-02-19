use chrono::Utc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

pub type TimestampFn = fn() -> u64;

pub struct SnowFlakeGenerator {
    machine_id: u32,
    timestamp_ms: AtomicU64,
    sequence: AtomicU32,
    epoch: u64,
    get_timestamp: TimestampFn,
}

impl SnowFlakeGenerator {
    const TIMESTAMP_MASK: u64 = 0x1FFFFFFFFFF;
    const MACHINE_ID_MASK: u64 = 0x3FF;
    const SEQUENCE_MASK: u64 = 0xFFF;

    const TIMESTAMP_SHIFT: usize = 22;
    const MACHINE_ID_SHIFT: usize = 12;

    pub fn new(machine_id: u32, epoch: u64, get_timestamp: TimestampFn) -> Self {
        let timestamp_ms = get_timestamp() - epoch;

        Self {
            machine_id,
            timestamp_ms: AtomicU64::new(timestamp_ms),
            sequence: AtomicU32::new(0),
            epoch,
            get_timestamp,
        }
    }

    pub fn generate(&self) -> u64 {
        let new_timestamp = self.get_epoch_relative_timestamp();
        let prev_timestamp = self.timestamp_ms.load(Ordering::SeqCst);

        let timestamp = if prev_timestamp != new_timestamp {
            self.timestamp_ms.store(new_timestamp, Ordering::SeqCst);
            self.sequence.store(0, Ordering::SeqCst);

            new_timestamp
        } else {
            prev_timestamp
        };

        let sequence_id = self.sequence.fetch_add(1, Ordering::SeqCst);
        let timestamp_bits = timestamp & Self::TIMESTAMP_MASK;
        let machine_id_bits = self.machine_id as u64 & Self::MACHINE_ID_MASK;
        let sequence_id_bits = sequence_id as u64 & Self::SEQUENCE_MASK;

        timestamp_bits << Self::TIMESTAMP_SHIFT
            | machine_id_bits << Self::MACHINE_ID_SHIFT
            | sequence_id_bits
    }

    fn get_epoch_relative_timestamp(&self) -> u64 {
        (self.get_timestamp)() - self.epoch
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_timestamp_generation() {
        const TIMESTAMP: u64 = 0x1234u64;
        let timestamp_fn = || TIMESTAMP;
        let machine_id = 0x10u32;
        let epoch = 0u64;

        let generator = SnowFlakeGenerator::new(machine_id, epoch, timestamp_fn);
        let snowflake = generator.generate();
        assert_eq!(snowflake, 0x48D010000);

        let snowflake = generator.generate();
        assert_eq!(snowflake, 0x48D010001);
    }
}

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

pub type TimestampFn = fn() -> u64;

#[derive(Debug)]
pub enum SnowFlakeGeneratorError {
    SequenceOverflow,
}

pub struct SnowFlakeGenerator {
    machine_id: u32,
    timestamp_ms: AtomicU64,
    sequence: AtomicU32,
    epoch: u64,
    get_timestamp: TimestampFn,
}

impl SnowFlakeGenerator {
    const TIMESTAMP_BITS: usize = 42;
    const MACHINE_ID_BITS: usize = 10;
    const SEQUENCE_ID_BITS: usize = 12;

    const TIMESTAMP_MASK: u64 = (1 << Self::TIMESTAMP_BITS) - 1;
    const MACHINE_ID_MASK: u64 = (1 << Self::MACHINE_ID_BITS) - 1;
    const SEQUENCE_MASK: u64 = (1 << Self::SEQUENCE_ID_BITS) - 1;

    const SEQUENCE_ID_MAX: usize = 2 ^ Self::SEQUENCE_ID_BITS;

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

    pub fn generate(&self) -> Result<u64, SnowFlakeGeneratorError> {
        let new_timestamp = self.get_epoch_relative_timestamp();
        let prev_timestamp = self.timestamp_ms.load(Ordering::SeqCst);

        if prev_timestamp != new_timestamp {
            let swap_result = self.timestamp_ms.compare_exchange(
                prev_timestamp,
                new_timestamp,
                Ordering::SeqCst,
                Ordering::SeqCst,
            );

            if swap_result.is_ok() {
                self.sequence.store(0, Ordering::SeqCst);
            }
        }

        let sequence_id = self.sequence.fetch_add(1, Ordering::SeqCst);
        if sequence_id as usize >= Self::SEQUENCE_ID_MAX {
            return Err(SnowFlakeGeneratorError::SequenceOverflow);
        }

        let timestamp_bits = new_timestamp & Self::TIMESTAMP_MASK;
        let machine_id_bits = self.machine_id as u64 & Self::MACHINE_ID_MASK;
        let sequence_id_bits = sequence_id as u64 & Self::SEQUENCE_MASK;

        Ok(
            timestamp_bits << (Self::MACHINE_ID_BITS + Self::SEQUENCE_ID_BITS)
                | machine_id_bits << Self::SEQUENCE_ID_BITS
                | sequence_id_bits,
        )
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
        let snowflake = generator.generate().unwrap();
        assert_eq!(snowflake, 0x48D010000);

        let snowflake = generator.generate().unwrap();
        assert_eq!(snowflake, 0x48D010001);
    }

    #[test]
    fn test_sequence_overflow() {
        const TIMESTAMP: u64 = 0x1234u64;
        let timestamp_fn = || TIMESTAMP;
        let machine_id = 0x10u32;
        let epoch = 0u64;

        let generator = SnowFlakeGenerator::new(machine_id, epoch, timestamp_fn);
        // iterate over generation until right before sequence overflow
        for _ in 0..SnowFlakeGenerator::SEQUENCE_ID_MAX {
            generator.generate().unwrap();
        }

        let res = generator.generate();
        assert!(matches!(
            res,
            Err(SnowFlakeGeneratorError::SequenceOverflow)
        ));
    }
}

use std::sync::atomic::Ordering;

use crate::sync::AtomicU64;
use crate::{build_mask, SnowFlakeConfig, SnowFlakeGeneratorError};

/// Stores both a 42-bit timestamp and 12-bit sequence in a single atomic.
///
/// This is allows us to synchronize both of these operations with a single atomic,
/// as both values are tied together. The layout is as follows:
///
/// Bits 0-12: sequence ID
/// Bits 13-20: unused
/// Bits 21-62: timestamp
///
/// Note that both the sequence and timestamp are 1 bit larger than needed. This allows
/// us to easily handle and check for overflows.
pub(crate) struct TimestampSequenceGenerator {
    inner: AtomicU64,
    config: SnowFlakeConfig,
    shifted_timestamp_mask: u64,
    extended_sequence_mask: u64,
}

impl TimestampSequenceGenerator {
    pub(crate) fn new(timestamp: u64, config: SnowFlakeConfig) -> Self {
        let shifted_timestamp = timestamp << config.timestamp_shift();
        let extended_sequence_mask = build_mask(config.sequence_bits + 1);
        let shifted_timestamp_mask = config.timestamp_mask << config.timestamp_shift();

        Self {
            inner: AtomicU64::new(shifted_timestamp),
            config,
            shifted_timestamp_mask,
            extended_sequence_mask,
        }
    }

    pub(crate) fn increment_sequence(
        &self,
        new_timestamp: u64,
    ) -> Result<TimestampSequence, SnowFlakeGeneratorError> {
        let mut prev_sequence = self.inner.load(Ordering::SeqCst);
        let new_timestamp_shifted = new_timestamp << self.config.timestamp_shift();

        loop {
            let prev_timestamp_shifted = prev_sequence & self.shifted_timestamp_mask;
            if new_timestamp_shifted <= prev_timestamp_shifted {
                break;
            }

            match self.inner.compare_exchange(
                prev_sequence,
                new_timestamp_shifted,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(updated) => prev_sequence = updated,
            }
        }

        let new_timestamp_sequence = self.inner.fetch_add(1, Ordering::SeqCst);
        let masked_sequence = new_timestamp_sequence & self.extended_sequence_mask;
        if masked_sequence > self.config.sequence_max {
            Err(SnowFlakeGeneratorError::SequenceOverflow)
        } else {
            let sequence = new_timestamp_sequence & self.extended_sequence_mask;
            let timestamp = (new_timestamp_sequence & self.shifted_timestamp_mask)
                >> self.config.timestamp_shift();
            Ok(TimestampSequence {
                sequence,
                timestamp,
            })
        }
    }
}

pub(crate) struct TimestampSequence {
    sequence: u64,
    timestamp: u64,
}

impl TimestampSequence {
    pub(crate) fn into_snowflake(self, machine_id: u64, config: &SnowFlakeConfig) -> u64 {
        let timestamp_bits = self.timestamp & config.timestamp_mask;
        let machine_id_bits = machine_id & config.machine_id_mask;
        let sequence_id_bits = self.sequence & config.sequence_mask;

        timestamp_bits << config.timestamp_shift()
            | machine_id_bits << config.sequence_bits
            | sequence_id_bits
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_sequence_increment() {
        let old_timestamp = 0x1234;
        let config = SnowFlakeConfig::default();
        let timestamp_sequence_generator = TimestampSequenceGenerator::new(old_timestamp, config);

        let timestamp_sequence = timestamp_sequence_generator
            .increment_sequence(old_timestamp)
            .unwrap();
        assert_eq!(timestamp_sequence.sequence, 0);
        assert_eq!(timestamp_sequence.timestamp, old_timestamp);

        let timestamp_sequence = timestamp_sequence_generator
            .increment_sequence(old_timestamp)
            .unwrap();
        assert_eq!(timestamp_sequence.sequence, 1);
        assert_eq!(timestamp_sequence.timestamp, old_timestamp);
    }

    #[test]
    fn test_new_timestamp() {
        let old_timestamp = 0x1234;
        let config = SnowFlakeConfig::default();
        let timestamp_sequence_generator = TimestampSequenceGenerator::new(old_timestamp, config);

        let timestamp_sequence = timestamp_sequence_generator
            .increment_sequence(old_timestamp)
            .unwrap();
        assert_eq!(timestamp_sequence.sequence, 0);
        assert_eq!(timestamp_sequence.timestamp, old_timestamp);

        let new_timestamp = 0x1235;
        let timestamp_sequence = timestamp_sequence_generator
            .increment_sequence(new_timestamp)
            .unwrap();
        assert_eq!(timestamp_sequence.sequence, 0);
        assert_eq!(timestamp_sequence.timestamp, new_timestamp);
    }

    #[test]
    fn test_into_snowflake() {
        let old_timestamp = 0x1234;
        let config = SnowFlakeConfig::default();
        let timestamp_sequence_generator = TimestampSequenceGenerator::new(old_timestamp, config);

        let timestamp_sequence = timestamp_sequence_generator
            .increment_sequence(old_timestamp)
            .unwrap();

        let snowflake = timestamp_sequence.into_snowflake(0x10, &config);
        assert_eq!(snowflake, 0x48d010000);
    }
}

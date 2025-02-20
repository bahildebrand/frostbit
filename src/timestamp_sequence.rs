use std::sync::atomic::Ordering;

use crate::sync::AtomicU64;
use crate::{
    SnowFlakeGeneratorError, MACHINE_ID_BITS, MACHINE_ID_MASK, SEQUENCE_ID_BITS, SEQUENCE_ID_MAX,
    SEQUENCE_MASK, TIMESTAMP_BITS, TIMESTAMP_MASK,
};

/// Stores both a 42-bit timestamp and 12-bit sequence in a single atomic.
///
/// This is allows us to synchronize both of these operations with a single atomic,
/// as both values are tied together. The layout is as follows:
///
/// Bits 0-12: sequence ID
/// Bits 13-19: unused
/// Bits 20-63: timestamp
///
/// Note that both the sequence and timestamp are 1 bit larger than needed. This allows
/// us to easily handle and check for overflows.
pub(crate) struct TimestampSequenceGenerator {
    inner: AtomicU64,
}

impl TimestampSequenceGenerator {
    const SEQUENCE_BITS: u64 = SEQUENCE_ID_BITS as u64 + 1;
    const TIMESTAMP_BITS: u64 = TIMESTAMP_BITS as u64 + 1;

    const TIMESTAMP_SHIFT: u64 = 20;
    const TIMESTAMP_MASK: u64 = ((1 << Self::TIMESTAMP_BITS) - 1) << Self::TIMESTAMP_SHIFT;
    const SEQUENCE_MASK: u64 = (1 << Self::SEQUENCE_BITS) - 1;

    pub(crate) fn new(timestamp: u64) -> Self {
        let shifted_timestamp = timestamp << Self::TIMESTAMP_SHIFT;

        Self {
            inner: AtomicU64::new(shifted_timestamp),
        }
    }

    pub(crate) fn increment_sequence(
        &self,
        new_timestamp: u64,
    ) -> Result<TimestampSequence, SnowFlakeGeneratorError> {
        let mut prev_sequence = self.inner.load(Ordering::SeqCst);
        let new_timestamp_shifted = new_timestamp << Self::TIMESTAMP_SHIFT;

        loop {
            let prev_timestamp_shifted = prev_sequence & Self::TIMESTAMP_MASK;
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
        let masked_sequence = new_timestamp_sequence & Self::SEQUENCE_MASK;
        if masked_sequence >= SEQUENCE_ID_MAX as u64 {
            Err(SnowFlakeGeneratorError::SequenceOverflow)
        } else {
            let sequence = new_timestamp_sequence & Self::SEQUENCE_MASK;
            let timestamp =
                (new_timestamp_sequence & Self::TIMESTAMP_MASK) >> Self::TIMESTAMP_SHIFT;
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
    pub(crate) fn into_snowflake(self, machine_id: u64) -> u64 {
        let timestamp_bits = self.timestamp & TIMESTAMP_MASK;
        let machine_id_bits = machine_id & MACHINE_ID_MASK;
        let sequence_id_bits = self.sequence & SEQUENCE_MASK;

        timestamp_bits << (MACHINE_ID_BITS + SEQUENCE_ID_BITS)
            | machine_id_bits << SEQUENCE_ID_BITS
            | sequence_id_bits
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_sequence_increment() {
        let old_timestamp = 0x1234;
        let timestamp_sequence_generator = TimestampSequenceGenerator::new(old_timestamp);

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
        let timestamp_sequence_generator = TimestampSequenceGenerator::new(old_timestamp);

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
        let timestamp_sequence_generator = TimestampSequenceGenerator::new(old_timestamp);

        let timestamp_sequence = timestamp_sequence_generator
            .increment_sequence(old_timestamp)
            .unwrap();

        let snowflake = timestamp_sequence.into_snowflake(0x10);
        assert_eq!(snowflake, 0x48d010000);
    }
}

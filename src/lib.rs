//! A library for generating unique snowflake IDs.
//!
//! Frostbit is a library that empowers you to create unique IDs for
//! distributed systems. By ensuring that IDs are unique to every machine
//! at every ms, you can create unique IDs without worrying about
//! synchronization across all of your boxes.
//!
//! By default, Frostbit generates IDs that follow the
//! [Twitter](https://en.wikipedia.org/wiki/Snowflake_ID) specification.
//! There are other variants of snowflakes, such as the one defined
//! by [Discord](https://discord.com/developers/docs/reference#snowflakes).
//! It is possible to recreate these, by defining them as such in
//! the [SnowFlakeConfig].
//!
//! ## Example Usage:
//!
//! ```rust
//! use frostbit::SnowFlakeGenerator;
//!
//! let timestamp_fn = { move ||
//!     // suggest using something like `chrono`
//!     Ok(0)
//! };
//!
//! let gen = SnowFlakeGenerator::new(0, 0, timestamp_fn).unwrap();
//! let snowflake = gen.generate().unwrap();
//! ```

mod sync;
mod timestamp_sequence;

use timestamp_sequence::TimestampSequenceGenerator;

const DEFAULT_TIMESTAMP_BITS: u64 = 41;
const DEFAULT_MACHINE_ID_BITS: u64 = 10;
const DEFAULT_SEQUENCE_ID_BITS: u64 = 12;

#[derive(Debug)]
pub enum SnowFlakeGeneratorError {
    SequenceOverflow,
    TimestampOverflow,
    TimestampError(&'static str),
    InvalidBitConfig,
}

impl From<&'static str> for SnowFlakeGeneratorError {
    fn from(error: &'static str) -> Self {
        Self::TimestampError(error)
    }
}

pub struct SnowFlakeGenerator<T>
where
    T: Fn() -> Result<u64, &'static str>,
{
    machine_id: u32,
    ts_gen: TimestampSequenceGenerator,
    epoch: u64,
    get_timestamp: T,
    config: SnowFlakeConfig,
}

impl<T: Fn() -> Result<u64, &'static str>> SnowFlakeGenerator<T> {
    pub fn new(
        machine_id: u32,
        epoch: u64,
        get_timestamp: T,
    ) -> Result<Self, SnowFlakeGeneratorError> {
        let config = SnowFlakeConfig::default();
        let timestamp_ms = Self::get_epoch_relative_timestamp(&get_timestamp, epoch, &config)?;
        let ts_gen = TimestampSequenceGenerator::new(timestamp_ms, config);
        Ok(Self {
            machine_id,
            ts_gen,
            epoch,
            get_timestamp,
            config,
        })
    }

    pub fn generate(&self) -> Result<u64, SnowFlakeGeneratorError> {
        let new_timestamp =
            Self::get_epoch_relative_timestamp(&self.get_timestamp, self.epoch, &self.config)?;
        let timestamp_sequence = self.ts_gen.increment_sequence(new_timestamp)?;

        Ok(timestamp_sequence.into_snowflake(self.machine_id as u64, &self.config))
    }

    fn get_epoch_relative_timestamp(
        get_timestamp: &T,
        epoch: u64,
        config: &SnowFlakeConfig,
    ) -> Result<u64, SnowFlakeGeneratorError> {
        let timestamp_ms = get_timestamp()? - epoch;
        if timestamp_ms < config.timestamp_max {
            Ok(timestamp_ms)
        } else {
            Err(SnowFlakeGeneratorError::TimestampOverflow)
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SnowFlakeConfig {
    machine_id_bits: u64,
    sequence_bits: u64,
    timestamp_mask: u64,
    machine_id_mask: u64,
    sequence_mask: u64,
    timestamp_max: u64,
    sequence_max: u64,
}

impl SnowFlakeConfig {
    pub fn new(
        timestamp_bits: u64,
        machine_id_bits: u64,
        sequence_bits: u64,
    ) -> Result<Self, SnowFlakeGeneratorError> {
        Self::validate_config(machine_id_bits, sequence_bits, timestamp_bits)?;

        let timestamp_mask = build_mask(timestamp_bits);
        let machine_id_mask = build_mask(machine_id_bits);
        let sequence_mask = build_mask(sequence_bits);

        let timestamp_max = calc_max(timestamp_bits);
        let sequence_max = calc_max(sequence_bits);

        Ok(Self {
            machine_id_bits,
            sequence_bits,
            timestamp_mask,
            machine_id_mask,
            sequence_mask,
            timestamp_max,
            sequence_max,
        })
    }

    pub(crate) fn timestamp_shift(&self) -> u64 {
        self.machine_id_bits + self.sequence_bits
    }

    fn validate_config(
        machine_id_bits: u64,
        sequence_bits: u64,
        timestamp_bits: u64,
    ) -> Result<(), SnowFlakeGeneratorError> {
        let bit_sum = timestamp_bits + machine_id_bits + sequence_bits;
        if bit_sum > 64 {
            return Err(SnowFlakeGeneratorError::InvalidBitConfig);
        }

        if machine_id_bits == 0 || sequence_bits == 0 || timestamp_bits == 0 {
            Err(SnowFlakeGeneratorError::InvalidBitConfig)
        } else {
            Ok(())
        }
    }
}

impl Default for SnowFlakeConfig {
    fn default() -> Self {
        Self::new(
            DEFAULT_TIMESTAMP_BITS,
            DEFAULT_MACHINE_ID_BITS,
            DEFAULT_SEQUENCE_ID_BITS,
        )
        .expect("Default values incorrect")
    }
}

pub(crate) fn build_mask(bits: u64) -> u64 {
    (1 << bits) - 1
}

pub(crate) fn calc_max(bits: u64) -> u64 {
    2u64.pow(bits as u32) - 1
}
#[cfg(test)]
mod test {
    use std::sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    };

    use rstest::rstest;

    use super::*;

    #[test]
    fn test_timestamp_generation() {
        const TIMESTAMP: u64 = 0x1234u64;
        let timestamp_fn = || Ok(TIMESTAMP);
        let machine_id = 0x10u32;
        let epoch = 0u64;

        let generator = SnowFlakeGenerator::new(machine_id, epoch, timestamp_fn).unwrap();
        let snowflake = generator.generate().unwrap();
        assert_eq!(snowflake, 0x48D010000);

        let snowflake = generator.generate().unwrap();
        assert_eq!(snowflake, 0x48D010001);
    }

    #[test]
    fn test_sequence_overflow() {
        const TIMESTAMP: u64 = 0x1234u64;
        let timestamp_fn = || Ok(TIMESTAMP);
        let machine_id = 0x10u32;
        let epoch = 0u64;
        let sequence_id_max = SnowFlakeConfig::default().sequence_max + 1;

        let generator = SnowFlakeGenerator::new(machine_id, epoch, timestamp_fn).unwrap();
        // iterate over generation until right before sequence overflow
        for _ in 0..sequence_id_max {
            generator.generate().unwrap();
        }

        let res = generator.generate();
        assert!(matches!(
            res,
            Err(SnowFlakeGeneratorError::SequenceOverflow)
        ));
    }

    #[test]
    fn test_timestamp_overflow() {
        let timestamp: u64 = SnowFlakeConfig::default().timestamp_max + 1;
        let call_count = Arc::new(AtomicU64::new(0));
        let timestamp_fn = || {
            let count = call_count.fetch_add(1, Ordering::SeqCst);
            if count < 1 {
                Ok(0)
            } else {
                Ok(timestamp)
            }
        };
        let machine_id = 0x10u32;
        let epoch = 0u64;

        let generator = SnowFlakeGenerator::new(machine_id, epoch, timestamp_fn).unwrap();
        let result = generator.generate();
        assert!(matches!(
            result,
            Err(SnowFlakeGeneratorError::TimestampOverflow)
        ));
    }

    #[test]
    fn test_timestamp_failure() {
        let timestamp_fn = || Err("Timestamp error");
        let machine_id = 0x10u32;
        let epoch = 0u64;

        let generator = SnowFlakeGenerator::new(machine_id, epoch, timestamp_fn);
        assert!(matches!(
            generator,
            Err(SnowFlakeGeneratorError::TimestampError("Timestamp error"))
        ));
    }

    #[test]
    fn test_invalid_config_too_many_bits() {
        let config = SnowFlakeConfig::new(41, 10, 24);
        assert!(matches!(
            config,
            Err(SnowFlakeGeneratorError::InvalidBitConfig)
        ));
    }

    #[rstest]
    #[case(0, 10, 24)]
    #[case(41, 0, 24)]
    #[case(41, 10, 0)]
    fn test_invalid_config_zero_machine_id(
        #[case] timestamp_bits: u64,
        #[case] machine_id_bits: u64,
        #[case] sequence_bits: u64,
    ) {
        let config = SnowFlakeConfig::new(timestamp_bits, machine_id_bits, sequence_bits);
        assert!(matches!(
            config,
            Err(SnowFlakeGeneratorError::InvalidBitConfig)
        ));
    }
}

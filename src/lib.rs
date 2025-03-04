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
//! use frostbit::SnowflakeGenerator;
//!
//! let timestamp_fn = { move ||
//!     // suggest using something like `chrono`
//!     Ok(0)
//! };
//!
//! let gen = SnowflakeGenerator::new(0, 0, timestamp_fn).unwrap();
//! let snowflake = gen.generate().unwrap();
//! ```

mod sync;
mod timestamp_sequence;

use timestamp_sequence::TimestampSequenceGenerator;

const DEFAULT_TIMESTAMP_BITS: u64 = 41;
const DEFAULT_MACHINE_ID_BITS: u64 = 10;
const DEFAULT_SEQUENCE_ID_BITS: u64 = 12;

/// Errors that can occur when generating snowflakes.
///
/// The SnowFlakeGeneratorError enum defines the errors that can occur when
/// generating snowflakes. These errors are generated in the following cases
///
/// - [SnowflakeGeneratorError::SequenceOverflow] - When the sequence ID overflows
///   in a given millisecond.
/// - [SnowflakeGeneratorError::TimestampOverflow] - When the timestamp overflows
///   the number of bits allocated for it.
/// - [SnowflakeGeneratorError::TimestampError] - When the timestamp generation
///   function returns an error.
/// - [SnowflakeGeneratorError::InvalidBitConfig] - When the configuration for
///   the snowflake generator is invalid.
#[derive(Debug)]
pub enum SnowflakeGeneratorError {
    SequenceOverflow,
    TimestampOverflow,
    TimestampError(&'static str),
    InvalidBitConfig,
}

impl From<&'static str> for SnowflakeGeneratorError {
    fn from(error: &'static str) -> Self {
        Self::TimestampError(error)
    }
}

/// A generator for creating unique snowflake IDs.
///
/// The SnowFlakeGenerator is the main struct for creating snowflakes. It
/// is responsible for generating unique IDs based on the current time and
/// the machine ID.
pub struct SnowflakeGenerator<T>
where
    T: Fn() -> Result<u64, &'static str>,
{
    machine_id: u32,
    ts_gen: TimestampSequenceGenerator,
    epoch: u64,
    get_timestamp: T,
    config: SnowflakeConfig,
}

impl<T: Fn() -> Result<u64, &'static str>> SnowflakeGenerator<T> {
    /// Create a new SnowFlakeGenerator with default configuration.
    ///
    /// This funcion creates a new SnowFlakeGenerator that creates snowflakes
    /// that use the Twitter specification.
    pub fn new(
        machine_id: u32,
        epoch: u64,
        get_timestamp: T,
    ) -> Result<Self, SnowflakeGeneratorError> {
        let config = SnowflakeConfig::default();
        Self::new_with_config(machine_id, epoch, get_timestamp, config)
    }

    /// Create a new SnowflakeGenerator with a custom configuration.
    ///
    /// Similar to [SnowflakeGenerator::new], but allows for a custom configuration to be used.
    pub fn new_with_config(
        machine_id: u32,
        epoch: u64,
        get_timestamp: T,
        config: SnowflakeConfig,
    ) -> Result<Self, SnowflakeGeneratorError> {
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

    /// Generate a new snowflake.
    ///
    /// This function generates a new snowflake ID. If the sequence overflows,
    /// it will return [SnowFlakeGeneratorError::SequenceOverflow].
    pub fn generate(&self) -> Result<u64, SnowflakeGeneratorError> {
        let new_timestamp =
            Self::get_epoch_relative_timestamp(&self.get_timestamp, self.epoch, &self.config)?;
        let timestamp_sequence = self.ts_gen.increment_sequence(new_timestamp)?;

        Ok(timestamp_sequence.into_snowflake(self.machine_id as u64, &self.config))
    }

    fn get_epoch_relative_timestamp(
        get_timestamp: &T,
        epoch: u64,
        config: &SnowflakeConfig,
    ) -> Result<u64, SnowflakeGeneratorError> {
        let timestamp_ms = get_timestamp()? - epoch;
        if timestamp_ms < config.timestamp_max {
            Ok(timestamp_ms)
        } else {
            Err(SnowflakeGeneratorError::TimestampOverflow)
        }
    }
}

/// Configuration for a snowflake generator.
///
/// The SnowFlakeConfig struct is used to define the configuration for a snowflake generator.
/// It defines the number of bits used for the timestamp, machine ID, and sequence ID.
#[derive(Debug, Clone, Copy)]
pub struct SnowflakeConfig {
    machine_id_bits: u64,
    sequence_bits: u64,
    timestamp_mask: u64,
    machine_id_mask: u64,
    sequence_mask: u64,
    timestamp_max: u64,
    sequence_max: u64,
}

impl SnowflakeConfig {
    /// Create a new [SnowFlakeConfig] with the given number of bits for each field.
    pub fn new(
        timestamp_bits: u64,
        machine_id_bits: u64,
        sequence_bits: u64,
    ) -> Result<Self, SnowflakeGeneratorError> {
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
    ) -> Result<(), SnowflakeGeneratorError> {
        let bit_sum = timestamp_bits + machine_id_bits + sequence_bits;
        if bit_sum > 64 {
            return Err(SnowflakeGeneratorError::InvalidBitConfig);
        }

        if machine_id_bits == 0 || sequence_bits == 0 || timestamp_bits == 0 {
            Err(SnowflakeGeneratorError::InvalidBitConfig)
        } else {
            Ok(())
        }
    }
}

impl Default for SnowflakeConfig {
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

        let generator = SnowflakeGenerator::new(machine_id, epoch, timestamp_fn).unwrap();
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
        let sequence_id_max = SnowflakeConfig::default().sequence_max + 1;

        let generator = SnowflakeGenerator::new(machine_id, epoch, timestamp_fn).unwrap();
        // iterate over generation until right before sequence overflow
        for _ in 0..sequence_id_max {
            generator.generate().unwrap();
        }

        let res = generator.generate();
        assert!(matches!(
            res,
            Err(SnowflakeGeneratorError::SequenceOverflow)
        ));
    }

    #[test]
    fn test_timestamp_overflow() {
        let timestamp: u64 = SnowflakeConfig::default().timestamp_max + 1;
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

        let generator = SnowflakeGenerator::new(machine_id, epoch, timestamp_fn).unwrap();
        let result = generator.generate();
        assert!(matches!(
            result,
            Err(SnowflakeGeneratorError::TimestampOverflow)
        ));
    }

    #[test]
    fn test_timestamp_failure() {
        let timestamp_fn = || Err("Timestamp error");
        let machine_id = 0x10u32;
        let epoch = 0u64;

        let generator = SnowflakeGenerator::new(machine_id, epoch, timestamp_fn);
        assert!(matches!(
            generator,
            Err(SnowflakeGeneratorError::TimestampError("Timestamp error"))
        ));
    }

    #[test]
    fn test_invalid_config_too_many_bits() {
        let config = SnowflakeConfig::new(41, 10, 24);
        assert!(matches!(
            config,
            Err(SnowflakeGeneratorError::InvalidBitConfig)
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
        let config = SnowflakeConfig::new(timestamp_bits, machine_id_bits, sequence_bits);
        assert!(matches!(
            config,
            Err(SnowflakeGeneratorError::InvalidBitConfig)
        ));
    }
}

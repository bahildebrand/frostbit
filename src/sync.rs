#[cfg(loom)]
pub(crate) use loom::sync::atomic::AtomicU64;

#[cfg(not(loom))]
pub(crate) use std::sync::atomic::AtomicU64;

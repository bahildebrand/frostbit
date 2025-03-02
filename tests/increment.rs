use std::sync::{atomic::AtomicU64, Arc};

use frostbit::SnowflakeGenerator;
use loom::model::Builder;
use loom::thread;

const TIMESTAMP_MASK: u64 = 0xFF00000;

#[test]
fn increment() {
    let mut builder = Builder::new();
    builder.preemption_bound = Some(3);
    builder.check(|| {
        let call_counter = Arc::new(AtomicU64::new(0));
        let call_fn = move || {
            let call_count = call_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let timestamp = if call_count < 3 { 0 } else { 1 };

            Ok(timestamp)
        };

        let generator = Arc::new(SnowflakeGenerator::new(0, 0, call_fn).unwrap());
        let mut handles = Vec::with_capacity(4);
        for _ in 0..4 {
            let generator = generator.clone();
            let handle = thread::spawn(move || generator.generate().unwrap());
            handles.push(handle);
        }
        let mut ids = handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect::<Vec<_>>();
        ids.sort();

        let timestamp = ids.last().unwrap() & TIMESTAMP_MASK;

        assert_eq!(timestamp, 0x400000);
    });
}

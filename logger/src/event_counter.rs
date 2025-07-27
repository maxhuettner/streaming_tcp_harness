use chrono::Utc;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Debug)]
pub struct EventCounter {
    current: AtomicUsize,
    total: AtomicUsize,

    first_elem_ts: Arc<AtomicI64>,
    last_elem_ts: Arc<AtomicI64>,
}

impl EventCounter {
    pub fn new() -> Self {
        Self {
            current: AtomicUsize::new(0),
            total: AtomicUsize::new(0),
            first_elem_ts: Arc::new(AtomicI64::new(0)),
            last_elem_ts: Arc::new(AtomicI64::new(0)),
        }
    }

    pub fn increment(&self) {
        let current_ts = Utc::now().timestamp_micros();

        self.last_elem_ts.store(current_ts, Ordering::Relaxed);

        if self.total.load(Ordering::Relaxed) == 0 {
            self.first_elem_ts.store(current_ts, Ordering::Relaxed);
        }

        self.current.fetch_add(1, Ordering::Relaxed);
        self.total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn reset_current(&self) -> usize {
        self.current.swap(0, Ordering::Relaxed)
    }

    pub fn get_total(&self) -> usize {
        self.total.load(Ordering::Relaxed)
    }

    pub fn get_counts(&self) -> (usize, usize) {
        let current = self.current.load(Ordering::Relaxed);
        let total = self.total.load(Ordering::Relaxed);
        (current, total)
    }

    pub fn get_first_elem_ts(&self) -> i64 {
        self.first_elem_ts.load(Ordering::Relaxed)
    }

    pub fn get_last_elem_ts(&self) -> i64 {
        self.last_elem_ts.load(Ordering::Relaxed)
    }
}

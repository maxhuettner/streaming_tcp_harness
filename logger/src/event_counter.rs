use chrono::Utc;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Debug)]
pub struct EventCounter {
    current: AtomicUsize,
    total: AtomicUsize,

    start_ts: Arc<AtomicI64>,
    end_ts: Arc<AtomicI64>,
    first_elem_ts: Arc<AtomicI64>,
    last_elem_ts: Arc<AtomicI64>,
}

impl EventCounter {
    pub fn new() -> Self {
        Self {
            current: AtomicUsize::new(0),
            total: AtomicUsize::new(0),
            start_ts: Arc::new(AtomicI64::new(0)),
            end_ts: Arc::new(AtomicI64::new(0)),
            first_elem_ts: Arc::new(AtomicI64::new(0)),
            last_elem_ts: Arc::new(AtomicI64::new(0)),
        }
    }

    pub fn set_start_ts(&self) {
        let current_ts = Utc::now().timestamp_micros();
        self.start_ts.store(current_ts, Ordering::Relaxed);
    }

    pub fn get_start_ts(&self) -> i64 {
        self.start_ts.load(Ordering::Relaxed)
    }

    pub fn set_end_ts(&self) {
        let current_ts = Utc::now().timestamp_micros();
        self.end_ts.store(current_ts, Ordering::Relaxed);
    }

    pub fn get_end_ts(&self) -> i64 {
        self.end_ts.load(Ordering::Relaxed)
    }

    pub fn increment(&self, num_events: usize) {
        let current_ts = Utc::now().timestamp_micros();

        self.last_elem_ts.store(current_ts, Ordering::Relaxed);

        if self.total.load(Ordering::Relaxed) == 0 {
            self.first_elem_ts.store(current_ts, Ordering::Relaxed);
        }

        self.current.fetch_add(num_events, Ordering::Relaxed);
        self.total.fetch_add(num_events, Ordering::Relaxed);
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

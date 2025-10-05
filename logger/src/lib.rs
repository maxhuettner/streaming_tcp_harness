mod benchmark_row;
mod event_counter;
mod time_row;

use crate::benchmark_row::BenchmarkRow;
use crate::event_counter::EventCounter;
use crate::time_row::TimeRow;
use chrono::{DateTime, Utc};
use csv::Writer;
use std::fs;
use std::fs::File;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Notify};
use tokio::time::{interval, Interval};

#[derive(Debug, Clone)]
pub struct BenchmarkLogger {
    log_interval: Arc<Mutex<Interval>>,
    num_events: Arc<EventCounter>,
    is_stop: Arc<AtomicBool>,
    log_thread: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    requested_rate: Option<usize>,
    event_log_writer: Arc<Mutex<Writer<File>>>,
    time_log_writer: Arc<Mutex<Writer<File>>>,
    start_signal: Arc<Notify>,
}

pub struct BenchmarkLoggerBuilder {
    folder_prefix: String,
    file_suffix: String,
    requested_rate: Option<usize>,
    log_interval: Duration,
}

const DEFAULT_LOG_INTERVAL: Duration = Duration::from_secs(1);

impl BenchmarkLoggerBuilder {
    pub fn new(folder_prefix: impl Into<String>, file_suffix: impl Into<String>) -> Self {
        Self {
            folder_prefix: folder_prefix.into(),
            file_suffix: file_suffix.into(),
            requested_rate: None,
            log_interval: DEFAULT_LOG_INTERVAL,
        }
    }

    pub fn requested_rate(mut self, rate: Option<usize>) -> Self {
        self.requested_rate = rate;
        self
    }

    pub fn log_interval(mut self, interval: Duration) -> Self {
        self.log_interval = interval;
        self
    }

    pub fn build(self) -> BenchmarkLogger {
        let folder_path = format!("logs/{}", self.folder_prefix);
        fs::create_dir_all(folder_path.clone()).unwrap();
        let event_log_writer =
            Writer::from_path(format!("{folder_path}/events_{}.csv", self.file_suffix)).unwrap();
        let time_log_writer =
            Writer::from_path(format!("{folder_path}/time_{}.csv", self.file_suffix)).unwrap();

        BenchmarkLogger {
            log_interval: Arc::new(Mutex::new(interval(self.log_interval))),
            is_stop: Arc::new(AtomicBool::new(false)),
            num_events: Arc::new(EventCounter::new()),
            log_thread: Arc::new(Mutex::new(None)),
            requested_rate: self.requested_rate,
            event_log_writer: Arc::new(Mutex::new(event_log_writer)),
            time_log_writer: Arc::new(Mutex::new(time_log_writer)),
            start_signal: Arc::new(Notify::new()),
        }
    }
}

impl BenchmarkLogger {
    pub async fn start(&mut self) {
        if self.log_thread.lock().await.is_some() {
            return;
        }

        self.num_events.set_start_ts();

        let requested_rate = self.requested_rate;
        let log_writer = self.event_log_writer.clone();
        let num_events = self.num_events.clone();
        let is_stop = self.is_stop.clone();
        let log_interval = self.log_interval.clone();
        let start_signal = self.start_signal.clone();

        *self.log_thread.lock().await = Some(tokio::spawn(async move {
            // Create a logger instance and get its interval
            start_signal.notified().await;
            loop {
                if is_stop.load(Ordering::Relaxed) {
                    break;
                }
                log_interval.lock().await.tick().await;

                let actual_rate = num_events.reset_current();
                println!("Number of tuples: {actual_rate}");

                log_writer
                    .lock()
                    .await
                    .serialize(BenchmarkRow {
                        time: Utc::now().to_rfc3339(),
                        rate: actual_rate,
                        requested_rate,
                    })
                    .unwrap();
            }
            log_writer.lock().await.flush().unwrap();
        }))
    }

    pub fn get_total_events(&self) -> usize {
        self.num_events.get_total()
    }

    pub async fn stop(&mut self) {
        self.num_events.set_end_ts();

        if let Some(log_thread) = self.log_thread.lock().await.take() {
            self.is_stop.store(true, Ordering::Relaxed);
            log_thread.await.unwrap();
        }

        self.write_times().await;
    }

    pub fn log_event(&self) {
        self.log_events(1);
    }

    pub fn log_events(&self, num_events: usize) {
        self.num_events.increment(num_events);
        self.start_signal.notify_one();
    }

    async fn write_times(&mut self) {
        let mut writer = self.time_log_writer.lock().await;

        let start_ts = self.num_events.get_start_ts();
        let end_ts = self.num_events.get_end_ts();
        let first_elem_ts = self.num_events.get_first_elem_ts();
        let last_elem_ts = self.num_events.get_last_elem_ts();
        let duration_us = last_elem_ts - first_elem_ts;
        let num_events = self.num_events.get_total();
        let tps = if num_events == 0 || duration_us <= 0 {
            0
        } else {
            ((num_events as f64) / (duration_us as f64) * 1_000_000.0) as usize
        };

        writer
            .serialize(TimeRow {
                start_time: DateTime::from_timestamp_micros(start_ts)
                    .unwrap()
                    .to_rfc3339(),
                end_time: DateTime::from_timestamp_micros(end_ts)
                    .unwrap()
                    .to_rfc3339(),
                first_elem_time: DateTime::from_timestamp_micros(first_elem_ts)
                    .unwrap()
                    .to_rfc3339(),
                last_elem_time: DateTime::from_timestamp_micros(last_elem_ts)
                    .unwrap()
                    .to_rfc3339(),
                duration_us,
                num_events,
                tps,
            })
            .unwrap()
    }
}

impl Drop for BenchmarkLogger {
    fn drop(&mut self) {
        self.is_stop.store(true, Ordering::Relaxed);
    }
}

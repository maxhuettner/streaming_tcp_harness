mod window_event;

use std::{fs, thread};
use std::fs::File;
use std::io::BufWriter;
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use chrono::{Utc};
use clap::{Parser};
use crossbeam_queue::SegQueue;
use csv::Writer;
use humantime::format_duration;
use parquet::file::reader::SerializedFileReader;
use parquet::record::Row;
use serde::{Serialize};
use std::io::prelude::*;
use tokio::time::interval;
use crate::window_event::WindowEvent;

#[derive(Parser, Debug)]
#[command()]
struct Cli {
    address: String,

    file_path: String,

    #[arg(long, short)]
    exp_name: Option<String>,

    /// Sets the event rate in elements per second
    #[arg(long, short)]
    rate: Option<usize>,

    #[arg(long, short, default_value_t = 1000)]
    window_size: usize,
}

#[derive(Serialize, Debug)]
struct BenchmarkRow {
    time: String,

    rate: usize,

    requested_rate: Option<usize>,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    fs::create_dir_all("logs").unwrap();
    let log_writer = Writer::from_path(format!(
        "logs/benchmark{}.csv",
        cli.exp_name.as_ref().map(|name| format!("_{}", name)).unwrap_or_else(|| "".to_string()),
    )).unwrap();
    let event_log_writer = Writer::from_path(format!(
        "logs/events{}.csv",
        cli.exp_name.as_ref().map(|name| format!("_{}", name)).unwrap_or_else(|| "".to_string()),
    )).unwrap();

    start_benchmark(cli.address, cli.file_path, cli.rate, cli.window_size, log_writer, event_log_writer).await;
}

async fn start_benchmark(
    address: String,
    file_path: String,
    requested_rate: Option<usize>,
    window_size: usize,
    log_writer: Writer<File>,
    mut window_events_writer: Writer<File>,
) {
    let file = File::open(file_path).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();

    let queue = init_queue_from_reader(reader);

    let window_events = init_window_events(queue.len(), window_size);

    let listener = TcpListener::bind(&address).unwrap();

    let connection_threads = Arc::new(Mutex::new(Vec::new()));
    let num_curr_sec_tuples_sent = Arc::new(AtomicUsize::new(0));

    let server_thread = create_server_thread(listener, queue.clone(), window_events.clone(), num_curr_sec_tuples_sent.clone(), connection_threads.clone(), window_size);

    let log_interval = interval(Duration::from_secs(1));
    let log_thread = create_log_thread(log_interval, queue.clone(), num_curr_sec_tuples_sent.clone(), log_writer, requested_rate);
    log_thread.await.unwrap();

    join_connection_threads(connection_threads);

    let _ = TcpStream::connect(address);
    server_thread.join().unwrap();

    for ts_event in window_events.iter() {
        window_events_writer.serialize(ts_event).unwrap();
    }
    window_events_writer.flush().unwrap();
}

fn join_connection_threads(connection_threads: Arc<Mutex<Vec<thread::JoinHandle<()>>>>) {
    let mut threads = connection_threads.lock().unwrap();
    while let Some(thread) = threads.pop() {
        thread.join().unwrap();
    }
}

fn init_queue_from_reader(reader: SerializedFileReader<File>) -> Arc<SegQueue<Row>> {
    let rows = reader.into_iter();

    let begin_reading = Instant::now();

    let queue = SegQueue::new();
    for row in rows {
        queue.push(row.unwrap());
    }

    let end_reading = Instant::now();

    println!(
        "Reading done (took: {})",
        format_duration(Duration::from_millis(end_reading.duration_since(begin_reading).as_millis() as u64))
    );

    Arc::new(queue)
}

fn init_window_events(queue_len: usize, window_size: usize) -> Arc<Vec<WindowEvent>> {
    let window_events_num = (queue_len + window_size - 1) / window_size;

    let window_events: Vec<WindowEvent> = (0..window_events_num).map(|_| WindowEvent::default()).collect();

    Arc::new(window_events)
}

fn create_server_thread(
    listener: TcpListener,
    queue: Arc<SegQueue<Row>>,
    window_events: Arc<Vec<WindowEvent>>,
    num_curr_sec_tuples_sent: Arc<AtomicUsize>,
    threads: Arc<Mutex<Vec<thread::JoinHandle<()>>>>,
    window_size: usize,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let mut conn_id = 0;
        let num_total_tuples_sent = Arc::new(AtomicUsize::new(0));
        for stream in listener.incoming() {
            if queue.is_empty() {
                break;
            }

            let stream = stream.unwrap();
            let writer = BufWriter::new(stream);
            let queue = queue.clone();
            let num_curr_sec_tuples_sent = num_curr_sec_tuples_sent.clone();
            let num_total_tuples_sent = num_total_tuples_sent.clone();
            let window_events = window_events.clone();

            conn_id += 1;
            println!("New connection | {}", conn_id);

            threads.lock().unwrap().push(thread::spawn(move || {
                handle_connection(writer, queue, window_events, num_curr_sec_tuples_sent, num_total_tuples_sent, window_size);
            }));
        }
    })
}

fn create_log_thread(
    mut log_interval: tokio::time::Interval,
    queue: Arc<SegQueue<Row>>,
    num_curr_sec_tuples_sent: Arc<AtomicUsize>,
    mut log_writer: Writer<File>,
    requested_rate: Option<usize>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut already_started = false;
        loop {
            if queue.is_empty() {
                break;
            }
            log_interval.tick().await;

            let actual_rate = num_curr_sec_tuples_sent.swap(0, Ordering::AcqRel);
            println!("Number of tuples sent: {}", actual_rate);

            if !already_started {
                if actual_rate == 0 {
                    continue;
                }
                already_started = true;
            }

            log_writer.serialize(
                BenchmarkRow {
                    time: Utc::now().to_rfc3339(),
                    rate: actual_rate,
                    requested_rate,
                }
            ).unwrap();
            log_writer.flush().unwrap();
        }
    })
}

fn handle_connection(
    mut writer: BufWriter<TcpStream>,
    queue: Arc<SegQueue<Row>>,
    window_events: Arc<Vec<WindowEvent>>,
    num_curr_sec_tuples_sent: Arc<AtomicUsize>,
    num_total_tuples_sent: Arc<AtomicUsize>,
    window_size: usize,
) {
    while let Some(row) = queue.pop() {
        let row_str = format!("{}\n", row.to_json_value().to_string());

        match writer.write_all(row_str.as_bytes()) {
            Ok(_) => (),
            Err(_) => break,
        }
        num_curr_sec_tuples_sent.fetch_add(1, Ordering::AcqRel);
        let current_tuple_num = num_total_tuples_sent.fetch_add(1, Ordering::AcqRel);

        let current_time = Utc::now();
        let index = current_tuple_num / window_size;
        window_events.get(index).unwrap().compare_and_set_first_elem_time(current_time);
        window_events.get(index).unwrap().compare_and_set_last_elem_time(current_time);
    }
}
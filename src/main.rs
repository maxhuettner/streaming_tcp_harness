use std::{fs, thread};
use std::fs::File;
use std::io::BufWriter;
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex, mpsc};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use chrono::{Utc};
use clap::{Parser};
use crossbeam_queue::SegQueue;
use csv::Writer;
use humantime::format_duration;
use parquet::file::reader::SerializedFileReader;
use parquet::record::Row;
use serde::Serialize;
use regex::Regex;
use std::io::prelude::*;
use tokio::time::interval;

#[derive(Parser, Debug)]
#[command()]
struct Cli {
    address: String,

    file_path: String,

    #[arg(long, short)]
    exp_name: Option<String>,

    /// Sets the event rate in elements per second
    #[arg(long, short)]
    rate: Option<u64>,
}

#[derive(Serialize)]
struct BenchmarkRow {
    time: String,

    rate: u64,

    requested_rate: Option<u64>,
}

#[derive(Serialize)]
struct EventLogEvent {
    time: String,
    conn_id: i32,
    // row: String,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    fs::create_dir_all("logs").unwrap();
    let log_writer = Writer::from_path(format!(
        "logs/benchmark{}_{}.csv",
        cli.exp_name.as_ref().map(|name| format!("_{}", name)).unwrap_or_else(|| "".to_string()),
        &Regex::new("^/(?:.+/)*(?<filename>.+)\\..+$").unwrap().captures(&cli.file_path).unwrap()["filename"]
    )).unwrap();
    let event_log_writer = Writer::from_path(format!(
        "logs/events{}_{}.csv",
        cli.exp_name.as_ref().map(|name| format!("_{}", name)).unwrap_or_else(|| "".to_string()),
        &Regex::new("^/(?:.+/)*(?<filename>.+)\\..+$").unwrap().captures(&cli.file_path).unwrap()["filename"]
    )).unwrap();

    start_benchmark(cli.address, cli.file_path, cli.rate, log_writer, event_log_writer).await;
}

async fn start_benchmark(
    address: String,
    file_path: String,
    requested_rate: Option<u64>,
    mut log_writer: Writer<File>,
    mut event_log_writer: Writer<File>,
) {
    let file = File::open(file_path).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let rows = reader.into_iter();

    let begin_reading = Instant::now();
    let queue = Arc::new(SegQueue::new());
    for row in rows {
        queue.push(row.unwrap());
    }
    let end_reading = Instant::now();
    println!(
        "Reading done (took: {})",
        format_duration(Duration::from_millis(end_reading.duration_since(begin_reading).as_millis() as u64))
    );

    let listener = TcpListener::bind(&address).unwrap();

    let (event_tx, event_rx) = mpsc::channel();
    let event_log_thread = thread::spawn(move || {
        for event in event_rx.iter() {
            event_log_writer.serialize(event).unwrap();
            event_log_writer.flush().unwrap();
        }
    });

    let threads = Arc::new(Mutex::new(Vec::new()));
    let number_of_tuples_sent = Arc::new(AtomicU64::new(0));

    let server_queue = queue.clone();
    let number_of_tuples_sent_server = number_of_tuples_sent.clone();
    let threads_server = threads.clone();

    let server_thread = thread::spawn(move || {
        let mut conn_id = 0;
        for stream in listener.incoming() {
            if server_queue.is_empty() {
                break;
            }
            let stream = stream.unwrap();
            let writer = BufWriter::new(stream);
            let queue = server_queue.clone();
            let event_tx = event_tx.clone();
            let number_of_tuples_sent = number_of_tuples_sent_server.clone();
            conn_id += 1;
            let curr_conn_id = conn_id;
            threads_server.lock().unwrap().push(thread::spawn(move || {
                handle_connection(writer, curr_conn_id, queue, event_tx, number_of_tuples_sent);
            }));
        }
    });

    let mut log_interval = interval(Duration::from_secs(1));

    let number_of_tuples_sent_log = number_of_tuples_sent.clone();
    let log_queue = queue.clone();
    let log_thread = tokio::spawn(async move {
        loop {
            if log_queue.is_empty() {
                break;
            }
            log_interval.tick().await;
            let actual_rate = number_of_tuples_sent_log.swap(0, Ordering::AcqRel);
            println!("Number of tuples sent: {}", actual_rate);
            log_writer.serialize(
                BenchmarkRow {
                    time: Utc::now().to_rfc3339(),
                    rate: actual_rate,
                    requested_rate,
                }
            ).unwrap();
            log_writer.flush().unwrap();
        }
    });
    log_thread.await.unwrap();

    {
        let mut threads = threads.lock().unwrap();
        while let Some(thread) = threads.pop() {
            thread.join().unwrap();
        }
    }

    let _ = TcpStream::connect(address);
    server_thread.join().unwrap();
    event_log_thread.join().unwrap();
}

fn handle_connection(
    mut writer: BufWriter<TcpStream>,
    conn_id: i32,
    queue: Arc<SegQueue<Row>>,
    event_tx: mpsc::Sender<EventLogEvent>,
    number_of_tuples_sent: Arc<AtomicU64>,
) {
    while let Some(row) = queue.pop() {
        let row_str = format!("{}\n", row.to_json_value().to_string());
        // event_tx.send(EventLogEvent {
        //     time: Utc::now().to_rfc3339(),
        //     conn_id,
        //     // row: row_str.clone(),
        // }).unwrap();
        match writer.write_all(row_str.as_bytes()) {
            Ok(_) => (),
            Err(_) => break,
        }
        number_of_tuples_sent.fetch_add(1, Ordering::AcqRel);
    }
}

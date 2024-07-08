use std::fs;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::Row;
use std::fs::File;
use std::sync::{Arc};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration};
use chrono::{Utc};
use clap::{Parser};
use crossbeam_channel::{bounded, Receiver};
use csv::Writer;
use serde::Serialize;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::time::{interval, Interval};

#[derive(Serialize)]
struct BenchmarkRow {
    time: String,

    rate: u64,

    #[serde(default = "inf")]
    requested_rate: Option<u64>,
}

#[derive(Parser, Debug)]
#[command()]
struct Cli {
    address: String,

    file_path: String,

    /// Sets the event rate in elements per second
    #[arg(long, short)]
    rate: Option<u64>,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    fs::create_dir_all("logs").unwrap();
    let writer = Writer::from_path(format!("logs/benchmark_{}.log", Utc::now().to_rfc3339())).unwrap();

    start_benchmark(cli.address, cli.file_path, cli.rate, writer).await;
}

async fn start_benchmark(address: String, file_path: String, requested_rate: Option<u64>, mut writer: Writer<File>) {
    let file = File::open(file_path).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let mut iter = reader.get_row_iter(None).unwrap().peekable();

    let listener = TcpListener::bind(address).await.unwrap();

    let done = Arc::new(AtomicBool::new(false));
    let (tx, rx) = bounded::<Row>(0);

    let mut producer_interval: Option<Interval> = match requested_rate {
        Some(val) => Some(interval(Duration::from_nanos(1_000_000_000 / val).into())),
        None => None,
    };

    let threads = Arc::new(Mutex::new(Vec::new()));

    let number_of_tuples_sent = Arc::new(AtomicU64::new(0));

    let rx_server = rx.clone();
    let done_server = done.clone();
    let number_of_tuples_sent_server = number_of_tuples_sent.clone();
    let threads_server = threads.clone();
    let server_thread = tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let rx = rx_server.clone();
            let done = done_server.clone();
            let number_of_tuples_sent = number_of_tuples_sent_server.clone();
            threads_server.lock().await.push(tokio::spawn(async move {
                handle_connection(stream, rx, done, number_of_tuples_sent).await;
            }));
        }
    });

    let mut log_interval = interval(Duration::from_secs(1));

    let done_log = done.clone();
    let number_of_tuples_sent_log = number_of_tuples_sent.clone();
    let log_thread = tokio::spawn(async move {
        loop {
            if done_log.load(Ordering::Relaxed) {
                break;
            }
            log_interval.tick().await;
            let actual_rate = number_of_tuples_sent_log.swap(0, Ordering::AcqRel);

            writer.serialize(
                BenchmarkRow {
                    time: Utc::now().to_rfc3339(),
                    rate: actual_rate,
                    requested_rate,
                }
            ).unwrap();
            writer.flush().unwrap();
        }
    });

    loop {
        if iter.peek().is_none() {
            done.store(true, Ordering::Relaxed);
            break;
        }
        if let Some(ref mut interval) = producer_interval {
            interval.tick().await;
        }
        tx.send(iter.next().unwrap().unwrap()).unwrap();
    }

    server_thread.abort();
    log_thread.await.unwrap();
    for thread in threads.lock().await.iter_mut() {
        thread.await.unwrap();
    }
}

async fn handle_connection(mut stream: TcpStream, rx: Receiver<Row>, done: Arc<AtomicBool>, number_of_tuples_sent: Arc<AtomicU64>) {
    loop {
        if rx.is_empty() && done.load(Ordering::Relaxed) {
            break;
        }
        let row = match rx.recv() {
            Ok(row) => row,
            Err(_) => break,
        };
        let row_str = format!("{}\n", row.to_json_value().to_string());
        match stream.write_all(row_str.as_bytes()).await {
            Ok(_) => (),
            Err(_) => break,
        };
        number_of_tuples_sent.fetch_add(1, Ordering::Relaxed);
    }
}


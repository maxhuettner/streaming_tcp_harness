mod shared_vec_iter;

use crate::shared_vec_iter::{SharedVec, SharedVecIterator};
use clap::Parser;
use humantime::format_duration;
use logger::{BenchmarkLogger, BenchmarkLoggerBuilder};
use parquet::file::reader::SerializedFileReader;
use parquet::record::Row;
use std::fs::File;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::{io, signal, stream, task};

const LOG_FOLDER_PREFIX: &str = "source";

#[derive(Parser, Debug)]
#[command()]
struct Cli {
    file_path: String,

    #[arg(long, short, default_value = "0.0.0.0:9000")]
    address: String,

    #[arg(long, short, default_value = "test")]
    exp_name: String,

    /// Sets the event rate in elements per second
    #[arg(long, short)]
    rate: Option<usize>,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    start_benchmark(cli.address, cli.file_path, cli.exp_name).await;
}

async fn start_benchmark(address: String, file_path: String, exp_name: String) {
    let rows = task::spawn_blocking(move || {
        let file = File::open(file_path).unwrap();
        let reader = SerializedFileReader::new(file).unwrap();
        init_queue_from_reader(reader)
    })
    .await
    .unwrap();

    let listener = TcpListener::bind(&address).await.unwrap();

    let connection_threads = Arc::new(Mutex::new(Vec::new()));
    let done = Arc::new(AtomicBool::new(false));

    let server_thread = create_server_thread(
        listener,
        rows,
        connection_threads.clone(),
        done.clone(),
        exp_name.clone(),
    );

    println!("Waiting for Ctrl+C...");

    let mut lines = BufReader::new(io::stdin()).lines();
    while let Some(line) = lines.next_line().await.unwrap() {
        if line.trim() == "q" {
            println!("Got 'q', shutting down…");
            break;
        }
    }

    done.store(true, Ordering::Relaxed);
    join_connection_threads(connection_threads).await;

    println!("Stopping server...");

    TcpStream::connect(address).await.unwrap();
    server_thread.await.unwrap();
}

async fn join_connection_threads(connection_threads: Arc<Mutex<Vec<task::JoinHandle<()>>>>) {
    let mut threads = connection_threads.lock().await;
    while let Some(thread) = threads.pop() {
        thread.await.unwrap();
    }
}

fn init_queue_from_reader(reader: SerializedFileReader<File>) -> SharedVec<Vec<u8>> {
    let begin_reading = Instant::now();

    let row_data = reader
        .into_iter()
        // .take(1000000) For testing
        .map(|r| {
            format!("{}\n", r.unwrap().to_json_value())
                .as_bytes()
                .to_vec()
        })
        .collect::<Vec<_>>();

    let rows = SharedVec::new(row_data);

    let end_reading = Instant::now();

    println!(
        "Reading done (took: {})",
        format_duration(Duration::from_millis(
            end_reading.duration_since(begin_reading).as_millis() as u64
        ))
    );

    rows
}

fn create_server_thread(
    listener: TcpListener,
    rows: SharedVec<Vec<u8>>,
    threads: Arc<Mutex<Vec<task::JoinHandle<()>>>>,
    done: Arc<AtomicBool>,
    exp_name: String,
) -> task::JoinHandle<()> {
    task::spawn(async move {
        let mut repetition_id = 0;
        let num_connections = Arc::new(AtomicUsize::new(0));
        let mut row_iter = rows.iter();

        let mut logger =
            BenchmarkLoggerBuilder::new(LOG_FOLDER_PREFIX, format!("{exp_name}_{repetition_id}"))
                .build();
        logger.start().await;

        while let Ok((stream, _)) = listener.accept().await {
            if done.load(Ordering::Relaxed) {
                break;
            }

            if repetition_id > 0 && (num_connections.load(Ordering::Relaxed)) == 0 {
                repetition_id += 1;
                logger = BenchmarkLoggerBuilder::new(
                    LOG_FOLDER_PREFIX,
                    format!("{exp_name}_{repetition_id}"),
                )
                .build();
                logger.start().await;
                row_iter = rows.iter();
            }

            let writer = BufWriter::new(stream);
            let row_iter = row_iter.clone();

            let logger = logger.clone();

            threads.lock().await.push(task::spawn(async move {
                handle_connection(writer, row_iter, logger).await;
            }));
        }
    })
}

async fn handle_connection(
    mut writer: BufWriter<TcpStream>,
    row_iter: SharedVecIterator<Vec<u8>>,
    mut logger: BenchmarkLogger,
) {
    for row in row_iter {
        match writer.write_all(&row).await {
            Ok(_) => (),
            Err(_) => break,
        }

        logger.log_event();
    }

    logger.stop().await;
}

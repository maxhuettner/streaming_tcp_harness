use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::Row;
use std::fs::File;
use std::net::TcpStream;
use std::io::Write;
use std::sync::{Arc};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::{Duration, Instant};
use clap::{Parser};
use crossbeam_channel::{bounded};
use tokio::time::{interval, Interval};
use tracing::info;

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

    let file_appender = tracing_appender::rolling::never("./log", "benchmark.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
    tracing_subscriber::fmt().with_writer(non_blocking).init();

    start_benchmark(cli.address, cli.file_path, cli.rate).await;
}

async fn start_benchmark(address: String, file_path: String, requested_rate: Option<u64>) {
    let file = File::open(file_path).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let mut iter = reader.get_row_iter(None).unwrap().peekable();

    let mut stream = TcpStream::connect(address).unwrap();

    let number_of_tuples_sent = Arc::new(AtomicU32::new(0));
    let start_time = Instant::now();

    let mut interval: Option<Interval> = match requested_rate {
        Some(val) => Some(interval(Duration::from_nanos(1_000_000_000 / val).into())),
        None => None,
    };

    let done = Arc::new(AtomicBool::new(false));
    let (tx, rx) = bounded::<Row>(0);

    let consumer_done = done.clone();
    let consumer_number_of_tuples_sent = number_of_tuples_sent.clone();
    let consumer_thread = tokio::spawn(async move {
        loop {
            if rx.is_empty() && consumer_done.load(Ordering::Relaxed) {
                break;
            }
            let row = rx.recv().unwrap();
            let row_str = format!("{}\n", row.to_json_value().to_string());
            stream.write_all(row_str.as_bytes()).unwrap();
            consumer_number_of_tuples_sent.fetch_add(1, Ordering::Relaxed);
        }
    });

    loop {
        if iter.peek().is_none() {
            done.clone().store(true, Ordering::Relaxed);
            break;
        }
        if let Some(ref mut intvl) = interval {
            intvl.tick().await;
        }
        tx.send(iter.next().unwrap().unwrap()).unwrap();
    }

    consumer_thread.await.unwrap();

    let actual_rate = number_of_tuples_sent.load(Ordering::Relaxed) as f32 / start_time.elapsed().as_secs_f32();

    info!("Requested rate: {} | Actual rate: {}", match requested_rate {
        Some(rate) => rate.to_string(),
        None => "∞".to_string(),
    }, actual_rate);
}

// fn try_reconnect(stream: &TcpStream, address: &String) {
//     loop {
//         let mut stream = stream.unwrap();
//         match TcpStream::connect(address) {
//             Ok(strm) => *stream = strm,
//             Err(_) => continue,
//         }
//     }
// }

mod shared_vec_iter;

use crate::shared_vec_iter::{SharedVec, SharedVecIterator};
use clap::{Parser, ValueEnum};
use humantime::format_duration;
use logger::{BenchmarkLogger, BenchmarkLoggerBuilder};
use parquet::file::reader::SerializedFileReader;
use serde_json::Value;
use std::fs::File;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use chrono::{DateTime, Utc};
use rmp::encode::{write_array_len, write_sint, write_str};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::{io, task};

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

    /// Selects which schema to use for decoding
    #[arg(long, value_enum, default_value_t = SchemaType::Bid)]
    schema: SchemaType,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum SchemaType {
    /// Flat bid events: auction, bidder, price, channel, url, extra, dateTime
    Bid,
    /// Nested auction events under key `auction`
    Auction,
    /// Nested person events under key `person`
    Person,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    start_benchmark(cli.address, cli.file_path, cli.exp_name, cli.schema).await;
}

async fn start_benchmark(address: String, file_path: String, exp_name: String, schema: SchemaType) {
    let rows = task::spawn_blocking(move || {
        let file = File::open(file_path).unwrap();
        let reader = SerializedFileReader::new(file).unwrap();
        init_queue_from_reader(reader, schema)
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

    println!("Waiting for q...");

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

fn init_queue_from_reader(reader: SerializedFileReader<File>, schema: SchemaType) -> SharedVec<Vec<u8>> {
    let begin_reading = Instant::now();

    let row_data = reader
        .into_iter()
        .filter_map(|r| {
            let rec = r.ok()?;
            let json = rec.to_json_value();
            match schema {
                SchemaType::Bid => encode_bid(&json),
                SchemaType::Auction => encode_auction(&json),
                SchemaType::Person => encode_person(&json),
            }
        })
        .collect::<Vec<_>>();

    let rows = SharedVec::new(row_data);
    let end_reading = Instant::now();
    println!(
        "Reading & MessagePack-encoding done (took: {})",
        format_duration(Duration::from_millis(
            end_reading.duration_since(begin_reading).as_millis() as u64
        ))
    );
    rows
}

fn encode_bid(json: &Value) -> Option<Vec<u8>> {
    // Flat schema; tolerate nulls by defaulting
    let auction = json.get("auction").and_then(Value::as_i64).unwrap_or(0);
    let bidder = json.get("bidder").and_then(Value::as_i64).unwrap_or(0);
    let price = json.get("price").and_then(Value::as_i64).unwrap_or(0);
    let channel = json.get("channel").and_then(Value::as_str).unwrap_or("");
    let url = json.get("url").and_then(Value::as_str).unwrap_or("");
    let ms_ts = parse_dt_millis(json.get("dateTime").and_then(Value::as_str));
    let extra = json.get("extra").and_then(Value::as_str).unwrap_or("");

    // Order per spec:
    // [auction, bidder, price, channel, url, dateTime_ms, extra]
    let mut buf = Vec::new();
    write_array_len(&mut buf, 7).ok()?;
    write_sint(&mut buf, auction).ok()?;
    write_sint(&mut buf, bidder).ok()?;
    write_sint(&mut buf, price).ok()?;
    write_str(&mut buf, channel).ok()?;
    write_str(&mut buf, url).ok()?;
    write_sint(&mut buf, ms_ts).ok()?;
    write_str(&mut buf, extra).ok()?;
    Some(buf)
}

fn encode_auction(json: &Value) -> Option<Vec<u8>> {
    // Nested under key `auction` in our data files
    let a: Option<&Value> = json.get("auction");
    if a.is_none() || a.unwrap().is_null() { return None; }
    let a = a.unwrap();

    let id = a.get("id").and_then(Value::as_i64).unwrap_or(0);
    let item_name = a.get("itemName").and_then(Value::as_str).unwrap_or("");
    let description = a.get("description").and_then(Value::as_str).unwrap_or("");
    let initial_bid = a.get("initialBid").and_then(Value::as_i64).unwrap_or(0);
    let reserve = a.get("reserve").and_then(Value::as_i64).unwrap_or(0);
    let dt_ms = parse_dt_millis(a.get("dateTime").and_then(Value::as_str).or_else(|| json.get("dateTime").and_then(Value::as_str)));
    let expires_ms = parse_dt_millis(a.get("expires").and_then(Value::as_str));
    let seller = a.get("seller").and_then(Value::as_i64).unwrap_or(0);
    let category = a.get("category").and_then(Value::as_i64).unwrap_or(0);
    let extra = a.get("extra").and_then(Value::as_str).unwrap_or("");

    // Order per spec (10 fields):
    // [id, itemName, description, initialBid, reserve, dateTime_ms, expires_ms, seller, category, extra]
    let mut buf = Vec::new();
    write_array_len(&mut buf, 10).ok()?;
    write_sint(&mut buf, id).ok()?;
    write_str(&mut buf, item_name).ok()?;
    write_str(&mut buf, description).ok()?;
    write_sint(&mut buf, initial_bid).ok()?;
    write_sint(&mut buf, reserve).ok()?;
    write_sint(&mut buf, dt_ms).ok()?;
    write_sint(&mut buf, expires_ms).ok()?;
    write_sint(&mut buf, seller).ok()?;
    write_sint(&mut buf, category).ok()?;
    write_str(&mut buf, extra).ok()?;
    Some(buf)
}

fn encode_person(json: &Value) -> Option<Vec<u8>> {
    // Nested under key `person` in our data files
    let p = json.get("person");
    if p.is_none() || p.unwrap().is_null() { return None; }
    let p = p.unwrap();

    // Adopt a NEXMark-like layout and tolerate missing keys
    let id = p.get("id").and_then(Value::as_i64).unwrap_or(0);
    let name = p.get("name").and_then(Value::as_str).unwrap_or("");
    let email = p.get("emailAddress").and_then(Value::as_str).unwrap_or("");
    let credit_card = p.get("creditCard").and_then(Value::as_str).unwrap_or("");
    let city = p.get("city").and_then(Value::as_str).unwrap_or("");
    let state = p.get("state").and_then(Value::as_str).unwrap_or("");
    let dt_ms = parse_dt_millis(p.get("dateTime").and_then(Value::as_str).or_else(|| json.get("dateTime").and_then(Value::as_str)));
    let extra = p.get("extra").and_then(Value::as_str).unwrap_or("");

    // Order per spec (8 fields):
    // [id, name, emailAddress, creditCard, city, state, dateTime_ms, extra]
    let mut buf = Vec::new();
    write_array_len(&mut buf, 8).ok()?;
    write_sint(&mut buf, id).ok()?;
    write_str(&mut buf, name).ok()?;
    write_str(&mut buf, email).ok()?;
    write_str(&mut buf, credit_card).ok()?;
    write_str(&mut buf, city).ok()?;
    write_str(&mut buf, state).ok()?;
    write_sint(&mut buf, dt_ms).ok()?;
    write_str(&mut buf, extra).ok()?;
    Some(buf)
}

fn parse_dt_millis(dt_opt: Option<&str>) -> i64 {
    let Some(dt_str) = dt_opt else { return 0; };
    // parse timestamp string to milliseconds; support RFC3339 or space-separated
    let parsed = if dt_str.contains('T') {
        chrono::NaiveDateTime::parse_from_str(dt_str, "%Y-%m-%dT%H:%M:%S%.f")
    } else {
        chrono::NaiveDateTime::parse_from_str(dt_str, "%Y-%m-%d %H:%M:%S%.f")
    };
    match parsed {
        Ok(naive_dt) => {
            let dt = DateTime::<Utc>::from_naive_utc_and_offset(naive_dt, Utc);
            dt.timestamp() * 1000 + dt.timestamp_subsec_millis() as i64
        }
        Err(_) => 0,
    }
}

fn create_server_thread(
    listener: TcpListener,
    rows: SharedVec<Vec<u8>>,
    threads: Arc<Mutex<Vec<task::JoinHandle<()>>>>,
    done: Arc<AtomicBool>,
    exp_name: String,
) -> task::JoinHandle<()> {
    task::spawn(async move {
        let repetition_id = Arc::new(AtomicUsize::new(0));
        let num_connections = Arc::new(AtomicUsize::new(0));
        let mut row_iter = rows.iter();

        let mut logger = BenchmarkLoggerBuilder::new(
            LOG_FOLDER_PREFIX,
            format!("{exp_name}_{}", repetition_id.load(Ordering::Relaxed)),
        )
        .build();
        logger.start().await;

        while let Ok((stream, _)) = listener.accept().await {
            if done.load(Ordering::Relaxed) {
                break;
            }

            if (repetition_id.load(Ordering::Relaxed) > 0)
                && (num_connections.load(Ordering::Relaxed) == 0)
            {
                println!("New repetition, starting logger");
                logger = BenchmarkLoggerBuilder::new(
                    LOG_FOLDER_PREFIX,
                    format!("{exp_name}_{}", repetition_id.load(Ordering::Relaxed)),
                )
                .build();
                logger.start().await;
                row_iter = rows.iter();
            }

            let writer = BufWriter::new(stream);

            let logger = logger.clone();

            let repetition_id = repetition_id.clone();
            let num_connections = num_connections.clone();
            let row_iter = row_iter.clone();
            threads.lock().await.push(task::spawn(async move {
                handle_connection(writer, row_iter, logger, num_connections, repetition_id).await;
            }));
        }
    })
}

async fn handle_connection(
    mut writer: BufWriter<TcpStream>,
    row_iter: SharedVecIterator<Vec<u8>>,
    mut logger: BenchmarkLogger,
    num_connections: Arc<AtomicUsize>,
    repetition_id: Arc<AtomicUsize>,
) {
    num_connections.fetch_add(1, Ordering::Relaxed);

    for row in row_iter {
        // let len = (row.len() as u32).to_le_bytes();
        // let mut frame = Vec::with_capacity(4 + row.len());
        // frame.extend_from_slice(&len);
        // frame.extend_from_slice(&row);
        // if writer.write_all(&frame).await.is_err() { break; }
        if writer.write_all(&row).await.is_err() { break; }

        logger.log_event();
    }

    if num_connections.fetch_sub(1, Ordering::Relaxed) == 1 {
        repetition_id.fetch_add(1, Ordering::Relaxed);
        println!("All connections closed, stopping logger");
        logger.stop().await;
    }
}

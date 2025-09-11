mod shared_vec_iter;

use crate::shared_vec_iter::{SharedVec, SharedVecIterator};
use chrono::{DateTime, Utc};
use clap::{Parser, ValueEnum};
use humantime::format_duration;
use logger::{BenchmarkLogger, BenchmarkLoggerBuilder};
use parquet::file::reader::SerializedFileReader;
use rmp::encode::{write_array_len, write_sint, write_str};
use serde_json::Value;
use std::fs::File;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use timerfd::{ClockId, SetTimeFlags, TimerFd, TimerState};
use tokio::io::unix::AsyncFd;
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

    /// Framing used by the source when sending rows
    #[arg(long, value_enum, default_value_t = Framing::Raw)]
    framing: Framing,
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

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum Framing {
    /// Write raw MessagePack without a length prefix
    Raw,
    /// Write 4-byte little-endian length followed by payload
    Len,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    start_benchmark(
        cli.address,
        cli.file_path,
        cli.exp_name,
        cli.schema,
        cli.rate,
        cli.framing,
    )
    .await;
}

async fn start_benchmark(
    address: String,
    file_path: String,
    exp_name: String,
    schema: SchemaType,
    rate: Option<usize>,
    framing: Framing,
) {
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
        rate,
        framing,
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

fn init_queue_from_reader(
    reader: SerializedFileReader<File>,
    schema: SchemaType,
) -> SharedVec<Vec<u8>> {
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
    let channel = non_empty_str(json.get("channel").and_then(Value::as_str));
    let url = non_empty_str(json.get("url").and_then(Value::as_str));
    let ms_ts = parse_dt_millis(json.get("dateTime").and_then(Value::as_str));
    let extra = non_empty_str(json.get("extra").and_then(Value::as_str));

    // Order per legacy spec (matches master and downstream parsers):
    // [auction, bidder, price, channel, url, extra, dateTime_ms]
    let mut buf = Vec::new();
    write_array_len(&mut buf, 7).ok()?;
    write_sint(&mut buf, auction).ok()?;
    write_sint(&mut buf, bidder).ok()?;
    write_sint(&mut buf, price).ok()?;
    write_str_safe(&mut buf, channel).ok()?;
    write_str_safe(&mut buf, url).ok()?;
    write_str_safe(&mut buf, extra).ok()?;
    write_sint(&mut buf, ms_ts).ok()?;
    Some(buf)
}

fn encode_auction(json: &Value) -> Option<Vec<u8>> {
    // Nested under key `auction` in our data files
    let a: Option<&Value> = json.get("auction");
    if a.is_none() || a.unwrap().is_null() {
        return None;
    }
    let a = a.unwrap();

    let id = a.get("id").and_then(Value::as_i64).unwrap_or(0);
    let item_name = non_empty_str(a.get("itemName").and_then(Value::as_str));
    let description = non_empty_str(a.get("description").and_then(Value::as_str));
    let initial_bid = a.get("initialBid").and_then(Value::as_i64).unwrap_or(0);
    let reserve = a.get("reserve").and_then(Value::as_i64).unwrap_or(0);
    let dt_ms = parse_dt_millis(
        a.get("dateTime")
            .and_then(Value::as_str)
            .or_else(|| json.get("dateTime").and_then(Value::as_str)),
    );
    let expires_ms = parse_dt_millis(a.get("expires").and_then(Value::as_str));
    let seller = a.get("seller").and_then(Value::as_i64).unwrap_or(0);
    let category = a.get("category").and_then(Value::as_i64).unwrap_or(0);
    let extra = non_empty_str(a.get("extra").and_then(Value::as_str));

    // Order per spec (10 fields):
    // [id, itemName, description, initialBid, reserve, dateTime_ms, expires_ms, seller, category, extra]
    let mut buf = Vec::new();
    write_array_len(&mut buf, 10).ok()?;
    write_sint(&mut buf, id).ok()?;
    write_str_safe(&mut buf, item_name).ok()?;
    write_str_safe(&mut buf, description).ok()?;
    write_sint(&mut buf, initial_bid).ok()?;
    write_sint(&mut buf, reserve).ok()?;
    write_sint(&mut buf, dt_ms).ok()?;
    write_sint(&mut buf, expires_ms).ok()?;
    write_sint(&mut buf, seller).ok()?;
    write_sint(&mut buf, category).ok()?;
    write_str_safe(&mut buf, extra).ok()?;
    Some(buf)
}

fn encode_person(json: &Value) -> Option<Vec<u8>> {
    // Nested under key `person` in our data files
    let p = json.get("person");
    if p.is_none() || p.unwrap().is_null() {
        return None;
    }
    let p = p.unwrap();

    // Adopt a NEXMark-like layout and tolerate missing keys
    let id = p.get("id").and_then(Value::as_i64).unwrap_or(0);
    let name = non_empty_str(p.get("name").and_then(Value::as_str));
    let email = non_empty_str(p.get("emailAddress").and_then(Value::as_str));
    let credit_card = non_empty_str(p.get("creditCard").and_then(Value::as_str));
    let city = non_empty_str(p.get("city").and_then(Value::as_str));
    let state = non_empty_str(p.get("state").and_then(Value::as_str));
    let dt_ms = parse_dt_millis(
        p.get("dateTime")
            .and_then(Value::as_str)
            .or_else(|| json.get("dateTime").and_then(Value::as_str)),
    );
    let extra = non_empty_str(p.get("extra").and_then(Value::as_str));

    // Order per spec (8 fields):
    // [id, name, emailAddress, creditCard, city, state, dateTime_ms, extra]
    let mut buf = Vec::new();
    write_array_len(&mut buf, 8).ok()?;
    write_sint(&mut buf, id).ok()?;
    write_str_safe(&mut buf, name).ok()?;
    write_str_safe(&mut buf, email).ok()?;
    write_str_safe(&mut buf, credit_card).ok()?;
    write_str_safe(&mut buf, city).ok()?;
    write_str_safe(&mut buf, state).ok()?;
    write_sint(&mut buf, dt_ms).ok()?;
    write_str_safe(&mut buf, extra).ok()?;
    Some(buf)
}

fn parse_dt_millis(dt_opt: Option<&str>) -> i64 {
    let Some(dt_str) = dt_opt else {
        return 0;
    };
    // Prefer RFC3339/ISO8601 with timezone (e.g., trailing 'Z' or offsets)
    if let Ok(dt_fixed) = chrono::DateTime::parse_from_rfc3339(dt_str) {
        let ts = dt_fixed.timestamp();
        let sub = dt_fixed.timestamp_subsec_millis() as i64;
        return ts * 1000 + sub;
    }
    // Fallback to naive UTC in either 'T' or space-separated format
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

#[inline]
fn non_empty_str(s: Option<&str>) -> &str {
    match s {
        Some(v) if !v.is_empty() => v,
        _ => " ",
    }
}

#[inline]
fn write_str_safe(buf: &mut Vec<u8>, s: &str) -> Result<(), rmp::encode::ValueWriteError> {
    // Centralizes the policy of converting empty strings to a single space
    // and writing via rmp encoder.
    let v = if s.is_empty() { " " } else { s };
    write_str(buf, v)
}

async fn write_frame(
    writer: &mut BufWriter<TcpStream>,
    row: &[u8],
    framing: Framing,
) -> io::Result<()> {
    if matches!(framing, Framing::Len) {
        writer.write_all(&(row.len() as u32).to_le_bytes()).await?;
    }
    writer.write_all(row).await
}

fn create_server_thread(
    listener: TcpListener,
    rows: SharedVec<Vec<u8>>,
    threads: Arc<Mutex<Vec<task::JoinHandle<()>>>>,
    done: Arc<AtomicBool>,
    exp_name: String,
    rate: Option<usize>,
    framing: Framing,
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
                handle_connection(
                    writer,
                    row_iter,
                    logger,
                    num_connections,
                    repetition_id,
                    rate,
                    framing,
                )
                .await;
            }));
        }
    })
}

async fn handle_connection(
    mut writer: BufWriter<TcpStream>,
    mut row_iter: SharedVecIterator<Vec<u8>>,
    mut logger: BenchmarkLogger,
    num_connections: Arc<AtomicUsize>,
    repetition_id: Arc<AtomicUsize>,
    rate: Option<usize>,
    framing: Framing,
) {
    num_connections.fetch_add(1, Ordering::Relaxed);

    if let Some(rate) = rate.filter(|r| *r > 0) {
        let period = std::time::Duration::from_nanos(1_000_000_000u64 / rate as u64);
        let mut tfd =
            TimerFd::new_custom(ClockId::Monotonic, true, true).expect("timerfd create failed");
        tfd.set_state(
            TimerState::Periodic {
                current: period,
                interval: period,
            },
            SetTimeFlags::Default,
        );
        let async_tfd = AsyncFd::new(tfd).expect("asyncfd wrap failed");

        'outer: loop {
            let expirations = wait_expirations(&async_tfd).await;

            let mut sent = 0usize;
            for row in row_iter.by_ref().take(expirations as usize) {
                if write_frame(&mut writer, &row, framing).await.is_err() {
                    break 'outer;
                }
                logger.log_event();
                sent += 1;
            }

            if sent < expirations as usize {
                break;
            }
        }
    } else {
        for row in row_iter {
            if write_frame(&mut writer, &row, framing).await.is_err() {
                break;
            }
            logger.log_event();
        }
    }

    if num_connections.fetch_sub(1, Ordering::Relaxed) == 1 {
        repetition_id.fetch_add(1, Ordering::Relaxed);
        println!("All connections closed, stopping logger");
        logger.stop().await;
    }
}

// Read the number of timer expirations from an AsyncFd-wrapped timerfd,
// handling spurious readiness (0 expirations) by clearing readiness and
// awaiting again.
async fn wait_expirations(tfd: &AsyncFd<TimerFd>) -> u64 {
    loop {
        let mut guard = tfd.readable().await.expect("timerfd not readable");
        let n = guard.get_ref().get_ref().read();
        if n == 0 {
            // Spurious readiness; clear and wait again
            guard.clear_ready();
            continue;
        }
        return n;
    }
}

mod shared_vec_iter;
mod binary_serde;

use crate::shared_vec_iter::{SharedVec, SharedVecIterator};
use crate::binary_serde::BinaryEncoder;
use chrono::{DateTime, Utc};
use clap::{Parser, ValueEnum};
use humantime::format_duration;
use logger::{BenchmarkLogger, BenchmarkLoggerBuilder};
use parquet::file::reader::SerializedFileReader;
use serde_json::Value;
use std::fs::File;
use std::io::{BufRead, BufReader as StdBufReader};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use timerfd::{ClockId, SetTimeFlags, TimerFd, TimerState};
use tokio::io::unix::AsyncFd;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::{io, task};

const LOG_FOLDER_PREFIX: &str = "source";
const SHUTDOWN_FLUSH_TIMEOUT_SECS: u64 = 2;

fn delete_previous_logs(folder_prefix: &str, exp_name: &str) {
    use std::fs;
    use std::io::ErrorKind;
    let folder_path = format!("logs/{}/{}", folder_prefix, exp_name);
    if let Err(e) = fs::remove_dir_all(&folder_path) {
        if e.kind() != ErrorKind::NotFound {
            // ignore
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum SchemaType {
    Bid,
    Auction,
    Person,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum Framing {
    None,
    LenPrefix,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
enum System {
    Default,
    Nes,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
enum InputFormat {
    Parquet,
    Csv,
}

#[derive(Parser, Debug)]
#[command()]
struct Cli {
    file_path: String,

    #[arg(long, short, default_value = "0.0.0.0:10000")]
    address: String,

    #[arg(long, short, default_value = "test")]
    exp_name: String,

    #[arg(long, short, value_enum, default_value_t = System::Default)]
    system: System,

    /// Selects how to read input data from `file_path`
    #[arg(long, value_enum, default_value_t = InputFormat::Parquet)]
    input_format: InputFormat,

    /// Sets the event rate in elements per second
    #[arg(long, short)]
    rate: Option<usize>,

    /// Limit number of events read from the input file
    #[arg(long, short)]
    num_events: Option<usize>,

    /// Selects which schema to use for decoding (parquet input only)
    #[arg(long, value_enum)]
    schema: SchemaType,

    /// Framing used by the source when sending rows
    #[arg(long, value_enum, default_value_t = Framing::LenPrefix)]
    framing: Framing,

    /// Enable periodic event-rate logging (writes events_*.csv). Off by default.
    #[arg(long, default_value_t = false)]
    disable_event_logging: bool,

    /// Append a high-resolution send timestamp (ns since Unix epoch) as an extra array field.
    #[arg(long, default_value_t = false)]
    latency: bool,

    /// Percentage (0-100) of Bid `price` values to encode as null (MessagePack nil).
    #[arg(long)]
    null_price_percent: Option<u8>,

    /// Initial repetition index used for repetition-related naming (e.g., logs ending in `_5`).
    #[arg(long, default_value_t = 0)]
    start_with_rep: usize,
}

/* -------------------- Simple PRNG + helper -------------------- */

struct XorShift64 {
    state: u64,
}

impl XorShift64 {
    fn new(mut seed: u64) -> Self {
        if seed == 0 {
            seed = 0x9E37_79B9_7F4A_7C15; // avoid zero-lock
        }
        Self { state: seed }
    }

    fn next_u32(&mut self) -> u32 {
        // xorshift64*
        let mut x = self.state;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.state = x;
        ((x.wrapping_mul(0x2545_F491_4F6C_DD1D)) >> 32) as u32
    }
}

#[inline]
fn should_null_price(pct: Option<u8>, rng: &mut XorShift64) -> bool {
    let p = pct.unwrap_or(0).min(100);
    if p == 0 {
        return false;
    }
    if p == 100 {
        return true;
    }
    (rng.next_u32() % 100) < p as u32
}

/* -------------------- Main -------------------- */

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    start_benchmark(
        cli.address,
        cli.file_path,
        cli.exp_name,
        cli.system,
        cli.input_format,
        cli.schema,
        cli.rate,
        cli.num_events,
        !cli.disable_event_logging,
        cli.framing,
        cli.latency,
        cli.null_price_percent,
        cli.start_with_rep,
    )
    .await;
}

async fn start_benchmark(
    address: String,
    file_path: String,
    exp_name: String,
    system: System,
    input_format: InputFormat,
    schema: SchemaType,
    rate: Option<usize>,
    num_events: Option<usize>,
    event_logging: bool,
    framing: Framing,
    latency: bool,
    null_price_percent: Option<u8>,
    start_with_rep: usize,
) {
    // Clean up previous logs for this experiment name on startup
    delete_previous_logs(LOG_FOLDER_PREFIX, &exp_name);

    let rows = task::spawn_blocking(move || match input_format {
        InputFormat::Parquet => {
            let file = File::open(&file_path).unwrap();
            let reader = SerializedFileReader::new(file).unwrap();
            init_queue_from_reader(reader, schema, system, num_events, null_price_percent)
        }
        InputFormat::Csv => init_queue_from_csv(&file_path, num_events),
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
        system.clone(),
        rate,
        event_logging,
        framing,
        latency,
        start_with_rep,
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
    system: System,
    num_events: Option<usize>,
    null_price_percent: Option<u8>,
) -> SharedVec<Vec<u8>> {
    let begin_reading = Instant::now();

    // Seed PRNG once per file load (stable-ish per run, not per row).
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(1))
        .as_nanos() as u64;
    let mut rng = XorShift64::new(seed);

    let iter = reader.into_iter().filter_map(move |r| {
        let rec = r.ok()?;
        let json = rec.to_json_value();
        match schema {
            SchemaType::Bid => {
                let null_price = should_null_price(null_price_percent, &mut rng);
                encode_bid(&json, &system, null_price)
            }
            SchemaType::Auction => encode_auction(&json, &system),
            SchemaType::Person => encode_person(&json, &system),
        }
    });

    let row_data = match num_events {
        Some(n) => iter.take(n).collect::<Vec<_>>(),
        None => iter.collect::<Vec<_>>(),
    };

    let rows = SharedVec::new(row_data);
    let end_reading = Instant::now();
    println!(
        "Reading & binary encoding done (took: {})",
        format_duration(Duration::from_millis(
            end_reading.duration_since(begin_reading).as_millis() as u64
        ))
    );
    rows
}

fn init_queue_from_csv(file_path: &str, num_events: Option<usize>) -> SharedVec<Vec<u8>> {
    let begin_reading = Instant::now();
    let file = File::open(file_path).unwrap();
    let reader = StdBufReader::new(file);

    let lines = reader.lines().filter_map(|line| match line {
        Ok(content) => encode_csv_line(&content),
        Err(_) => None,
    });

    let row_data = match num_events {
        Some(n) => lines.take(n).collect::<Vec<_>>(),
        None => lines.collect::<Vec<_>>(),
    };

    let rows = SharedVec::new(row_data);
    let end_reading = Instant::now();
    println!(
        "Reading & binary encoding done (took: {})",
        format_duration(Duration::from_millis(
            end_reading.duration_since(begin_reading).as_millis() as u64
        ))
    );
    rows
}

fn encode_bid(json: &Value, system: &System, null_price: bool) -> Option<Vec<u8>> {
    let auction = json.get("auction").and_then(Value::as_i64).unwrap_or(0);
    let bidder = json.get("bidder").and_then(Value::as_i64).unwrap_or(0);
    let price = json.get("price").and_then(Value::as_i64).unwrap_or(0);
    let channel = non_empty_str(json.get("channel").and_then(Value::as_str));
    let url = non_empty_str(json.get("url").and_then(Value::as_str));
    let ms_ts = parse_dt_millis(json.get("dateTime").and_then(Value::as_str));
    let extra = non_empty_str(json.get("extra").and_then(Value::as_str));

    // Schema: [auction, bidder, price, channel, url, dateTime, extra, latency_ts]
    let mut encoder = match system {
        System::Default => BinaryEncoder::with_capacity(8),
        System::Nes => BinaryEncoder::with_capacity(4),
    };

    match system {
        System::Default => {
            encoder.add_int64(auction);
            encoder.add_int64(bidder);
            // Use null if null_price is true, otherwise use the actual price
            if null_price {
                encoder.add_null();
            } else {
                encoder.add_int64(price);
            }
            encoder.add_string(channel.to_string());
            encoder.add_string(url.to_string());
            encoder.add_timestamp(ms_ts);
            encoder.add_string(extra.to_string());
            encoder.add_int64(0); // latency_ts placeholder
        }
        System::Nes => {
            encoder.add_int64(auction);
            encoder.add_int64(bidder);
            if null_price {
                encoder.add_null();
            } else {
                encoder.add_int64(price);
            }
            encoder.add_timestamp(ms_ts);
        }
    }

    Some(encoder.encode_payload())
}

fn encode_csv_line(line: &str) -> Option<Vec<u8>> {
    let mut encoder = BinaryEncoder::with_capacity(1);
    encoder.add_string(line.to_string());
    Some(encoder.encode_payload())
}

fn encode_auction(json: &Value, system: &System) -> Option<Vec<u8>> {
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
    let mut encoder = match system {
        System::Default => BinaryEncoder::with_capacity(10),
        System::Nes => BinaryEncoder::with_capacity(7),
    };

    match system {
        System::Default => {
            encoder.add_int64(id);
            encoder.add_string(item_name.to_string());
            encoder.add_string(description.to_string());
            encoder.add_int64(initial_bid);
            encoder.add_int64(reserve);
            encoder.add_timestamp(dt_ms);
            encoder.add_timestamp(expires_ms);
            encoder.add_int64(seller);
            encoder.add_int64(category);
            encoder.add_string(extra.to_string());
        }
        System::Nes => {
            encoder.add_int64(id);
            encoder.add_int64(initial_bid);
            encoder.add_int64(reserve);
            encoder.add_timestamp(dt_ms);
            encoder.add_timestamp(expires_ms);
            encoder.add_int64(seller);
            encoder.add_int64(category);
        }
    }

    Some(encoder.encode_payload())
}

fn encode_person(json: &Value, system: &System) -> Option<Vec<u8>> {
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
    let mut encoder = match system {
        System::Default => BinaryEncoder::with_capacity(8),
        System::Nes => BinaryEncoder::with_capacity(4),
    };

    match system {
        System::Default => {
            encoder.add_int64(id);
            encoder.add_string(name.to_string());
            encoder.add_string(email.to_string());
            encoder.add_string(credit_card.to_string());
            encoder.add_string(city.to_string());
            encoder.add_string(state.to_string());
            encoder.add_timestamp(dt_ms);
            encoder.add_string(extra.to_string());
        }
        System::Nes => {
            encoder.add_int64(id);
            encoder.add_string(credit_card.to_string());
            encoder.add_timestamp(dt_ms);
            encoder.add_string(extra.to_string());
        }
    }

    Some(encoder.encode_payload())
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


fn with_latency_field(row: &[u8], ts_ns: u64) -> io::Result<Vec<u8>> {
    // Replace the last 8 bytes (latency_ts placeholder) with actual timestamp
    if row.len() < 8 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "row too short",
        ));
    }

    let mut out = row.to_vec();
    let len = out.len();
    out[len - 8..].copy_from_slice(&(ts_ns as i64).to_be_bytes());

    Ok(out)
}

async fn write_frame(
    writer: &mut BufWriter<TcpStream>,
    row: &[u8],
    framing: Framing,
    latency: bool,
) -> io::Result<()> {
    if latency {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| Duration::from_secs(0));
        let row_bytes = with_latency_field(row, now.as_nanos() as u64)?;

        if matches!(framing, Framing::LenPrefix) {
            writer
                .write_all(&(row_bytes.len() as i32).to_be_bytes())
                .await?;
        }
        writer.write_all(&row_bytes).await?;
    } else {
        if matches!(framing, Framing::LenPrefix) {
            writer
                .write_all(&(row.len() as i32).to_be_bytes())
                .await?;
        }
        writer.write_all(row).await?;
    }

    Ok(())
}

fn create_server_thread(
    listener: TcpListener,
    rows: SharedVec<Vec<u8>>,
    threads: Arc<Mutex<Vec<task::JoinHandle<()>>>>,
    done: Arc<AtomicBool>,
    exp_name: String,
    system: System,
    rate: Option<usize>,
    event_logging: bool,
    framing: Framing,
    latency: bool,
    start_with_rep: usize,
) -> task::JoinHandle<()> {
    task::spawn(async move {
        let repetition_id = Arc::new(AtomicUsize::new(start_with_rep));
        let num_connections = Arc::new(AtomicUsize::new(0));
        let mut row_iter = rows.iter();

        let mut logger = BenchmarkLoggerBuilder::new(
            format!("{}/{}", LOG_FOLDER_PREFIX, exp_name),
            format!("{exp_name}_{}", repetition_id.load(Ordering::Relaxed)),
        )
        .build();
        if event_logging {
            logger.start().await;
        }

        while let Ok((stream, _)) = listener.accept().await {
            if done.load(Ordering::Relaxed) {
                break;
            }

            println!("Accepted connection");

            if (repetition_id.load(Ordering::Relaxed) > start_with_rep)
                && (num_connections.load(Ordering::Relaxed) == 0)
            {
                println!("Repetition {}, starting logger", repetition_id.load(Ordering::Relaxed));
                logger = BenchmarkLoggerBuilder::new(
                    format!("{}/{}", LOG_FOLDER_PREFIX, exp_name),
                    format!("{exp_name}_{}", repetition_id.load(Ordering::Relaxed)),
                )
                .build();
                if event_logging {
                    logger.start().await;
                }
                row_iter = rows.iter();
            }

            let writer = BufWriter::new(stream);

            let logger = logger.clone();

            let repetition_id = repetition_id.clone();
            let num_connections = num_connections.clone();
            let row_iter = row_iter.clone();
            let done = done.clone();
            threads.lock().await.push(task::spawn(async move {
                handle_connection(
                    writer,
                    row_iter,
                    logger,
                    num_connections,
                    repetition_id,
                    done,
                    rate,
                    framing,
                    latency,
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
    done: Arc<AtomicBool>,
    rate: Option<usize>,
    framing: Framing,
    latency: bool,
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
            if done.load(Ordering::Relaxed) {
                break;
            }
            let expirations = wait_expirations(&async_tfd).await;
            if done.load(Ordering::Relaxed) {
                break;
            }

            let mut sent = 0usize;
            for row in row_iter.by_ref().take(expirations as usize) {
                if done.load(Ordering::Relaxed) {
                    break 'outer;
                }
                if write_frame(&mut writer, &row, framing, latency)
                    .await
                    .is_err()
                {
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
            if done.load(Ordering::Relaxed) {
                break;
            }
            if write_frame(&mut writer, &row, framing, latency)
                .await
                .is_err()
            {
                break;
            }
            logger.log_event();
        }
    }

    let flush_result = if done.load(Ordering::Relaxed) {
        match tokio::time::timeout(
            Duration::from_secs(SHUTDOWN_FLUSH_TIMEOUT_SECS),
            writer.flush(),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "flush timed out during shutdown",
            )),
        }
    } else {
        writer.flush().await
    };

    if let Err(e) = flush_result {
        eprintln!("Error flushing writer: {e}");
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

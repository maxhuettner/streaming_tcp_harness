use chrono::{DateTime, SecondsFormat, Utc};
use clap::Parser;
use csv::Writer;
use logger::{BenchmarkLogger, BenchmarkLoggerBuilder};
use std::fs;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::{io, task};

const LOG_FOLDER_PREFIX: &str = "sink";
const SHUTDOWN_POLL_INTERVAL_MS: u64 = 200;

fn delete_previous_logs(folder_prefix: &str, exp_name: &str) {
    use std::fs;
    use std::io::ErrorKind;
    let folder_path = format!("logs/{folder_prefix}/{exp_name}");
    if let Err(e) = fs::remove_dir_all(&folder_path) {
        if e.kind() != ErrorKind::NotFound {
            // Ignore errors; logging path cleanup is best-effort
        }
    }
}

#[derive(Parser, Debug)]
#[command()]
struct Cli {
    #[arg(long, short, default_value = "0.0.0.0:9000")]
    address: String,

    #[arg(long, short, default_value = "test")]
    exp_name: String,

    /// Enable periodic event-rate logging (writes events_*.csv). Off by default.
    #[arg(long, default_value_t = false)]
    disable_event_logging: bool,

    /// Parse the last value from MessagePack array as send_ts_ns and compute receive latency.
    #[arg(long, default_value_t = false)]
    latency: bool,

    /// Initial repetition index used for repetition-related naming (e.g., logs ending in `_5`).
    #[arg(long, default_value_t = 0)]
    start_with_rep: usize,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    start_benchmark(
        cli.address,
        cli.exp_name,
        !cli.disable_event_logging,
        cli.latency,
        cli.start_with_rep,
    )
    .await;
}

async fn start_benchmark(
    address: String,
    exp_name: String,
    event_logging: bool,
    latency: bool,
    start_with_rep: usize,
) {
    let listener = TcpListener::bind(&address).await.unwrap();

    let connection_threads = Arc::new(Mutex::new(Vec::new()));
    let done = Arc::new(AtomicBool::new(false));

    // Clean up previous logs for this experiment name on startup
    delete_previous_logs(LOG_FOLDER_PREFIX, &exp_name);

    let server_thread = create_server_thread(
        listener,
        connection_threads.clone(),
        done.clone(),
        exp_name.clone(),
        event_logging,
        latency,
        start_with_rep,
    );

    println!("Waiting for q");

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

fn create_server_thread(
    listener: TcpListener,
    threads: Arc<Mutex<Vec<task::JoinHandle<()>>>>,
    done: Arc<AtomicBool>,
    exp_name: String,
    event_logging: bool,
    latency: bool,
    start_with_rep: usize,
) -> task::JoinHandle<()> {
    task::spawn(async move {
        let repetition_id = Arc::new(AtomicUsize::new(start_with_rep));
        let num_connections = Arc::new(AtomicUsize::new(0));
        let folder_prefix = format!("{LOG_FOLDER_PREFIX}/{exp_name}");

        let mut file_suffix = format!("{exp_name}_{}", repetition_id.load(Ordering::Relaxed));
        let mut logger = BenchmarkLoggerBuilder::new(folder_prefix.clone(), file_suffix.clone())
            .build();
        if event_logging {
            logger.start().await;
        }
        let mut latency_writer = if latency {
            Some(Arc::new(Mutex::new(build_latency_writer(
                &folder_prefix,
                &file_suffix,
            ))))
        } else {
            None
        };

        while let Ok((stream, _)) = listener.accept().await {
            if done.load(Ordering::Relaxed) {
                break;
            }

            println!("Accepted connection");

            if (repetition_id.load(Ordering::Relaxed) > start_with_rep)
                && (num_connections.load(Ordering::Relaxed) == 0)
            {
                println!("New repetition, starting logger");
                file_suffix = format!("{exp_name}_{}", repetition_id.load(Ordering::Relaxed));
                logger = BenchmarkLoggerBuilder::new(folder_prefix.clone(), file_suffix.clone())
                    .build();
                if event_logging {
                    logger.start().await;
                }
                latency_writer = if latency {
                    Some(Arc::new(Mutex::new(build_latency_writer(
                        &folder_prefix,
                        &file_suffix,
                    ))))
                } else {
                    None
                };
            }

            let reader = BufReader::with_capacity(256 * 1024, stream);

            let logger = logger.clone();

            let repetition_id = repetition_id.clone();
            let num_connections = num_connections.clone();
            let latency_writer = latency_writer.clone();
            let done = done.clone();
            threads.lock().await.push(task::spawn(async move {
                handle_connection(
                    reader,
                    logger,
                    num_connections,
                    repetition_id,
                    done,
                    latency,
                    latency_writer,
                )
                .await;
            }));
        }
    })
}

async fn handle_connection(
    mut reader: BufReader<TcpStream>,
    mut logger: BenchmarkLogger,
    num_connections: Arc<AtomicUsize>,
    repetition_id: Arc<AtomicUsize>,
    done: Arc<AtomicBool>,
    latency: bool,
    latency_writer: Option<Arc<Mutex<Writer<std::fs::File>>>>,
) {
    num_connections.fetch_add(1, Ordering::Relaxed);

    // Length-prefixed frames: [i32 big-endian length][payload bytes]
    let mut len_buf = [0u8; 4];
    let mut payload = Vec::new();
    let mut stats = LatencyStats::default();
    let mut latency_samples: Vec<(u64, u64)> = Vec::new();
    while read_exact_or_shutdown(&mut reader, &mut len_buf, &done)
        .await
        .unwrap_or(false)
    {
        let len = i32::from_be_bytes(len_buf) as usize;
        payload.resize(len, 0);
        match read_exact_or_shutdown(&mut reader, &mut payload, &done).await {
            Ok(true) => {}
            Ok(false) | Err(_) => break,
        }

        if latency {
            if let Ok(Some(send_ts_ns)) = extract_send_ts_ns(&payload) {
                if send_ts_ns >= 0 {
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default();
                    let now_ns = now.as_nanos() as u64;
                    let latency_ns = now_ns.saturating_sub(send_ts_ns as u64);
                    stats.update(latency_ns);
                    if latency_writer.is_some() {
                        latency_samples.push((latency_ns, now_ns));
                    }
                }
            }
        }

        logger.log_event();
    }

    if latency && stats.count > 0 {
        println!(
            "Latency ns (count={}, avg={}, min={}, max={})",
            stats.count,
            stats.avg_ns(),
            stats.min_ns,
            stats.max_ns
        );
    }
    if let Some(writer) = &latency_writer {
        if !latency_samples.is_empty() {
            let mut w = writer.lock().await;
            for (latency_ns, recv_ns) in latency_samples {
                let recv_ts = format_ns_rfc3339(recv_ns);
                let _ = w.write_record([latency_ns.to_string(), recv_ts]);
            }
            let _ = w.flush();
        }
    }

    if num_connections.fetch_sub(1, Ordering::Relaxed) == 1 {
        repetition_id.fetch_add(1, Ordering::Relaxed);
        println!("All connections closed, stopping logger");
        logger.stop().await;
    }
}

async fn read_exact_or_shutdown(
    reader: &mut BufReader<TcpStream>,
    buf: &mut [u8],
    done: &AtomicBool,
) -> io::Result<bool> {
    loop {
        if done.load(Ordering::Relaxed) {
            return Ok(false);
        }

        match tokio::time::timeout(
            Duration::from_millis(SHUTDOWN_POLL_INTERVAL_MS),
            reader.read_exact(buf),
        )
        .await
        {
            Ok(Ok(_)) => return Ok(true),
            Ok(Err(e)) => return Err(e),
            Err(_) => continue,
        }
    }
}

#[derive(Default)]
struct LatencyStats {
    count: u64,
    sum_ns: u128,
    min_ns: u64,
    max_ns: u64,
}

impl LatencyStats {
    fn update(&mut self, latency_ns: u64) {
        if self.count == 0 {
            self.min_ns = latency_ns;
            self.max_ns = latency_ns;
        } else {
            self.min_ns = self.min_ns.min(latency_ns);
            self.max_ns = self.max_ns.max(latency_ns);
        }
        self.count += 1;
        self.sum_ns += latency_ns as u128;
    }

    fn avg_ns(&self) -> u64 {
        if self.count == 0 {
            0
        } else {
            (self.sum_ns / self.count as u128) as u64
        }
    }
}

fn extract_send_ts_ns(buf: &[u8]) -> io::Result<Option<i64>> {
    // Wire format: [nullBitmap[N]][field values]
    // The last field should be an int64 timestamp
    // Read the last 8 bytes of the buffer as the timestamp
    if buf.len() < 8 {
        return Ok(None);
    }

    let ts_bytes = &buf[buf.len() - 8..];
    let ts = i64::from_be_bytes([
        ts_bytes[0],
        ts_bytes[1],
        ts_bytes[2],
        ts_bytes[3],
        ts_bytes[4],
        ts_bytes[5],
        ts_bytes[6],
        ts_bytes[7],
    ]);

    Ok(Some(ts))
}


fn build_latency_writer(folder_prefix: &str, file_suffix: &str) -> Writer<std::fs::File> {
    let folder_path = format!("logs/{folder_prefix}");
    fs::create_dir_all(&folder_path).unwrap();
    let mut writer =
        Writer::from_path(format!("{folder_path}/latency_{file_suffix}.csv")).unwrap();
    writer.write_record(["latency_ns", "recv_ts"]).unwrap();
    writer
}

fn format_ns_rfc3339(ns: u64) -> String {
    let secs = (ns / 1_000_000_000) as i64;
    let nanos = (ns % 1_000_000_000) as u32;
    match DateTime::<Utc>::from_timestamp(secs, nanos) {
        Some(dt) => dt.to_rfc3339_opts(SecondsFormat::Nanos, true),
        None => String::new(),
    }
}

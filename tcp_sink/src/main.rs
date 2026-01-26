use clap::Parser;
use csv::Writer;
use logger::{BenchmarkLogger, BenchmarkLoggerBuilder};
use std::fs;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::{io, task};
use rmp::decode::{read_array_len, read_marker, RmpRead, ValueReadError};
use rmp::Marker;
use std::io::Cursor;

const LOG_FOLDER_PREFIX: &str = "sink";

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
    event_logging: bool,

    /// Parse the last value from MessagePack array as send_ts_ns and compute receive latency.
    #[arg(long, default_value_t = false)]
    latency: bool,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    start_benchmark(cli.address, cli.exp_name, cli.event_logging, cli.latency).await;
}

async fn start_benchmark(address: String, exp_name: String, event_logging: bool, latency: bool) {
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
) -> task::JoinHandle<()> {
    task::spawn(async move {
        let repetition_id = Arc::new(AtomicUsize::new(0));
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

            if (repetition_id.load(Ordering::Relaxed) > 0)
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
            threads.lock().await.push(task::spawn(async move {
                handle_connection(
                    reader,
                    logger,
                    num_connections,
                    repetition_id,
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
    latency: bool,
    latency_writer: Option<Arc<Mutex<Writer<std::fs::File>>>>,
) {
    num_connections.fetch_add(1, Ordering::Relaxed);

    // Length-prefixed frames: [u32 little-endian length][payload bytes]
    let mut len_buf = [0u8; 4];
    let mut payload = Vec::new();
    let mut stats = LatencyStats::default();
    while reader.read_exact(&mut len_buf).await.is_ok() {
        let len = u32::from_le_bytes(len_buf) as usize;
        if latency {
            payload.resize(len, 0);
            if reader.read_exact(&mut payload).await.is_err() {
                break;
            }
            if let Ok(Some(send_ts_ns)) = extract_send_ts_ns(&payload) {
                if send_ts_ns >= 0 {
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default();
                    let now_ns = now.as_nanos();
                    let latency_ns = now_ns.saturating_sub(send_ts_ns as u128) as u64;
                    stats.update(latency_ns);
                    if let Some(writer) = &latency_writer {
                        let mut w = writer.lock().await;
                        let _ = w.write_record([latency_ns.to_string()]);
                    }
                }
            }
        } else {
            let mut limited = (&mut reader).take(len as u64);
            if io::copy(&mut limited, &mut io::sink()).await.is_err() {
                break;
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
        let _ = writer.lock().await.flush();
    }

    if num_connections.fetch_sub(1, Ordering::Relaxed) == 1 {
        repetition_id.fetch_add(1, Ordering::Relaxed);
        println!("All connections closed, stopping logger");
        logger.stop().await;
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
    let mut cur = Cursor::new(buf);
    let len = read_array_len(&mut cur).map_err(to_io_err)?;
    if len == 0 {
        return Ok(None);
    }
    for _ in 0..(len - 1) {
        skip_value(&mut cur).map_err(to_io_err)?;
    }
    let marker = read_marker(&mut cur).map_err(to_io_err)?;
    let ts = match marker {
        Marker::FixPos(val) => val as i64,
        Marker::FixNeg(val) => val as i64,
        Marker::U8 => cur.read_data_u8().map_err(to_io_err)? as i64,
        Marker::U16 => cur.read_data_u16().map_err(to_io_err)? as i64,
        Marker::U32 => cur.read_data_u32().map_err(to_io_err)? as i64,
        Marker::U64 => {
            let v = cur.read_data_u64().map_err(to_io_err)?;
            v.min(i64::MAX as u64) as i64
        }
        Marker::I8 => cur.read_data_i8().map_err(to_io_err)? as i64,
        Marker::I16 => cur.read_data_i16().map_err(to_io_err)? as i64,
        Marker::I32 => cur.read_data_i32().map_err(to_io_err)? as i64,
        Marker::I64 => cur.read_data_i64().map_err(to_io_err)?,
        _ => {
            skip_value_with_marker(&mut cur, marker).map_err(to_io_err)?;
            return Ok(None);
        }
    };
    Ok(Some(ts))
}

fn to_io_err<E: std::fmt::Debug>(err: E) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("{err:?}"))
}

fn skip_value<R: RmpRead>(rd: &mut R) -> Result<(), ValueReadError<R::Error>> {
    let marker = read_marker(rd)?;
    skip_value_with_marker(rd, marker)
}

fn skip_value_with_marker<R: RmpRead>(
    rd: &mut R,
    marker: Marker,
) -> Result<(), ValueReadError<R::Error>> {
    match marker {
        Marker::FixPos(_) | Marker::FixNeg(_) | Marker::Null | Marker::True | Marker::False => Ok(()),
        Marker::U8 => {
            rd.read_data_u8()?;
            Ok(())
        }
        Marker::U16 => {
            rd.read_data_u16()?;
            Ok(())
        }
        Marker::U32 => {
            rd.read_data_u32()?;
            Ok(())
        }
        Marker::U64 => {
            rd.read_data_u64()?;
            Ok(())
        }
        Marker::I8 => {
            rd.read_data_i8()?;
            Ok(())
        }
        Marker::I16 => {
            rd.read_data_i16()?;
            Ok(())
        }
        Marker::I32 => {
            rd.read_data_i32()?;
            Ok(())
        }
        Marker::I64 => {
            rd.read_data_i64()?;
            Ok(())
        }
        Marker::F32 => {
            rd.read_data_f32()?;
            Ok(())
        }
        Marker::F64 => {
            rd.read_data_f64()?;
            Ok(())
        }
        Marker::FixStr(len) => skip_bytes(rd, len as usize),
        Marker::Str8 => {
            let len = rd.read_data_u8()? as usize;
            skip_bytes(rd, len)
        }
        Marker::Str16 => {
            let len = rd.read_data_u16()? as usize;
            skip_bytes(rd, len)
        }
        Marker::Str32 => {
            let len = rd.read_data_u32()? as usize;
            skip_bytes(rd, len)
        }
        Marker::Bin8 => {
            let len = rd.read_data_u8()? as usize;
            skip_bytes(rd, len)
        }
        Marker::Bin16 => {
            let len = rd.read_data_u16()? as usize;
            skip_bytes(rd, len)
        }
        Marker::Bin32 => {
            let len = rd.read_data_u32()? as usize;
            skip_bytes(rd, len)
        }
        Marker::FixArray(len) => {
            for _ in 0..len {
                skip_value(rd)?;
            }
            Ok(())
        }
        Marker::Array16 => {
            let len = rd.read_data_u16()?;
            for _ in 0..len {
                skip_value(rd)?;
            }
            Ok(())
        }
        Marker::Array32 => {
            let len = rd.read_data_u32()?;
            for _ in 0..len {
                skip_value(rd)?;
            }
            Ok(())
        }
        Marker::FixMap(len) => {
            for _ in 0..len {
                skip_value(rd)?;
                skip_value(rd)?;
            }
            Ok(())
        }
        Marker::Map16 => {
            let len = rd.read_data_u16()?;
            for _ in 0..len {
                skip_value(rd)?;
                skip_value(rd)?;
            }
            Ok(())
        }
        Marker::Map32 => {
            let len = rd.read_data_u32()?;
            for _ in 0..len {
                skip_value(rd)?;
                skip_value(rd)?;
            }
            Ok(())
        }
        Marker::FixExt1 => {
            rd.read_data_i8()?;
            skip_bytes(rd, 1)
        }
        Marker::FixExt2 => {
            rd.read_data_i8()?;
            skip_bytes(rd, 2)
        }
        Marker::FixExt4 => {
            rd.read_data_i8()?;
            skip_bytes(rd, 4)
        }
        Marker::FixExt8 => {
            rd.read_data_i8()?;
            skip_bytes(rd, 8)
        }
        Marker::FixExt16 => {
            rd.read_data_i8()?;
            skip_bytes(rd, 16)
        }
        Marker::Ext8 => {
            let len = rd.read_data_u8()? as usize;
            rd.read_data_i8()?;
            skip_bytes(rd, len)
        }
        Marker::Ext16 => {
            let len = rd.read_data_u16()? as usize;
            rd.read_data_i8()?;
            skip_bytes(rd, len)
        }
        Marker::Ext32 => {
            let len = rd.read_data_u32()? as usize;
            rd.read_data_i8()?;
            skip_bytes(rd, len)
        }
        Marker::Reserved => Ok(()),
    }
}

fn skip_bytes<R: RmpRead>(rd: &mut R, mut len: usize) -> Result<(), ValueReadError<R::Error>> {
    let mut buf = [0u8; 256];
    while len > 0 {
        let chunk = len.min(buf.len());
        rd.read_exact_buf(&mut buf[..chunk])
            .map_err(ValueReadError::InvalidDataRead)?;
        len -= chunk;
    }
    Ok(())
}

fn build_latency_writer(folder_prefix: &str, file_suffix: &str) -> Writer<std::fs::File> {
    let folder_path = format!("logs/{folder_prefix}");
    fs::create_dir_all(&folder_path).unwrap();
    let mut writer =
        Writer::from_path(format!("{folder_path}/latency_{file_suffix}.csv")).unwrap();
    writer.write_record(["latency_ns"]).unwrap();
    writer
}

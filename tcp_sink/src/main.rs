use clap::Parser;
use logger::{BenchmarkLogger, BenchmarkLoggerBuilder};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::{io, task};

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
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    start_benchmark(cli.address, cli.exp_name, cli.event_logging).await;
}

async fn start_benchmark(address: String, exp_name: String, event_logging: bool) {
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
) -> task::JoinHandle<()> {
    task::spawn(async move {
        let repetition_id = Arc::new(AtomicUsize::new(0));
        let num_connections = Arc::new(AtomicUsize::new(0));

        let mut logger = BenchmarkLoggerBuilder::new(
            format!("{LOG_FOLDER_PREFIX}/{exp_name}"),
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

            if (repetition_id.load(Ordering::Relaxed) > 0)
                && (num_connections.load(Ordering::Relaxed) == 0)
            {
                println!("New repetition, starting logger");
                logger = BenchmarkLoggerBuilder::new(
                    format!("{LOG_FOLDER_PREFIX}/{exp_name}"),
                    format!("{exp_name}_{}", repetition_id.load(Ordering::Relaxed)),
                )
                .build();
                if event_logging {
                    logger.start().await;
                }
            }

            let reader = BufReader::with_capacity(256 * 1024, stream);

            let logger = logger.clone();

            let repetition_id = repetition_id.clone();
            let num_connections = num_connections.clone();
            threads.lock().await.push(task::spawn(async move {
                handle_connection(reader, logger, num_connections, repetition_id).await;
            }));
        }
    })
}

async fn handle_connection(
    mut reader: BufReader<TcpStream>,
    mut logger: BenchmarkLogger,
    num_connections: Arc<AtomicUsize>,
    repetition_id: Arc<AtomicUsize>,
) {
    num_connections.fetch_add(1, Ordering::Relaxed);

    // Length-prefixed frames: [u32 little-endian length][payload bytes]
    let mut len_buf = [0u8; 4];
    while reader.read_exact(&mut len_buf).await.is_ok() {
        let len = u32::from_le_bytes(len_buf) as u64;
        let mut limited = (&mut reader).take(len);
        if io::copy(&mut limited, &mut io::sink()).await.is_err() {
            break;
        }

        logger.log_event();
    }

    if num_connections.fetch_sub(1, Ordering::Relaxed) == 1 {
        repetition_id.fetch_add(1, Ordering::Relaxed);
        println!("All connections closed, stopping logger");
        logger.stop().await;
    }
}

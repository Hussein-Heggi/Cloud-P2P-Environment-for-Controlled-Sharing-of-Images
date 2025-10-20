use clap::Parser;
use tokio::net::TcpListener;
use tracing::{info, warn};

#[derive(Parser, Debug)]
struct Args {
    /// Address to bind the server on, e.g., 127.0.0.1:5001
    #[clap(long, default_value = "127.0.0.1:5001")]
    bind: String,
}

fn init_logging() {
    use tracing_subscriber::{fmt, prelude::*, EnvFilter};
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::registry().with(filter).with(fmt::layer()).init();
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logging();
    let args = Args::parse();
    info!(bind=%args.bind, "starting server (binding)");

    // 1) Bind: reserve the (IP, port) and start listening for TCP connections
    let listener = TcpListener::bind(&args.bind).await?;
    info!("listening; waiting for incoming connections...");

    // 2) Accept loop: print whenever a client connects
    loop {
        match listener.accept().await {
            Ok((_socket, addr)) => {
                info!(?addr, "accepted a connection");
                // For now we immediately drop the socket; we just want to see it working.
            }
            Err(e) => {
                warn!(error=?e, "accept failed; continuing");
            }
        }
    }
}
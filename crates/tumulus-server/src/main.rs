use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use lloggs::LoggingArgs;
use tracing::info;

use tumulus_server::{api, db::UploadDb, storage::FsStorage};

#[derive(Parser)]
#[command(name = "tumulus-server")]
#[command(about = "Tumulus backup storage server")]
struct Args {
    /// Address to listen on
    #[arg(long, short, default_value = "127.0.0.1:3000")]
    listen: SocketAddr,

    /// Storage directory path
    #[arg(long, short)]
    storage: PathBuf,

    #[command(flatten)]
    logging: LoggingArgs,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();
    let _guard = args.logging.setup(|v| match v {
        0 => "warn",
        1 => "info",
        2 => "debug",
        _ => "trace",
    })?;

    info!(listen = %args.listen, storage = ?args.storage, "Starting server");

    // Initialize storage
    let storage = FsStorage::new(&args.storage);
    storage.init().await?;

    // Initialize upload tracking database
    let db_path = args.storage.join("uploads.db");
    let db = UploadDb::open(&db_path)?;
    info!(db_path = ?db_path, "Initialized upload tracking database");

    // Build router
    let app = api::router(storage, db);

    // Start server
    let listener = tokio::net::TcpListener::bind(&args.listen).await?;
    info!("Listening on {}", args.listen);
    axum::serve(listener, app).await?;

    Ok(())
}

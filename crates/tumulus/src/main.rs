use clap::{Parser, Subcommand};
use lloggs::LoggingArgs;

mod commands;

/// Tumulus - Snapshot catalog builder and uploader
#[derive(Parser)]
#[command(name = "tumulus")]
#[command(about = "Build and manage snapshot catalogs")]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    #[command(flatten)]
    logging: LoggingArgs,
}

#[derive(Subcommand)]
enum Commands {
    /// Build a snapshot catalog from a directory tree
    Catalog(commands::catalog::CatalogArgs),

    /// Compare two catalogs and report transfer requirements
    Compare(commands::compare::CompareArgs),

    /// Display extent information for files
    DebugExtents(commands::debug_extents::DebugExtentsArgs),

    /// Upload a catalog to a tumulus server
    Upload(commands::upload::UploadArgs),
}

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cli = Cli::parse();
    let _guard = cli.logging.setup(|v| match v {
        0 => "warn",
        1 => "info",
        2 => "debug",
        _ => "trace",
    })?;

    match cli.command {
        Commands::Catalog(args) => commands::catalog::run(args),
        Commands::Compare(args) => commands::compare::run(args),
        Commands::DebugExtents(args) => commands::debug_extents::run(args),
        Commands::Upload(args) => commands::upload::run(args),
    }
}

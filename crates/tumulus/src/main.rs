use std::fmt::Debug;

use diesel::insert_into;
use diesel_async::{AsyncMigrationHarness, pooled_connection::AsyncDieselConnectionManager};
use diesel_migrations::MigrationHarness;
use diesel_turso::AsyncTursoConnection;
use facet::Facet;
use jiff::Timestamp;
use miette::{IntoDiagnostic, Result, WrapErr, miette};
use mobc::Pool;
use uuid::Uuid;

mod models;
mod schema;

type DbPool = Pool<AsyncDieselConnectionManager<AsyncTursoConnection>>;

const MIGRATIONS: diesel_migrations::EmbeddedMigrations = diesel_migrations::embed_migrations!();

fn create_pool(database_path: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncTursoConnection>::new(database_path);
    Pool::new(manager)
}

async fn run_migrations(pool: DbPool) -> Result<()> {
    AsyncMigrationHarness::new(pool.get().await.into_diagnostic()?)
        .run_pending_migrations(MIGRATIONS)
        .map_err(|err| miette!("{err}"))
        .map(drop)
}

async fn insert_metadata<'facet, T>(
    conn: &mut AsyncTursoConnection,
    key: &str,
    value: &T,
) -> Result<()>
where
    T: Facet<'facet> + Debug,
{
    let metadata = models::Metadata::new(key, value)
        .wrap_err_with(|| format!("insert_metadata({key}, {value:?})"))?;
    diesel_async::RunQueryDsl::execute(insert_into(schema::metadata::table).values(&metadata), conn)
        .await
        .into_diagnostic()
        .wrap_err_with(|| format!("insert_metadata({key}, {value:?})"))
        .map(drop)
}

#[tokio::main]
async fn main() -> Result<()> {
    let database_path = "catalog.db";

    let pool = create_pool(database_path);
    run_migrations(pool.clone()).await?;

    let mut conn = pool.get().await.into_diagnostic()?;

    let catalog_id = format!("{:x}", Uuid::new_v4().as_u128());
    let machine_id = format!("{:x}", Uuid::new_v4().as_u128());
    let tree_hash = format!("{:x}", Uuid::new_v4().as_u128());
    let created = Timestamp::now().as_millisecond();

    insert_metadata(&mut conn, "protocol", &1).await?;
    insert_metadata(&mut conn, "id", &catalog_id).await?;
    insert_metadata(&mut conn, "machine", &machine_id).await?;
    insert_metadata(&mut conn, "tree", &tree_hash).await?;
    insert_metadata(&mut conn, "created", &created).await?;

    println!("Catalog database created successfully at {database_path}");
    println!("Catalog ID: {catalog_id}");
    println!("Machine ID: {machine_id}");
    println!("Tree Hash: {tree_hash}");
    println!("Created: {created}");

    Ok(())
}

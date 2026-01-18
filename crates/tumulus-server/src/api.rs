use std::sync::Arc;

use axum::Router;
use std::sync::Mutex;

use crate::db::UploadDb;
use crate::storage::Storage;

mod catalogs;
mod error;
mod extents;

pub use catalogs::{
    CatalogError, FinalizeResponse, InitiateRequest, InitiateResponse, UploadResponse,
};
pub use error::ErrorResponse;

pub struct AppState<S: Storage> {
    pub storage: Arc<S>,
    pub db: Arc<Mutex<UploadDb>>,
}

impl<S: Storage> Clone for AppState<S> {
    fn clone(&self) -> Self {
        Self {
            storage: Arc::clone(&self.storage),
            db: Arc::clone(&self.db),
        }
    }
}

pub fn router<S: Storage>(storage: S, db: UploadDb) -> Router {
    let state = AppState {
        storage: Arc::new(storage),
        db: Arc::new(Mutex::new(db)),
    };

    Router::new()
        .nest("/extents", extents::router())
        .nest("/catalogs", catalogs::router())
        .with_state(state)
}

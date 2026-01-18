use std::sync::Arc;

use axum::Router;

use crate::storage::Storage;

mod blobs;
mod catalogs;
mod error;
mod extents;

pub use error::ErrorResponse;

pub struct AppState<S: Storage> {
    pub storage: Arc<S>,
}

impl<S: Storage> Clone for AppState<S> {
    fn clone(&self) -> Self {
        Self {
            storage: Arc::clone(&self.storage),
        }
    }
}

pub fn router<S: Storage>(storage: S) -> Router {
    let state = AppState {
        storage: Arc::new(storage),
    };

    Router::new()
        .nest("/extents", extents::router())
        .nest("/blobs", blobs::router())
        .nest("/catalogs", catalogs::router())
        .with_state(state)
}

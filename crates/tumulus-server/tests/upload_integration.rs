//! Integration tests for the tumulus server upload flow.
//!
//! These tests start an in-process server and test the full upload flow
//! using HTTP requests.

#![allow(dead_code)]

use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use reqwest::blocking::Client;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::io::Write;
use tempfile::TempDir;
use tokio::sync::oneshot;
use uuid::Uuid;

use tumulus::{create_catalog_schema, process_file, write_catalog};
use tumulus_server::{FsStorage, UploadDb, router};

/// Request body for initiating a catalog upload.
#[derive(Debug, Serialize)]
struct InitiateRequest {
    id: Uuid,
    checksum: String,
}

/// Response from initiating a catalog upload.
#[derive(Debug, Deserialize)]
struct InitiateResponse {
    id: String,
    resuming: bool,
    #[serde(default)]
    missing_extents: Option<Vec<String>>,
}

/// Response from uploading a catalog.
#[derive(Debug, Deserialize)]
struct UploadResponse {
    missing_extents: Vec<String>,
}

/// Response from finalizing a catalog.
#[derive(Debug, Deserialize)]
struct FinalizeResponse {
    complete: bool,
    #[serde(default)]
    missing_extents: Option<Vec<String>>,
}

/// Error response from the server.
#[derive(Debug, Deserialize)]
struct ErrorResponse {
    error: String,
    #[serde(default)]
    detail: Option<String>,
}

/// Request for batch checking catalog existence.
#[derive(Debug, Serialize)]
struct CheckCatalogsRequest {
    ids: Vec<String>,
}

/// Response from checking catalog existence.
/// Returns catalog IDs sorted by preference (best choice first).
#[derive(Debug, Deserialize)]
struct CheckCatalogsResponse {
    existing: Vec<String>,
}

/// Test server handle that manages the server lifecycle.
struct TestServer {
    addr: SocketAddr,
    shutdown_tx: Option<oneshot::Sender<()>>,
    #[allow(dead_code)]
    runtime: Arc<tokio::runtime::Runtime>,
    _storage_dir: TempDir,
}

impl TestServer {
    /// Start a new test server with a temporary storage directory.
    fn start() -> Self {
        let runtime = Arc::new(tokio::runtime::Runtime::new().unwrap());

        // Create temporary storage directory
        let storage_dir = TempDir::new().expect("Failed to create temp storage dir");

        // Initialize storage and database
        let storage = FsStorage::new(storage_dir.path());
        runtime.block_on(async {
            storage.init().await.expect("Failed to init storage");
        });

        let db_path = storage_dir.path().join("uploads.db");
        let db = UploadDb::open(&db_path).expect("Failed to open upload db");

        // Build router
        let app = router(storage, db);

        // Bind to a random available port
        let listener = runtime.block_on(async {
            tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("Failed to bind")
        });
        let addr = listener.local_addr().expect("Failed to get local addr");

        // Create shutdown channel
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        // Spawn server in background
        let rt = Arc::clone(&runtime);
        std::thread::spawn(move || {
            rt.block_on(async move {
                axum::serve(listener, app)
                    .with_graceful_shutdown(async {
                        let _ = shutdown_rx.await;
                    })
                    .await
                    .expect("Server error");
            });
        });

        // Give server a moment to start
        std::thread::sleep(std::time::Duration::from_millis(50));

        TestServer {
            addr,
            shutdown_tx: Some(shutdown_tx),
            runtime,
            _storage_dir: storage_dir,
        }
    }

    fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    fn storage_path(&self) -> &Path {
        self._storage_dir.path()
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

/// Test fixture that creates a test source directory and catalog using the tumulus library.
struct TestFixture {
    _source_dir: TempDir,
    _catalog_dir: TempDir,
    catalog_path: std::path::PathBuf,
    catalog_id: Uuid,
    catalog_checksum: String,
    extent_ids: Vec<String>,
    /// The file contents used to create this fixture (for extent lookup)
    file_contents: Vec<(&'static str, &'static str)>,
}

impl TestFixture {
    /// Create a new test fixture with some test files.
    fn new() -> Self {
        Self::with_files(&[
            ("file1.txt", "Hello, world!"),
            ("file2.txt", "This is a test file with some content."),
            ("subdir/file3.txt", "Nested file content here."),
        ])
    }

    /// Create a test fixture with custom file contents.
    fn with_files(files: &[(&'static str, &'static str)]) -> Self {
        let source_dir = TempDir::new().expect("Failed to create source dir");
        let catalog_dir = TempDir::new().expect("Failed to create catalog dir");
        let catalog_path = catalog_dir.path().join("test.catalog");

        // Create the test files
        for (path, content) in files {
            let full_path = source_dir.path().join(path);
            if let Some(parent) = full_path.parent() {
                fs::create_dir_all(parent).unwrap();
            }
            fs::write(&full_path, content).unwrap();
        }

        // Create a catalog using the tumulus library
        let catalog_id = Uuid::new_v4();
        let conn = Connection::open(&catalog_path).expect("Failed to create catalog db");

        // Initialize schema using tumulus library
        create_catalog_schema(&conn).expect("Failed to create schema");

        // Insert metadata
        let machine_id = "test-machine-id";
        conn.execute(
            "INSERT INTO metadata (key, value) VALUES ('id', ?)",
            params![json!(catalog_id.simple().to_string()).to_string()],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO metadata (key, value) VALUES ('machine', ?)",
            params![json!(machine_id).to_string()],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO metadata (key, value) VALUES ('source_path', ?)",
            params![json!(source_dir.path().to_string_lossy()).to_string()],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO metadata (key, value) VALUES ('created', ?)",
            params![
                json!(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as i64
                )
                .to_string()
            ],
        )
        .unwrap();

        // Process files using tumulus library
        let mut file_infos = Vec::new();
        for (path, _content) in files {
            let full_path = source_dir.path().join(path);
            let file_info =
                process_file(&full_path, source_dir.path()).expect("Failed to process file");
            file_infos.push(file_info);
        }

        // Write catalog using tumulus library
        write_catalog(&conn, &file_infos).expect("Failed to write catalog");

        // Collect extent IDs from the catalog
        let extent_ids = {
            let mut stmt = conn
                .prepare("SELECT DISTINCT extent_id FROM blob_extents WHERE extent_id IS NOT NULL")
                .unwrap();
            let rows = stmt
                .query_map([], |row| {
                    let extent_id: Vec<u8> = row.get(0)?;
                    Ok(hex::encode(extent_id))
                })
                .unwrap();
            rows.map(|r| r.unwrap()).collect::<Vec<_>>()
        };

        drop(conn);

        // Compute catalog checksum
        let catalog_data = fs::read(&catalog_path).expect("Failed to read catalog");
        let catalog_checksum = blake3::hash(&catalog_data).to_hex().to_string();

        TestFixture {
            _source_dir: source_dir,
            _catalog_dir: catalog_dir,
            catalog_path,
            catalog_id,
            catalog_checksum,
            extent_ids,
            file_contents: files.to_vec(),
        }
    }

    fn catalog_data(&self) -> Vec<u8> {
        fs::read(&self.catalog_path).expect("Failed to read catalog")
    }

    /// Find extent data by hash from the source files.
    fn find_extent_data(&self, extent_id: &str) -> Vec<u8> {
        for (_path, content) in &self.file_contents {
            let hash = blake3::hash(content.as_bytes()).to_hex().to_string();
            if hash.to_lowercase() == extent_id.to_lowercase() {
                return content.as_bytes().to_vec();
            }
        }
        panic!("Extent {} not found in test files", extent_id);
    }
}

// ============================================================================
// Integration Tests
// ============================================================================

#[test]
fn test_initiate_new_catalog() {
    let server = TestServer::start();
    let client = Client::new();

    let catalog_id = Uuid::new_v4();
    // Checksum must be a valid 64-character hex string (32 bytes BLAKE3 hash)
    let checksum = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    let resp = client
        .post(format!("{}/catalogs", server.url()))
        .json(&InitiateRequest {
            id: catalog_id,
            checksum: checksum.to_string(),
        })
        .send()
        .expect("Request failed");

    assert!(resp.status().is_success(), "Status: {}", resp.status());

    let body: InitiateResponse = resp.json().expect("Failed to parse response");
    assert_eq!(body.id, catalog_id.simple().to_string());
    assert!(!body.resuming);
    assert!(body.missing_extents.is_none());
}

#[test]
fn test_full_upload_flow() {
    let server = TestServer::start();
    let fixture = TestFixture::new();
    let client = Client::new();

    // Step 1: Initiate upload
    let resp = client
        .post(format!("{}/catalogs", server.url()))
        .json(&InitiateRequest {
            id: fixture.catalog_id,
            checksum: fixture.catalog_checksum.clone(),
        })
        .send()
        .expect("Initiate request failed");

    assert!(resp.status().is_success());
    let init_resp: InitiateResponse = resp.json().expect("Failed to parse init response");
    assert!(!init_resp.resuming);

    // Step 2: Upload catalog
    let resp = client
        .put(format!(
            "{}/catalogs/{}",
            server.url(),
            fixture.catalog_id.simple()
        ))
        .header("Content-Type", "application/octet-stream")
        .body(fixture.catalog_data())
        .send()
        .expect("Upload request failed");

    assert!(
        resp.status().is_success(),
        "Upload failed: {:?}",
        resp.text()
    );

    // Re-read to get JSON
    let resp = client
        .put(format!(
            "{}/catalogs/{}",
            server.url(),
            fixture.catalog_id.simple()
        ))
        .header("Content-Type", "application/octet-stream")
        .body(fixture.catalog_data())
        .send()
        .expect("Upload request failed");

    let upload_resp: UploadResponse = resp.json().expect("Failed to parse upload response");

    // Should report all extents as missing
    assert_eq!(
        upload_resp.missing_extents.len(),
        fixture.extent_ids.len(),
        "Expected {} missing extents, got {}",
        fixture.extent_ids.len(),
        upload_resp.missing_extents.len()
    );

    // Step 3: Upload each extent
    for extent_id in &fixture.extent_ids {
        // Find the file content for this extent
        let extent_data = find_extent_data(&fixture, extent_id);

        let resp = client
            .put(format!(
                "{}/extents/{}",
                server.url(),
                extent_id.to_lowercase()
            ))
            .header("Content-Type", "application/octet-stream")
            .body(extent_data)
            .send()
            .expect("Extent upload failed");

        assert!(
            resp.status().is_success(),
            "Extent upload failed for {}: {:?}",
            extent_id,
            resp.text()
        );
    }

    // Step 4: Finalize
    let resp = client
        .post(format!(
            "{}/catalogs/{}",
            server.url(),
            fixture.catalog_id.simple()
        ))
        .send()
        .expect("Finalize request failed");

    // Should be 204 No Content (complete)
    assert_eq!(
        resp.status().as_u16(),
        204,
        "Expected 204, got {}",
        resp.status()
    );

    // Verify storage contains all expected files
    let catalog_path = server
        .storage_path()
        .join("catalogs")
        .join(fixture.catalog_id.simple().to_string());
    assert!(catalog_path.exists(), "Catalog not stored");

    for extent_id in &fixture.extent_ids {
        let extent_path = server
            .storage_path()
            .join("extents")
            .join(&extent_id[0..2])
            .join(&extent_id[2..4])
            .join(&extent_id[4..]);
        assert!(extent_path.exists(), "Extent {} not stored", extent_id);
    }
}

#[test]
fn test_resume_upload_no_missing_extents() {
    let server = TestServer::start();
    let fixture = TestFixture::new();
    let client = Client::new();

    // Complete a full upload first
    // Initiate
    client
        .post(format!("{}/catalogs", server.url()))
        .json(&InitiateRequest {
            id: fixture.catalog_id,
            checksum: fixture.catalog_checksum.clone(),
        })
        .send()
        .expect("Initiate failed");

    // Upload catalog
    client
        .put(format!(
            "{}/catalogs/{}",
            server.url(),
            fixture.catalog_id.simple()
        ))
        .body(fixture.catalog_data())
        .send()
        .expect("Upload failed");

    // Upload extents
    for extent_id in &fixture.extent_ids {
        let extent_data = find_extent_data(&fixture, extent_id);
        client
            .put(format!(
                "{}/extents/{}",
                server.url(),
                extent_id.to_lowercase()
            ))
            .body(extent_data)
            .send()
            .expect("Extent upload failed");
    }

    // Finalize
    let resp = client
        .post(format!(
            "{}/catalogs/{}",
            server.url(),
            fixture.catalog_id.simple()
        ))
        .send()
        .expect("Finalize failed");
    assert_eq!(resp.status().as_u16(), 204);

    // Now try to resume - should indicate already complete with no missing extents
    let resp = client
        .post(format!("{}/catalogs", server.url()))
        .json(&InitiateRequest {
            id: fixture.catalog_id,
            checksum: fixture.catalog_checksum.clone(),
        })
        .send()
        .expect("Resume initiate failed");

    assert!(resp.status().is_success());
    let resume_resp: InitiateResponse = resp.json().expect("Failed to parse resume response");

    assert!(resume_resp.resuming);
    assert!(
        resume_resp.missing_extents.is_none()
            || resume_resp.missing_extents.as_ref().unwrap().is_empty()
    );
}

#[test]
fn test_resume_upload_with_missing_extents() {
    let server = TestServer::start();
    let fixture = TestFixture::new();
    let client = Client::new();

    // Need at least 2 extents to test partial upload
    if fixture.extent_ids.len() < 2 {
        // Skip test if not enough extents
        return;
    }

    // Initiate and upload catalog but NOT extents
    client
        .post(format!("{}/catalogs", server.url()))
        .json(&InitiateRequest {
            id: fixture.catalog_id,
            checksum: fixture.catalog_checksum.clone(),
        })
        .send()
        .expect("Initiate failed");

    client
        .put(format!(
            "{}/catalogs/{}",
            server.url(),
            fixture.catalog_id.simple()
        ))
        .body(fixture.catalog_data())
        .send()
        .expect("Upload failed");

    // Upload only the first extent
    let extent_id = &fixture.extent_ids[0];
    let extent_data = find_extent_data(&fixture, extent_id);
    client
        .put(format!(
            "{}/extents/{}",
            server.url(),
            extent_id.to_lowercase()
        ))
        .body(extent_data)
        .send()
        .expect("Extent upload failed");

    // Now try to resume
    let resp = client
        .post(format!("{}/catalogs", server.url()))
        .json(&InitiateRequest {
            id: fixture.catalog_id,
            checksum: fixture.catalog_checksum.clone(),
        })
        .send()
        .expect("Resume initiate failed");

    let resume_resp: InitiateResponse = resp.json().expect("Failed to parse resume response");

    assert!(resume_resp.resuming);
    // Should have some but not all extents missing
    let missing = resume_resp.missing_extents.unwrap_or_default();

    // We uploaded 1 extent, so we should have (total - 1) missing
    let expected_missing = fixture.extent_ids.len() - 1;
    assert_eq!(
        missing.len(),
        expected_missing,
        "Expected {} missing extents after uploading 1, got {}",
        expected_missing,
        missing.len()
    );
}

#[test]
fn test_extent_hash_verification() {
    let server = TestServer::start();
    let client = Client::new();

    // Try to upload extent with wrong hash
    let extent_id = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    let wrong_data = b"This data does not match the hash";

    let resp = client
        .put(format!("{}/extents/{}", server.url(), extent_id))
        .header("Content-Type", "application/octet-stream")
        .body(wrong_data.to_vec())
        .send()
        .expect("Request failed");

    // Should fail with hash mismatch
    assert!(!resp.status().is_success());

    let error: ErrorResponse = resp.json().expect("Failed to parse error");
    assert!(
        error.error.contains("hash") || error.error.contains("mismatch"),
        "Expected hash error, got: {}",
        error.error
    );
}

#[test]
fn test_extent_already_exists() {
    let server = TestServer::start();
    let client = Client::new();

    // Create data and compute its hash
    let data = b"Test extent data for dedup test";
    let hash = blake3::hash(data);
    let extent_id = hash.to_hex().to_string();

    // Upload extent first time
    let resp = client
        .put(format!("{}/extents/{}", server.url(), extent_id))
        .body(data.to_vec())
        .send()
        .expect("First upload failed");

    assert_eq!(resp.status().as_u16(), 201, "Expected 201 Created");

    // Upload same extent again
    let resp = client
        .put(format!("{}/extents/{}", server.url(), extent_id))
        .body(data.to_vec())
        .send()
        .expect("Second upload failed");

    // Should succeed but indicate already existed
    assert!(resp.status().is_success());
    // Could be 200 OK (already exists) or 201 (re-created) depending on implementation
}

#[test]
fn test_finalize_with_missing_extents() {
    let server = TestServer::start();
    let fixture = TestFixture::new();
    let client = Client::new();

    // Initiate and upload catalog but NOT extents
    client
        .post(format!("{}/catalogs", server.url()))
        .json(&InitiateRequest {
            id: fixture.catalog_id,
            checksum: fixture.catalog_checksum.clone(),
        })
        .send()
        .expect("Initiate failed");

    client
        .put(format!(
            "{}/catalogs/{}",
            server.url(),
            fixture.catalog_id.simple()
        ))
        .body(fixture.catalog_data())
        .send()
        .expect("Upload failed");

    // Try to finalize without uploading extents
    let resp = client
        .post(format!(
            "{}/catalogs/{}",
            server.url(),
            fixture.catalog_id.simple()
        ))
        .send()
        .expect("Finalize request failed");

    // Should return 200 with missing extents list (not 204 which means complete)
    let status = resp.status().as_u16();
    if status == 200 {
        let finalize_resp: FinalizeResponse =
            resp.json().expect("Failed to parse finalize response");
        assert!(!finalize_resp.complete);
        assert!(finalize_resp.missing_extents.is_some());

        let missing = finalize_resp.missing_extents.unwrap();
        assert_eq!(missing.len(), fixture.extent_ids.len());
    } else if status == 204 {
        // If we get 204, it means there were no extents to upload (edge case)
        // This can happen if the fixture has no extents
        assert!(
            fixture.extent_ids.is_empty(),
            "Got 204 but fixture has {} extents",
            fixture.extent_ids.len()
        );
    } else {
        panic!("Unexpected status: {}", status);
    }
}

#[test]
fn test_catalog_checksum_mismatch() {
    let server = TestServer::start();
    let fixture = TestFixture::new();
    let client = Client::new();

    // First upload with correct checksum
    client
        .post(format!("{}/catalogs", server.url()))
        .json(&InitiateRequest {
            id: fixture.catalog_id,
            checksum: fixture.catalog_checksum.clone(),
        })
        .send()
        .expect("First initiate failed");

    client
        .put(format!(
            "{}/catalogs/{}",
            server.url(),
            fixture.catalog_id.simple()
        ))
        .body(fixture.catalog_data())
        .send()
        .expect("First upload failed");

    // Try to initiate again with different checksum (simulating modified catalog)
    let different_checksum = "different_checksum_value_1234567890abcdef";

    let resp = client
        .post(format!("{}/catalogs", server.url()))
        .json(&InitiateRequest {
            id: fixture.catalog_id,
            checksum: different_checksum.to_string(),
        })
        .send()
        .expect("Second initiate failed");

    // Server should either:
    // 1. Return a new ID (303-like behavior in the response)
    // 2. Or reject the request
    // Based on the implementation, it returns a different ID
    if resp.status().is_success() {
        let init_resp: InitiateResponse = resp.json().expect("Failed to parse response");
        // ID should be different from the original
        assert_ne!(
            init_resp.id,
            fixture.catalog_id.simple().to_string(),
            "Expected different ID for checksum mismatch"
        );
    }
}

#[test]
fn test_check_catalogs_empty() {
    let server = TestServer::start();
    let client = Client::new();

    // Check for catalogs that don't exist
    let check_req = CheckCatalogsRequest {
        ids: vec![
            Uuid::new_v4().simple().to_string(),
            Uuid::new_v4().simple().to_string(),
        ],
    };

    let resp = client
        .post(format!("{}/catalogs/check", server.url()))
        .json(&check_req)
        .send()
        .unwrap();

    assert!(resp.status().is_success());

    let check_resp: CheckCatalogsResponse = resp.json().unwrap();
    assert!(check_resp.existing.is_empty());
}

#[test]
fn test_check_catalogs_with_existing() {
    let server = TestServer::start();
    let client = Client::new();
    let fixture = TestFixture::new();

    // First, upload a catalog completely
    let catalog_data = fixture.catalog_data();

    // Initiate
    let init_req = InitiateRequest {
        id: fixture.catalog_id,
        checksum: fixture.catalog_checksum.clone(),
    };
    let resp = client
        .post(format!("{}/catalogs", server.url()))
        .json(&init_req)
        .send()
        .unwrap();
    assert!(resp.status().is_success());

    // Upload catalog
    let resp = client
        .put(format!(
            "{}/catalogs/{}",
            server.url(),
            fixture.catalog_id.simple()
        ))
        .header("Content-Type", "application/octet-stream")
        .body(catalog_data)
        .send()
        .unwrap();
    assert!(resp.status().is_success());
    let upload_resp: UploadResponse = resp.json().unwrap();

    // Upload extents
    for extent_id in &upload_resp.missing_extents {
        let extent_data = find_extent_data(&fixture, extent_id);
        let resp = client
            .put(format!(
                "{}/extents/{}",
                server.url(),
                extent_id.to_lowercase()
            ))
            .header("Content-Type", "application/octet-stream")
            .body(extent_data)
            .send()
            .unwrap();
        assert!(resp.status().is_success());
    }

    // Finalize
    let resp = client
        .post(format!(
            "{}/catalogs/{}",
            server.url(),
            fixture.catalog_id.simple()
        ))
        .send()
        .unwrap();
    assert_eq!(resp.status().as_u16(), 204);

    // Now check for this catalog plus some non-existent ones
    let check_req = CheckCatalogsRequest {
        ids: vec![
            fixture.catalog_id.simple().to_string(),
            Uuid::new_v4().simple().to_string(),
            Uuid::new_v4().simple().to_string(),
        ],
    };

    let resp = client
        .post(format!("{}/catalogs/check", server.url()))
        .json(&check_req)
        .send()
        .unwrap();

    assert!(resp.status().is_success());

    let check_resp: CheckCatalogsResponse = resp.json().unwrap();
    assert_eq!(check_resp.existing.len(), 1);
    assert_eq!(
        check_resp.existing[0].to_lowercase(),
        fixture.catalog_id.simple().to_string().to_lowercase()
    );
}

#[test]
fn test_patch_upload() {
    let server = TestServer::start();
    let client = Client::new();

    // Create reference fixture with initial files
    let reference_fixture = TestFixture::with_files(&[
        ("file1.txt", "Hello, world!"),
        ("file2.txt", "This is a test file with some content."),
        ("subdir/file3.txt", "Nested file content here."),
    ]);

    // Create target fixture with modified files (some same, some different)
    let target_fixture = TestFixture::with_files(&[
        ("file1.txt", "Hello, world!"), // Same as reference
        ("file2.txt", "This is MODIFIED content in the test file."), // Different
        ("subdir/file3.txt", "Nested file content here."), // Same as reference
        (
            "file4.txt",
            "A brand new file that wasn't in the reference.",
        ), // New file
    ]);

    // First, upload the reference catalog completely
    let reference_data = reference_fixture.catalog_data();

    // Initiate reference
    let init_req = InitiateRequest {
        id: reference_fixture.catalog_id,
        checksum: reference_fixture.catalog_checksum.clone(),
    };
    let resp = client
        .post(format!("{}/catalogs", server.url()))
        .json(&init_req)
        .send()
        .unwrap();
    assert!(resp.status().is_success());

    // Upload reference catalog
    let resp = client
        .put(format!(
            "{}/catalogs/{}",
            server.url(),
            reference_fixture.catalog_id.simple()
        ))
        .header("Content-Type", "application/octet-stream")
        .body(reference_data.clone())
        .send()
        .unwrap();
    assert!(resp.status().is_success());
    let upload_resp: UploadResponse = resp.json().unwrap();

    // Upload reference extents
    for extent_id in &upload_resp.missing_extents {
        let extent_data = reference_fixture.find_extent_data(extent_id);
        let resp = client
            .put(format!(
                "{}/extents/{}",
                server.url(),
                extent_id.to_lowercase()
            ))
            .header("Content-Type", "application/octet-stream")
            .body(extent_data)
            .send()
            .unwrap();
        assert!(resp.status().is_success());
    }

    // Finalize the reference catalog
    let resp = client
        .post(format!(
            "{}/catalogs/{}",
            server.url(),
            reference_fixture.catalog_id.simple()
        ))
        .send()
        .unwrap();
    assert_eq!(resp.status().as_u16(), 204);

    // Now upload the target catalog via patch
    let target_data = target_fixture.catalog_data();

    // Generate a binary patch from reference to target
    let mut patch_data = Vec::new();
    qbsdiff::Bsdiff::new(&reference_data, &target_data)
        .compare(&mut patch_data)
        .expect("Failed to generate patch");

    // Verify the patch is smaller than the full catalog (sanity check for similar catalogs)
    assert!(
        patch_data.len() < target_data.len(),
        "Patch should be smaller than full catalog for similar data"
    );

    // Compress the patch
    let mut compressed_patch = Vec::new();
    {
        let mut encoder = zstd::stream::Encoder::new(&mut compressed_patch, 3).unwrap();
        encoder.write_all(&patch_data).unwrap();
        encoder.finish().unwrap();
    }

    // Initiate the target catalog upload
    let init_req = InitiateRequest {
        id: target_fixture.catalog_id,
        checksum: target_fixture.catalog_checksum.clone(),
    };
    let resp = client
        .post(format!("{}/catalogs", server.url()))
        .json(&init_req)
        .send()
        .unwrap();
    assert!(resp.status().is_success());

    // Upload via patch endpoint
    let patch_url = format!(
        "{}/catalogs/{}/patch?reference={}&checksum={}",
        server.url(),
        target_fixture.catalog_id.simple(),
        reference_fixture.catalog_id.simple(),
        target_fixture.catalog_checksum
    );

    let resp = client
        .put(&patch_url)
        .header("Content-Type", "application/octet-stream")
        .body(compressed_patch)
        .send()
        .unwrap();

    assert!(
        resp.status().is_success(),
        "Patch upload failed: {:?}",
        resp.text()
    );

    let patch_resp: UploadResponse = resp.json().unwrap();

    // Should have some missing extents for the new/modified files
    // file2.txt was modified, file4.txt is new
    assert!(
        patch_resp.missing_extents.len() >= 2,
        "Expected at least 2 missing extents for modified and new files, got {}",
        patch_resp.missing_extents.len()
    );

    // Upload the missing extents from the target fixture
    for extent_id in &patch_resp.missing_extents {
        let extent_data = target_fixture.find_extent_data(extent_id);
        let resp = client
            .put(format!(
                "{}/extents/{}",
                server.url(),
                extent_id.to_lowercase()
            ))
            .header("Content-Type", "application/octet-stream")
            .body(extent_data)
            .send()
            .unwrap();
        assert!(resp.status().is_success());
    }

    // Finalize the patched catalog
    let resp = client
        .post(format!(
            "{}/catalogs/{}",
            server.url(),
            target_fixture.catalog_id.simple()
        ))
        .send()
        .unwrap();
    assert_eq!(resp.status().as_u16(), 204);

    // Verify the catalog was stored correctly by checking it exists
    let check_req = CheckCatalogsRequest {
        ids: vec![target_fixture.catalog_id.simple().to_string()],
    };
    let resp = client
        .post(format!("{}/catalogs/check", server.url()))
        .json(&check_req)
        .send()
        .unwrap();
    assert!(resp.status().is_success());
    let check_resp: CheckCatalogsResponse = resp.json().unwrap();
    assert_eq!(check_resp.existing.len(), 1);
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Find the content data for an extent by its hash.
fn find_extent_data(fixture: &TestFixture, extent_id: &str) -> Vec<u8> {
    fixture.find_extent_data(extent_id)
}

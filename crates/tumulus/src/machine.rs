//! Machine identification functionality.

use std::error::Error;

/// Get the unique machine identifier.
///
/// This uses the system's machine ID (e.g., `/etc/machine-id` on Linux).
/// Returns an error if the machine ID cannot be determined.
pub fn get_machine_id() -> Result<String, Box<dyn Error + Send + Sync>> {
    machine_uid::get().map_err(|e| format!("Failed to get machine ID: {}", e).into())
}

/// Get the hostname of the current machine.
pub fn get_hostname() -> Option<String> {
    hostname::get().ok().and_then(|h| h.into_string().ok())
}

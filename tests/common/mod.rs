// Common test utilities

use l3_object_storage::config::Config;
use l3_object_storage::storage::StorageEngine;
use tempfile::TempDir;

/// Create a test storage engine with a temporary directory
pub async fn create_test_storage() -> (StorageEngine, TempDir) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let config = Config::new().with_data_dir(temp_dir.path());
    let storage = StorageEngine::new(config)
        .await
        .expect("Failed to create storage engine");
    (storage, temp_dir)
}

/// Create a test storage engine with a specific region
pub async fn create_test_storage_with_region(region: &str) -> (StorageEngine, TempDir) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let config = Config::new()
        .with_data_dir(temp_dir.path())
        .with_region(region);
    let storage = StorageEngine::new(config)
        .await
        .expect("Failed to create storage engine");
    (storage, temp_dir)
}

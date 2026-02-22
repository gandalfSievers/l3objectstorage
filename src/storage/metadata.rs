//! Metadata storage for buckets and system configuration

use std::path::{Path, PathBuf};

/// Manages system-level metadata storage
pub struct MetadataStore {
    data_dir: PathBuf,
}

impl MetadataStore {
    /// Create a new metadata store
    pub fn new(data_dir: &Path) -> Self {
        Self {
            data_dir: data_dir.to_path_buf(),
        }
    }

    /// Get the system metadata directory
    pub fn system_dir(&self) -> PathBuf {
        self.data_dir.join(".system")
    }

    /// Initialize the metadata store
    pub async fn init(&self) -> std::io::Result<()> {
        tokio::fs::create_dir_all(self.system_dir()).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_metadata_store_init() {
        let temp_dir = TempDir::new().unwrap();
        let store = MetadataStore::new(temp_dir.path());

        store.init().await.unwrap();

        assert!(store.system_dir().exists());
    }
}

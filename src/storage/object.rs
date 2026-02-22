//! Object storage operations

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use bytes::Bytes;
use chrono::Utc;
use uuid::Uuid;

use crate::types::bucket::{AccessControlList, ObjectLegalHold, ObjectRetention, SseAlgorithm, TagSet, VersioningStatus};
use crate::types::error::{S3Error, S3ErrorCode, S3Result};
use crate::types::object::{
    CurrentVersionPointer, DeleteMarker, DeleteResult, ListObjectsResult, Object, ObjectMetadata,
    ObjectVersion,
};
use crate::utils::etag::calculate_etag;

/// Manages object storage operations
pub struct ObjectStore {
    data_dir: PathBuf,
}

impl ObjectStore {
    /// Create a new object store
    pub fn new(data_dir: &Path) -> Self {
        Self {
            data_dir: data_dir.to_path_buf(),
        }
    }

    /// Get the path to an object's data file
    fn object_data_path(&self, bucket: &str, key: &str) -> PathBuf {
        // Use a hash-based directory structure to avoid too many files in one dir
        let key_hash = hex::encode(&calculate_etag(key.as_bytes())[1..9]);
        self.data_dir
            .join("buckets")
            .join(bucket)
            .join("objects")
            .join(&key_hash[..2])
            .join(&key_hash[2..4])
            .join(Self::encode_key(key))
    }

    /// Get the path to an object's metadata file
    fn object_metadata_path(&self, bucket: &str, key: &str) -> PathBuf {
        let data_path = self.object_data_path(bucket, key);
        data_path.with_extension("meta.json")
    }

    /// Encode a key for use as a filename
    /// For very long keys, we use a hash to avoid file system limits
    fn encode_key(key: &str) -> String {
        // URL encode the key
        let encoded = percent_encoding::utf8_percent_encode(key, percent_encoding::NON_ALPHANUMERIC)
            .to_string();

        // If the encoded key is too long (file systems typically limit to 255 bytes),
        // use a hash instead
        if encoded.len() > 200 {
            // Use full hash of the key to ensure uniqueness
            format!("_hash_{}", hex::encode(calculate_etag(key.as_bytes())))
        } else {
            encoded
        }
    }

    // =========================================================================
    // Versioned Storage Helpers
    // =========================================================================

    /// Generate a new version ID
    /// Format: {timestamp_millis}.{uuid_suffix} for lexicographic ordering
    fn generate_version_id() -> String {
        let timestamp = Utc::now().timestamp_millis();
        let uuid_suffix = &Uuid::new_v4().to_string()[..8];
        format!("{}.{}", timestamp, uuid_suffix)
    }

    /// Get the versioned object directory
    fn object_version_dir(&self, bucket: &str, key: &str) -> PathBuf {
        let key_hash = hex::encode(&calculate_etag(key.as_bytes())[1..9]);
        self.data_dir
            .join("buckets")
            .join(bucket)
            .join("objects")
            .join(&key_hash[..2])
            .join(&key_hash[2..4])
            .join(Self::encode_key(key))
    }

    /// Get the versions subdirectory
    fn versions_dir(&self, bucket: &str, key: &str) -> PathBuf {
        self.object_version_dir(bucket, key).join("versions")
    }

    /// Get the current.json path
    fn current_pointer_path(&self, bucket: &str, key: &str) -> PathBuf {
        self.object_version_dir(bucket, key).join("current.json")
    }

    /// Get path for a specific version's data
    fn version_data_path(&self, bucket: &str, key: &str, version_id: &str) -> PathBuf {
        self.versions_dir(bucket, key).join(format!("{}.data", version_id))
    }

    /// Get path for a specific version's metadata
    fn version_metadata_path(&self, bucket: &str, key: &str, version_id: &str) -> PathBuf {
        self.versions_dir(bucket, key)
            .join(format!("{}.meta.json", version_id))
    }

    /// Check if an object uses the legacy (non-versioned) layout
    fn is_legacy_layout(&self, bucket: &str, key: &str) -> bool {
        let old_data_path = self.object_data_path(bucket, key);
        old_data_path.is_file()
    }

    /// Check if an object uses the versioned layout
    fn is_versioned_layout(&self, bucket: &str, key: &str) -> bool {
        let version_dir = self.object_version_dir(bucket, key);
        version_dir.is_dir() && self.current_pointer_path(bucket, key).exists()
    }

    /// Read the current version pointer
    async fn get_current_version(&self, bucket: &str, key: &str) -> S3Result<CurrentVersionPointer> {
        let pointer_path = self.current_pointer_path(bucket, key);
        if !pointer_path.exists() {
            return Err(S3Error::no_such_key(key));
        }
        let content = tokio::fs::read_to_string(&pointer_path).await?;
        let pointer: CurrentVersionPointer = serde_json::from_str(&content)?;
        Ok(pointer)
    }

    /// Write the current version pointer
    async fn set_current_version(
        &self,
        bucket: &str,
        key: &str,
        pointer: &CurrentVersionPointer,
    ) -> S3Result<()> {
        let pointer_path = self.current_pointer_path(bucket, key);
        if let Some(parent) = pointer_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&pointer_path, serde_json::to_string_pretty(pointer)?).await?;
        Ok(())
    }

    /// Migrate a legacy object to versioned layout
    #[allow(dead_code)]
    async fn migrate_to_versioned(&self, bucket: &str, key: &str) -> S3Result<()> {
        let old_data_path = self.object_data_path(bucket, key);
        let old_metadata_path = self.object_metadata_path(bucket, key);

        if !old_data_path.exists() {
            return Ok(()); // Nothing to migrate
        }

        // Read existing data and metadata
        let data = tokio::fs::read(&old_data_path).await?;
        let metadata_content = tokio::fs::read_to_string(&old_metadata_path).await?;
        let mut metadata: ObjectMetadata = serde_json::from_str(&metadata_content)?;

        // Use "null" as the version ID for pre-versioning objects
        let version_id = "null".to_string();
        metadata.version_id = Some(version_id.clone());

        // Create versioned directory structure
        let versions_dir = self.versions_dir(bucket, key);
        tokio::fs::create_dir_all(&versions_dir).await?;

        // Write to versioned location
        let new_data_path = self.version_data_path(bucket, key, &version_id);
        let new_metadata_path = self.version_metadata_path(bucket, key, &version_id);
        tokio::fs::write(&new_data_path, &data).await?;
        tokio::fs::write(&new_metadata_path, serde_json::to_string_pretty(&metadata)?).await?;

        // Set current pointer
        let pointer = CurrentVersionPointer::new(&version_id, false);
        self.set_current_version(bucket, key, &pointer).await?;

        // Remove old files
        tokio::fs::remove_file(&old_data_path).await.ok();
        tokio::fs::remove_file(&old_metadata_path).await.ok();

        Ok(())
    }

    // =========================================================================
    // Core Operations
    // =========================================================================

    /// Store an object (version-aware)
    pub async fn put(
        &self,
        bucket: &str,
        key: &str,
        data: Bytes,
        content_type: Option<&str>,
        metadata: Option<HashMap<String, String>>,
    ) -> S3Result<Object> {
        // Default: non-versioned behavior
        self.put_versioned(bucket, key, data, content_type, metadata, VersioningStatus::Disabled)
            .await
    }

    /// Store an object with versioning support
    pub async fn put_versioned(
        &self,
        bucket: &str,
        key: &str,
        data: Bytes,
        content_type: Option<&str>,
        metadata: Option<HashMap<String, String>>,
        versioning_status: VersioningStatus,
    ) -> S3Result<Object> {
        // Calculate ETag before writing
        let etag = calculate_etag(&data);
        let size = data.len() as u64;

        // Determine content type
        let content_type = content_type
            .map(String::from)
            .unwrap_or_else(|| {
                mime_guess::from_path(key)
                    .first_or_octet_stream()
                    .to_string()
            });

        match versioning_status {
            VersioningStatus::Enabled => {
                // Generate new version ID
                let version_id = Self::generate_version_id();

                // Create versioned directory structure
                let versions_dir = self.versions_dir(bucket, key);
                tokio::fs::create_dir_all(&versions_dir).await?;

                // Create object
                let object = Object {
                    key: key.to_string(),
                    size,
                    etag: etag.clone(),
                    last_modified: Utc::now(),
                    content_type: content_type.clone(),
                    storage_class: crate::types::object::StorageClass::Standard,
                    metadata: metadata.unwrap_or_default(),
                    version_id: Some(version_id.clone()),
                    is_delete_marker: false,
                    sse_algorithm: None,
                };

                // Create metadata
                let mut object_metadata = ObjectMetadata::from(object.clone());
                object_metadata.is_latest = true;

                // Mark previous version as not latest
                if let Ok(current) = self.get_current_version(bucket, key).await {
                    let prev_metadata_path =
                        self.version_metadata_path(bucket, key, &current.version_id);
                    if prev_metadata_path.exists() {
                        if let Ok(content) = tokio::fs::read_to_string(&prev_metadata_path).await {
                            if let Ok(mut prev_meta) =
                                serde_json::from_str::<ObjectMetadata>(&content)
                            {
                                prev_meta.is_latest = false;
                                tokio::fs::write(
                                    &prev_metadata_path,
                                    serde_json::to_string_pretty(&prev_meta)?,
                                )
                                .await
                                .ok();
                            }
                        }
                    }
                }

                // Write version data and metadata
                let data_path = self.version_data_path(bucket, key, &version_id);
                let metadata_path = self.version_metadata_path(bucket, key, &version_id);
                tokio::fs::write(&data_path, &data).await?;
                tokio::fs::write(&metadata_path, serde_json::to_string_pretty(&object_metadata)?)
                    .await?;

                // Update current pointer
                let pointer = CurrentVersionPointer::new(&version_id, false);
                self.set_current_version(bucket, key, &pointer).await?;

                Ok(object)
            }
            VersioningStatus::Suspended => {
                // Use "null" as version ID, overwrite any existing "null" version
                let version_id = "null".to_string();

                // Create versioned directory structure
                let versions_dir = self.versions_dir(bucket, key);
                tokio::fs::create_dir_all(&versions_dir).await?;

                // Create object
                let object = Object {
                    key: key.to_string(),
                    size,
                    etag: etag.clone(),
                    last_modified: Utc::now(),
                    content_type: content_type.clone(),
                    storage_class: crate::types::object::StorageClass::Standard,
                    metadata: metadata.unwrap_or_default(),
                    version_id: Some(version_id.clone()),
                    is_delete_marker: false,
                    sse_algorithm: None,
                };

                // Create metadata
                let mut object_metadata = ObjectMetadata::from(object.clone());
                object_metadata.is_latest = true;

                // Write version data and metadata (overwrites existing "null" version)
                let data_path = self.version_data_path(bucket, key, &version_id);
                let metadata_path = self.version_metadata_path(bucket, key, &version_id);
                tokio::fs::write(&data_path, &data).await?;
                tokio::fs::write(&metadata_path, serde_json::to_string_pretty(&object_metadata)?)
                    .await?;

                // Update current pointer
                let pointer = CurrentVersionPointer::new(&version_id, false);
                self.set_current_version(bucket, key, &pointer).await?;

                Ok(object)
            }
            VersioningStatus::Disabled => {
                // Legacy non-versioned behavior
                let data_path = self.object_data_path(bucket, key);
                let metadata_path = self.object_metadata_path(bucket, key);

                // Ensure parent directories exist
                if let Some(parent) = data_path.parent() {
                    tokio::fs::create_dir_all(parent).await?;
                }

                // Create object
                let object = Object {
                    key: key.to_string(),
                    size,
                    etag: etag.clone(),
                    last_modified: Utc::now(),
                    content_type: content_type.clone(),
                    storage_class: crate::types::object::StorageClass::Standard,
                    metadata: metadata.unwrap_or_default(),
                    version_id: None,
                    is_delete_marker: false,
                    sse_algorithm: None,
                };

                // Create metadata
                let object_metadata = ObjectMetadata::from(object.clone());

                // Write data and metadata
                tokio::fs::write(&data_path, &data).await?;
                tokio::fs::write(&metadata_path, serde_json::to_string_pretty(&object_metadata)?)
                    .await?;

                Ok(object)
            }
        }
    }

    /// Store an object with versioning support and SSE
    pub async fn put_versioned_with_sse(
        &self,
        bucket: &str,
        key: &str,
        data: Bytes,
        content_type: Option<&str>,
        metadata: Option<HashMap<String, String>>,
        versioning_status: VersioningStatus,
        sse_algorithm: Option<&SseAlgorithm>,
    ) -> S3Result<Object> {
        // Calculate ETag before writing
        let etag = calculate_etag(&data);
        let size = data.len() as u64;

        // Determine content type
        let content_type = content_type
            .map(String::from)
            .unwrap_or_else(|| {
                mime_guess::from_path(key)
                    .first_or_octet_stream()
                    .to_string()
            });

        // Convert SSE algorithm to string
        let sse_algorithm_str = sse_algorithm.map(|alg| alg.as_str().to_string());

        match versioning_status {
            VersioningStatus::Enabled => {
                // Generate new version ID
                let version_id = Self::generate_version_id();

                // Create versioned directory structure
                let versions_dir = self.versions_dir(bucket, key);
                tokio::fs::create_dir_all(&versions_dir).await?;

                // Create object
                let object = Object {
                    key: key.to_string(),
                    size,
                    etag: etag.clone(),
                    last_modified: Utc::now(),
                    content_type: content_type.clone(),
                    storage_class: crate::types::object::StorageClass::Standard,
                    metadata: metadata.unwrap_or_default(),
                    version_id: Some(version_id.clone()),
                    is_delete_marker: false,
                    sse_algorithm: sse_algorithm_str.clone(),
                };

                // Create metadata
                let mut object_metadata = ObjectMetadata::from(object.clone());
                object_metadata.is_latest = true;

                // Mark previous version as not latest
                if let Ok(current) = self.get_current_version(bucket, key).await {
                    let prev_metadata_path =
                        self.version_metadata_path(bucket, key, &current.version_id);
                    if prev_metadata_path.exists() {
                        if let Ok(content) = tokio::fs::read_to_string(&prev_metadata_path).await {
                            if let Ok(mut prev_meta) =
                                serde_json::from_str::<ObjectMetadata>(&content)
                            {
                                prev_meta.is_latest = false;
                                tokio::fs::write(
                                    &prev_metadata_path,
                                    serde_json::to_string_pretty(&prev_meta)?,
                                )
                                .await
                                .ok();
                            }
                        }
                    }
                }

                // Write version data and metadata
                let data_path = self.version_data_path(bucket, key, &version_id);
                let metadata_path = self.version_metadata_path(bucket, key, &version_id);
                tokio::fs::write(&data_path, &data).await?;
                tokio::fs::write(&metadata_path, serde_json::to_string_pretty(&object_metadata)?)
                    .await?;

                // Update current pointer
                let pointer = CurrentVersionPointer::new(&version_id, false);
                self.set_current_version(bucket, key, &pointer).await?;

                Ok(object)
            }
            VersioningStatus::Suspended => {
                // Use "null" as version ID, overwrite any existing "null" version
                let version_id = "null".to_string();

                // Create versioned directory structure
                let versions_dir = self.versions_dir(bucket, key);
                tokio::fs::create_dir_all(&versions_dir).await?;

                // Create object
                let object = Object {
                    key: key.to_string(),
                    size,
                    etag: etag.clone(),
                    last_modified: Utc::now(),
                    content_type: content_type.clone(),
                    storage_class: crate::types::object::StorageClass::Standard,
                    metadata: metadata.unwrap_or_default(),
                    version_id: Some(version_id.clone()),
                    is_delete_marker: false,
                    sse_algorithm: sse_algorithm_str.clone(),
                };

                // Create metadata
                let mut object_metadata = ObjectMetadata::from(object.clone());
                object_metadata.is_latest = true;

                // Write version data and metadata (overwrites existing "null" version)
                let data_path = self.version_data_path(bucket, key, &version_id);
                let metadata_path = self.version_metadata_path(bucket, key, &version_id);
                tokio::fs::write(&data_path, &data).await?;
                tokio::fs::write(&metadata_path, serde_json::to_string_pretty(&object_metadata)?)
                    .await?;

                // Update current pointer
                let pointer = CurrentVersionPointer::new(&version_id, false);
                self.set_current_version(bucket, key, &pointer).await?;

                Ok(object)
            }
            VersioningStatus::Disabled => {
                // Legacy non-versioned behavior
                let data_path = self.object_data_path(bucket, key);
                let metadata_path = self.object_metadata_path(bucket, key);

                // Ensure parent directories exist
                if let Some(parent) = data_path.parent() {
                    tokio::fs::create_dir_all(parent).await?;
                }

                // Create object
                let object = Object {
                    key: key.to_string(),
                    size,
                    etag: etag.clone(),
                    last_modified: Utc::now(),
                    content_type: content_type.clone(),
                    storage_class: crate::types::object::StorageClass::Standard,
                    metadata: metadata.unwrap_or_default(),
                    version_id: None,
                    is_delete_marker: false,
                    sse_algorithm: sse_algorithm_str,
                };

                // Create metadata
                let object_metadata = ObjectMetadata::from(object.clone());

                // Write data and metadata
                tokio::fs::write(&data_path, &data).await?;
                tokio::fs::write(&metadata_path, serde_json::to_string_pretty(&object_metadata)?)
                    .await?;

                Ok(object)
            }
        }
    }

    /// Get an object and its data
    pub async fn get(&self, bucket: &str, key: &str) -> S3Result<(Object, Bytes)> {
        self.get_versioned(bucket, key, None).await
    }

    /// Get an object with optional version ID
    pub async fn get_versioned(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> S3Result<(Object, Bytes)> {
        // Check for versioned layout first
        if self.is_versioned_layout(bucket, key) {
            let target_version_id = match version_id {
                Some(vid) => vid.to_string(),
                None => {
                    // Get current version
                    let current = self.get_current_version(bucket, key).await?;
                    if current.is_delete_marker {
                        // Current is a delete marker, return NoSuchKey
                        return Err(S3Error::no_such_key(key));
                    }
                    current.version_id
                }
            };

            let data_path = self.version_data_path(bucket, key, &target_version_id);
            let metadata_path = self.version_metadata_path(bucket, key, &target_version_id);

            if !data_path.exists() && !metadata_path.exists() {
                return Err(S3Error::no_such_key(key));
            }

            // Read metadata
            let metadata_content = tokio::fs::read_to_string(&metadata_path).await?;
            let metadata: ObjectMetadata = serde_json::from_str(&metadata_content)?;

            // Check if this is a delete marker
            if metadata.is_delete_marker {
                return Err(S3Error::no_such_key(key));
            }

            // Read data
            let data = tokio::fs::read(&data_path).await?;

            let object = Object {
                key: metadata.key,
                size: metadata.size,
                etag: metadata.etag,
                last_modified: metadata.last_modified,
                content_type: metadata.content_type,
                storage_class: metadata.storage_class,
                metadata: metadata.metadata,
                version_id: metadata.version_id,
                is_delete_marker: metadata.is_delete_marker,
                sse_algorithm: metadata.sse_algorithm,
            };

            Ok((object, Bytes::from(data)))
        } else if self.is_legacy_layout(bucket, key) {
            // Legacy non-versioned layout
            if version_id.is_some() {
                // Can't request specific version on non-versioned object
                return Err(S3Error::no_such_key(key));
            }

            let data_path = self.object_data_path(bucket, key);
            let metadata_path = self.object_metadata_path(bucket, key);

            // Read metadata
            let metadata_content = tokio::fs::read_to_string(&metadata_path).await?;
            let metadata: ObjectMetadata = serde_json::from_str(&metadata_content)?;

            // Read data
            let data = tokio::fs::read(&data_path).await?;

            let object = Object {
                key: metadata.key,
                size: metadata.size,
                etag: metadata.etag,
                last_modified: metadata.last_modified,
                content_type: metadata.content_type,
                storage_class: metadata.storage_class,
                metadata: metadata.metadata,
                version_id: metadata.version_id,
                is_delete_marker: false,
                sse_algorithm: metadata.sse_algorithm,
            };

            Ok((object, Bytes::from(data)))
        } else {
            Err(S3Error::no_such_key(key))
        }
    }

    /// Get object metadata only (for HEAD requests)
    pub async fn head(&self, bucket: &str, key: &str) -> S3Result<Object> {
        self.head_versioned(bucket, key, None).await
    }

    /// Get object metadata with optional version ID
    pub async fn head_versioned(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> S3Result<Object> {
        // Check for versioned layout first
        if self.is_versioned_layout(bucket, key) {
            let target_version_id = match version_id {
                Some(vid) => vid.to_string(),
                None => {
                    let current = self.get_current_version(bucket, key).await?;
                    if current.is_delete_marker {
                        return Err(S3Error::no_such_key(key));
                    }
                    current.version_id
                }
            };

            let metadata_path = self.version_metadata_path(bucket, key, &target_version_id);

            if !metadata_path.exists() {
                return Err(S3Error::no_such_key(key));
            }

            let metadata_content = tokio::fs::read_to_string(&metadata_path).await?;
            let metadata: ObjectMetadata = serde_json::from_str(&metadata_content)?;

            if metadata.is_delete_marker {
                return Err(S3Error::no_such_key(key));
            }

            Ok(Object {
                key: metadata.key,
                size: metadata.size,
                etag: metadata.etag,
                last_modified: metadata.last_modified,
                content_type: metadata.content_type,
                storage_class: metadata.storage_class,
                metadata: metadata.metadata,
                version_id: metadata.version_id,
                is_delete_marker: metadata.is_delete_marker,
                sse_algorithm: metadata.sse_algorithm,
            })
        } else if self.is_legacy_layout(bucket, key) {
            if version_id.is_some() {
                return Err(S3Error::no_such_key(key));
            }

            let metadata_path = self.object_metadata_path(bucket, key);

            let metadata_content = tokio::fs::read_to_string(&metadata_path).await?;
            let metadata: ObjectMetadata = serde_json::from_str(&metadata_content)?;

            Ok(Object {
                key: metadata.key,
                size: metadata.size,
                etag: metadata.etag,
                last_modified: metadata.last_modified,
                content_type: metadata.content_type,
                storage_class: metadata.storage_class,
                metadata: metadata.metadata,
                version_id: metadata.version_id,
                is_delete_marker: false,
                sse_algorithm: metadata.sse_algorithm,
            })
        } else {
            Err(S3Error::no_such_key(key))
        }
    }

    /// Delete an object (legacy non-versioned)
    pub async fn delete(&self, bucket: &str, key: &str) -> S3Result<()> {
        let result = self
            .delete_versioned(bucket, key, None, VersioningStatus::Disabled)
            .await?;
        // Ignore the result for backwards compatibility
        let _ = result;
        Ok(())
    }

    /// Delete an object with versioning support
    pub async fn delete_versioned(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        versioning_status: VersioningStatus,
    ) -> S3Result<DeleteResult> {
        match (versioning_status, version_id) {
            (VersioningStatus::Enabled, None) => {
                // Create delete marker as new version
                let delete_marker_id = Self::generate_version_id();

                // Create versioned directory structure if needed
                let versions_dir = self.versions_dir(bucket, key);
                tokio::fs::create_dir_all(&versions_dir).await?;

                // Mark previous current version as not latest
                if let Ok(current) = self.get_current_version(bucket, key).await {
                    let prev_metadata_path =
                        self.version_metadata_path(bucket, key, &current.version_id);
                    if prev_metadata_path.exists() {
                        if let Ok(content) = tokio::fs::read_to_string(&prev_metadata_path).await {
                            if let Ok(mut prev_meta) =
                                serde_json::from_str::<ObjectMetadata>(&content)
                            {
                                prev_meta.is_latest = false;
                                tokio::fs::write(
                                    &prev_metadata_path,
                                    serde_json::to_string_pretty(&prev_meta)?,
                                )
                                .await
                                .ok();
                            }
                        }
                    }
                }

                // Create delete marker metadata (no data file)
                let delete_marker_metadata = ObjectMetadata {
                    key: key.to_string(),
                    size: 0,
                    etag: String::new(),
                    last_modified: Utc::now(),
                    content_type: String::new(),
                    storage_class: crate::types::object::StorageClass::Standard,
                    metadata: HashMap::new(),
                    version_id: Some(delete_marker_id.clone()),
                    content_encoding: None,
                    content_disposition: None,
                    content_language: None,
                    cache_control: None,
                    expires: None,
                    tags: None,
                    acl: None,
                    is_delete_marker: true,
                    is_latest: true,
                    legal_hold: None,
                    retention: None,
                    sse_algorithm: None,
                    sse_nonce: None,
                };

                let metadata_path = self.version_metadata_path(bucket, key, &delete_marker_id);
                tokio::fs::write(&metadata_path, serde_json::to_string_pretty(&delete_marker_metadata)?)
                    .await?;

                // Update current pointer to delete marker
                let pointer = CurrentVersionPointer::new(&delete_marker_id, true);
                self.set_current_version(bucket, key, &pointer).await?;

                Ok(DeleteResult::delete_marker_created(delete_marker_id))
            }
            (VersioningStatus::Enabled, Some(vid)) | (VersioningStatus::Suspended, Some(vid)) => {
                // Delete specific version permanently
                let data_path = self.version_data_path(bucket, key, vid);
                let metadata_path = self.version_metadata_path(bucket, key, vid);

                // Remove files
                if data_path.exists() {
                    tokio::fs::remove_file(&data_path).await?;
                }
                if metadata_path.exists() {
                    tokio::fs::remove_file(&metadata_path).await?;
                }

                // If we deleted the current version, update the pointer
                if let Ok(current) = self.get_current_version(bucket, key).await {
                    if current.version_id == vid {
                        // Find the next most recent version
                        let versions_dir = self.versions_dir(bucket, key);
                        if let Ok(mut entries) = tokio::fs::read_dir(&versions_dir).await {
                            let mut version_files: Vec<String> = Vec::new();
                            while let Ok(Some(entry)) = entries.next_entry().await {
                                let name = entry.file_name().to_string_lossy().to_string();
                                if name.ends_with(".meta.json") {
                                    let ver = name.trim_end_matches(".meta.json").to_string();
                                    if ver != vid {
                                        version_files.push(ver);
                                    }
                                }
                            }
                            version_files.sort_by(|a, b| b.cmp(a)); // Descending order

                            if let Some(next_version) = version_files.first() {
                                // Read metadata to check if it's a delete marker
                                let next_metadata_path =
                                    self.version_metadata_path(bucket, key, next_version);
                                if let Ok(content) =
                                    tokio::fs::read_to_string(&next_metadata_path).await
                                {
                                    if let Ok(next_meta) =
                                        serde_json::from_str::<ObjectMetadata>(&content)
                                    {
                                        let pointer = CurrentVersionPointer::new(
                                            next_version,
                                            next_meta.is_delete_marker,
                                        );
                                        self.set_current_version(bucket, key, &pointer).await?;

                                        // Mark new current as latest
                                        let mut updated_meta = next_meta;
                                        updated_meta.is_latest = true;
                                        tokio::fs::write(
                                            &next_metadata_path,
                                            serde_json::to_string_pretty(&updated_meta)?,
                                        )
                                        .await
                                        .ok();
                                    }
                                }
                            } else {
                                // No more versions, remove the object directory
                                let obj_dir = self.object_version_dir(bucket, key);
                                tokio::fs::remove_dir_all(&obj_dir).await.ok();
                            }
                        }
                    }
                }

                Ok(DeleteResult::permanent_delete(Some(vid.to_string())))
            }
            (VersioningStatus::Suspended, None) => {
                // Create delete marker with version_id = "null"
                let delete_marker_id = "null".to_string();

                let versions_dir = self.versions_dir(bucket, key);
                tokio::fs::create_dir_all(&versions_dir).await?;

                // Remove existing "null" version if any
                let old_data = self.version_data_path(bucket, key, "null");
                let old_meta = self.version_metadata_path(bucket, key, "null");
                if old_data.exists() {
                    tokio::fs::remove_file(&old_data).await.ok();
                }
                if old_meta.exists() {
                    tokio::fs::remove_file(&old_meta).await.ok();
                }

                // Create delete marker metadata
                let delete_marker_metadata = ObjectMetadata {
                    key: key.to_string(),
                    size: 0,
                    etag: String::new(),
                    last_modified: Utc::now(),
                    content_type: String::new(),
                    storage_class: crate::types::object::StorageClass::Standard,
                    metadata: HashMap::new(),
                    version_id: Some(delete_marker_id.clone()),
                    content_encoding: None,
                    content_disposition: None,
                    content_language: None,
                    cache_control: None,
                    expires: None,
                    tags: None,
                    acl: None,
                    is_delete_marker: true,
                    is_latest: true,
                    legal_hold: None,
                    retention: None,
                    sse_algorithm: None,
                    sse_nonce: None,
                };

                let metadata_path = self.version_metadata_path(bucket, key, &delete_marker_id);
                tokio::fs::write(&metadata_path, serde_json::to_string_pretty(&delete_marker_metadata)?)
                    .await?;

                let pointer = CurrentVersionPointer::new(&delete_marker_id, true);
                self.set_current_version(bucket, key, &pointer).await?;

                Ok(DeleteResult::delete_marker_created(delete_marker_id))
            }
            (VersioningStatus::Disabled, _) => {
                // Legacy permanent delete
                let data_path = self.object_data_path(bucket, key);
                let metadata_path = self.object_metadata_path(bucket, key);

                if data_path.exists() {
                    tokio::fs::remove_file(&data_path).await?;
                }
                if metadata_path.exists() {
                    tokio::fs::remove_file(&metadata_path).await?;
                }

                // Also check for versioned layout and clean up if present
                let obj_dir = self.object_version_dir(bucket, key);
                if obj_dir.exists() {
                    tokio::fs::remove_dir_all(&obj_dir).await.ok();
                }

                Ok(DeleteResult::permanent_delete(None))
            }
        }
    }

    /// Check if an object exists (sync check)
    pub async fn exists(&self, bucket: &str, key: &str) -> bool {
        // Check versioned layout first
        if self.is_versioned_layout(bucket, key) {
            // Check if current version is not a delete marker
            if let Ok(current) = self.get_current_version(bucket, key).await {
                return !current.is_delete_marker;
            }
            return false;
        }
        // Check legacy layout
        let data_path = self.object_data_path(bucket, key);
        data_path.exists()
    }

    /// Check if an object exists (sync, for quick checks)
    pub fn exists_sync(&self, bucket: &str, key: &str) -> bool {
        // Quick sync check - doesn't verify delete markers
        self.is_versioned_layout(bucket, key) || self.is_legacy_layout(bucket, key)
    }

    /// List objects in a bucket with pagination and delimiter support
    pub async fn list(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        max_keys: i32,
        continuation_token: Option<&str>,
    ) -> S3Result<ListObjectsResult> {
        use std::collections::BTreeSet;

        let objects_dir = self.data_dir.join("buckets").join(bucket).join("objects");
        let mut all_objects = Vec::new();

        if !objects_dir.exists() {
            return Ok(ListObjectsResult::default());
        }

        // Recursively find all .meta.json files and collect ALL matching objects first
        let mut stack = vec![objects_dir];
        while let Some(dir) = stack.pop() {
            let mut entries = match tokio::fs::read_dir(&dir).await {
                Ok(entries) => entries,
                Err(_) => continue,
            };

            while let Some(entry) = entries.next_entry().await? {
                let path = entry.path();

                if path.is_dir() {
                    stack.push(path);
                } else if path.extension().and_then(|s| s.to_str()) == Some("json")
                    && path.to_string_lossy().contains(".meta.json")
                {
                    // Read metadata
                    if let Ok(content) = tokio::fs::read_to_string(&path).await {
                        if let Ok(metadata) = serde_json::from_str::<ObjectMetadata>(&content) {
                            // Apply prefix filter
                            if let Some(prefix) = prefix {
                                if !metadata.key.starts_with(prefix) {
                                    continue;
                                }
                            }

                            all_objects.push(Object {
                                key: metadata.key,
                                size: metadata.size,
                                etag: metadata.etag,
                                last_modified: metadata.last_modified,
                                content_type: metadata.content_type,
                                storage_class: metadata.storage_class,
                                metadata: metadata.metadata,
                                version_id: metadata.version_id,
                                is_delete_marker: false,
                                sse_algorithm: metadata.sse_algorithm,
                            });
                        }
                    }
                }
            }
        }

        // Sort by key for consistent ordering
        all_objects.sort_by(|a, b| a.key.cmp(&b.key));

        // Handle continuation token (skip objects before the token)
        let start_after = continuation_token.map(|t| {
            // Token is base64 encoded key
            String::from_utf8(base64::Engine::decode(
                &base64::engine::general_purpose::STANDARD,
                t,
            ).unwrap_or_default()).unwrap_or_default()
        });

        if let Some(ref start_key) = start_after {
            all_objects.retain(|obj| obj.key.as_str() > start_key.as_str());
        }

        // Handle delimiter - extract common prefixes
        let mut common_prefixes: BTreeSet<String> = BTreeSet::new();
        let prefix_len = prefix.map(|p| p.len()).unwrap_or(0);

        if let Some(delim) = delimiter {
            let mut filtered_objects = Vec::new();

            for obj in all_objects {
                // Look for delimiter after the prefix
                let key_after_prefix = &obj.key[prefix_len..];

                if let Some(delim_pos) = key_after_prefix.find(delim) {
                    // This key contains the delimiter after prefix - it's a "folder"
                    let common_prefix = format!(
                        "{}{}",
                        prefix.unwrap_or(""),
                        &key_after_prefix[..=delim_pos]
                    );
                    common_prefixes.insert(common_prefix);
                } else {
                    // No delimiter after prefix - it's a direct child
                    filtered_objects.push(obj);
                }
            }

            all_objects = filtered_objects;
        }

        // Apply pagination
        let total_count = all_objects.len() + common_prefixes.len();
        let is_truncated = total_count > max_keys as usize;

        // Truncate to max_keys
        all_objects.truncate(max_keys as usize);

        // Generate next continuation token if truncated
        let next_continuation_token = if is_truncated && !all_objects.is_empty() {
            let last_key = &all_objects.last().unwrap().key;
            Some(base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                last_key.as_bytes(),
            ))
        } else {
            None
        };

        Ok(ListObjectsResult {
            objects: all_objects,
            common_prefixes: common_prefixes.into_iter().collect(),
            is_truncated,
            next_continuation_token,
        })
    }

    /// List all versions of objects in a bucket
    pub async fn list_versions(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        _key_marker: Option<&str>,
        _version_id_marker: Option<&str>,
        max_keys: i32,
    ) -> S3Result<(Vec<ObjectVersion>, Vec<DeleteMarker>)> {
        let objects_dir = self.data_dir.join("buckets").join(bucket).join("objects");
        let mut versions: Vec<ObjectVersion> = Vec::new();
        let mut delete_markers: Vec<DeleteMarker> = Vec::new();

        if !objects_dir.exists() {
            return Ok((versions, delete_markers));
        }

        // Recursively find all metadata files (both legacy and versioned)
        // NOTE: We complete the full traversal first, then apply max_keys limit
        // to ensure prefix filtering works correctly across the entire bucket
        let mut stack = vec![objects_dir];
        while let Some(dir) = stack.pop() {
            let mut entries = match tokio::fs::read_dir(&dir).await {
                Ok(entries) => entries,
                Err(_) => continue,
            };

            while let Some(entry) = entries.next_entry().await? {
                let path = entry.path();

                if path.is_dir() {
                    // Check if this is a versioned object directory (has versions/ subdirectory)
                    let versions_subdir = path.join("versions");
                    if versions_subdir.exists() {
                        // Process versioned object
                        let mut version_entries = match tokio::fs::read_dir(&versions_subdir).await
                        {
                            Ok(entries) => entries,
                            Err(_) => continue,
                        };

                        while let Some(version_entry) = version_entries.next_entry().await? {
                            let version_path = version_entry.path();
                            if version_path
                                .extension()
                                .and_then(|s| s.to_str())
                                == Some("json")
                                && version_path.to_string_lossy().contains(".meta.json")
                            {
                                if let Ok(content) =
                                    tokio::fs::read_to_string(&version_path).await
                                {
                                    if let Ok(metadata) =
                                        serde_json::from_str::<ObjectMetadata>(&content)
                                    {
                                        // Apply prefix filter
                                        if let Some(prefix) = prefix {
                                            if !metadata.key.starts_with(prefix) {
                                                continue;
                                            }
                                        }

                                        if metadata.is_delete_marker {
                                            delete_markers.push(DeleteMarker {
                                                key: metadata.key,
                                                version_id: metadata.version_id.unwrap_or_default(),
                                                last_modified: metadata.last_modified,
                                                owner_id: "local-owner".to_string(),
                                                owner_display_name: "Local Owner".to_string(),
                                                is_latest: metadata.is_latest,
                                            });
                                        } else {
                                            versions.push(ObjectVersion {
                                                key: metadata.key,
                                                version_id: metadata.version_id.unwrap_or_default(),
                                                is_latest: metadata.is_latest,
                                                last_modified: metadata.last_modified,
                                                etag: metadata.etag,
                                                size: metadata.size,
                                                storage_class: metadata.storage_class,
                                                owner_id: "local-owner".to_string(),
                                                owner_display_name: "Local Owner".to_string(),
                                            });
                                        }
                                    }
                                }
                            }
                        }
                    } else {
                        // Regular directory, add to stack
                        stack.push(path);
                    }
                } else if path.extension().and_then(|s| s.to_str()) == Some("json")
                    && path.to_string_lossy().contains(".meta.json")
                {
                    // Legacy non-versioned object
                    if let Ok(content) = tokio::fs::read_to_string(&path).await {
                        if let Ok(metadata) = serde_json::from_str::<ObjectMetadata>(&content) {
                            // Apply prefix filter
                            if let Some(prefix) = prefix {
                                if !metadata.key.starts_with(prefix) {
                                    continue;
                                }
                            }

                            // Legacy objects have implicit "null" version and are latest
                            versions.push(ObjectVersion {
                                key: metadata.key,
                                version_id: "null".to_string(),
                                is_latest: true,
                                last_modified: metadata.last_modified,
                                etag: metadata.etag,
                                size: metadata.size,
                                storage_class: metadata.storage_class,
                                owner_id: "local-owner".to_string(),
                                owner_display_name: "Local Owner".to_string(),
                            });
                        }
                    }
                }
            }
        }

        // Sort versions by key, then by version_id descending (newest first)
        versions.sort_by(|a, b| {
            match a.key.cmp(&b.key) {
                std::cmp::Ordering::Equal => b.version_id.cmp(&a.version_id),
                other => other,
            }
        });

        delete_markers.sort_by(|a, b| {
            match a.key.cmp(&b.key) {
                std::cmp::Ordering::Equal => b.version_id.cmp(&a.version_id),
                other => other,
            }
        });

        // Apply max_keys limit after sorting
        let max = max_keys as usize;
        if versions.len() + delete_markers.len() > max {
            // Truncate to max_keys total
            let versions_to_take = std::cmp::min(versions.len(), max);
            versions.truncate(versions_to_take);
            let remaining = max.saturating_sub(versions_to_take);
            delete_markers.truncate(remaining);
        }

        Ok((versions, delete_markers))
    }

    /// Copy an object
    pub async fn copy(
        &self,
        source_bucket: &str,
        source_key: &str,
        dest_bucket: &str,
        dest_key: &str,
    ) -> S3Result<Object> {
        // Get source object
        let (source_obj, data) = self.get(source_bucket, source_key).await?;

        // Put to destination
        self.put(
            dest_bucket,
            dest_key,
            data,
            Some(&source_obj.content_type),
            Some(source_obj.metadata),
        )
        .await
    }

    /// Copy an object with SSE
    pub async fn copy_with_sse(
        &self,
        source_bucket: &str,
        source_key: &str,
        dest_bucket: &str,
        dest_key: &str,
        sse_algorithm: Option<&SseAlgorithm>,
    ) -> S3Result<Object> {
        // Get source object
        let (source_obj, data) = self.get(source_bucket, source_key).await?;

        // Put to destination with SSE
        self.put_versioned_with_sse(
            dest_bucket,
            dest_key,
            data,
            Some(&source_obj.content_type),
            Some(source_obj.metadata),
            VersioningStatus::Disabled,
            sse_algorithm,
        )
        .await
    }

    /// Copy an object with optional metadata replacement
    ///
    /// If custom_metadata is Some, it replaces the source object's metadata.
    /// If custom_metadata is None, the source object's metadata is preserved.
    pub async fn copy_with_metadata(
        &self,
        source_bucket: &str,
        source_key: &str,
        dest_bucket: &str,
        dest_key: &str,
        sse_algorithm: Option<&SseAlgorithm>,
        custom_metadata: Option<std::collections::HashMap<String, String>>,
        content_type: Option<&str>,
    ) -> S3Result<Object> {
        // Get source object
        let (source_obj, data) = self.get(source_bucket, source_key).await?;

        // Use custom metadata or source metadata
        let metadata = custom_metadata.unwrap_or(source_obj.metadata);
        let content_type = content_type.unwrap_or(&source_obj.content_type);

        // Put to destination with SSE
        self.put_versioned_with_sse(
            dest_bucket,
            dest_key,
            data,
            Some(content_type),
            Some(metadata),
            VersioningStatus::Disabled,
            sse_algorithm,
        )
        .await
    }

    /// Rename an object within a bucket
    ///
    /// This is an atomic rename operation that copies the object to the new key
    /// and then deletes the source. Metadata is preserved.
    pub async fn rename(
        &self,
        bucket: &str,
        source_key: &str,
        dest_key: &str,
    ) -> S3Result<Object> {
        // Get source object with all its data and metadata
        let (source_obj, data) = self.get(bucket, source_key).await?;

        // Put to destination with preserved metadata
        let dest_obj = self
            .put(
                bucket,
                dest_key,
                data,
                Some(&source_obj.content_type),
                Some(source_obj.metadata),
            )
            .await?;

        // Delete source object
        self.delete(bucket, source_key).await?;

        Ok(dest_obj)
    }

    /// Get tags for an object (supports both versioned and legacy layouts)
    pub async fn get_tags(&self, bucket: &str, key: &str) -> S3Result<TagSet> {
        // Get metadata path based on layout
        let metadata_path = if self.is_versioned_layout(bucket, key) {
            let current = self.get_current_version(bucket, key).await?;
            if current.is_delete_marker {
                return Err(S3Error::no_such_key(key));
            }
            self.version_metadata_path(bucket, key, &current.version_id)
        } else if self.is_legacy_layout(bucket, key) {
            self.object_metadata_path(bucket, key)
        } else {
            return Err(S3Error::no_such_key(key));
        };

        // Read metadata
        let metadata_content = tokio::fs::read_to_string(&metadata_path).await?;
        let metadata: ObjectMetadata = serde_json::from_str(&metadata_content)?;

        // Return tags or empty set (S3 returns empty TagSet, not error, when no tags)
        Ok(metadata.tags.unwrap_or_default())
    }

    /// Set tags for an object (supports both versioned and legacy layouts)
    pub async fn set_tags(&self, bucket: &str, key: &str, tags: TagSet) -> S3Result<()> {
        // Get metadata path based on layout
        let metadata_path = if self.is_versioned_layout(bucket, key) {
            let current = self.get_current_version(bucket, key).await?;
            if current.is_delete_marker {
                return Err(S3Error::no_such_key(key));
            }
            self.version_metadata_path(bucket, key, &current.version_id)
        } else if self.is_legacy_layout(bucket, key) {
            self.object_metadata_path(bucket, key)
        } else {
            return Err(S3Error::no_such_key(key));
        };

        // Read current metadata
        let metadata_content = tokio::fs::read_to_string(&metadata_path).await?;
        let mut metadata: ObjectMetadata = serde_json::from_str(&metadata_content)?;

        // Update tags
        metadata.tags = Some(tags);

        // Write updated metadata
        tokio::fs::write(&metadata_path, serde_json::to_string_pretty(&metadata)?).await?;

        Ok(())
    }

    /// Delete tags for an object (supports both versioned and legacy layouts)
    pub async fn delete_tags(&self, bucket: &str, key: &str) -> S3Result<()> {
        // Get metadata path based on layout
        let metadata_path = if self.is_versioned_layout(bucket, key) {
            let current = self.get_current_version(bucket, key).await?;
            if current.is_delete_marker {
                return Err(S3Error::no_such_key(key));
            }
            self.version_metadata_path(bucket, key, &current.version_id)
        } else if self.is_legacy_layout(bucket, key) {
            self.object_metadata_path(bucket, key)
        } else {
            return Err(S3Error::no_such_key(key));
        };

        // Read current metadata
        let metadata_content = tokio::fs::read_to_string(&metadata_path).await?;
        let mut metadata: ObjectMetadata = serde_json::from_str(&metadata_content)?;

        // Remove tags
        metadata.tags = None;

        // Write updated metadata
        tokio::fs::write(&metadata_path, serde_json::to_string_pretty(&metadata)?).await?;

        Ok(())
    }

    /// Get tags for a specific version of an object
    pub async fn get_tags_versioned(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> S3Result<TagSet> {
        // Determine metadata path based on version_id and layout
        let metadata_path = if self.is_versioned_layout(bucket, key) {
            let target_version_id = match version_id {
                Some(vid) => vid.to_string(),
                None => {
                    let current = self.get_current_version(bucket, key).await?;
                    if current.is_delete_marker {
                        return Err(S3Error::no_such_key(key));
                    }
                    current.version_id
                }
            };
            self.version_metadata_path(bucket, key, &target_version_id)
        } else if self.is_legacy_layout(bucket, key) {
            if version_id.is_some() {
                return Err(S3Error::no_such_key(key));
            }
            self.object_metadata_path(bucket, key)
        } else {
            return Err(S3Error::no_such_key(key));
        };

        // Read metadata
        let metadata_content = tokio::fs::read_to_string(&metadata_path).await?;
        let metadata: ObjectMetadata = serde_json::from_str(&metadata_content)?;

        // Return tags or empty set
        Ok(metadata.tags.unwrap_or_default())
    }

    /// Set tags for a specific version of an object
    pub async fn set_tags_versioned(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        tags: TagSet,
    ) -> S3Result<()> {
        // Determine metadata path based on version_id and layout
        let metadata_path = if self.is_versioned_layout(bucket, key) {
            let target_version_id = match version_id {
                Some(vid) => vid.to_string(),
                None => {
                    let current = self.get_current_version(bucket, key).await?;
                    if current.is_delete_marker {
                        return Err(S3Error::no_such_key(key));
                    }
                    current.version_id
                }
            };
            self.version_metadata_path(bucket, key, &target_version_id)
        } else if self.is_legacy_layout(bucket, key) {
            if version_id.is_some() {
                return Err(S3Error::no_such_key(key));
            }
            self.object_metadata_path(bucket, key)
        } else {
            return Err(S3Error::no_such_key(key));
        };

        // Read current metadata
        let metadata_content = tokio::fs::read_to_string(&metadata_path).await?;
        let mut metadata: ObjectMetadata = serde_json::from_str(&metadata_content)?;

        // Update tags
        metadata.tags = Some(tags);

        // Write updated metadata
        tokio::fs::write(&metadata_path, serde_json::to_string_pretty(&metadata)?).await?;

        Ok(())
    }

    /// Delete tags for a specific version of an object
    pub async fn delete_tags_versioned(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> S3Result<()> {
        // Determine metadata path based on version_id and layout
        let metadata_path = if self.is_versioned_layout(bucket, key) {
            let target_version_id = match version_id {
                Some(vid) => vid.to_string(),
                None => {
                    let current = self.get_current_version(bucket, key).await?;
                    if current.is_delete_marker {
                        return Err(S3Error::no_such_key(key));
                    }
                    current.version_id
                }
            };
            self.version_metadata_path(bucket, key, &target_version_id)
        } else if self.is_legacy_layout(bucket, key) {
            if version_id.is_some() {
                return Err(S3Error::no_such_key(key));
            }
            self.object_metadata_path(bucket, key)
        } else {
            return Err(S3Error::no_such_key(key));
        };

        // Read current metadata
        let metadata_content = tokio::fs::read_to_string(&metadata_path).await?;
        let mut metadata: ObjectMetadata = serde_json::from_str(&metadata_content)?;

        // Remove tags
        metadata.tags = None;

        // Write updated metadata
        tokio::fs::write(&metadata_path, serde_json::to_string_pretty(&metadata)?).await?;

        Ok(())
    }

    /// Get ACL for an object (supports both versioned and legacy layouts)
    pub async fn get_acl(&self, bucket: &str, key: &str) -> S3Result<AccessControlList> {
        // Get metadata path based on layout
        let metadata_path = if self.is_versioned_layout(bucket, key) {
            let current = self.get_current_version(bucket, key).await?;
            if current.is_delete_marker {
                return Err(S3Error::no_such_key(key));
            }
            self.version_metadata_path(bucket, key, &current.version_id)
        } else if self.is_legacy_layout(bucket, key) {
            self.object_metadata_path(bucket, key)
        } else {
            return Err(S3Error::no_such_key(key));
        };

        // Read metadata
        let metadata_content = tokio::fs::read_to_string(&metadata_path).await?;
        let metadata: ObjectMetadata = serde_json::from_str(&metadata_content)?;

        // Return ACL or default private ACL
        Ok(metadata.acl.unwrap_or_default())
    }

    /// Set ACL for an object (supports both versioned and legacy layouts)
    pub async fn set_acl(&self, bucket: &str, key: &str, acl: AccessControlList) -> S3Result<()> {
        // Get metadata path based on layout
        let metadata_path = if self.is_versioned_layout(bucket, key) {
            let current = self.get_current_version(bucket, key).await?;
            if current.is_delete_marker {
                return Err(S3Error::no_such_key(key));
            }
            self.version_metadata_path(bucket, key, &current.version_id)
        } else if self.is_legacy_layout(bucket, key) {
            self.object_metadata_path(bucket, key)
        } else {
            return Err(S3Error::no_such_key(key));
        };

        // Read current metadata
        let metadata_content = tokio::fs::read_to_string(&metadata_path).await?;
        let mut metadata: ObjectMetadata = serde_json::from_str(&metadata_content)?;

        // Update ACL
        metadata.acl = Some(acl);

        // Write updated metadata
        tokio::fs::write(&metadata_path, serde_json::to_string_pretty(&metadata)?).await?;

        Ok(())
    }

    // =========================================================================
    // Object Lock Operations
    // =========================================================================

    /// Get legal hold for an object
    pub async fn get_legal_hold(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> S3Result<ObjectLegalHold> {
        let metadata = self.get_metadata(bucket, key, version_id).await?;

        // Return legal hold or default (OFF)
        Ok(metadata.legal_hold.unwrap_or_default())
    }

    /// Set legal hold for an object
    pub async fn set_legal_hold(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        legal_hold: ObjectLegalHold,
    ) -> S3Result<()> {
        let metadata_path = self.get_metadata_path(bucket, key, version_id).await?;

        // Read current metadata
        let metadata_content = tokio::fs::read_to_string(&metadata_path).await?;
        let mut metadata: ObjectMetadata = serde_json::from_str(&metadata_content)?;

        // Update legal hold
        metadata.legal_hold = Some(legal_hold);

        // Write updated metadata
        tokio::fs::write(&metadata_path, serde_json::to_string_pretty(&metadata)?).await?;

        Ok(())
    }

    /// Get retention for an object
    pub async fn get_retention(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> S3Result<ObjectRetention> {
        let metadata = self.get_metadata(bucket, key, version_id).await?;

        metadata.retention.ok_or_else(|| {
            S3Error::new(
                S3ErrorCode::NoSuchObjectLockConfiguration,
                "The object does not have a retention configuration",
            )
        })
    }

    /// Set retention for an object
    pub async fn set_retention(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        retention: ObjectRetention,
    ) -> S3Result<()> {
        let metadata_path = self.get_metadata_path(bucket, key, version_id).await?;

        // Read current metadata
        let metadata_content = tokio::fs::read_to_string(&metadata_path).await?;
        let mut metadata: ObjectMetadata = serde_json::from_str(&metadata_content)?;

        // Update retention
        metadata.retention = Some(retention);

        // Write updated metadata
        tokio::fs::write(&metadata_path, serde_json::to_string_pretty(&metadata)?).await?;

        Ok(())
    }

    /// Helper to get metadata for an object
    async fn get_metadata(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> S3Result<ObjectMetadata> {
        let metadata_path = self.get_metadata_path(bucket, key, version_id).await?;
        let metadata_content = tokio::fs::read_to_string(&metadata_path).await?;
        let metadata: ObjectMetadata = serde_json::from_str(&metadata_content)?;
        Ok(metadata)
    }

    /// Helper to get metadata path for an object
    async fn get_metadata_path(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> S3Result<std::path::PathBuf> {
        if let Some(vid) = version_id {
            // Specific version requested
            let path = self.version_metadata_path(bucket, key, vid);
            if !path.exists() {
                return Err(S3Error::new(S3ErrorCode::NoSuchVersion, "The specified version does not exist"));
            }
            Ok(path)
        } else if self.is_versioned_layout(bucket, key) {
            // Get current version
            let current = self.get_current_version(bucket, key).await?;
            if current.is_delete_marker {
                return Err(S3Error::no_such_key(key));
            }
            Ok(self.version_metadata_path(bucket, key, &current.version_id))
        } else if self.is_legacy_layout(bucket, key) {
            Ok(self.object_metadata_path(bucket, key))
        } else {
            Err(S3Error::no_such_key(key))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    async fn create_test_store() -> (ObjectStore, TempDir) {
        let temp_dir = TempDir::new().unwrap();

        // Create bucket directory
        let bucket_dir = temp_dir.path().join("buckets").join("test-bucket").join("objects");
        tokio::fs::create_dir_all(&bucket_dir).await.unwrap();

        let store = ObjectStore::new(temp_dir.path());
        (store, temp_dir)
    }

    #[tokio::test]
    async fn test_put_and_get_object() {
        let (store, _temp) = create_test_store().await;

        let data = Bytes::from("hello world");
        let obj = store
            .put("test-bucket", "test-key", data.clone(), Some("text/plain"), None)
            .await
            .unwrap();

        assert_eq!(obj.key, "test-key");
        assert_eq!(obj.size, 11);
        assert_eq!(obj.content_type, "text/plain");

        let (retrieved_obj, retrieved_data) = store.get("test-bucket", "test-key").await.unwrap();

        assert_eq!(retrieved_obj.key, "test-key");
        assert_eq!(retrieved_data, data);
    }

    #[tokio::test]
    async fn test_head_object() {
        let (store, _temp) = create_test_store().await;

        let data = Bytes::from("test data");
        store
            .put("test-bucket", "test-key", data, None, None)
            .await
            .unwrap();

        let obj = store.head("test-bucket", "test-key").await.unwrap();

        assert_eq!(obj.key, "test-key");
        assert_eq!(obj.size, 9);
    }

    #[tokio::test]
    async fn test_delete_object() {
        let (store, _temp) = create_test_store().await;

        store
            .put("test-bucket", "test-key", Bytes::from("data"), None, None)
            .await
            .unwrap();

        assert!(store.exists("test-bucket", "test-key").await);

        store.delete("test-bucket", "test-key").await.unwrap();

        assert!(!store.exists("test-bucket", "test-key").await);
    }

    #[tokio::test]
    async fn test_get_nonexistent_object() {
        let (store, _temp) = create_test_store().await;

        let result = store.get("test-bucket", "nonexistent").await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_list_objects() {
        let (store, _temp) = create_test_store().await;

        store
            .put("test-bucket", "prefix/key1", Bytes::from("data1"), None, None)
            .await
            .unwrap();
        store
            .put("test-bucket", "prefix/key2", Bytes::from("data2"), None, None)
            .await
            .unwrap();
        store
            .put("test-bucket", "other/key3", Bytes::from("data3"), None, None)
            .await
            .unwrap();

        // List all
        let objects = store.list("test-bucket", None, None, 1000, None).await.unwrap();
        assert_eq!(objects.objects.len(), 3);

        // List with prefix
        let objects = store
            .list("test-bucket", Some("prefix/"), None, 1000, None)
            .await
            .unwrap();
        assert_eq!(objects.objects.len(), 2);
    }

    #[tokio::test]
    async fn test_copy_object() {
        let (store, temp) = create_test_store().await;

        // Create destination bucket
        let dest_bucket_dir = temp.path().join("buckets").join("dest-bucket").join("objects");
        tokio::fs::create_dir_all(&dest_bucket_dir).await.unwrap();

        store
            .put("test-bucket", "source-key", Bytes::from("copy me"), None, None)
            .await
            .unwrap();

        let copied = store
            .copy("test-bucket", "source-key", "dest-bucket", "dest-key")
            .await
            .unwrap();

        assert_eq!(copied.key, "dest-key");
        assert!(store.exists("dest-bucket", "dest-key").await);

        let (_, data) = store.get("dest-bucket", "dest-key").await.unwrap();
        assert_eq!(data, Bytes::from("copy me"));
    }

    #[tokio::test]
    async fn test_rename_object() {
        let (store, _temp) = create_test_store().await;

        // Put original object with metadata
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("custom-key".to_string(), "custom-value".to_string());
        store
            .put(
                "test-bucket",
                "original-key",
                Bytes::from("rename me"),
                Some("application/json"),
                Some(metadata),
            )
            .await
            .unwrap();

        // Rename the object
        let renamed = store
            .rename("test-bucket", "original-key", "new-key")
            .await
            .unwrap();

        // Verify new key exists with correct data
        assert_eq!(renamed.key, "new-key");
        assert!(store.exists("test-bucket", "new-key").await);

        let (obj, data) = store.get("test-bucket", "new-key").await.unwrap();
        assert_eq!(data, Bytes::from("rename me"));
        assert_eq!(obj.content_type, "application/json");
        assert_eq!(
            obj.metadata.get("custom-key"),
            Some(&"custom-value".to_string())
        );

        // Verify original key no longer exists
        assert!(!store.exists("test-bucket", "original-key").await);
    }

    #[tokio::test]
    async fn test_rename_object_not_found() {
        let (store, _temp) = create_test_store().await;

        // Try to rename non-existent object
        let result = store
            .rename("test-bucket", "nonexistent-key", "new-key")
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_object_with_metadata() {
        let (store, _temp) = create_test_store().await;

        let mut metadata = HashMap::new();
        metadata.insert("x-amz-meta-custom".to_string(), "value".to_string());

        store
            .put(
                "test-bucket",
                "test-key",
                Bytes::from("data"),
                Some("application/json"),
                Some(metadata),
            )
            .await
            .unwrap();

        let (obj, _) = store.get("test-bucket", "test-key").await.unwrap();

        assert_eq!(obj.content_type, "application/json");
        assert_eq!(
            obj.metadata.get("x-amz-meta-custom"),
            Some(&"value".to_string())
        );
    }

    #[tokio::test]
    async fn test_content_type_inference() {
        let (store, _temp) = create_test_store().await;

        // Without explicit content type, should infer from key
        store
            .put("test-bucket", "image.png", Bytes::from("data"), None, None)
            .await
            .unwrap();

        let obj = store.head("test-bucket", "image.png").await.unwrap();
        assert_eq!(obj.content_type, "image/png");
    }
}

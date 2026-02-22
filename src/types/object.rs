//! Object type definitions

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::bucket::{AccessControlList, ObjectLegalHold, ObjectRetention, TagSet};

/// Represents an S3 object
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Object {
    /// Object key (path)
    pub key: String,
    /// Size in bytes
    pub size: u64,
    /// ETag (usually MD5 hash, quoted)
    pub etag: String,
    /// Last modified timestamp
    pub last_modified: DateTime<Utc>,
    /// Content type
    pub content_type: String,
    /// Storage class
    pub storage_class: StorageClass,
    /// User-defined metadata
    pub metadata: HashMap<String, String>,
    /// Version ID (if versioning enabled)
    pub version_id: Option<String>,
    /// Whether this is a delete marker
    pub is_delete_marker: bool,
    /// Server-side encryption algorithm (e.g., "AES256")
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sse_algorithm: Option<String>,
}

impl Object {
    /// Create a new object with minimal required fields
    pub fn new(key: impl Into<String>, size: u64, etag: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            size,
            etag: etag.into(),
            last_modified: Utc::now(),
            content_type: "application/octet-stream".to_string(),
            storage_class: StorageClass::Standard,
            metadata: HashMap::new(),
            version_id: None,
            is_delete_marker: false,
            sse_algorithm: None,
        }
    }

    /// Set content type
    pub fn with_content_type(mut self, content_type: impl Into<String>) -> Self {
        self.content_type = content_type.into();
        self
    }

    /// Set metadata
    pub fn with_metadata(mut self, metadata: HashMap<String, String>) -> Self {
        self.metadata = metadata;
        self
    }

    /// Validate object key according to S3 rules
    pub fn validate_key(key: &str) -> Result<(), ObjectKeyError> {
        // Key cannot be empty
        if key.is_empty() {
            return Err(ObjectKeyError::Empty);
        }

        // Maximum key length is 1024 bytes
        if key.len() > 1024 {
            return Err(ObjectKeyError::TooLong);
        }

        Ok(())
    }
}

/// S3 storage classes
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum StorageClass {
    #[default]
    Standard,
    ReducedRedundancy,
    StandardIA,
    OnezoneIA,
    IntelligentTiering,
    Glacier,
    DeepArchive,
    GlacierIR,
}

impl StorageClass {
    pub fn as_str(&self) -> &'static str {
        match self {
            StorageClass::Standard => "STANDARD",
            StorageClass::ReducedRedundancy => "REDUCED_REDUNDANCY",
            StorageClass::StandardIA => "STANDARD_IA",
            StorageClass::OnezoneIA => "ONEZONE_IA",
            StorageClass::IntelligentTiering => "INTELLIGENT_TIERING",
            StorageClass::Glacier => "GLACIER",
            StorageClass::DeepArchive => "DEEP_ARCHIVE",
            StorageClass::GlacierIR => "GLACIER_IR",
        }
    }
}

impl std::fmt::Display for StorageClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Object key validation errors
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectKeyError {
    Empty,
    TooLong,
}

impl std::fmt::Display for ObjectKeyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ObjectKeyError::Empty => write!(f, "Object key cannot be empty"),
            ObjectKeyError::TooLong => write!(f, "Object key exceeds maximum length of 1024 bytes"),
        }
    }
}

impl std::error::Error for ObjectKeyError {}

/// Represents object metadata stored on disk
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectMetadata {
    pub key: String,
    pub size: u64,
    pub etag: String,
    pub last_modified: DateTime<Utc>,
    pub content_type: String,
    pub storage_class: StorageClass,
    pub metadata: HashMap<String, String>,
    pub version_id: Option<String>,
    pub content_encoding: Option<String>,
    pub content_disposition: Option<String>,
    pub content_language: Option<String>,
    pub cache_control: Option<String>,
    pub expires: Option<DateTime<Utc>>,
    /// Object tags
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tags: Option<TagSet>,
    /// Access Control List
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acl: Option<AccessControlList>,
    /// Whether this is a delete marker (for versioning)
    #[serde(default)]
    pub is_delete_marker: bool,
    /// Whether this is the latest version (for versioning)
    #[serde(default)]
    pub is_latest: bool,
    /// Legal hold status (Object Lock)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub legal_hold: Option<ObjectLegalHold>,
    /// Retention settings (Object Lock)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention: Option<ObjectRetention>,
    /// Server-side encryption algorithm used (e.g., "AES256")
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sse_algorithm: Option<String>,
    /// SSE nonce/IV for AES-256-GCM (base64 encoded, 12 bytes)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sse_nonce: Option<String>,
}

impl From<Object> for ObjectMetadata {
    fn from(obj: Object) -> Self {
        Self {
            key: obj.key,
            size: obj.size,
            etag: obj.etag,
            last_modified: obj.last_modified,
            content_type: obj.content_type,
            storage_class: obj.storage_class,
            metadata: obj.metadata,
            version_id: obj.version_id,
            content_encoding: None,
            content_disposition: None,
            content_language: None,
            cache_control: None,
            expires: None,
            tags: None,
            acl: Some(AccessControlList::default()),
            is_delete_marker: obj.is_delete_marker,
            is_latest: true, // When converting, assume it's the latest
            legal_hold: None,
            retention: None,
            sse_algorithm: obj.sse_algorithm,
            sse_nonce: None,
        }
    }
}

/// Represents a multipart upload in progress
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultipartUpload {
    pub upload_id: String,
    pub bucket: String,
    pub key: String,
    pub initiated: DateTime<Utc>,
    pub parts: Vec<UploadedPart>,
    /// Server-side encryption algorithm specified at creation time
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sse_algorithm: Option<String>,
}

impl MultipartUpload {
    pub fn new(bucket: impl Into<String>, key: impl Into<String>, upload_id: impl Into<String>) -> Self {
        Self {
            upload_id: upload_id.into(),
            bucket: bucket.into(),
            key: key.into(),
            initiated: Utc::now(),
            parts: Vec::new(),
            sse_algorithm: None,
        }
    }

    pub fn with_sse(mut self, sse_algorithm: Option<String>) -> Self {
        self.sse_algorithm = sse_algorithm;
        self
    }
}

/// Represents a single part of a multipart upload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadedPart {
    pub part_number: i32,
    pub etag: String,
    pub size: u64,
    pub last_modified: DateTime<Utc>,
}

/// Represents the current version pointer for versioned objects
/// Stored in current.json alongside the versions directory
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CurrentVersionPointer {
    /// The version ID of the current version
    pub version_id: String,
    /// Whether the current version is a delete marker
    pub is_delete_marker: bool,
}

impl CurrentVersionPointer {
    /// Create a new current version pointer
    pub fn new(version_id: impl Into<String>, is_delete_marker: bool) -> Self {
        Self {
            version_id: version_id.into(),
            is_delete_marker,
        }
    }
}

/// Represents a delete marker (tombstone for versioned deletes)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteMarker {
    /// Object key
    pub key: String,
    /// Version ID of the delete marker
    pub version_id: String,
    /// When the delete marker was created
    pub last_modified: DateTime<Utc>,
    /// Owner ID
    pub owner_id: String,
    /// Owner display name
    pub owner_display_name: String,
    /// Whether this is the latest version
    pub is_latest: bool,
}

impl DeleteMarker {
    /// Create a new delete marker
    pub fn new(key: impl Into<String>, version_id: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            version_id: version_id.into(),
            last_modified: Utc::now(),
            owner_id: "local-owner".to_string(),
            owner_display_name: "Local Owner".to_string(),
            is_latest: true,
        }
    }
}

/// Result of a delete operation in a versioned bucket
#[derive(Debug, Clone)]
pub struct DeleteResult {
    /// Version ID of the deleted object/marker (if versioned)
    pub version_id: Option<String>,
    /// Whether a delete marker was created (vs permanent delete)
    pub delete_marker: bool,
    /// Version ID of the delete marker (if created)
    pub delete_marker_version_id: Option<String>,
}

impl DeleteResult {
    /// Create a result for a permanent delete (non-versioned or specific version)
    pub fn permanent_delete(version_id: Option<String>) -> Self {
        Self {
            version_id,
            delete_marker: false,
            delete_marker_version_id: None,
        }
    }

    /// Create a result for a delete marker creation
    pub fn delete_marker_created(delete_marker_version_id: String) -> Self {
        Self {
            version_id: Some(delete_marker_version_id.clone()),
            delete_marker: true,
            delete_marker_version_id: Some(delete_marker_version_id),
        }
    }
}

/// Information about an object version (for ListObjectVersions)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectVersion {
    /// Object key
    pub key: String,
    /// Version ID
    pub version_id: String,
    /// Whether this is the latest version
    pub is_latest: bool,
    /// Last modified timestamp
    pub last_modified: DateTime<Utc>,
    /// ETag
    pub etag: String,
    /// Size in bytes
    pub size: u64,
    /// Storage class
    pub storage_class: StorageClass,
    /// Owner ID
    pub owner_id: String,
    /// Owner display name
    pub owner_display_name: String,
}

/// Result of listing objects, including pagination and delimiter info
#[derive(Debug, Clone)]
pub struct ListObjectsResult {
    /// Objects that match the listing criteria
    pub objects: Vec<Object>,
    /// Common prefixes when using delimiter (i.e., "folders")
    pub common_prefixes: Vec<String>,
    /// Whether there are more objects to list
    pub is_truncated: bool,
    /// Token for the next page of results
    pub next_continuation_token: Option<String>,
}

impl Default for ListObjectsResult {
    fn default() -> Self {
        Self {
            objects: Vec::new(),
            common_prefixes: Vec::new(),
            is_truncated: false,
            next_continuation_token: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_object_creation() {
        let obj = Object::new("my-key", 1024, "\"abc123\"");

        assert_eq!(obj.key, "my-key");
        assert_eq!(obj.size, 1024);
        assert_eq!(obj.etag, "\"abc123\"");
        assert_eq!(obj.content_type, "application/octet-stream");
        assert_eq!(obj.storage_class, StorageClass::Standard);
        assert!(!obj.is_delete_marker);
    }

    #[test]
    fn test_object_with_content_type() {
        let obj = Object::new("my-key", 1024, "\"abc123\"")
            .with_content_type("text/plain");

        assert_eq!(obj.content_type, "text/plain");
    }

    #[test]
    fn test_object_with_metadata() {
        let mut metadata = HashMap::new();
        metadata.insert("x-amz-meta-author".to_string(), "test".to_string());

        let obj = Object::new("my-key", 1024, "\"abc123\"")
            .with_metadata(metadata);

        assert_eq!(obj.metadata.get("x-amz-meta-author"), Some(&"test".to_string()));
    }

    #[test]
    fn test_valid_object_keys() {
        let valid_keys = vec![
            "simple-key",
            "path/to/object",
            "key with spaces",
            "key-with-special-chars!@#$%",
            "unicode-key-日本語",
            "/leading/slash",
            "trailing/slash/",
        ];

        for key in valid_keys {
            assert!(
                Object::validate_key(key).is_ok(),
                "Expected '{}' to be valid",
                key
            );
        }
    }

    #[test]
    fn test_empty_object_key() {
        assert_eq!(Object::validate_key(""), Err(ObjectKeyError::Empty));
    }

    #[test]
    fn test_object_key_too_long() {
        let long_key = "a".repeat(1025);
        assert_eq!(Object::validate_key(&long_key), Err(ObjectKeyError::TooLong));
    }

    #[test]
    fn test_storage_class() {
        assert_eq!(StorageClass::Standard.as_str(), "STANDARD");
        assert_eq!(StorageClass::Glacier.as_str(), "GLACIER");
        assert_eq!(format!("{}", StorageClass::Standard), "STANDARD");
    }

    #[test]
    fn test_multipart_upload_creation() {
        let upload = MultipartUpload::new("my-bucket", "my-key", "upload-123");

        assert_eq!(upload.bucket, "my-bucket");
        assert_eq!(upload.key, "my-key");
        assert_eq!(upload.upload_id, "upload-123");
        assert!(upload.parts.is_empty());
    }

    #[test]
    fn test_object_metadata_from_object() {
        let obj = Object::new("my-key", 1024, "\"abc123\"")
            .with_content_type("text/plain");

        let metadata: ObjectMetadata = obj.into();

        assert_eq!(metadata.key, "my-key");
        assert_eq!(metadata.size, 1024);
        assert_eq!(metadata.content_type, "text/plain");
    }
}

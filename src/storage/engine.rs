//! Core storage engine

use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::config::Config;
use crate::crypto::SseContext;
use crate::types::bucket::{AccessControlList, CannedAcl, SseAlgorithm, TagSet};
use crate::types::response::CorsConfiguration;
use crate::types::error::{S3Error, S3Result};
use crate::types::{Bucket, Object};

use super::bucket::BucketStore;
use super::metadata::MetadataStore;
use super::multipart::MultipartStore;
use super::object::ObjectStore;

/// Main storage engine that coordinates all storage operations
#[derive(Clone)]
pub struct StorageEngine {
    config: Config,
    bucket_store: Arc<RwLock<BucketStore>>,
    object_store: Arc<ObjectStore>,
    #[allow(dead_code)]
    metadata_store: Arc<MetadataStore>,
    multipart_store: Arc<MultipartStore>,
    /// Server-side encryption context (if encryption is enabled)
    sse_context: Option<Arc<SseContext>>,
}

impl StorageEngine {
    /// Create a new storage engine with the given configuration
    pub async fn new(config: Config) -> S3Result<Self> {
        // Ensure data directory exists
        tokio::fs::create_dir_all(&config.data_dir).await?;

        let bucket_store = BucketStore::new(&config.data_dir).await?;
        let object_store = ObjectStore::new(&config.data_dir);
        let metadata_store = MetadataStore::new(&config.data_dir);
        let multipart_store = MultipartStore::new(&config.data_dir);

        // Initialize SSE context if encryption key is configured
        let sse_context = config.encryption_key.as_ref().and_then(|key| {
            match SseContext::new(key.clone()) {
                Ok(ctx) => {
                    tracing::info!("Server-side encryption is ENABLED");
                    Some(Arc::new(ctx))
                }
                Err(e) => {
                    tracing::warn!("Failed to initialize SSE context: {:?}", e);
                    None
                }
            }
        });

        if sse_context.is_none() {
            tracing::info!("Server-side encryption is DISABLED (no encryption key configured)");
        }

        Ok(Self {
            config,
            bucket_store: Arc::new(RwLock::new(bucket_store)),
            object_store: Arc::new(object_store),
            metadata_store: Arc::new(metadata_store),
            multipart_store: Arc::new(multipart_store),
            sse_context,
        })
    }

    /// Get the configured region
    pub fn region(&self) -> &str {
        &self.config.region
    }

    /// Get the data directory
    pub fn data_dir(&self) -> &PathBuf {
        &self.config.data_dir
    }

    /// Check if server-side encryption is available
    pub fn encryption_enabled(&self) -> bool {
        self.sse_context.is_some()
    }

    /// Get the SSE context (if available)
    pub fn sse_context(&self) -> Option<&Arc<SseContext>> {
        self.sse_context.as_ref()
    }

    /// Determine the SSE algorithm to use for an object
    /// Returns the algorithm from the request header, or from the bucket default
    pub async fn get_sse_algorithm_for_object(
        &self,
        bucket: &str,
        sse_header: Option<&str>,
    ) -> S3Result<Option<SseAlgorithm>> {
        // If explicit header provided, use it
        if let Some(header) = sse_header {
            return SseAlgorithm::from_str(header)
                .map(Some)
                .ok_or_else(|| S3Error::new(
                    crate::types::error::S3ErrorCode::InvalidArgument,
                    format!("Invalid x-amz-server-side-encryption value: {}", header),
                ));
        }

        // Check bucket default encryption
        if let Ok(config) = self.get_bucket_encryption(bucket).await {
            if let Some(rule) = config.rules.first() {
                if let Some(default) = &rule.apply_server_side_encryption_by_default {
                    return Ok(Some(default.sse_algorithm));
                }
            }
        }

        Ok(None)
    }

    // Bucket operations

    /// Create a new bucket
    pub async fn create_bucket(&self, name: &str) -> S3Result<Bucket> {
        // Validate bucket name
        Bucket::validate_name(name).map_err(|e| S3Error::invalid_bucket_name(&e.to_string()))?;

        let mut store = self.bucket_store.write().await;
        store.create(name, &self.config.region).await
    }

    /// Delete a bucket
    pub async fn delete_bucket(&self, name: &str) -> S3Result<()> {
        // Check if bucket is empty
        let result = self.list_objects(name, None, None, 1, None).await?;
        if !result.objects.is_empty() || !result.common_prefixes.is_empty() {
            return Err(S3Error::bucket_not_empty(name));
        }

        let mut store = self.bucket_store.write().await;
        store.delete(name).await
    }

    /// Check if a bucket exists
    pub async fn bucket_exists(&self, name: &str) -> bool {
        let store = self.bucket_store.read().await;
        store.exists(name)
    }

    /// Get a bucket by name
    pub async fn get_bucket(&self, name: &str) -> S3Result<Bucket> {
        let store = self.bucket_store.read().await;
        store.get(name).ok_or_else(|| S3Error::no_such_bucket(name))
    }

    /// List all buckets
    pub async fn list_buckets(&self) -> Vec<Bucket> {
        let store = self.bucket_store.read().await;
        store.list()
    }

    /// Set tags on a bucket
    pub async fn set_bucket_tags(&self, name: &str, tags: TagSet) -> S3Result<()> {
        let mut store = self.bucket_store.write().await;
        store.set_tags(name, tags).await
    }

    /// Get tags from a bucket
    pub async fn get_bucket_tags(&self, name: &str) -> S3Result<TagSet> {
        let store = self.bucket_store.read().await;
        store.get_tags(name)
    }

    /// Delete tags from a bucket
    pub async fn delete_bucket_tags(&self, name: &str) -> S3Result<()> {
        let mut store = self.bucket_store.write().await;
        store.delete_tags(name).await
    }

    /// Set CORS configuration on a bucket
    pub async fn set_bucket_cors(&self, name: &str, cors: CorsConfiguration) -> S3Result<()> {
        let mut store = self.bucket_store.write().await;
        store.set_cors(name, cors).await
    }

    /// Get CORS configuration from a bucket
    pub async fn get_bucket_cors(&self, name: &str) -> S3Result<CorsConfiguration> {
        let store = self.bucket_store.read().await;
        store.get_cors(name)
    }

    /// Delete CORS configuration from a bucket
    pub async fn delete_bucket_cors(&self, name: &str) -> S3Result<()> {
        let mut store = self.bucket_store.write().await;
        store.delete_cors(name).await
    }

    /// Set versioning status on a bucket
    pub async fn set_bucket_versioning(
        &self,
        name: &str,
        status: crate::types::bucket::VersioningStatus,
    ) -> S3Result<()> {
        let mut store = self.bucket_store.write().await;
        store.set_versioning(name, status).await
    }

    /// Get versioning status from a bucket
    pub async fn get_bucket_versioning(
        &self,
        name: &str,
    ) -> S3Result<crate::types::bucket::VersioningStatus> {
        let store = self.bucket_store.read().await;
        store.get_versioning(name)
    }

    /// Set policy on a bucket
    pub async fn set_bucket_policy(&self, name: &str, policy: String) -> S3Result<()> {
        let mut store = self.bucket_store.write().await;
        store.set_policy(name, policy).await
    }

    /// Get policy from a bucket
    pub async fn get_bucket_policy(&self, name: &str) -> S3Result<String> {
        let store = self.bucket_store.read().await;
        store.get_policy(name)
    }

    /// Delete policy from a bucket
    pub async fn delete_bucket_policy(&self, name: &str) -> S3Result<()> {
        let mut store = self.bucket_store.write().await;
        store.delete_policy(name).await
    }

    /// Get ACL from a bucket
    pub async fn get_bucket_acl(&self, name: &str) -> S3Result<AccessControlList> {
        let store = self.bucket_store.read().await;
        store.get_acl(name)
    }

    /// Set ACL on a bucket
    pub async fn set_bucket_acl(&self, name: &str, acl: AccessControlList) -> S3Result<()> {
        let mut store = self.bucket_store.write().await;
        store.set_acl(name, acl).await
    }

    /// Create a bucket with a canned ACL
    pub async fn create_bucket_with_acl(
        &self,
        name: &str,
        canned_acl: Option<CannedAcl>,
    ) -> S3Result<Bucket> {
        // Validate bucket name
        Bucket::validate_name(name).map_err(|e| S3Error::invalid_bucket_name(&e.to_string()))?;

        let mut store = self.bucket_store.write().await;
        store.create_with_acl(name, &self.config.region, canned_acl).await
    }

    /// Set lifecycle configuration on a bucket
    pub async fn set_bucket_lifecycle(
        &self,
        name: &str,
        lifecycle: crate::types::bucket::LifecycleConfiguration,
    ) -> S3Result<()> {
        let mut store = self.bucket_store.write().await;
        store.set_lifecycle(name, lifecycle).await
    }

    /// Get lifecycle configuration from a bucket
    pub async fn get_bucket_lifecycle(
        &self,
        name: &str,
    ) -> S3Result<crate::types::bucket::LifecycleConfiguration> {
        let store = self.bucket_store.read().await;
        store.get_lifecycle(name)
    }

    /// Delete lifecycle configuration from a bucket
    pub async fn delete_bucket_lifecycle(&self, name: &str) -> S3Result<()> {
        let mut store = self.bucket_store.write().await;
        store.delete_lifecycle(name).await
    }

    /// Set encryption configuration for a bucket
    pub async fn set_bucket_encryption(
        &self,
        name: &str,
        encryption: crate::types::bucket::ServerSideEncryptionConfiguration,
    ) -> S3Result<()> {
        let mut store = self.bucket_store.write().await;
        store.set_encryption(name, encryption).await
    }

    /// Get encryption configuration from a bucket
    pub async fn get_bucket_encryption(
        &self,
        name: &str,
    ) -> S3Result<crate::types::bucket::ServerSideEncryptionConfiguration> {
        let store = self.bucket_store.read().await;
        store.get_encryption(name)
    }

    /// Delete encryption configuration from a bucket
    pub async fn delete_bucket_encryption(&self, name: &str) -> S3Result<()> {
        let mut store = self.bucket_store.write().await;
        store.delete_encryption(name).await
    }

    /// Set public access block configuration for a bucket
    pub async fn set_public_access_block(
        &self,
        name: &str,
        config: crate::types::bucket::PublicAccessBlockConfiguration,
    ) -> S3Result<()> {
        let mut store = self.bucket_store.write().await;
        store.set_public_access_block(name, config).await
    }

    /// Get public access block configuration from a bucket
    pub async fn get_public_access_block(
        &self,
        name: &str,
    ) -> S3Result<crate::types::bucket::PublicAccessBlockConfiguration> {
        let store = self.bucket_store.read().await;
        store.get_public_access_block(name)
    }

    /// Delete public access block configuration from a bucket
    pub async fn delete_public_access_block(&self, name: &str) -> S3Result<()> {
        let mut store = self.bucket_store.write().await;
        store.delete_public_access_block(name).await
    }

    /// Set website configuration for a bucket
    pub async fn set_bucket_website(
        &self,
        name: &str,
        config: crate::types::bucket::WebsiteConfiguration,
    ) -> S3Result<()> {
        let mut store = self.bucket_store.write().await;
        store.set_website(name, config).await
    }

    /// Get website configuration from a bucket
    pub async fn get_bucket_website(
        &self,
        name: &str,
    ) -> S3Result<crate::types::bucket::WebsiteConfiguration> {
        let store = self.bucket_store.read().await;
        store.get_website(name)
    }

    /// Delete website configuration from a bucket
    pub async fn delete_bucket_website(&self, name: &str) -> S3Result<()> {
        let mut store = self.bucket_store.write().await;
        store.delete_website(name).await
    }

    /// Set ownership controls for a bucket
    pub async fn set_ownership_controls(
        &self,
        name: &str,
        config: crate::types::bucket::OwnershipControls,
    ) -> S3Result<()> {
        let mut store = self.bucket_store.write().await;
        store.set_ownership_controls(name, config).await
    }

    /// Get ownership controls from a bucket
    pub async fn get_ownership_controls(
        &self,
        name: &str,
    ) -> S3Result<crate::types::bucket::OwnershipControls> {
        let store = self.bucket_store.read().await;
        store.get_ownership_controls(name)
    }

    /// Delete ownership controls from a bucket
    pub async fn delete_ownership_controls(&self, name: &str) -> S3Result<()> {
        let mut store = self.bucket_store.write().await;
        store.delete_ownership_controls(name).await
    }

    /// Set logging configuration for a bucket
    pub async fn set_bucket_logging(
        &self,
        name: &str,
        config: crate::types::bucket::LoggingConfiguration,
    ) -> S3Result<()> {
        let mut store = self.bucket_store.write().await;
        store.set_logging(name, config).await
    }

    /// Get logging configuration from a bucket
    pub async fn get_bucket_logging(
        &self,
        name: &str,
    ) -> S3Result<Option<crate::types::bucket::LoggingConfiguration>> {
        let store = self.bucket_store.read().await;
        store.get_logging(name)
    }

    /// Set notification configuration for a bucket
    pub async fn set_bucket_notification(
        &self,
        name: &str,
        config: crate::types::bucket::NotificationConfiguration,
    ) -> S3Result<()> {
        let mut store = self.bucket_store.write().await;
        store.set_notification(name, config).await
    }

    /// Get notification configuration from a bucket
    pub async fn get_bucket_notification(
        &self,
        name: &str,
    ) -> S3Result<crate::types::bucket::NotificationConfiguration> {
        let store = self.bucket_store.read().await;
        store.get_notification(name)
    }

    /// Set replication configuration for a bucket
    pub async fn set_bucket_replication(
        &self,
        name: &str,
        config: crate::types::bucket::ReplicationConfiguration,
    ) -> S3Result<()> {
        let mut store = self.bucket_store.write().await;
        store.set_replication(name, config).await
    }

    /// Get replication configuration from a bucket
    pub async fn get_bucket_replication(
        &self,
        name: &str,
    ) -> S3Result<crate::types::bucket::ReplicationConfiguration> {
        let store = self.bucket_store.read().await;
        store.get_replication(name)
    }

    /// Delete replication configuration from a bucket
    pub async fn delete_bucket_replication(&self, name: &str) -> S3Result<()> {
        let mut store = self.bucket_store.write().await;
        store.delete_replication(name).await
    }

    /// Set request payment configuration for a bucket
    pub async fn set_bucket_request_payment(
        &self,
        name: &str,
        config: crate::types::bucket::RequestPaymentConfiguration,
    ) -> S3Result<()> {
        let mut store = self.bucket_store.write().await;
        store.set_request_payment(name, config).await
    }

    /// Get request payment configuration from a bucket
    pub async fn get_bucket_request_payment(
        &self,
        name: &str,
    ) -> S3Result<crate::types::bucket::RequestPaymentConfiguration> {
        let store = self.bucket_store.read().await;
        store.get_request_payment(name)
    }

    // Object operations

    /// Store an object
    pub async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        data: bytes::Bytes,
        content_type: Option<&str>,
        metadata: Option<std::collections::HashMap<String, String>>,
    ) -> S3Result<Object> {
        // Check bucket exists
        if !self.bucket_exists(bucket).await {
            return Err(S3Error::no_such_bucket(bucket));
        }

        // Validate key
        Object::validate_key(key).map_err(|e| S3Error::new(
            crate::types::error::S3ErrorCode::InvalidArgument,
            e.to_string(),
        ))?;

        // Store the object
        self.object_store
            .put(bucket, key, data, content_type, metadata)
            .await
    }

    /// Get an object
    pub async fn get_object(&self, bucket: &str, key: &str) -> S3Result<(Object, bytes::Bytes)> {
        // Check bucket exists
        if !self.bucket_exists(bucket).await {
            return Err(S3Error::no_such_bucket(bucket));
        }

        self.object_store.get(bucket, key).await
    }

    /// Get object metadata only (for HEAD requests)
    pub async fn head_object(&self, bucket: &str, key: &str) -> S3Result<Object> {
        // Check bucket exists
        if !self.bucket_exists(bucket).await {
            return Err(S3Error::no_such_bucket(bucket));
        }

        self.object_store.head(bucket, key).await
    }

    /// Delete an object
    pub async fn delete_object(&self, bucket: &str, key: &str) -> S3Result<()> {
        // Check bucket exists
        if !self.bucket_exists(bucket).await {
            return Err(S3Error::no_such_bucket(bucket));
        }

        self.object_store.delete(bucket, key).await
    }

    /// Check if an object exists
    pub async fn object_exists(&self, bucket: &str, key: &str) -> bool {
        self.object_store.exists(bucket, key).await
    }

    /// List objects in a bucket
    pub async fn list_objects(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        max_keys: i32,
        continuation_token: Option<&str>,
    ) -> S3Result<crate::types::object::ListObjectsResult> {
        // Check bucket exists
        if !self.bucket_exists(bucket).await {
            return Err(S3Error::no_such_bucket(bucket));
        }

        self.object_store.list(bucket, prefix, delimiter, max_keys, continuation_token).await
    }

    /// Copy an object
    pub async fn copy_object(
        &self,
        source_bucket: &str,
        source_key: &str,
        dest_bucket: &str,
        dest_key: &str,
    ) -> S3Result<Object> {
        // Check source bucket exists
        if !self.bucket_exists(source_bucket).await {
            return Err(S3Error::no_such_bucket(source_bucket));
        }

        // Check destination bucket exists
        if !self.bucket_exists(dest_bucket).await {
            return Err(S3Error::no_such_bucket(dest_bucket));
        }

        self.object_store
            .copy(source_bucket, source_key, dest_bucket, dest_key)
            .await
    }

    /// Copy an object with SSE
    pub async fn copy_object_with_sse(
        &self,
        source_bucket: &str,
        source_key: &str,
        dest_bucket: &str,
        dest_key: &str,
        sse_algorithm: Option<&SseAlgorithm>,
    ) -> S3Result<Object> {
        self.copy_object_with_metadata(
            source_bucket, source_key, dest_bucket, dest_key,
            sse_algorithm, None, None,
        ).await
    }

    /// Copy an object with optional metadata replacement
    ///
    /// If custom_metadata is Some, it replaces the source object's metadata.
    /// If custom_metadata is None, the source object's metadata is preserved.
    pub async fn copy_object_with_metadata(
        &self,
        source_bucket: &str,
        source_key: &str,
        dest_bucket: &str,
        dest_key: &str,
        sse_algorithm: Option<&SseAlgorithm>,
        custom_metadata: Option<std::collections::HashMap<String, String>>,
        content_type: Option<&str>,
    ) -> S3Result<Object> {
        // Check source bucket exists
        if !self.bucket_exists(source_bucket).await {
            return Err(S3Error::no_such_bucket(source_bucket));
        }

        // Check destination bucket exists
        if !self.bucket_exists(dest_bucket).await {
            return Err(S3Error::no_such_bucket(dest_bucket));
        }

        self.object_store
            .copy_with_metadata(
                source_bucket, source_key, dest_bucket, dest_key,
                sse_algorithm, custom_metadata, content_type,
            )
            .await
    }

    /// Rename an object within a bucket
    ///
    /// This is an atomic rename operation that moves an object from one key to another
    /// within the same bucket, preserving all metadata.
    pub async fn rename_object(
        &self,
        bucket: &str,
        source_key: &str,
        dest_key: &str,
    ) -> S3Result<Object> {
        // Check bucket exists
        if !self.bucket_exists(bucket).await {
            return Err(S3Error::no_such_bucket(bucket));
        }

        // Validate destination key
        Object::validate_key(dest_key).map_err(|e| {
            S3Error::new(crate::types::error::S3ErrorCode::InvalidArgument, e.to_string())
        })?;

        self.object_store
            .rename(bucket, source_key, dest_key)
            .await
    }

    // Versioned object operations

    /// Store an object with versioning support
    pub async fn put_object_versioned(
        &self,
        bucket: &str,
        key: &str,
        data: bytes::Bytes,
        content_type: Option<&str>,
        metadata: Option<std::collections::HashMap<String, String>>,
    ) -> S3Result<Object> {
        // Check bucket exists
        if !self.bucket_exists(bucket).await {
            return Err(S3Error::no_such_bucket(bucket));
        }

        // Validate key
        Object::validate_key(key).map_err(|e| {
            S3Error::new(crate::types::error::S3ErrorCode::InvalidArgument, e.to_string())
        })?;

        // Get bucket versioning status
        let versioning_status = self.get_bucket_versioning(bucket).await?;

        // Store the object with versioning
        self.object_store
            .put_versioned(bucket, key, data, content_type, metadata, versioning_status)
            .await
    }

    /// Store an object with versioning support and SSE
    pub async fn put_object_versioned_with_sse(
        &self,
        bucket: &str,
        key: &str,
        data: bytes::Bytes,
        content_type: Option<&str>,
        metadata: Option<std::collections::HashMap<String, String>>,
        sse_algorithm: Option<&SseAlgorithm>,
    ) -> S3Result<Object> {
        // Check bucket exists
        if !self.bucket_exists(bucket).await {
            return Err(S3Error::no_such_bucket(bucket));
        }

        // Validate key
        Object::validate_key(key).map_err(|e| {
            S3Error::new(crate::types::error::S3ErrorCode::InvalidArgument, e.to_string())
        })?;

        // Get bucket versioning status
        let versioning_status = self.get_bucket_versioning(bucket).await?;

        // Store the object with versioning and SSE
        self.object_store
            .put_versioned_with_sse(bucket, key, data, content_type, metadata, versioning_status, sse_algorithm)
            .await
    }

    /// Get an object with optional version ID
    pub async fn get_object_versioned(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> S3Result<(Object, bytes::Bytes)> {
        // Check bucket exists
        if !self.bucket_exists(bucket).await {
            return Err(S3Error::no_such_bucket(bucket));
        }

        self.object_store.get_versioned(bucket, key, version_id).await
    }

    /// Get object metadata with optional version ID
    pub async fn head_object_versioned(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> S3Result<Object> {
        // Check bucket exists
        if !self.bucket_exists(bucket).await {
            return Err(S3Error::no_such_bucket(bucket));
        }

        self.object_store.head_versioned(bucket, key, version_id).await
    }

    /// Delete an object with versioning support
    pub async fn delete_object_versioned(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> S3Result<crate::types::object::DeleteResult> {
        // Check bucket exists
        if !self.bucket_exists(bucket).await {
            return Err(S3Error::no_such_bucket(bucket));
        }

        // Get bucket versioning status
        let versioning_status = self.get_bucket_versioning(bucket).await?;

        self.object_store
            .delete_versioned(bucket, key, version_id, versioning_status)
            .await
    }

    /// List all versions of objects in a bucket
    pub async fn list_object_versions(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        key_marker: Option<&str>,
        version_id_marker: Option<&str>,
        max_keys: i32,
    ) -> S3Result<(
        Vec<crate::types::object::ObjectVersion>,
        Vec<crate::types::object::DeleteMarker>,
    )> {
        // Check bucket exists
        if !self.bucket_exists(bucket).await {
            return Err(S3Error::no_such_bucket(bucket));
        }

        self.object_store
            .list_versions(bucket, prefix, key_marker, version_id_marker, max_keys)
            .await
    }

    // Multipart upload operations

    /// Create a new multipart upload
    pub async fn create_multipart_upload(&self, bucket: &str, key: &str) -> S3Result<String> {
        // Check bucket exists
        if !self.bucket_exists(bucket).await {
            return Err(S3Error::no_such_bucket(bucket));
        }

        // Validate key
        Object::validate_key(key).map_err(|e| S3Error::new(
            crate::types::error::S3ErrorCode::InvalidArgument,
            e.to_string(),
        ))?;

        self.multipart_store.create(bucket, key, None).await
    }

    /// Create a new multipart upload with SSE
    pub async fn create_multipart_upload_with_sse(
        &self,
        bucket: &str,
        key: &str,
        sse_algorithm: Option<&SseAlgorithm>,
    ) -> S3Result<String> {
        // Check bucket exists
        if !self.bucket_exists(bucket).await {
            return Err(S3Error::no_such_bucket(bucket));
        }

        // Validate key
        Object::validate_key(key).map_err(|e| S3Error::new(
            crate::types::error::S3ErrorCode::InvalidArgument,
            e.to_string(),
        ))?;

        let sse_str = sse_algorithm.map(|alg| alg.as_str().to_string());
        self.multipart_store.create(bucket, key, sse_str).await
    }

    /// Upload a part for a multipart upload
    pub async fn upload_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: i32,
        data: bytes::Bytes,
    ) -> S3Result<crate::types::object::UploadedPart> {
        // Check bucket exists
        if !self.bucket_exists(bucket).await {
            return Err(S3Error::no_such_bucket(bucket));
        }

        self.multipart_store
            .put_part(bucket, key, upload_id, part_number, data)
            .await
    }

    /// List parts for a multipart upload
    pub async fn list_parts(
        &self,
        bucket: &str,
        upload_id: &str,
    ) -> S3Result<Vec<crate::types::object::UploadedPart>> {
        // Check bucket exists
        if !self.bucket_exists(bucket).await {
            return Err(S3Error::no_such_bucket(bucket));
        }

        self.multipart_store.list_parts(bucket, upload_id).await
    }

    /// Complete a multipart upload
    pub async fn complete_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        parts: Vec<(i32, String)>,
    ) -> S3Result<Object> {
        // Check bucket exists
        if !self.bucket_exists(bucket).await {
            return Err(S3Error::no_such_bucket(bucket));
        }

        // Assemble parts and get final data
        let (data, etag) = self.multipart_store.complete(bucket, upload_id, parts).await?;

        // Check if versioning is enabled and use versioned put
        let versioning_status = self.get_bucket_versioning(bucket).await.unwrap_or_default();

        let obj = self
            .object_store
            .put_versioned(bucket, key, data, None, None, versioning_status)
            .await?;

        // Return object with the multipart ETag
        Ok(Object {
            etag,
            ..obj
        })
    }

    /// Complete a multipart upload with SSE
    pub async fn complete_multipart_upload_with_sse(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        parts: Vec<(i32, String)>,
        sse_algorithm: Option<&SseAlgorithm>,
    ) -> S3Result<Object> {
        // Check bucket exists
        if !self.bucket_exists(bucket).await {
            return Err(S3Error::no_such_bucket(bucket));
        }

        // Assemble parts and get final data
        let (data, etag) = self.multipart_store.complete(bucket, upload_id, parts).await?;

        // Check if versioning is enabled and use versioned put
        let versioning_status = self.get_bucket_versioning(bucket).await.unwrap_or_default();

        let obj = self
            .object_store
            .put_versioned_with_sse(bucket, key, data, None, None, versioning_status, sse_algorithm)
            .await?;

        // Return object with the multipart ETag
        Ok(Object {
            etag,
            ..obj
        })
    }

    /// Abort a multipart upload
    pub async fn abort_multipart_upload(
        &self,
        bucket: &str,
        upload_id: &str,
    ) -> S3Result<()> {
        // Check bucket exists
        if !self.bucket_exists(bucket).await {
            return Err(S3Error::no_such_bucket(bucket));
        }

        self.multipart_store.abort(bucket, upload_id).await
    }

    /// List multipart uploads for a bucket
    pub async fn list_multipart_uploads(
        &self,
        bucket: &str,
        prefix: Option<&str>,
    ) -> S3Result<Vec<crate::types::object::MultipartUpload>> {
        // Check bucket exists
        if !self.bucket_exists(bucket).await {
            return Err(S3Error::no_such_bucket(bucket));
        }

        self.multipart_store.list_uploads(bucket, prefix).await
    }

    /// Get a multipart upload
    pub async fn get_multipart_upload(
        &self,
        bucket: &str,
        upload_id: &str,
    ) -> S3Result<crate::types::object::MultipartUpload> {
        // Check bucket exists
        if !self.bucket_exists(bucket).await {
            return Err(S3Error::no_such_bucket(bucket));
        }

        self.multipart_store.get(bucket, upload_id).await
    }

    // Object tagging operations

    /// Get tags for an object
    pub async fn get_object_tags(&self, bucket: &str, key: &str) -> S3Result<TagSet> {
        // Check bucket exists
        if !self.bucket_exists(bucket).await {
            return Err(S3Error::no_such_bucket(bucket));
        }

        self.object_store.get_tags(bucket, key).await
    }

    /// Set tags for an object
    pub async fn set_object_tags(&self, bucket: &str, key: &str, tags: TagSet) -> S3Result<()> {
        // Check bucket exists
        if !self.bucket_exists(bucket).await {
            return Err(S3Error::no_such_bucket(bucket));
        }

        self.object_store.set_tags(bucket, key, tags).await
    }

    /// Delete tags for an object
    pub async fn delete_object_tags(&self, bucket: &str, key: &str) -> S3Result<()> {
        // Check bucket exists
        if !self.bucket_exists(bucket).await {
            return Err(S3Error::no_such_bucket(bucket));
        }

        self.object_store.delete_tags(bucket, key).await
    }

    /// Get tags for a specific version of an object
    pub async fn get_object_tags_versioned(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> S3Result<TagSet> {
        // Check bucket exists
        if !self.bucket_exists(bucket).await {
            return Err(S3Error::no_such_bucket(bucket));
        }

        self.object_store.get_tags_versioned(bucket, key, version_id).await
    }

    /// Set tags for a specific version of an object
    pub async fn set_object_tags_versioned(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        tags: TagSet,
    ) -> S3Result<()> {
        // Check bucket exists
        if !self.bucket_exists(bucket).await {
            return Err(S3Error::no_such_bucket(bucket));
        }

        self.object_store.set_tags_versioned(bucket, key, version_id, tags).await
    }

    /// Delete tags for a specific version of an object
    pub async fn delete_object_tags_versioned(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> S3Result<()> {
        // Check bucket exists
        if !self.bucket_exists(bucket).await {
            return Err(S3Error::no_such_bucket(bucket));
        }

        self.object_store.delete_tags_versioned(bucket, key, version_id).await
    }

    // Object ACL operations

    /// Get ACL for an object
    pub async fn get_object_acl(&self, bucket: &str, key: &str) -> S3Result<AccessControlList> {
        // Check bucket exists
        if !self.bucket_exists(bucket).await {
            return Err(S3Error::no_such_bucket(bucket));
        }

        self.object_store.get_acl(bucket, key).await
    }

    /// Set ACL for an object
    pub async fn set_object_acl(
        &self,
        bucket: &str,
        key: &str,
        acl: AccessControlList,
    ) -> S3Result<()> {
        // Check bucket exists
        if !self.bucket_exists(bucket).await {
            return Err(S3Error::no_such_bucket(bucket));
        }

        self.object_store.set_acl(bucket, key, acl).await
    }

    // =========================================================================
    // Object Lock Operations
    // =========================================================================

    /// Create a bucket with Object Lock enabled
    pub async fn create_bucket_with_object_lock(&self, name: &str) -> S3Result<Bucket> {
        Bucket::validate_name(name).map_err(|e| S3Error::invalid_bucket_name(&e.to_string()))?;

        let mut store = self.bucket_store.write().await;
        store.create_with_object_lock(name, &self.config.region).await
    }

    /// Create a bucket with Object Lock enabled and a canned ACL
    pub async fn create_bucket_with_object_lock_and_acl(
        &self,
        name: &str,
        acl: Option<CannedAcl>,
    ) -> S3Result<Bucket> {
        Bucket::validate_name(name).map_err(|e| S3Error::invalid_bucket_name(&e.to_string()))?;

        let mut store = self.bucket_store.write().await;
        store.create_with_object_lock_and_acl(name, &self.config.region, acl).await
    }

    /// Check if Object Lock is enabled for a bucket
    pub async fn is_object_lock_enabled(&self, name: &str) -> S3Result<bool> {
        let store = self.bucket_store.read().await;
        let bucket = store.get(name).ok_or_else(|| S3Error::no_such_bucket(name))?;
        Ok(bucket.object_lock_enabled)
    }

    /// Get Object Lock configuration for a bucket
    pub async fn get_object_lock_configuration(
        &self,
        name: &str,
    ) -> S3Result<crate::types::bucket::ObjectLockConfiguration> {
        let store = self.bucket_store.read().await;
        let bucket = store.get(name).ok_or_else(|| S3Error::no_such_bucket(name))?;

        if !bucket.object_lock_enabled {
            return Err(S3Error::new(
                crate::types::error::S3ErrorCode::ObjectLockConfigurationNotFoundError,
                "Object Lock configuration does not exist for this bucket",
            ));
        }

        bucket.object_lock_configuration.ok_or_else(|| {
            S3Error::new(
                crate::types::error::S3ErrorCode::ObjectLockConfigurationNotFoundError,
                "Object Lock configuration does not exist for this bucket",
            )
        })
    }

    /// Set Object Lock configuration for a bucket
    pub async fn set_object_lock_configuration(
        &self,
        name: &str,
        config: crate::types::bucket::ObjectLockConfiguration,
    ) -> S3Result<()> {
        let mut store = self.bucket_store.write().await;
        store.set_object_lock_configuration(name, config).await
    }

    /// Get legal hold for an object
    pub async fn get_object_legal_hold(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> S3Result<crate::types::bucket::ObjectLegalHold> {
        if !self.bucket_exists(bucket).await {
            return Err(S3Error::no_such_bucket(bucket));
        }

        self.object_store.get_legal_hold(bucket, key, version_id).await
    }

    /// Set legal hold for an object
    pub async fn set_object_legal_hold(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        legal_hold: crate::types::bucket::ObjectLegalHold,
    ) -> S3Result<()> {
        if !self.bucket_exists(bucket).await {
            return Err(S3Error::no_such_bucket(bucket));
        }

        self.object_store
            .set_legal_hold(bucket, key, version_id, legal_hold)
            .await
    }

    /// Get retention for an object
    pub async fn get_object_retention(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> S3Result<crate::types::bucket::ObjectRetention> {
        if !self.bucket_exists(bucket).await {
            return Err(S3Error::no_such_bucket(bucket));
        }

        self.object_store.get_retention(bucket, key, version_id).await
    }

    /// Set retention for an object
    pub async fn set_object_retention(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        retention: crate::types::bucket::ObjectRetention,
    ) -> S3Result<()> {
        if !self.bucket_exists(bucket).await {
            return Err(S3Error::no_such_bucket(bucket));
        }

        self.object_store
            .set_retention(bucket, key, version_id, retention)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    async fn create_test_engine() -> (StorageEngine, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::new().with_data_dir(temp_dir.path());
        let engine = StorageEngine::new(config).await.unwrap();
        (engine, temp_dir)
    }

    #[tokio::test]
    async fn test_create_bucket() {
        let (engine, _temp) = create_test_engine().await;

        let bucket = engine.create_bucket("test-bucket").await.unwrap();

        assert_eq!(bucket.name, "test-bucket");
        assert!(engine.bucket_exists("test-bucket").await);
    }

    #[tokio::test]
    async fn test_create_bucket_invalid_name() {
        let (engine, _temp) = create_test_engine().await;

        let result = engine.create_bucket("ab").await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_delete_bucket() {
        let (engine, _temp) = create_test_engine().await;

        engine.create_bucket("test-bucket").await.unwrap();
        engine.delete_bucket("test-bucket").await.unwrap();

        assert!(!engine.bucket_exists("test-bucket").await);
    }

    #[tokio::test]
    async fn test_delete_bucket_not_empty() {
        let (engine, _temp) = create_test_engine().await;

        engine.create_bucket("test-bucket").await.unwrap();
        engine
            .put_object("test-bucket", "test-key", bytes::Bytes::from("data"), None, None)
            .await
            .unwrap();

        let result = engine.delete_bucket("test-bucket").await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_list_buckets() {
        let (engine, _temp) = create_test_engine().await;

        engine.create_bucket("bucket1").await.unwrap();
        engine.create_bucket("bucket2").await.unwrap();

        let buckets = engine.list_buckets().await;

        assert_eq!(buckets.len(), 2);
    }

    #[tokio::test]
    async fn test_put_and_get_object() {
        let (engine, _temp) = create_test_engine().await;

        engine.create_bucket("test-bucket").await.unwrap();

        let data = bytes::Bytes::from("hello world");
        let obj = engine
            .put_object("test-bucket", "test-key", data.clone(), Some("text/plain"), None)
            .await
            .unwrap();

        assert_eq!(obj.key, "test-key");
        assert_eq!(obj.size, 11);
        assert_eq!(obj.content_type, "text/plain");

        let (retrieved_obj, retrieved_data) = engine.get_object("test-bucket", "test-key").await.unwrap();

        assert_eq!(retrieved_obj.key, "test-key");
        assert_eq!(retrieved_data, data);
    }

    #[tokio::test]
    async fn test_delete_object() {
        let (engine, _temp) = create_test_engine().await;

        engine.create_bucket("test-bucket").await.unwrap();
        engine
            .put_object("test-bucket", "test-key", bytes::Bytes::from("data"), None, None)
            .await
            .unwrap();

        engine.delete_object("test-bucket", "test-key").await.unwrap();

        assert!(!engine.object_exists("test-bucket", "test-key").await);
    }

    #[tokio::test]
    async fn test_list_objects() {
        let (engine, _temp) = create_test_engine().await;

        engine.create_bucket("test-bucket").await.unwrap();
        engine
            .put_object("test-bucket", "key1", bytes::Bytes::from("data1"), None, None)
            .await
            .unwrap();
        engine
            .put_object("test-bucket", "key2", bytes::Bytes::from("data2"), None, None)
            .await
            .unwrap();

        let objects = engine.list_objects("test-bucket", None, None, 1000, None).await.unwrap();

        assert_eq!(objects.objects.len(), 2);
    }

    #[tokio::test]
    async fn test_copy_object() {
        let (engine, _temp) = create_test_engine().await;

        engine.create_bucket("source-bucket").await.unwrap();
        engine.create_bucket("dest-bucket").await.unwrap();
        engine
            .put_object("source-bucket", "source-key", bytes::Bytes::from("data"), None, None)
            .await
            .unwrap();

        let copied = engine
            .copy_object("source-bucket", "source-key", "dest-bucket", "dest-key")
            .await
            .unwrap();

        assert_eq!(copied.key, "dest-key");
        assert!(engine.object_exists("dest-bucket", "dest-key").await);
    }

    #[tokio::test]
    async fn test_object_in_nonexistent_bucket() {
        let (engine, _temp) = create_test_engine().await;

        let result = engine
            .put_object("nonexistent", "key", bytes::Bytes::from("data"), None, None)
            .await;

        assert!(result.is_err());
    }
}

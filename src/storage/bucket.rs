//! Bucket storage operations

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::types::bucket::{AccessControlList, CannedAcl, ObjectLockConfiguration, TagSet};
use crate::types::error::{S3Error, S3ErrorCode, S3Result};
use crate::types::response::CorsConfiguration;
use crate::types::Bucket;

/// Manages bucket storage and metadata
pub struct BucketStore {
    data_dir: PathBuf,
    buckets: HashMap<String, Bucket>,
}

impl BucketStore {
    /// Create a new bucket store
    pub async fn new(data_dir: &Path) -> S3Result<Self> {
        let buckets_dir = data_dir.join("buckets");
        tokio::fs::create_dir_all(&buckets_dir).await?;

        // Load existing buckets
        let mut buckets = HashMap::new();
        let mut entries = tokio::fs::read_dir(&buckets_dir).await?;

        while let Some(entry) = entries.next_entry().await? {
            if entry.file_type().await?.is_dir() {
                let name = entry.file_name().to_string_lossy().to_string();
                let metadata_path = entry.path().join(".metadata").join("bucket.json");

                if metadata_path.exists() {
                    let content = tokio::fs::read_to_string(&metadata_path).await?;
                    if let Ok(bucket) = serde_json::from_str::<Bucket>(&content) {
                        buckets.insert(name, bucket);
                    }
                }
            }
        }

        Ok(Self {
            data_dir: data_dir.to_path_buf(),
            buckets,
        })
    }

    /// Create a new bucket
    pub async fn create(&mut self, name: &str, region: &str) -> S3Result<Bucket> {
        self.create_with_acl(name, region, None).await
    }

    /// Create a new bucket with optional canned ACL
    pub async fn create_with_acl(
        &mut self,
        name: &str,
        region: &str,
        canned_acl: Option<CannedAcl>,
    ) -> S3Result<Bucket> {
        if self.buckets.contains_key(name) {
            return Err(S3Error::bucket_already_exists(name));
        }

        let bucket = match canned_acl {
            Some(acl) => Bucket::new_with_acl(name, region, acl),
            None => Bucket::new(name, region),
        };

        // Create bucket directory structure
        let bucket_dir = self.data_dir.join("buckets").join(name);
        let metadata_dir = bucket_dir.join(".metadata");
        let objects_dir = bucket_dir.join("objects");

        tokio::fs::create_dir_all(&metadata_dir).await?;
        tokio::fs::create_dir_all(&objects_dir).await?;

        // Save bucket metadata
        let metadata_path = metadata_dir.join("bucket.json");
        let content = serde_json::to_string_pretty(&bucket)?;
        tokio::fs::write(&metadata_path, content).await?;

        self.buckets.insert(name.to_string(), bucket.clone());

        Ok(bucket)
    }

    /// Delete a bucket
    pub async fn delete(&mut self, name: &str) -> S3Result<()> {
        if !self.buckets.contains_key(name) {
            return Err(S3Error::no_such_bucket(name));
        }

        let bucket_dir = self.data_dir.join("buckets").join(name);

        // Remove the bucket directory
        tokio::fs::remove_dir_all(&bucket_dir).await?;

        self.buckets.remove(name);

        Ok(())
    }

    /// Check if a bucket exists
    pub fn exists(&self, name: &str) -> bool {
        self.buckets.contains_key(name)
    }

    /// Get a bucket by name
    pub fn get(&self, name: &str) -> Option<Bucket> {
        self.buckets.get(name).cloned()
    }

    /// List all buckets
    pub fn list(&self) -> Vec<Bucket> {
        let mut buckets: Vec<_> = self.buckets.values().cloned().collect();
        buckets.sort_by(|a, b| a.name.cmp(&b.name));
        buckets
    }

    /// Update bucket metadata
    pub async fn update(&mut self, bucket: &Bucket) -> S3Result<()> {
        if !self.buckets.contains_key(&bucket.name) {
            return Err(S3Error::no_such_bucket(&bucket.name));
        }

        let metadata_path = self
            .data_dir
            .join("buckets")
            .join(&bucket.name)
            .join(".metadata")
            .join("bucket.json");

        let content = serde_json::to_string_pretty(bucket)?;
        tokio::fs::write(&metadata_path, content).await?;

        self.buckets.insert(bucket.name.clone(), bucket.clone());

        Ok(())
    }

    /// Get the path to a bucket's objects directory
    pub fn objects_path(&self, bucket: &str) -> PathBuf {
        self.data_dir.join("buckets").join(bucket).join("objects")
    }

    /// Get the path to a bucket's metadata directory
    pub fn metadata_path(&self, bucket: &str) -> PathBuf {
        self.data_dir.join("buckets").join(bucket).join(".metadata")
    }

    /// Set tags on a bucket
    pub async fn set_tags(&mut self, name: &str, tags: TagSet) -> S3Result<()> {
        let mut bucket = self
            .buckets
            .get(name)
            .cloned()
            .ok_or_else(|| S3Error::no_such_bucket(name))?;

        bucket.tags = Some(tags);
        self.update(&bucket).await
    }

    /// Get tags from a bucket
    pub fn get_tags(&self, name: &str) -> S3Result<TagSet> {
        let bucket = self
            .buckets
            .get(name)
            .ok_or_else(|| S3Error::no_such_bucket(name))?;

        bucket.tags.clone().ok_or_else(|| {
            S3Error::new(
                S3ErrorCode::NoSuchTagSet,
                "The TagSet does not exist",
            )
            .with_resource(name.to_string())
        })
    }

    /// Delete tags from a bucket
    pub async fn delete_tags(&mut self, name: &str) -> S3Result<()> {
        let mut bucket = self
            .buckets
            .get(name)
            .cloned()
            .ok_or_else(|| S3Error::no_such_bucket(name))?;

        bucket.tags = None;
        self.update(&bucket).await
    }

    /// Set CORS configuration on a bucket
    pub async fn set_cors(&mut self, name: &str, cors: CorsConfiguration) -> S3Result<()> {
        let mut bucket = self
            .buckets
            .get(name)
            .cloned()
            .ok_or_else(|| S3Error::no_such_bucket(name))?;

        bucket.cors = Some(cors);
        self.update(&bucket).await
    }

    /// Get CORS configuration from a bucket
    pub fn get_cors(&self, name: &str) -> S3Result<CorsConfiguration> {
        let bucket = self
            .buckets
            .get(name)
            .ok_or_else(|| S3Error::no_such_bucket(name))?;

        bucket.cors.clone().ok_or_else(|| {
            S3Error::new(
                S3ErrorCode::NoSuchCORSConfiguration,
                "The CORS configuration does not exist",
            )
            .with_resource(name.to_string())
        })
    }

    /// Delete CORS configuration from a bucket
    pub async fn delete_cors(&mut self, name: &str) -> S3Result<()> {
        let mut bucket = self
            .buckets
            .get(name)
            .cloned()
            .ok_or_else(|| S3Error::no_such_bucket(name))?;

        bucket.cors = None;
        self.update(&bucket).await
    }

    /// Set versioning status on a bucket
    pub async fn set_versioning(
        &mut self,
        name: &str,
        status: crate::types::bucket::VersioningStatus,
    ) -> S3Result<()> {
        let mut bucket = self
            .buckets
            .get(name)
            .cloned()
            .ok_or_else(|| S3Error::no_such_bucket(name))?;

        bucket.versioning = status;
        self.update(&bucket).await
    }

    /// Get versioning status from a bucket
    pub fn get_versioning(&self, name: &str) -> S3Result<crate::types::bucket::VersioningStatus> {
        let bucket = self
            .buckets
            .get(name)
            .ok_or_else(|| S3Error::no_such_bucket(name))?;

        Ok(bucket.versioning)
    }

    /// Set policy on a bucket
    pub async fn set_policy(&mut self, name: &str, policy: String) -> S3Result<()> {
        let mut bucket = self
            .buckets
            .get(name)
            .cloned()
            .ok_or_else(|| S3Error::no_such_bucket(name))?;

        bucket.policy = Some(policy);
        self.update(&bucket).await
    }

    /// Get policy from a bucket
    pub fn get_policy(&self, name: &str) -> S3Result<String> {
        let bucket = self
            .buckets
            .get(name)
            .ok_or_else(|| S3Error::no_such_bucket(name))?;

        bucket.policy.clone().ok_or_else(|| {
            S3Error::new(
                S3ErrorCode::NoSuchBucketPolicy,
                "The bucket policy does not exist",
            )
            .with_resource(name.to_string())
        })
    }

    /// Delete policy from a bucket
    pub async fn delete_policy(&mut self, name: &str) -> S3Result<()> {
        let mut bucket = self
            .buckets
            .get(name)
            .cloned()
            .ok_or_else(|| S3Error::no_such_bucket(name))?;

        bucket.policy = None;
        self.update(&bucket).await
    }

    /// Get ACL from a bucket
    pub fn get_acl(&self, name: &str) -> S3Result<AccessControlList> {
        let bucket = self
            .buckets
            .get(name)
            .ok_or_else(|| S3Error::no_such_bucket(name))?;

        // If no ACL is set, return the default (private) ACL
        Ok(bucket.acl.clone().unwrap_or_default())
    }

    /// Set ACL on a bucket
    pub async fn set_acl(&mut self, name: &str, acl: AccessControlList) -> S3Result<()> {
        let mut bucket = self
            .buckets
            .get(name)
            .cloned()
            .ok_or_else(|| S3Error::no_such_bucket(name))?;

        bucket.acl = Some(acl);
        self.update(&bucket).await
    }

    /// Set lifecycle configuration on a bucket
    pub async fn set_lifecycle(
        &mut self,
        name: &str,
        lifecycle: crate::types::bucket::LifecycleConfiguration,
    ) -> S3Result<()> {
        let mut bucket = self
            .buckets
            .get(name)
            .cloned()
            .ok_or_else(|| S3Error::no_such_bucket(name))?;

        bucket.lifecycle = Some(lifecycle);
        self.update(&bucket).await
    }

    /// Get lifecycle configuration from a bucket
    pub fn get_lifecycle(
        &self,
        name: &str,
    ) -> S3Result<crate::types::bucket::LifecycleConfiguration> {
        let bucket = self
            .buckets
            .get(name)
            .ok_or_else(|| S3Error::no_such_bucket(name))?;

        bucket.lifecycle.clone().ok_or_else(|| {
            S3Error::new(
                S3ErrorCode::NoSuchLifecycleConfiguration,
                "The lifecycle configuration does not exist",
            )
            .with_resource(name.to_string())
        })
    }

    /// Delete lifecycle configuration from a bucket
    pub async fn delete_lifecycle(&mut self, name: &str) -> S3Result<()> {
        let mut bucket = self
            .buckets
            .get(name)
            .cloned()
            .ok_or_else(|| S3Error::no_such_bucket(name))?;

        bucket.lifecycle = None;
        self.update(&bucket).await
    }

    // =========================================================================
    // Object Lock Operations
    // =========================================================================

    /// Create a bucket with Object Lock enabled
    pub async fn create_with_object_lock(&mut self, name: &str, region: &str) -> S3Result<Bucket> {
        if self.buckets.contains_key(name) {
            return Err(S3Error::bucket_already_exists(name));
        }

        let bucket = Bucket::new_with_object_lock(name, region);

        // Create bucket directory structure
        let bucket_dir = self.data_dir.join("buckets").join(name);
        let metadata_dir = bucket_dir.join(".metadata");
        let objects_dir = bucket_dir.join("objects");

        tokio::fs::create_dir_all(&metadata_dir).await?;
        tokio::fs::create_dir_all(&objects_dir).await?;

        // Save bucket metadata
        let metadata_path = metadata_dir.join("bucket.json");
        let content = serde_json::to_string_pretty(&bucket)?;
        tokio::fs::write(&metadata_path, content).await?;

        self.buckets.insert(name.to_string(), bucket.clone());

        Ok(bucket)
    }

    /// Create a bucket with Object Lock enabled and a canned ACL
    pub async fn create_with_object_lock_and_acl(
        &mut self,
        name: &str,
        region: &str,
        acl: Option<CannedAcl>,
    ) -> S3Result<Bucket> {
        if self.buckets.contains_key(name) {
            return Err(S3Error::bucket_already_exists(name));
        }

        let mut bucket = Bucket::new_with_object_lock(name, region);

        // Apply canned ACL if provided
        if let Some(canned) = acl {
            bucket.acl = Some(AccessControlList::from_canned(canned));
        }

        // Create bucket directory structure
        let bucket_dir = self.data_dir.join("buckets").join(name);
        let metadata_dir = bucket_dir.join(".metadata");
        let objects_dir = bucket_dir.join("objects");

        tokio::fs::create_dir_all(&metadata_dir).await?;
        tokio::fs::create_dir_all(&objects_dir).await?;

        // Save bucket metadata
        let metadata_path = metadata_dir.join("bucket.json");
        let content = serde_json::to_string_pretty(&bucket)?;
        tokio::fs::write(&metadata_path, content).await?;

        self.buckets.insert(name.to_string(), bucket.clone());

        Ok(bucket)
    }

    /// Set Object Lock configuration for a bucket
    pub async fn set_object_lock_configuration(
        &mut self,
        name: &str,
        config: ObjectLockConfiguration,
    ) -> S3Result<()> {
        let mut bucket = self
            .buckets
            .get(name)
            .cloned()
            .ok_or_else(|| S3Error::no_such_bucket(name))?;

        if !bucket.object_lock_enabled {
            return Err(S3Error::new(
                S3ErrorCode::InvalidRequest,
                "Object Lock is not enabled on this bucket",
            ));
        }

        bucket.object_lock_configuration = Some(config);
        self.update(&bucket).await
    }

    // =========================================================================
    // Server-Side Encryption Operations
    // =========================================================================

    /// Set encryption configuration for a bucket
    pub async fn set_encryption(
        &mut self,
        name: &str,
        encryption: crate::types::bucket::ServerSideEncryptionConfiguration,
    ) -> S3Result<()> {
        let mut bucket = self
            .buckets
            .get(name)
            .cloned()
            .ok_or_else(|| S3Error::no_such_bucket(name))?;

        bucket.encryption = Some(encryption);
        self.update(&bucket).await
    }

    /// Get encryption configuration from a bucket
    pub fn get_encryption(
        &self,
        name: &str,
    ) -> S3Result<crate::types::bucket::ServerSideEncryptionConfiguration> {
        let bucket = self
            .buckets
            .get(name)
            .ok_or_else(|| S3Error::no_such_bucket(name))?;

        bucket.encryption.clone().ok_or_else(|| {
            S3Error::new(
                S3ErrorCode::ServerSideEncryptionConfigurationNotFoundError,
                "The server side encryption configuration was not found",
            )
            .with_resource(name.to_string())
        })
    }

    /// Delete encryption configuration from a bucket
    pub async fn delete_encryption(&mut self, name: &str) -> S3Result<()> {
        let mut bucket = self
            .buckets
            .get(name)
            .cloned()
            .ok_or_else(|| S3Error::no_such_bucket(name))?;

        bucket.encryption = None;
        self.update(&bucket).await
    }

    // =========================================================================
    // Public Access Block Operations
    // =========================================================================

    /// Set public access block configuration for a bucket
    pub async fn set_public_access_block(
        &mut self,
        name: &str,
        config: crate::types::bucket::PublicAccessBlockConfiguration,
    ) -> S3Result<()> {
        let mut bucket = self
            .buckets
            .get(name)
            .cloned()
            .ok_or_else(|| S3Error::no_such_bucket(name))?;

        bucket.public_access_block = Some(config);
        self.update(&bucket).await
    }

    /// Get public access block configuration from a bucket
    pub fn get_public_access_block(
        &self,
        name: &str,
    ) -> S3Result<crate::types::bucket::PublicAccessBlockConfiguration> {
        let bucket = self
            .buckets
            .get(name)
            .ok_or_else(|| S3Error::no_such_bucket(name))?;

        bucket.public_access_block.clone().ok_or_else(|| {
            S3Error::new(
                S3ErrorCode::NoSuchPublicAccessBlockConfiguration,
                "The public access block configuration was not found",
            )
            .with_resource(name.to_string())
        })
    }

    /// Delete public access block configuration from a bucket
    pub async fn delete_public_access_block(&mut self, name: &str) -> S3Result<()> {
        let mut bucket = self
            .buckets
            .get(name)
            .cloned()
            .ok_or_else(|| S3Error::no_such_bucket(name))?;

        bucket.public_access_block = None;
        self.update(&bucket).await
    }

    // =========================================================================
    // Website Configuration Operations
    // =========================================================================

    /// Set website configuration for a bucket
    pub async fn set_website(
        &mut self,
        name: &str,
        config: crate::types::bucket::WebsiteConfiguration,
    ) -> S3Result<()> {
        let mut bucket = self
            .buckets
            .get(name)
            .cloned()
            .ok_or_else(|| S3Error::no_such_bucket(name))?;

        bucket.website = Some(config);
        self.update(&bucket).await
    }

    /// Get website configuration from a bucket
    pub fn get_website(
        &self,
        name: &str,
    ) -> S3Result<crate::types::bucket::WebsiteConfiguration> {
        let bucket = self
            .buckets
            .get(name)
            .ok_or_else(|| S3Error::no_such_bucket(name))?;

        bucket.website.clone().ok_or_else(|| {
            S3Error::new(
                crate::types::error::S3ErrorCode::NoSuchWebsiteConfiguration,
                "The specified bucket does not have a website configuration",
            )
            .with_resource(name.to_string())
        })
    }

    /// Delete website configuration from a bucket
    pub async fn delete_website(&mut self, name: &str) -> S3Result<()> {
        let mut bucket = self
            .buckets
            .get(name)
            .cloned()
            .ok_or_else(|| S3Error::no_such_bucket(name))?;

        bucket.website = None;
        self.update(&bucket).await
    }

    // =========================================================================
    // Ownership Controls Operations
    // =========================================================================

    /// Set ownership controls for a bucket
    pub async fn set_ownership_controls(
        &mut self,
        name: &str,
        config: crate::types::bucket::OwnershipControls,
    ) -> S3Result<()> {
        let mut bucket = self
            .buckets
            .get(name)
            .cloned()
            .ok_or_else(|| S3Error::no_such_bucket(name))?;

        bucket.ownership_controls = Some(config);
        self.update(&bucket).await
    }

    /// Get ownership controls from a bucket
    pub fn get_ownership_controls(
        &self,
        name: &str,
    ) -> S3Result<crate::types::bucket::OwnershipControls> {
        let bucket = self
            .buckets
            .get(name)
            .ok_or_else(|| S3Error::no_such_bucket(name))?;

        bucket.ownership_controls.clone().ok_or_else(|| {
            S3Error::new(
                crate::types::error::S3ErrorCode::OwnershipControlsNotFoundError,
                "The bucket ownership controls were not found",
            )
            .with_resource(name.to_string())
        })
    }

    /// Delete ownership controls from a bucket
    pub async fn delete_ownership_controls(&mut self, name: &str) -> S3Result<()> {
        let mut bucket = self
            .buckets
            .get(name)
            .cloned()
            .ok_or_else(|| S3Error::no_such_bucket(name))?;

        bucket.ownership_controls = None;
        self.update(&bucket).await
    }

    // =========================================================================
    // Logging Configuration Operations
    // =========================================================================

    /// Set logging configuration for a bucket
    pub async fn set_logging(
        &mut self,
        name: &str,
        config: crate::types::bucket::LoggingConfiguration,
    ) -> S3Result<()> {
        let mut bucket = self
            .buckets
            .get(name)
            .cloned()
            .ok_or_else(|| S3Error::no_such_bucket(name))?;

        // If config has no target bucket, treat as disabling logging
        if config.target_bucket.is_none() {
            bucket.logging = None;
        } else {
            bucket.logging = Some(config);
        }
        self.update(&bucket).await
    }

    /// Get logging configuration from a bucket
    /// Note: Unlike other configs, this returns an empty config when not set
    /// (per S3 behavior - GetBucketLogging always returns BucketLoggingStatus)
    pub fn get_logging(
        &self,
        name: &str,
    ) -> S3Result<Option<crate::types::bucket::LoggingConfiguration>> {
        let bucket = self
            .buckets
            .get(name)
            .ok_or_else(|| S3Error::no_such_bucket(name))?;

        Ok(bucket.logging.clone())
    }

    // =========================================================================
    // Notification Configuration Operations
    // =========================================================================

    /// Set notification configuration for a bucket
    pub async fn set_notification(
        &mut self,
        name: &str,
        config: crate::types::bucket::NotificationConfiguration,
    ) -> S3Result<()> {
        let mut bucket = self
            .buckets
            .get(name)
            .cloned()
            .ok_or_else(|| S3Error::no_such_bucket(name))?;

        // If config has no configurations, store None (cleared)
        if config.is_configured() {
            bucket.notification = Some(config);
        } else {
            bucket.notification = None;
        }
        self.update(&bucket).await
    }

    /// Get notification configuration from a bucket
    /// Note: Returns empty config when not set (per S3 behavior)
    pub fn get_notification(
        &self,
        name: &str,
    ) -> S3Result<crate::types::bucket::NotificationConfiguration> {
        let bucket = self
            .buckets
            .get(name)
            .ok_or_else(|| S3Error::no_such_bucket(name))?;

        Ok(bucket
            .notification
            .clone()
            .unwrap_or_else(crate::types::bucket::NotificationConfiguration::new))
    }

    // =========================================================================
    // Replication Configuration Operations
    // =========================================================================

    /// Set replication configuration for a bucket
    pub async fn set_replication(
        &mut self,
        name: &str,
        config: crate::types::bucket::ReplicationConfiguration,
    ) -> S3Result<()> {
        let mut bucket = self
            .buckets
            .get(name)
            .cloned()
            .ok_or_else(|| S3Error::no_such_bucket(name))?;

        bucket.replication = Some(config);
        self.update(&bucket).await
    }

    /// Get replication configuration from a bucket
    pub fn get_replication(
        &self,
        name: &str,
    ) -> S3Result<crate::types::bucket::ReplicationConfiguration> {
        let bucket = self
            .buckets
            .get(name)
            .ok_or_else(|| S3Error::no_such_bucket(name))?;

        bucket.replication.clone().ok_or_else(|| {
            S3Error::new(
                S3ErrorCode::ReplicationConfigurationNotFoundError,
                "The replication configuration was not found",
            )
            .with_resource(name.to_string())
        })
    }

    /// Delete replication configuration from a bucket
    pub async fn delete_replication(&mut self, name: &str) -> S3Result<()> {
        let mut bucket = self
            .buckets
            .get(name)
            .cloned()
            .ok_or_else(|| S3Error::no_such_bucket(name))?;

        bucket.replication = None;
        self.update(&bucket).await
    }

    // =========================================================================
    // Request Payment Configuration Operations
    // =========================================================================

    /// Set request payment configuration for a bucket
    pub async fn set_request_payment(
        &mut self,
        name: &str,
        config: crate::types::bucket::RequestPaymentConfiguration,
    ) -> S3Result<()> {
        let mut bucket = self
            .buckets
            .get(name)
            .cloned()
            .ok_or_else(|| S3Error::no_such_bucket(name))?;

        bucket.request_payment = Some(config);
        self.update(&bucket).await
    }

    /// Get request payment configuration from a bucket
    /// Returns default (BucketOwner) when not set (per S3 behavior)
    pub fn get_request_payment(
        &self,
        name: &str,
    ) -> S3Result<crate::types::bucket::RequestPaymentConfiguration> {
        let bucket = self
            .buckets
            .get(name)
            .ok_or_else(|| S3Error::no_such_bucket(name))?;

        Ok(bucket
            .request_payment
            .clone()
            .unwrap_or_else(crate::types::bucket::RequestPaymentConfiguration::new))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    async fn create_test_store() -> (BucketStore, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let store = BucketStore::new(temp_dir.path()).await.unwrap();
        (store, temp_dir)
    }

    #[tokio::test]
    async fn test_create_bucket() {
        let (mut store, _temp) = create_test_store().await;

        let bucket = store.create("test-bucket", "us-east-1").await.unwrap();

        assert_eq!(bucket.name, "test-bucket");
        assert_eq!(bucket.region, "us-east-1");
        assert!(store.exists("test-bucket"));
    }

    #[tokio::test]
    async fn test_create_duplicate_bucket() {
        let (mut store, _temp) = create_test_store().await;

        store.create("test-bucket", "us-east-1").await.unwrap();
        let result = store.create("test-bucket", "us-east-1").await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_delete_bucket() {
        let (mut store, _temp) = create_test_store().await;

        store.create("test-bucket", "us-east-1").await.unwrap();
        store.delete("test-bucket").await.unwrap();

        assert!(!store.exists("test-bucket"));
    }

    #[tokio::test]
    async fn test_delete_nonexistent_bucket() {
        let (mut store, _temp) = create_test_store().await;

        let result = store.delete("nonexistent").await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_list_buckets() {
        let (mut store, _temp) = create_test_store().await;

        store.create("bucket-a", "us-east-1").await.unwrap();
        store.create("bucket-b", "us-east-1").await.unwrap();
        store.create("bucket-c", "us-east-1").await.unwrap();

        let buckets = store.list();

        assert_eq!(buckets.len(), 3);
        assert_eq!(buckets[0].name, "bucket-a");
        assert_eq!(buckets[1].name, "bucket-b");
        assert_eq!(buckets[2].name, "bucket-c");
    }

    #[tokio::test]
    async fn test_bucket_persistence() {
        let temp_dir = TempDir::new().unwrap();

        // Create a bucket
        {
            let mut store = BucketStore::new(temp_dir.path()).await.unwrap();
            store.create("persistent-bucket", "us-east-1").await.unwrap();
        }

        // Reload and verify
        {
            let store = BucketStore::new(temp_dir.path()).await.unwrap();
            assert!(store.exists("persistent-bucket"));

            let bucket = store.get("persistent-bucket").unwrap();
            assert_eq!(bucket.region, "us-east-1");
        }
    }

    #[tokio::test]
    async fn test_get_bucket() {
        let (mut store, _temp) = create_test_store().await;

        store.create("test-bucket", "eu-west-1").await.unwrap();

        let bucket = store.get("test-bucket").unwrap();
        assert_eq!(bucket.name, "test-bucket");
        assert_eq!(bucket.region, "eu-west-1");

        let not_found = store.get("nonexistent");
        assert!(not_found.is_none());
    }

    #[tokio::test]
    async fn test_bucket_paths() {
        let (store, temp) = create_test_store().await;

        let objects_path = store.objects_path("my-bucket");
        let metadata_path = store.metadata_path("my-bucket");

        assert!(objects_path.ends_with("buckets/my-bucket/objects"));
        assert!(metadata_path.ends_with("buckets/my-bucket/.metadata"));
    }
}

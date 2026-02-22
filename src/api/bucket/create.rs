//! CreateBucket operation

use bytes::Bytes;
use http_body_util::Full;
use hyper::{Response, StatusCode};

use crate::storage::StorageEngine;
use crate::types::bucket::CannedAcl;
use crate::types::error::S3Result;

/// Handle CreateBucket request
pub async fn create_bucket(
    storage: &StorageEngine,
    bucket: &str,
    _region: &str,
    canned_acl: Option<&str>,
) -> S3Result<Response<Full<Bytes>>> {
    let acl = canned_acl.and_then(CannedAcl::from_header);
    storage.create_bucket_with_acl(bucket, acl).await?;

    let response = Response::builder()
        .status(StatusCode::OK)
        .header("Location", format!("/{}", bucket))
        .body(Full::new(Bytes::new()))
        .unwrap();

    Ok(response)
}

/// Handle CreateBucket request with Object Lock enabled
pub async fn create_bucket_with_object_lock(
    storage: &StorageEngine,
    bucket: &str,
    _region: &str,
    canned_acl: Option<&str>,
) -> S3Result<Response<Full<Bytes>>> {
    let acl = canned_acl.and_then(CannedAcl::from_header);
    storage.create_bucket_with_object_lock_and_acl(bucket, acl).await?;

    let response = Response::builder()
        .status(StatusCode::OK)
        .header("Location", format!("/{}", bucket))
        .body(Full::new(Bytes::new()))
        .unwrap();

    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use tempfile::TempDir;

    async fn create_test_storage() -> (StorageEngine, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::new().with_data_dir(temp_dir.path());
        let storage = StorageEngine::new(config).await.unwrap();
        (storage, temp_dir)
    }

    #[tokio::test]
    async fn test_create_bucket_success() {
        let (storage, _temp) = create_test_storage().await;

        let response = create_bucket(&storage, "test-bucket", "us-east-1", None)
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert!(storage.bucket_exists("test-bucket").await);
    }

    #[tokio::test]
    async fn test_create_bucket_already_exists() {
        let (storage, _temp) = create_test_storage().await;

        // Create first time
        create_bucket(&storage, "test-bucket", "us-east-1", None)
            .await
            .unwrap();

        // Try to create again
        let result = create_bucket(&storage, "test-bucket", "us-east-1", None).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_create_bucket_invalid_name() {
        let (storage, _temp) = create_test_storage().await;

        let result = create_bucket(&storage, "ab", "us-east-1", None).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_create_bucket_with_public_read_acl() {
        let (storage, _temp) = create_test_storage().await;

        let response = create_bucket(&storage, "public-bucket", "us-east-1", Some("public-read"))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        // Verify ACL was set correctly
        let acl = storage.get_bucket_acl("public-bucket").await.unwrap();
        assert_eq!(acl.grants.len(), 2); // Owner + AllUsers READ
    }
}

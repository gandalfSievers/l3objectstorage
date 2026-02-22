//! DeleteObject operation

use bytes::Bytes;
use http_body_util::Full;
use hyper::{Response, StatusCode};

use crate::storage::StorageEngine;
use crate::types::bucket::ObjectLockRetentionMode;
use crate::types::error::{S3Error, S3ErrorCode, S3Result};

/// Handle DeleteObject request
pub async fn delete_object(
    storage: &StorageEngine,
    bucket: &str,
    key: &str,
) -> S3Result<Response<Full<Bytes>>> {
    storage.delete_object(bucket, key).await?;

    let response = Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Full::new(Bytes::new()))
        .unwrap();

    Ok(response)
}

/// Handle DeleteObject request with versioning support
pub async fn delete_object_versioned(
    storage: &StorageEngine,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
) -> S3Result<Response<Full<Bytes>>> {
    delete_object_versioned_with_bypass(storage, bucket, key, version_id, false).await
}

/// Handle DeleteObject request with versioning support and bypass governance option
pub async fn delete_object_versioned_with_bypass(
    storage: &StorageEngine,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
    bypass_governance_retention: bool,
) -> S3Result<Response<Full<Bytes>>> {
    // Check object lock constraints before deletion
    // Legal hold prevents deletion regardless of bypass flag
    if let Ok(legal_hold) = storage.get_object_legal_hold(bucket, key, version_id).await {
        if legal_hold.status.is_on() {
            return Err(S3Error::new(
                S3ErrorCode::AccessDenied,
                "Object is under legal hold and cannot be deleted",
            ));
        }
    }

    // Check retention - COMPLIANCE mode cannot be bypassed, GOVERNANCE can with bypass flag
    if let Ok(retention) = storage.get_object_retention(bucket, key, version_id).await {
        if !retention.is_expired() {
            match retention.mode {
                ObjectLockRetentionMode::Compliance => {
                    return Err(S3Error::new(
                        S3ErrorCode::AccessDenied,
                        "Object is protected by COMPLIANCE retention and cannot be deleted",
                    ));
                }
                ObjectLockRetentionMode::Governance => {
                    if !bypass_governance_retention {
                        return Err(S3Error::new(
                            S3ErrorCode::AccessDenied,
                            "Object is protected by GOVERNANCE retention. Use x-amz-bypass-governance-retention header to delete",
                        ));
                    }
                }
            }
        }
    }

    let result = storage.delete_object_versioned(bucket, key, version_id).await?;

    let mut response_builder = Response::builder().status(StatusCode::NO_CONTENT);

    // Add version ID header if present
    if let Some(ref vid) = result.version_id {
        response_builder = response_builder.header("x-amz-version-id", vid);
    }

    // Add delete marker header if a delete marker was created
    if result.delete_marker {
        response_builder = response_builder.header("x-amz-delete-marker", "true");
    }

    let response = response_builder.body(Full::new(Bytes::new())).unwrap();

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
    async fn test_delete_object_success() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();
        storage
            .put_object("test-bucket", "test-key", Bytes::from("data"), None, None)
            .await
            .unwrap();

        let response = delete_object(&storage, "test-bucket", "test-key")
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert!(!storage.object_exists("test-bucket", "test-key").await);
    }

    #[tokio::test]
    async fn test_delete_nonexistent_object() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        // S3 returns 204 even for non-existent objects (idempotent delete)
        let response = delete_object(&storage, "test-bucket", "nonexistent")
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn test_delete_object_bucket_not_found() {
        let (storage, _temp) = create_test_storage().await;

        let result = delete_object(&storage, "nonexistent", "key").await;

        assert!(result.is_err());
    }
}

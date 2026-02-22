//! DeleteBucket operation

use bytes::Bytes;
use http_body_util::Full;
use hyper::{Response, StatusCode};

use crate::storage::StorageEngine;
use crate::types::error::S3Result;

/// Handle DeleteBucket request
pub async fn delete_bucket(
    storage: &StorageEngine,
    bucket: &str,
) -> S3Result<Response<Full<Bytes>>> {
    storage.delete_bucket(bucket).await?;

    let response = Response::builder()
        .status(StatusCode::NO_CONTENT)
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
    async fn test_delete_bucket_success() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        let response = delete_bucket(&storage, "test-bucket").await.unwrap();

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert!(!storage.bucket_exists("test-bucket").await);
    }

    #[tokio::test]
    async fn test_delete_bucket_not_found() {
        let (storage, _temp) = create_test_storage().await;

        let result = delete_bucket(&storage, "nonexistent").await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_delete_bucket_not_empty() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();
        storage
            .put_object("test-bucket", "key", Bytes::from("data"), None, None)
            .await
            .unwrap();

        let result = delete_bucket(&storage, "test-bucket").await;

        assert!(result.is_err());
    }
}

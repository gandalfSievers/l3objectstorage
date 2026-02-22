//! HeadBucket operation

use bytes::Bytes;
use http_body_util::Full;
use hyper::{Response, StatusCode};

use crate::storage::StorageEngine;
use crate::types::error::{S3Error, S3Result};

/// Handle HeadBucket request
pub async fn head_bucket(
    storage: &StorageEngine,
    bucket: &str,
) -> S3Result<Response<Full<Bytes>>> {
    if !storage.bucket_exists(bucket).await {
        return Err(S3Error::no_such_bucket(bucket));
    }

    let bucket_info = storage.get_bucket(bucket).await?;

    let response = Response::builder()
        .status(StatusCode::OK)
        .header("x-amz-bucket-region", &bucket_info.region)
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
    async fn test_head_bucket_exists() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        let response = head_bucket(&storage, "test-bucket").await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().contains_key("x-amz-bucket-region"));
    }

    #[tokio::test]
    async fn test_head_bucket_not_found() {
        let (storage, _temp) = create_test_storage().await;

        let result = head_bucket(&storage, "nonexistent").await;

        assert!(result.is_err());
    }
}

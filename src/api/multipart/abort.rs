//! AbortMultipartUpload operation

use bytes::Bytes;
use http_body_util::Full;
use hyper::{Response, StatusCode};

use crate::storage::StorageEngine;
use crate::types::error::S3Result;

/// Handle AbortMultipartUpload request
pub async fn abort_multipart_upload(
    storage: &StorageEngine,
    bucket: &str,
    upload_id: &str,
) -> S3Result<Response<Full<Bytes>>> {
    storage.abort_multipart_upload(bucket, upload_id).await?;

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
    async fn test_abort_multipart_upload() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();
        let upload_id = storage
            .create_multipart_upload("test-bucket", "test-key")
            .await
            .unwrap();

        // Upload a part
        storage
            .upload_part("test-bucket", "test-key", &upload_id, 1, Bytes::from("data"))
            .await
            .unwrap();

        let response = abort_multipart_upload(&storage, "test-bucket", &upload_id)
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        // Verify upload no longer exists
        let result = storage.get_multipart_upload("test-bucket", &upload_id).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_abort_nonexistent_upload() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        let result = abort_multipart_upload(&storage, "test-bucket", "nonexistent").await;

        assert!(result.is_err());
    }
}

//! UploadPart operation

use bytes::Bytes;
use http_body_util::Full;
use hyper::{Response, StatusCode};

use crate::storage::StorageEngine;
use crate::types::error::{S3Error, S3ErrorCode, S3Result};

/// Handle UploadPart request
pub async fn upload_part(
    storage: &StorageEngine,
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number: i32,
    body: Bytes,
) -> S3Result<Response<Full<Bytes>>> {
    // Validate part number (must be 1-10000)
    if part_number < 1 || part_number > 10000 {
        return Err(S3Error::new(
            S3ErrorCode::InvalidArgument,
            format!("Part number must be an integer between 1 and 10000, inclusive. Received: {}", part_number),
        ));
    }

    let part = storage
        .upload_part(bucket, key, upload_id, part_number, body)
        .await?;

    let response = Response::builder()
        .status(StatusCode::OK)
        .header("ETag", &part.etag)
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
    async fn test_upload_part() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();
        let upload_id = storage
            .create_multipart_upload("test-bucket", "test-key")
            .await
            .unwrap();

        let response = upload_part(
            &storage,
            "test-bucket",
            "test-key",
            &upload_id,
            1,
            Bytes::from("part data"),
        )
        .await
        .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().contains_key("etag"));
    }

    #[tokio::test]
    async fn test_upload_part_invalid_upload() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        let result = upload_part(
            &storage,
            "test-bucket",
            "test-key",
            "nonexistent-upload",
            1,
            Bytes::from("data"),
        )
        .await;

        assert!(result.is_err());
    }
}

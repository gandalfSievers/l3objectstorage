//! CreateMultipartUpload operation

use bytes::Bytes;
use http_body_util::Full;
use hyper::{Response, StatusCode};

use crate::storage::StorageEngine;
use crate::types::error::S3Result;
use crate::types::response::InitiateMultipartUploadResponse;
use crate::utils::xml::to_xml;

/// Handle CreateMultipartUpload request
pub async fn create_multipart_upload(
    storage: &StorageEngine,
    bucket: &str,
    key: &str,
    sse_header: Option<&str>,
) -> S3Result<Response<Full<Bytes>>> {
    // Determine effective SSE algorithm (explicit header or bucket default)
    let sse_algorithm = storage
        .get_sse_algorithm_for_object(bucket, sse_header)
        .await?;

    // Create multipart upload with SSE stored for later use in CompleteMultipartUpload
    let upload_id = storage.create_multipart_upload_with_sse(bucket, key, sse_algorithm.as_ref()).await?;

    let response_body = InitiateMultipartUploadResponse {
        bucket: bucket.to_string(),
        key: key.to_string(),
        upload_id,
    };

    let xml = to_xml(&response_body)?;

    let mut response_builder = Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/xml");

    // Add SSE header if encryption is configured
    if let Some(sse) = sse_algorithm {
        response_builder = response_builder.header("x-amz-server-side-encryption", sse.as_str());
    }

    let response = response_builder
        .body(Full::new(Bytes::from(xml)))
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
    async fn test_create_multipart_upload() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        let response = create_multipart_upload(&storage, "test-bucket", "test-key", None)
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        // Verify response contains required fields
        let body = response.into_body();
        let body_bytes = http_body_util::BodyExt::collect(body)
            .await
            .unwrap()
            .to_bytes();
        let body_str = String::from_utf8_lossy(&body_bytes);

        assert!(body_str.contains("<Bucket>test-bucket</Bucket>"));
        assert!(body_str.contains("<Key>test-key</Key>"));
        assert!(body_str.contains("<UploadId>"));
    }

    #[tokio::test]
    async fn test_create_multipart_upload_bucket_not_found() {
        let (storage, _temp) = create_test_storage().await;

        let result = create_multipart_upload(&storage, "nonexistent", "key", None).await;

        assert!(result.is_err());
    }
}

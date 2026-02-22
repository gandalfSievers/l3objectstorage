//! HeadObject operation

use bytes::Bytes;
use http_body_util::Full;
use hyper::{Response, StatusCode};

use crate::storage::StorageEngine;
use crate::types::error::{S3Error, S3ErrorCode, S3Result};
use crate::utils::time::format_http_date;

/// Handle HeadObject request
pub async fn head_object(
    storage: &StorageEngine,
    bucket: &str,
    key: &str,
) -> S3Result<Response<Full<Bytes>>> {
    let object = storage.head_object(bucket, key).await?;

    let mut response_builder = Response::builder()
        .status(StatusCode::OK)
        .header("ETag", &object.etag)
        .header("Content-Type", &object.content_type)
        .header("Content-Length", object.size)
        .header("Last-Modified", format_http_date(&object.last_modified));

    // Add SSE header if encryption was applied
    if let Some(ref sse) = object.sse_algorithm {
        response_builder = response_builder.header("x-amz-server-side-encryption", sse);
    }

    // Add custom metadata headers
    for (key, value) in &object.metadata {
        if key.starts_with("x-amz-meta-") {
            response_builder = response_builder.header(key, value);
        }
    }

    let response = response_builder.body(Full::new(Bytes::new())).unwrap();

    Ok(response)
}

/// Handle HeadObject request with optional version ID
pub async fn head_object_versioned(
    storage: &StorageEngine,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
) -> S3Result<Response<Full<Bytes>>> {
    let object = storage.head_object_versioned(bucket, key, version_id).await?;

    let mut response_builder = Response::builder()
        .status(StatusCode::OK)
        .header("ETag", &object.etag)
        .header("Content-Type", &object.content_type)
        .header("Content-Length", object.size)
        .header("Last-Modified", format_http_date(&object.last_modified));

    // Add version ID header if present
    if let Some(ref vid) = object.version_id {
        response_builder = response_builder.header("x-amz-version-id", vid);
    }

    // Add SSE header if encryption was applied
    if let Some(ref sse) = object.sse_algorithm {
        response_builder = response_builder.header("x-amz-server-side-encryption", sse);
    }

    // Add custom metadata headers
    for (key, value) in &object.metadata {
        if key.starts_with("x-amz-meta-") {
            response_builder = response_builder.header(key, value);
        }
    }

    let response = response_builder.body(Full::new(Bytes::new())).unwrap();

    Ok(response)
}

/// Handle HeadObject request with conditional headers (If-Match, If-None-Match)
pub async fn head_object_conditional(
    storage: &StorageEngine,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
    if_match: Option<&str>,
    if_none_match: Option<&str>,
) -> S3Result<Response<Full<Bytes>>> {
    let object = storage.head_object_versioned(bucket, key, version_id).await?;

    // Check If-Match condition
    if let Some(expected_etag) = if_match {
        let object_etag = object.etag.trim_matches('"');
        let expected_etag = expected_etag.trim_matches('"');
        if object_etag != expected_etag {
            return Err(S3Error::new(
                S3ErrorCode::PreconditionFailed,
                "At least one of the pre-conditions you specified did not hold",
            ));
        }
    }

    // Check If-None-Match condition
    if let Some(etag) = if_none_match {
        let object_etag = object.etag.trim_matches('"');
        let etag = etag.trim_matches('"');
        if object_etag == etag {
            // Return 304 Not Modified
            return Ok(Response::builder()
                .status(StatusCode::NOT_MODIFIED)
                .header("ETag", &object.etag)
                .body(Full::new(Bytes::new()))
                .unwrap());
        }
    }

    let mut response_builder = Response::builder()
        .status(StatusCode::OK)
        .header("ETag", &object.etag)
        .header("Content-Type", &object.content_type)
        .header("Content-Length", object.size)
        .header("Last-Modified", format_http_date(&object.last_modified));

    // Add version ID header if present
    if let Some(ref vid) = object.version_id {
        response_builder = response_builder.header("x-amz-version-id", vid);
    }

    // Add SSE header if encryption was applied
    if let Some(ref sse) = object.sse_algorithm {
        response_builder = response_builder.header("x-amz-server-side-encryption", sse);
    }

    // Add custom metadata headers
    for (key, value) in &object.metadata {
        if key.starts_with("x-amz-meta-") {
            response_builder = response_builder.header(key, value);
        }
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
    async fn test_head_object_success() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();
        storage
            .put_object(
                "test-bucket",
                "test-key",
                Bytes::from("hello world"),
                Some("text/plain"),
                None,
            )
            .await
            .unwrap();

        let response = head_object(&storage, "test-bucket", "test-key")
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().contains_key("etag"));
        assert_eq!(
            response.headers().get("content-type").unwrap(),
            "text/plain"
        );
        assert_eq!(response.headers().get("content-length").unwrap(), "11");

        // HEAD should have empty body
        let body = response.into_body();
        let bytes = http_body_util::BodyExt::collect(body)
            .await
            .unwrap()
            .to_bytes();
        assert!(bytes.is_empty());
    }

    #[tokio::test]
    async fn test_head_object_not_found() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        let result = head_object(&storage, "test-bucket", "nonexistent").await;

        assert!(result.is_err());
    }
}

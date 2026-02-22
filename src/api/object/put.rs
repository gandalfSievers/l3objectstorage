//! PutObject operation

use bytes::Bytes;
use http_body_util::Full;
use hyper::{Response, StatusCode};

use crate::storage::StorageEngine;
use crate::types::bucket::{AccessControlList, CannedAcl};
use crate::types::error::{S3Error, S3ErrorCode, S3Result};
use crate::utils::time::format_http_date;

/// Handle PutObject request
pub async fn put_object(
    storage: &StorageEngine,
    bucket: &str,
    key: &str,
    data: Bytes,
    content_type: Option<String>,
    canned_acl: Option<&str>,
) -> S3Result<Response<Full<Bytes>>> {
    let object = storage
        .put_object(bucket, key, data, content_type.as_deref(), None)
        .await?;

    // Apply canned ACL if specified
    if let Some(acl_header) = canned_acl {
        if let Some(canned) = CannedAcl::from_header(acl_header) {
            let acl = AccessControlList::from_canned(canned);
            storage.set_object_acl(bucket, key, acl).await?;
        }
    }

    let response = Response::builder()
        .status(StatusCode::OK)
        .header("ETag", &object.etag)
        .header("Last-Modified", format_http_date(&object.last_modified))
        .body(Full::new(Bytes::new()))
        .unwrap();

    Ok(response)
}

/// Handle PutObject request with versioning support
pub async fn put_object_versioned(
    storage: &StorageEngine,
    bucket: &str,
    key: &str,
    data: Bytes,
    content_type: Option<String>,
    canned_acl: Option<&str>,
    sse_header: Option<&str>,
) -> S3Result<Response<Full<Bytes>>> {
    // Determine effective SSE algorithm (explicit header or bucket default)
    let sse_algorithm = storage
        .get_sse_algorithm_for_object(bucket, sse_header)
        .await?;

    let object = storage
        .put_object_versioned_with_sse(bucket, key, data, content_type.as_deref(), None, sse_algorithm.as_ref())
        .await?;

    // Apply canned ACL if specified
    if let Some(acl_header) = canned_acl {
        if let Some(canned) = CannedAcl::from_header(acl_header) {
            let acl = AccessControlList::from_canned(canned);
            storage.set_object_acl(bucket, key, acl).await?;
        }
    }

    let mut response_builder = Response::builder()
        .status(StatusCode::OK)
        .header("ETag", &object.etag)
        .header("Last-Modified", format_http_date(&object.last_modified));

    // Add version ID header if versioning is enabled
    if let Some(ref version_id) = object.version_id {
        response_builder = response_builder.header("x-amz-version-id", version_id);
    }

    // Add SSE header if encryption was applied
    if let Some(ref sse) = object.sse_algorithm {
        response_builder = response_builder.header("x-amz-server-side-encryption", sse);
    }

    let response = response_builder.body(Full::new(Bytes::new())).unwrap();

    Ok(response)
}

/// Handle PutObject request with conditional support (If-None-Match)
pub async fn put_object_conditional(
    storage: &StorageEngine,
    bucket: &str,
    key: &str,
    data: Bytes,
    content_type: Option<String>,
    canned_acl: Option<&str>,
    if_none_match: Option<&str>,
    sse_header: Option<&str>,
) -> S3Result<Response<Full<Bytes>>> {
    put_object_conditional_with_metadata(
        storage, bucket, key, data, content_type,
        canned_acl, if_none_match, sse_header, None,
    ).await
}

/// Handle PutObject request with conditional support and custom metadata
pub async fn put_object_conditional_with_metadata(
    storage: &StorageEngine,
    bucket: &str,
    key: &str,
    data: Bytes,
    content_type: Option<String>,
    canned_acl: Option<&str>,
    if_none_match: Option<&str>,
    sse_header: Option<&str>,
    custom_metadata: Option<std::collections::HashMap<String, String>>,
) -> S3Result<Response<Full<Bytes>>> {
    // Check If-None-Match: * condition (create only if object doesn't exist)
    if let Some(condition) = if_none_match {
        if condition.trim() == "*" {
            // Check if object already exists
            if storage.head_object(bucket, key).await.is_ok() {
                return Err(S3Error::new(
                    S3ErrorCode::PreconditionFailed,
                    "At least one of the pre-conditions you specified did not hold",
                ));
            }
        }
    }

    // Determine effective SSE algorithm (explicit header or bucket default)
    let sse_algorithm = storage
        .get_sse_algorithm_for_object(bucket, sse_header)
        .await?;

    let object = storage
        .put_object_versioned_with_sse(bucket, key, data, content_type.as_deref(), custom_metadata, sse_algorithm.as_ref())
        .await?;

    // Apply canned ACL if specified
    if let Some(acl_header) = canned_acl {
        if let Some(canned) = CannedAcl::from_header(acl_header) {
            let acl = AccessControlList::from_canned(canned);
            storage.set_object_acl(bucket, key, acl).await?;
        }
    }

    let mut response_builder = Response::builder()
        .status(StatusCode::OK)
        .header("ETag", &object.etag)
        .header("Last-Modified", format_http_date(&object.last_modified));

    // Add version ID header if versioning is enabled
    if let Some(ref version_id) = object.version_id {
        response_builder = response_builder.header("x-amz-version-id", version_id);
    }

    // Add SSE header if encryption was applied
    if let Some(ref sse) = object.sse_algorithm {
        response_builder = response_builder.header("x-amz-server-side-encryption", sse);
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
    async fn test_put_object_success() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        let data = Bytes::from("hello world");
        let response = put_object(&storage, "test-bucket", "test-key", data, None, None)
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().contains_key("etag"));
        assert!(response.headers().contains_key("last-modified"));
    }

    #[tokio::test]
    async fn test_put_object_with_content_type() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        let data = Bytes::from("{}");
        let response = put_object(
            &storage,
            "test-bucket",
            "test.json",
            data,
            Some("application/json".to_string()),
            None,
        )
        .await
        .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        // Verify the object was stored with correct content type
        let obj = storage.head_object("test-bucket", "test.json").await.unwrap();
        assert_eq!(obj.content_type, "application/json");
    }

    #[tokio::test]
    async fn test_put_object_bucket_not_found() {
        let (storage, _temp) = create_test_storage().await;

        let result = put_object(
            &storage,
            "nonexistent",
            "key",
            Bytes::from("data"),
            None,
            None,
        )
        .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_put_object_overwrite() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        // Put first version
        put_object(
            &storage,
            "test-bucket",
            "key",
            Bytes::from("version1"),
            None,
            None,
        )
        .await
        .unwrap();

        // Put second version
        put_object(
            &storage,
            "test-bucket",
            "key",
            Bytes::from("version2"),
            None,
            None,
        )
        .await
        .unwrap();

        // Verify second version is stored
        let (_, data) = storage.get_object("test-bucket", "key").await.unwrap();
        assert_eq!(data, Bytes::from("version2"));
    }

    #[tokio::test]
    async fn test_put_object_with_canned_acl() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        let data = Bytes::from("public content");
        let response = put_object(
            &storage,
            "test-bucket",
            "public-key",
            data,
            None,
            Some("public-read"),
        )
        .await
        .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        // Verify the ACL was set
        let acl = storage.get_object_acl("test-bucket", "public-key").await.unwrap();
        assert_eq!(acl.grants.len(), 2); // owner + AllUsers READ
    }

    #[tokio::test]
    async fn test_put_object_conditional_create_new() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        // First PUT with If-None-Match: * should succeed (object doesn't exist)
        let result = put_object_conditional(
            &storage,
            "test-bucket",
            "new-key",
            Bytes::from("initial content"),
            None,
            None,
            Some("*"),
            None,
        )
        .await;

        assert!(result.is_ok());

        // Verify object was created
        let (_, data) = storage.get_object("test-bucket", "new-key").await.unwrap();
        assert_eq!(data, Bytes::from("initial content"));
    }

    #[tokio::test]
    async fn test_put_object_conditional_exists_fails() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        // Create the object first
        put_object(
            &storage,
            "test-bucket",
            "existing-key",
            Bytes::from("original content"),
            None,
            None,
        )
        .await
        .unwrap();

        // Second PUT with If-None-Match: * should fail (object exists)
        let result = put_object_conditional(
            &storage,
            "test-bucket",
            "existing-key",
            Bytes::from("updated content"),
            None,
            None,
            Some("*"),
            None,
        )
        .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, S3ErrorCode::PreconditionFailed);

        // Verify original content is unchanged
        let (_, data) = storage
            .get_object("test-bucket", "existing-key")
            .await
            .unwrap();
        assert_eq!(data, Bytes::from("original content"));
    }

    #[tokio::test]
    async fn test_put_object_conditional_no_condition() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        // PUT without If-None-Match should always succeed
        let result = put_object_conditional(
            &storage,
            "test-bucket",
            "key",
            Bytes::from("content"),
            None,
            None,
            None, // No condition
            None,
        )
        .await;

        assert!(result.is_ok());
    }
}

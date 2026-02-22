//! Object ACL operations

use bytes::Bytes;
use http_body_util::Full;
use hyper::{Response, StatusCode};

use crate::storage::StorageEngine;
use crate::types::bucket::{AccessControlList, CannedAcl};
use crate::types::error::{S3Error, S3ErrorCode, S3Result};
use crate::types::response::GetAclResponse;
use crate::utils::xml::to_xml;

/// Handle GetObjectAcl request
pub async fn get_object_acl(
    storage: &StorageEngine,
    bucket: &str,
    key: &str,
) -> S3Result<Response<Full<Bytes>>> {
    let acl = storage.get_object_acl(bucket, key).await?;

    let response_body = GetAclResponse::from(&acl);
    let xml = to_xml(&response_body)?;

    let response = Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/xml")
        .body(Full::new(Bytes::from(xml)))
        .unwrap();

    Ok(response)
}

/// Handle PutObjectAcl request
pub async fn put_object_acl(
    storage: &StorageEngine,
    bucket: &str,
    key: &str,
    canned_acl: Option<&str>,
    _body: Bytes,
) -> S3Result<Response<Full<Bytes>>> {
    // Check bucket exists
    if !storage.bucket_exists(bucket).await {
        return Err(S3Error::no_such_bucket(bucket));
    }

    // Check object exists
    if !storage.object_exists(bucket, key).await {
        return Err(S3Error::no_such_key(key));
    }

    // For now, we only support canned ACLs via the x-amz-acl header
    // Full ACL XML body support can be added later
    let acl = if let Some(acl_header) = canned_acl {
        let canned = CannedAcl::from_header(acl_header).ok_or_else(|| {
            S3Error::new(
                S3ErrorCode::InvalidArgument,
                format!("Invalid canned ACL: {}", acl_header),
            )
        })?;
        AccessControlList::from_canned(canned)
    } else {
        // If no canned ACL header, default to private
        AccessControlList::default()
    };

    storage.set_object_acl(bucket, key, acl).await?;

    let response = Response::builder()
        .status(StatusCode::OK)
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
    async fn test_get_object_acl_default() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();
        storage
            .put_object("test-bucket", "test-key", Bytes::from("hello"), None, None)
            .await
            .unwrap();

        let response = get_object_acl(&storage, "test-bucket", "test-key")
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Parse body and verify it contains expected ACL elements
        let body = response.into_body();
        let body_bytes = http_body_util::BodyExt::collect(body)
            .await
            .unwrap()
            .to_bytes();
        let body_str = String::from_utf8_lossy(&body_bytes);

        assert!(body_str.contains("AccessControlPolicy"));
        assert!(body_str.contains("Owner"));
        assert!(body_str.contains("FULL_CONTROL"));
    }

    #[tokio::test]
    async fn test_get_object_acl_bucket_not_found() {
        let (storage, _temp) = create_test_storage().await;

        let result = get_object_acl(&storage, "nonexistent", "key").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_object_acl_object_not_found() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        let result = get_object_acl(&storage, "test-bucket", "nonexistent").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_put_object_acl_canned_private() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();
        storage
            .put_object("test-bucket", "test-key", Bytes::from("hello"), None, None)
            .await
            .unwrap();

        let response = put_object_acl(
            &storage,
            "test-bucket",
            "test-key",
            Some("private"),
            Bytes::new(),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Verify ACL was set
        let acl = storage.get_object_acl("test-bucket", "test-key").await.unwrap();
        assert_eq!(acl.grants.len(), 1);
    }

    #[tokio::test]
    async fn test_put_object_acl_canned_public_read() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();
        storage
            .put_object("test-bucket", "test-key", Bytes::from("hello"), None, None)
            .await
            .unwrap();

        let response = put_object_acl(
            &storage,
            "test-bucket",
            "test-key",
            Some("public-read"),
            Bytes::new(),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Verify ACL has 2 grants (owner + AllUsers READ)
        let acl = storage.get_object_acl("test-bucket", "test-key").await.unwrap();
        assert_eq!(acl.grants.len(), 2);
    }

    #[tokio::test]
    async fn test_put_object_acl_invalid_canned() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();
        storage
            .put_object("test-bucket", "test-key", Bytes::from("hello"), None, None)
            .await
            .unwrap();

        let result = put_object_acl(
            &storage,
            "test-bucket",
            "test-key",
            Some("invalid-acl"),
            Bytes::new(),
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_put_object_acl_bucket_not_found() {
        let (storage, _temp) = create_test_storage().await;

        let result =
            put_object_acl(&storage, "nonexistent", "key", Some("private"), Bytes::new()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_put_object_acl_object_not_found() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        let result = put_object_acl(
            &storage,
            "test-bucket",
            "nonexistent",
            Some("private"),
            Bytes::new(),
        )
        .await;
        assert!(result.is_err());
    }
}

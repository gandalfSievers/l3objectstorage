//! Bucket versioning operations

use bytes::Bytes;
use http_body_util::Full;
use hyper::{Response, StatusCode};

use crate::storage::StorageEngine;
use crate::types::bucket::VersioningStatus;
use crate::types::error::{S3Error, S3ErrorCode, S3Result};
use crate::types::response::GetBucketVersioningResponse;
use crate::utils::xml::to_xml;

/// Handle GetBucketVersioning request
pub async fn get_bucket_versioning(
    storage: &StorageEngine,
    bucket: &str,
) -> S3Result<Response<Full<Bytes>>> {
    let bucket_info = storage.get_bucket(bucket).await?;

    let response_body = GetBucketVersioningResponse {
        status: bucket_info.versioning.as_str().map(String::from),
        mfa_delete: None, // Not supported
    };

    let xml = to_xml(&response_body)?;

    let response = Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/xml")
        .body(Full::new(Bytes::from(xml)))
        .unwrap();

    Ok(response)
}

/// Handle PutBucketVersioning request
pub async fn put_bucket_versioning(
    storage: &StorageEngine,
    bucket: &str,
    body: Bytes,
) -> S3Result<Response<Full<Bytes>>> {
    // Check bucket exists
    if !storage.bucket_exists(bucket).await {
        return Err(S3Error::no_such_bucket(bucket));
    }

    // Parse the request body XML
    let status = parse_versioning_configuration(&body)?;

    storage.set_bucket_versioning(bucket, status).await?;

    let response = Response::builder()
        .status(StatusCode::OK)
        .body(Full::new(Bytes::new()))
        .unwrap();

    Ok(response)
}

/// Parse the VersioningConfiguration request XML body
/// Format:
/// <VersioningConfiguration>
///   <Status>Enabled|Suspended</Status>
/// </VersioningConfiguration>
fn parse_versioning_configuration(body: &[u8]) -> S3Result<VersioningStatus> {
    let body_str = std::str::from_utf8(body)
        .map_err(|_| S3Error::new(S3ErrorCode::MalformedXML, "Invalid UTF-8 in request body"))?;

    // Extract Status value
    if let Some(status) = extract_xml_value(body_str, "Status") {
        match status.as_str() {
            "Enabled" => Ok(VersioningStatus::Enabled),
            "Suspended" => Ok(VersioningStatus::Suspended),
            _ => Err(S3Error::new(
                S3ErrorCode::MalformedXML,
                "Invalid versioning status. Must be 'Enabled' or 'Suspended'",
            )),
        }
    } else {
        // If no Status element, treat as disabled (no-op)
        Ok(VersioningStatus::Disabled)
    }
}

/// Extract a value from an XML element
fn extract_xml_value(content: &str, tag: &str) -> Option<String> {
    let open_tag = format!("<{}>", tag);
    let close_tag = format!("</{}>", tag);

    if let Some(start) = content.find(&open_tag) {
        let after_open = &content[start + open_tag.len()..];
        if let Some(end) = after_open.find(&close_tag) {
            let value = &after_open[..end];
            return Some(value.trim().to_string());
        }
    }
    None
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

    #[test]
    fn test_parse_versioning_configuration_enabled() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
        <VersioningConfiguration>
            <Status>Enabled</Status>
        </VersioningConfiguration>"#;

        let status = parse_versioning_configuration(xml.as_bytes()).unwrap();
        assert_eq!(status, VersioningStatus::Enabled);
    }

    #[test]
    fn test_parse_versioning_configuration_suspended() {
        let xml = r#"<VersioningConfiguration><Status>Suspended</Status></VersioningConfiguration>"#;

        let status = parse_versioning_configuration(xml.as_bytes()).unwrap();
        assert_eq!(status, VersioningStatus::Suspended);
    }

    #[test]
    fn test_parse_versioning_configuration_empty() {
        let xml = r#"<VersioningConfiguration></VersioningConfiguration>"#;

        let status = parse_versioning_configuration(xml.as_bytes()).unwrap();
        assert_eq!(status, VersioningStatus::Disabled);
    }

    #[test]
    fn test_parse_versioning_configuration_invalid_status() {
        let xml = r#"<VersioningConfiguration><Status>Invalid</Status></VersioningConfiguration>"#;

        let result = parse_versioning_configuration(xml.as_bytes());
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_bucket_versioning_disabled() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        let response = get_bucket_versioning(&storage, "test-bucket").await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_put_get_bucket_versioning() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        // Enable versioning
        let xml = r#"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"#;
        let response = put_bucket_versioning(&storage, "test-bucket", Bytes::from(xml))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Verify via get_bucket (versioning is stored on the bucket)
        let bucket = storage.get_bucket("test-bucket").await.unwrap();
        assert_eq!(bucket.versioning, VersioningStatus::Enabled);
    }

    #[tokio::test]
    async fn test_put_bucket_versioning_bucket_not_found() {
        let (storage, _temp) = create_test_storage().await;

        let xml = r#"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"#;
        let result = put_bucket_versioning(&storage, "nonexistent", Bytes::from(xml)).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_versioning_lifecycle() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        // Initially disabled
        let bucket = storage.get_bucket("test-bucket").await.unwrap();
        assert_eq!(bucket.versioning, VersioningStatus::Disabled);

        // Enable
        let xml = r#"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"#;
        put_bucket_versioning(&storage, "test-bucket", Bytes::from(xml))
            .await
            .unwrap();

        let bucket = storage.get_bucket("test-bucket").await.unwrap();
        assert_eq!(bucket.versioning, VersioningStatus::Enabled);

        // Suspend
        let xml = r#"<VersioningConfiguration><Status>Suspended</Status></VersioningConfiguration>"#;
        put_bucket_versioning(&storage, "test-bucket", Bytes::from(xml))
            .await
            .unwrap();

        let bucket = storage.get_bucket("test-bucket").await.unwrap();
        assert_eq!(bucket.versioning, VersioningStatus::Suspended);
    }
}

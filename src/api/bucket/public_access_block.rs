//! Public Access Block Configuration operations

use bytes::Bytes;
use http_body_util::Full;
use hyper::{Response, StatusCode};

use crate::storage::StorageEngine;
use crate::types::bucket::PublicAccessBlockConfiguration;
use crate::types::error::{S3Error, S3ErrorCode, S3Result};

/// Handle GetPublicAccessBlock request
pub async fn get_public_access_block(
    storage: &StorageEngine,
    bucket: &str,
) -> S3Result<Response<Full<Bytes>>> {
    let config = storage.get_public_access_block(bucket).await?;

    let xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<PublicAccessBlockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <BlockPublicAcls>{}</BlockPublicAcls>
  <IgnorePublicAcls>{}</IgnorePublicAcls>
  <BlockPublicPolicy>{}</BlockPublicPolicy>
  <RestrictPublicBuckets>{}</RestrictPublicBuckets>
</PublicAccessBlockConfiguration>"#,
        config.block_public_acls,
        config.ignore_public_acls,
        config.block_public_policy,
        config.restrict_public_buckets
    );

    let response = Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/xml")
        .body(Full::new(Bytes::from(xml)))
        .unwrap();

    Ok(response)
}

/// Handle PutPublicAccessBlock request
pub async fn put_public_access_block(
    storage: &StorageEngine,
    bucket: &str,
    body: Bytes,
) -> S3Result<Response<Full<Bytes>>> {
    // Check bucket exists
    if !storage.bucket_exists(bucket).await {
        return Err(S3Error::no_such_bucket(bucket));
    }

    // Parse the request body XML
    let config = parse_public_access_block_configuration(&body)?;

    storage.set_public_access_block(bucket, config).await?;

    let response = Response::builder()
        .status(StatusCode::OK)
        .body(Full::new(Bytes::new()))
        .unwrap();

    Ok(response)
}

/// Handle DeletePublicAccessBlock request
pub async fn delete_public_access_block(
    storage: &StorageEngine,
    bucket: &str,
) -> S3Result<Response<Full<Bytes>>> {
    // Check bucket exists
    if !storage.bucket_exists(bucket).await {
        return Err(S3Error::no_such_bucket(bucket));
    }

    storage.delete_public_access_block(bucket).await?;

    let response = Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Full::new(Bytes::new()))
        .unwrap();

    Ok(response)
}

/// Parse the PublicAccessBlockConfiguration request XML body
/// Format:
/// <PublicAccessBlockConfiguration>
///   <BlockPublicAcls>true</BlockPublicAcls>
///   <IgnorePublicAcls>true</IgnorePublicAcls>
///   <BlockPublicPolicy>true</BlockPublicPolicy>
///   <RestrictPublicBuckets>true</RestrictPublicBuckets>
/// </PublicAccessBlockConfiguration>
fn parse_public_access_block_configuration(body: &[u8]) -> S3Result<PublicAccessBlockConfiguration> {
    let body_str = std::str::from_utf8(body)
        .map_err(|_| S3Error::new(S3ErrorCode::MalformedXML, "Invalid UTF-8 in request body"))?;

    let mut config = PublicAccessBlockConfiguration::new();

    // Extract BlockPublicAcls
    if let Some(value) = extract_xml_value(body_str, "BlockPublicAcls") {
        config.block_public_acls = parse_bool(&value);
    }

    // Extract IgnorePublicAcls
    if let Some(value) = extract_xml_value(body_str, "IgnorePublicAcls") {
        config.ignore_public_acls = parse_bool(&value);
    }

    // Extract BlockPublicPolicy
    if let Some(value) = extract_xml_value(body_str, "BlockPublicPolicy") {
        config.block_public_policy = parse_bool(&value);
    }

    // Extract RestrictPublicBuckets
    if let Some(value) = extract_xml_value(body_str, "RestrictPublicBuckets") {
        config.restrict_public_buckets = parse_bool(&value);
    }

    Ok(config)
}

/// Parse a boolean value from string
fn parse_bool(s: &str) -> bool {
    s.to_lowercase() == "true"
}

/// Extract a single value from an XML element
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
    fn test_parse_public_access_block_all_true() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
        <PublicAccessBlockConfiguration>
            <BlockPublicAcls>true</BlockPublicAcls>
            <IgnorePublicAcls>true</IgnorePublicAcls>
            <BlockPublicPolicy>true</BlockPublicPolicy>
            <RestrictPublicBuckets>true</RestrictPublicBuckets>
        </PublicAccessBlockConfiguration>"#;

        let config = parse_public_access_block_configuration(xml.as_bytes()).unwrap();

        assert!(config.block_public_acls);
        assert!(config.ignore_public_acls);
        assert!(config.block_public_policy);
        assert!(config.restrict_public_buckets);
    }

    #[test]
    fn test_parse_public_access_block_mixed() {
        let xml = r#"<PublicAccessBlockConfiguration>
            <BlockPublicAcls>true</BlockPublicAcls>
            <BlockPublicPolicy>false</BlockPublicPolicy>
        </PublicAccessBlockConfiguration>"#;

        let config = parse_public_access_block_configuration(xml.as_bytes()).unwrap();

        assert!(config.block_public_acls);
        assert!(!config.ignore_public_acls); // Not specified, defaults to false
        assert!(!config.block_public_policy);
        assert!(!config.restrict_public_buckets); // Not specified, defaults to false
    }

    #[test]
    fn test_parse_public_access_block_empty() {
        let xml = r#"<PublicAccessBlockConfiguration></PublicAccessBlockConfiguration>"#;

        let config = parse_public_access_block_configuration(xml.as_bytes()).unwrap();

        assert!(!config.block_public_acls);
        assert!(!config.ignore_public_acls);
        assert!(!config.block_public_policy);
        assert!(!config.restrict_public_buckets);
    }

    #[tokio::test]
    async fn test_put_get_delete_public_access_block() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        // Put public access block configuration
        let xml = r#"<PublicAccessBlockConfiguration>
            <BlockPublicAcls>true</BlockPublicAcls>
            <IgnorePublicAcls>true</IgnorePublicAcls>
            <BlockPublicPolicy>true</BlockPublicPolicy>
            <RestrictPublicBuckets>true</RestrictPublicBuckets>
        </PublicAccessBlockConfiguration>"#;

        let response = put_public_access_block(&storage, "test-bucket", Bytes::from(xml))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Get public access block configuration
        let response = get_public_access_block(&storage, "test-bucket").await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Delete public access block configuration
        let response = delete_public_access_block(&storage, "test-bucket")
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        // Get should fail now
        let result = get_public_access_block(&storage, "test-bucket").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_public_access_block_no_config() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        // Should fail with NoSuchPublicAccessBlockConfiguration
        let result = get_public_access_block(&storage, "test-bucket").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_put_public_access_block_bucket_not_found() {
        let (storage, _temp) = create_test_storage().await;

        let xml = r#"<PublicAccessBlockConfiguration>
            <BlockPublicAcls>true</BlockPublicAcls>
        </PublicAccessBlockConfiguration>"#;

        let result = put_public_access_block(&storage, "nonexistent", Bytes::from(xml)).await;
        assert!(result.is_err());
    }
}

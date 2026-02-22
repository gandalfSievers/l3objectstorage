//! Bucket Logging Configuration operations

use bytes::Bytes;
use http_body_util::Full;
use hyper::{Response, StatusCode};

use crate::storage::StorageEngine;
use crate::types::bucket::LoggingConfiguration;
use crate::types::error::{S3Error, S3ErrorCode, S3Result};

/// Handle GetBucketLogging request
pub async fn get_bucket_logging(
    storage: &StorageEngine,
    bucket: &str,
) -> S3Result<Response<Full<Bytes>>> {
    // Check bucket exists
    if !storage.bucket_exists(bucket).await {
        return Err(S3Error::no_such_bucket(bucket));
    }

    let config = storage.get_bucket_logging(bucket).await?;

    // Generate XML response
    let xml = match config {
        Some(logging) if logging.is_enabled() => {
            format!(
                r#"<?xml version="1.0" encoding="UTF-8"?>
<BucketLoggingStatus xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <LoggingEnabled>
        <TargetBucket>{}</TargetBucket>
        <TargetPrefix>{}</TargetPrefix>
    </LoggingEnabled>
</BucketLoggingStatus>"#,
                logging.target_bucket.unwrap_or_default(),
                logging.target_prefix.unwrap_or_default()
            )
        }
        _ => {
            // No logging configured - return empty BucketLoggingStatus
            r#"<?xml version="1.0" encoding="UTF-8"?>
<BucketLoggingStatus xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></BucketLoggingStatus>"#
                .to_string()
        }
    };

    let response = Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/xml")
        .body(Full::new(Bytes::from(xml)))
        .unwrap();

    Ok(response)
}

/// Handle PutBucketLogging request
pub async fn put_bucket_logging(
    storage: &StorageEngine,
    bucket: &str,
    body: Bytes,
) -> S3Result<Response<Full<Bytes>>> {
    // Check bucket exists
    if !storage.bucket_exists(bucket).await {
        return Err(S3Error::no_such_bucket(bucket));
    }

    // Parse the request body XML
    let config = parse_bucket_logging_status(&body)?;

    storage.set_bucket_logging(bucket, config).await?;

    let response = Response::builder()
        .status(StatusCode::OK)
        .body(Full::new(Bytes::new()))
        .unwrap();

    Ok(response)
}

/// Parse the BucketLoggingStatus request XML body
fn parse_bucket_logging_status(body: &[u8]) -> S3Result<LoggingConfiguration> {
    let body_str = std::str::from_utf8(body)
        .map_err(|_| S3Error::new(S3ErrorCode::MalformedXML, "Invalid UTF-8 in request body"))?;

    let mut config = LoggingConfiguration::new();

    // Check if LoggingEnabled is present
    if let Some(logging_content) = extract_xml_block(body_str, "LoggingEnabled") {
        // Parse TargetBucket (required when LoggingEnabled is present)
        if let Some(target_bucket) = extract_xml_value(&logging_content, "TargetBucket") {
            config.target_bucket = Some(target_bucket);
        }

        // Parse TargetPrefix (optional)
        if let Some(target_prefix) = extract_xml_value(&logging_content, "TargetPrefix") {
            config.target_prefix = Some(target_prefix);
        }

        // Note: TargetGrants parsing is omitted for simplicity
        // (grants are rarely used in practice)
    }

    // If no LoggingEnabled block, return empty config (disables logging)
    Ok(config)
}

/// Extract a block of XML content between tags
fn extract_xml_block(content: &str, tag: &str) -> Option<String> {
    let open_tag = format!("<{}", tag);
    let close_tag = format!("</{}>", tag);

    if let Some(start) = content.find(&open_tag) {
        // Find the end of the opening tag (handle attributes)
        let after_open_tag = &content[start + open_tag.len()..];
        let tag_end = after_open_tag.find('>')?;
        let content_start = start + open_tag.len() + tag_end + 1;

        if let Some(end) = content[content_start..].find(&close_tag) {
            return Some(content[content_start..content_start + end].to_string());
        }
    }
    None
}

/// Extract a single value from an XML element
fn extract_xml_value(content: &str, tag: &str) -> Option<String> {
    let open_tag = format!("<{}>", tag);
    let close_tag = format!("</{}>", tag);

    if let Some(start) = content.find(&open_tag) {
        let after_open = &content[start + open_tag.len()..];
        if let Some(end) = after_open.find(&close_tag) {
            let value = &after_open[..end];
            return Some(decode_xml_entities(value.trim()));
        }
    }
    None
}

/// Decode XML entities
fn decode_xml_entities(s: &str) -> String {
    s.replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
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
    fn test_parse_bucket_logging_status_basic() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
        <BucketLoggingStatus>
            <LoggingEnabled>
                <TargetBucket>mybucket-logs</TargetBucket>
                <TargetPrefix>logs/</TargetPrefix>
            </LoggingEnabled>
        </BucketLoggingStatus>"#;

        let config = parse_bucket_logging_status(xml.as_bytes()).unwrap();

        assert!(config.is_enabled());
        assert_eq!(config.target_bucket, Some("mybucket-logs".to_string()));
        assert_eq!(config.target_prefix, Some("logs/".to_string()));
    }

    #[test]
    fn test_parse_bucket_logging_status_empty() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
        <BucketLoggingStatus></BucketLoggingStatus>"#;

        let config = parse_bucket_logging_status(xml.as_bytes()).unwrap();

        assert!(!config.is_enabled());
        assert!(config.target_bucket.is_none());
    }

    #[test]
    fn test_parse_bucket_logging_status_no_prefix() {
        let xml = r#"<BucketLoggingStatus>
            <LoggingEnabled>
                <TargetBucket>mybucket-logs</TargetBucket>
            </LoggingEnabled>
        </BucketLoggingStatus>"#;

        let config = parse_bucket_logging_status(xml.as_bytes()).unwrap();

        assert!(config.is_enabled());
        assert_eq!(config.target_bucket, Some("mybucket-logs".to_string()));
        assert!(config.target_prefix.is_none());
    }

    #[tokio::test]
    async fn test_put_get_bucket_logging() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        // Put logging configuration
        let xml = r#"<BucketLoggingStatus>
            <LoggingEnabled>
                <TargetBucket>log-bucket</TargetBucket>
                <TargetPrefix>logs/</TargetPrefix>
            </LoggingEnabled>
        </BucketLoggingStatus>"#;

        let response = put_bucket_logging(&storage, "test-bucket", Bytes::from(xml))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Get logging configuration
        let response = get_bucket_logging(&storage, "test-bucket").await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_bucket_logging_not_configured() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        // Should return empty BucketLoggingStatus (not an error)
        let response = get_bucket_logging(&storage, "test-bucket").await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_put_bucket_logging_disable() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        // Enable logging first
        let xml = r#"<BucketLoggingStatus>
            <LoggingEnabled>
                <TargetBucket>log-bucket</TargetBucket>
                <TargetPrefix>logs/</TargetPrefix>
            </LoggingEnabled>
        </BucketLoggingStatus>"#;

        put_bucket_logging(&storage, "test-bucket", Bytes::from(xml))
            .await
            .unwrap();

        // Disable logging
        let disable_xml = r#"<BucketLoggingStatus></BucketLoggingStatus>"#;
        let response = put_bucket_logging(&storage, "test-bucket", Bytes::from(disable_xml))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_put_bucket_logging_bucket_not_found() {
        let (storage, _temp) = create_test_storage().await;

        let xml = r#"<BucketLoggingStatus>
            <LoggingEnabled>
                <TargetBucket>log-bucket</TargetBucket>
            </LoggingEnabled>
        </BucketLoggingStatus>"#;

        let result = put_bucket_logging(&storage, "nonexistent", Bytes::from(xml)).await;
        assert!(result.is_err());
    }
}

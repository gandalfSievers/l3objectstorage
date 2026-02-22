//! Bucket Server-Side Encryption Configuration operations

use bytes::Bytes;
use http_body_util::Full;
use hyper::{Response, StatusCode};

use crate::storage::StorageEngine;
use crate::types::bucket::{
    ServerSideEncryptionByDefault, ServerSideEncryptionConfiguration, ServerSideEncryptionRule,
    SseAlgorithm,
};
use crate::types::error::{S3Error, S3ErrorCode, S3Result};
use crate::types::response::GetBucketEncryptionResponse;
use crate::utils::xml::to_xml;

/// Handle GetBucketEncryption request
pub async fn get_bucket_encryption(
    storage: &StorageEngine,
    bucket: &str,
) -> S3Result<Response<Full<Bytes>>> {
    let encryption = storage.get_bucket_encryption(bucket).await?;

    let response_body = GetBucketEncryptionResponse::from(&encryption);

    let xml = to_xml(&response_body)?;

    let response = Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/xml")
        .body(Full::new(Bytes::from(xml)))
        .unwrap();

    Ok(response)
}

/// Handle PutBucketEncryption request
pub async fn put_bucket_encryption(
    storage: &StorageEngine,
    bucket: &str,
    body: Bytes,
) -> S3Result<Response<Full<Bytes>>> {
    // Check bucket exists
    if !storage.bucket_exists(bucket).await {
        return Err(S3Error::no_such_bucket(bucket));
    }

    // Parse the request body XML
    let encryption = parse_encryption_configuration(&body)?;

    storage.set_bucket_encryption(bucket, encryption).await?;

    let response = Response::builder()
        .status(StatusCode::OK)
        .body(Full::new(Bytes::new()))
        .unwrap();

    Ok(response)
}

/// Handle DeleteBucketEncryption request
pub async fn delete_bucket_encryption(
    storage: &StorageEngine,
    bucket: &str,
) -> S3Result<Response<Full<Bytes>>> {
    // Check bucket exists
    if !storage.bucket_exists(bucket).await {
        return Err(S3Error::no_such_bucket(bucket));
    }

    storage.delete_bucket_encryption(bucket).await?;

    let response = Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Full::new(Bytes::new()))
        .unwrap();

    Ok(response)
}

/// Parse the ServerSideEncryptionConfiguration request XML body
/// Format:
/// <ServerSideEncryptionConfiguration>
///   <Rule>
///     <ApplyServerSideEncryptionByDefault>
///       <SSEAlgorithm>AES256</SSEAlgorithm>
///     </ApplyServerSideEncryptionByDefault>
///     <BucketKeyEnabled>false</BucketKeyEnabled>
///   </Rule>
/// </ServerSideEncryptionConfiguration>
fn parse_encryption_configuration(body: &[u8]) -> S3Result<ServerSideEncryptionConfiguration> {
    let body_str = std::str::from_utf8(body)
        .map_err(|_| S3Error::new(S3ErrorCode::MalformedXML, "Invalid UTF-8 in request body"))?;

    let mut rules = Vec::new();
    let mut remaining = body_str;

    // Parse each Rule element
    while let Some(rule_start) = remaining.find("<Rule>") {
        let after_rule_start = &remaining[rule_start + 6..];

        if let Some(rule_end) = after_rule_start.find("</Rule>") {
            let rule_content = &after_rule_start[..rule_end];

            let rule = parse_encryption_rule(rule_content)?;
            rules.push(rule);

            remaining = &after_rule_start[rule_end + 7..];
        } else {
            break;
        }
    }

    if rules.is_empty() {
        return Err(S3Error::new(
            S3ErrorCode::MalformedXML,
            "ServerSideEncryptionConfiguration must have at least one Rule",
        ));
    }

    Ok(ServerSideEncryptionConfiguration { rules })
}

/// Parse a single encryption rule
fn parse_encryption_rule(content: &str) -> S3Result<ServerSideEncryptionRule> {
    let mut apply_sse_by_default = None;
    let mut bucket_key_enabled = false;

    // Parse ApplyServerSideEncryptionByDefault
    if let Some(sse_start) = content.find("<ApplyServerSideEncryptionByDefault>") {
        let after_sse = &content[sse_start + 36..];
        if let Some(sse_end) = after_sse.find("</ApplyServerSideEncryptionByDefault>") {
            let sse_content = &after_sse[..sse_end];

            // Extract SSEAlgorithm (required)
            let algorithm_str = extract_xml_value(sse_content, "SSEAlgorithm").ok_or_else(|| {
                S3Error::new(
                    S3ErrorCode::MalformedXML,
                    "ApplyServerSideEncryptionByDefault must have SSEAlgorithm",
                )
            })?;

            let sse_algorithm = SseAlgorithm::from_str(&algorithm_str).ok_or_else(|| {
                S3Error::new(
                    S3ErrorCode::InvalidArgument,
                    format!("Invalid SSEAlgorithm: {}", algorithm_str),
                )
            })?;

            // Extract KMSMasterKeyID (optional, for SSE-KMS)
            let kms_master_key_id = extract_xml_value(sse_content, "KMSMasterKeyID");

            apply_sse_by_default = Some(ServerSideEncryptionByDefault {
                sse_algorithm,
                kms_master_key_id,
            });
        }
    }

    // Parse BucketKeyEnabled (optional)
    if let Some(bke_str) = extract_xml_value(content, "BucketKeyEnabled") {
        bucket_key_enabled = bke_str.to_lowercase() == "true";
    }

    Ok(ServerSideEncryptionRule {
        apply_server_side_encryption_by_default: apply_sse_by_default,
        bucket_key_enabled,
    })
}

/// Extract a single value from an XML element
fn extract_xml_value(content: &str, tag: &str) -> Option<String> {
    let open_tag = format!("<{}>", tag);
    let close_tag = format!("</{}>", tag);

    if let Some(start) = content.find(&open_tag) {
        let after_open = &content[start + open_tag.len()..];
        if let Some(end) = after_open.find(&close_tag) {
            let value = &after_open[..end];
            return Some(decode_xml_entities(value));
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
    fn test_parse_encryption_configuration_basic() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
        <ServerSideEncryptionConfiguration>
            <Rule>
                <ApplyServerSideEncryptionByDefault>
                    <SSEAlgorithm>AES256</SSEAlgorithm>
                </ApplyServerSideEncryptionByDefault>
            </Rule>
        </ServerSideEncryptionConfiguration>"#;

        let config = parse_encryption_configuration(xml.as_bytes()).unwrap();

        assert_eq!(config.rules.len(), 1);
        let default = config.rules[0]
            .apply_server_side_encryption_by_default
            .as_ref()
            .unwrap();
        assert_eq!(default.sse_algorithm, SseAlgorithm::Aes256);
        assert!(default.kms_master_key_id.is_none());
    }

    #[test]
    fn test_parse_encryption_configuration_with_bucket_key() {
        let xml = r#"<ServerSideEncryptionConfiguration>
            <Rule>
                <ApplyServerSideEncryptionByDefault>
                    <SSEAlgorithm>AES256</SSEAlgorithm>
                </ApplyServerSideEncryptionByDefault>
                <BucketKeyEnabled>true</BucketKeyEnabled>
            </Rule>
        </ServerSideEncryptionConfiguration>"#;

        let config = parse_encryption_configuration(xml.as_bytes()).unwrap();

        assert_eq!(config.rules.len(), 1);
        assert!(config.rules[0].bucket_key_enabled);
    }

    #[test]
    fn test_parse_encryption_configuration_empty() {
        let xml = r#"<ServerSideEncryptionConfiguration></ServerSideEncryptionConfiguration>"#;

        let result = parse_encryption_configuration(xml.as_bytes());
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_encryption_configuration_missing_algorithm() {
        let xml = r#"<ServerSideEncryptionConfiguration>
            <Rule>
                <ApplyServerSideEncryptionByDefault>
                </ApplyServerSideEncryptionByDefault>
            </Rule>
        </ServerSideEncryptionConfiguration>"#;

        let result = parse_encryption_configuration(xml.as_bytes());
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_put_get_delete_bucket_encryption() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        // Put encryption configuration
        let xml = r#"<ServerSideEncryptionConfiguration>
            <Rule>
                <ApplyServerSideEncryptionByDefault>
                    <SSEAlgorithm>AES256</SSEAlgorithm>
                </ApplyServerSideEncryptionByDefault>
            </Rule>
        </ServerSideEncryptionConfiguration>"#;

        let response = put_bucket_encryption(&storage, "test-bucket", Bytes::from(xml))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Get encryption configuration
        let response = get_bucket_encryption(&storage, "test-bucket").await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Delete encryption configuration
        let response = delete_bucket_encryption(&storage, "test-bucket")
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        // Get encryption should fail now
        let result = get_bucket_encryption(&storage, "test-bucket").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_bucket_encryption_no_config() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        // Should fail with ServerSideEncryptionConfigurationNotFoundError
        let result = get_bucket_encryption(&storage, "test-bucket").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_put_bucket_encryption_bucket_not_found() {
        let (storage, _temp) = create_test_storage().await;

        let xml = r#"<ServerSideEncryptionConfiguration>
            <Rule>
                <ApplyServerSideEncryptionByDefault>
                    <SSEAlgorithm>AES256</SSEAlgorithm>
                </ApplyServerSideEncryptionByDefault>
            </Rule>
        </ServerSideEncryptionConfiguration>"#;

        let result = put_bucket_encryption(&storage, "nonexistent", Bytes::from(xml)).await;
        assert!(result.is_err());
    }
}

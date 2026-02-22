//! Bucket Lifecycle Configuration operations

use bytes::Bytes;
use http_body_util::Full;
use hyper::{Response, StatusCode};

use crate::storage::StorageEngine;
use crate::types::bucket::{
    LifecycleConfiguration, LifecycleExpiration, LifecycleRule, LifecycleRuleFilter,
    LifecycleRuleStatus, NoncurrentVersionExpiration, Tag,
};
use crate::types::error::{S3Error, S3ErrorCode, S3Result};
use crate::types::response::GetBucketLifecycleConfigurationResponse;
use crate::utils::xml::to_xml;

/// Handle GetBucketLifecycleConfiguration request
pub async fn get_bucket_lifecycle_configuration(
    storage: &StorageEngine,
    bucket: &str,
) -> S3Result<Response<Full<Bytes>>> {
    let lifecycle = storage.get_bucket_lifecycle(bucket).await?;

    let response_body = GetBucketLifecycleConfigurationResponse::from(&lifecycle);

    let xml = to_xml(&response_body)?;

    let response = Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/xml")
        .body(Full::new(Bytes::from(xml)))
        .unwrap();

    Ok(response)
}

/// Handle PutBucketLifecycleConfiguration request
pub async fn put_bucket_lifecycle_configuration(
    storage: &StorageEngine,
    bucket: &str,
    body: Bytes,
) -> S3Result<Response<Full<Bytes>>> {
    // Check bucket exists
    if !storage.bucket_exists(bucket).await {
        return Err(S3Error::no_such_bucket(bucket));
    }

    // Parse the request body XML
    let lifecycle = parse_lifecycle_configuration(&body)?;

    storage.set_bucket_lifecycle(bucket, lifecycle).await?;

    let response = Response::builder()
        .status(StatusCode::OK)
        .body(Full::new(Bytes::new()))
        .unwrap();

    Ok(response)
}

/// Handle DeleteBucketLifecycle request
pub async fn delete_bucket_lifecycle(
    storage: &StorageEngine,
    bucket: &str,
) -> S3Result<Response<Full<Bytes>>> {
    // Check bucket exists
    if !storage.bucket_exists(bucket).await {
        return Err(S3Error::no_such_bucket(bucket));
    }

    storage.delete_bucket_lifecycle(bucket).await?;

    let response = Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Full::new(Bytes::new()))
        .unwrap();

    Ok(response)
}

/// Parse the Lifecycle configuration request XML body
/// Format:
/// <LifecycleConfiguration>
///   <Rule>
///     <ID>rule-id</ID>
///     <Status>Enabled</Status>
///     <Filter>
///       <Prefix>logs/</Prefix>
///     </Filter>
///     <Expiration>
///       <Days>30</Days>
///     </Expiration>
///   </Rule>
/// </LifecycleConfiguration>
fn parse_lifecycle_configuration(body: &[u8]) -> S3Result<LifecycleConfiguration> {
    let body_str = std::str::from_utf8(body)
        .map_err(|_| S3Error::new(S3ErrorCode::MalformedXML, "Invalid UTF-8 in request body"))?;

    let mut rules = Vec::new();
    let mut remaining = body_str;

    // Parse each Rule element
    while let Some(rule_start) = remaining.find("<Rule>") {
        let after_rule_start = &remaining[rule_start + 6..];

        if let Some(rule_end) = after_rule_start.find("</Rule>") {
            let rule_content = &after_rule_start[..rule_end];

            let rule = parse_lifecycle_rule(rule_content)?;
            rules.push(rule);

            remaining = &after_rule_start[rule_end + 7..];
        } else {
            break;
        }
    }

    if rules.is_empty() {
        return Err(S3Error::new(
            S3ErrorCode::MalformedXML,
            "LifecycleConfiguration must have at least one Rule",
        ));
    }

    Ok(LifecycleConfiguration { rules })
}

/// Parse a single lifecycle rule
fn parse_lifecycle_rule(content: &str) -> S3Result<LifecycleRule> {
    // Extract ID (optional)
    let id = extract_xml_value(content, "ID");

    // Extract Status (required)
    let status_str = extract_xml_value(content, "Status").ok_or_else(|| {
        S3Error::new(S3ErrorCode::MalformedXML, "Rule must have a Status element")
    })?;

    let status = LifecycleRuleStatus::from_str(&status_str).ok_or_else(|| {
        S3Error::new(
            S3ErrorCode::MalformedXML,
            format!("Invalid Status value: {}", status_str),
        )
    })?;

    // Extract Filter
    let filter = parse_lifecycle_filter(content)?;

    // Extract Expiration (optional)
    let expiration = parse_expiration(content);

    // Extract NoncurrentVersionExpiration (optional)
    let noncurrent_version_expiration = parse_noncurrent_version_expiration(content);

    Ok(LifecycleRule {
        id,
        status,
        filter,
        expiration,
        noncurrent_version_expiration,
        transitions: None,
        noncurrent_version_transitions: None,
        abort_incomplete_multipart_upload: None,
    })
}

/// Parse the Filter element
fn parse_lifecycle_filter(content: &str) -> S3Result<LifecycleRuleFilter> {
    let mut filter = LifecycleRuleFilter::default();

    // Check if Filter element exists
    if let Some(filter_start) = content.find("<Filter>") {
        let after_filter = &content[filter_start + 8..];
        if let Some(filter_end) = after_filter.find("</Filter>") {
            let filter_content = &after_filter[..filter_end];

            // Extract Prefix (can be empty string)
            if let Some(prefix) = extract_xml_value(filter_content, "Prefix") {
                filter.prefix = Some(prefix);
            } else if filter_content.contains("<Prefix/>") || filter_content.contains("<Prefix></Prefix>") {
                filter.prefix = Some(String::new());
            }

            // Extract Tag (optional)
            if let Some(tag_start) = filter_content.find("<Tag>") {
                let after_tag = &filter_content[tag_start + 5..];
                if let Some(tag_end) = after_tag.find("</Tag>") {
                    let tag_content = &after_tag[..tag_end];
                    if let (Some(key), Some(value)) = (
                        extract_xml_value(tag_content, "Key"),
                        extract_xml_value(tag_content, "Value"),
                    ) {
                        filter.tag = Some(Tag { key, value });
                    }
                }
            }

            // Extract ObjectSizeGreaterThan (optional)
            if let Some(size_str) = extract_xml_value(filter_content, "ObjectSizeGreaterThan") {
                if let Ok(size) = size_str.parse::<i64>() {
                    filter.object_size_greater_than = Some(size);
                }
            }

            // Extract ObjectSizeLessThan (optional)
            if let Some(size_str) = extract_xml_value(filter_content, "ObjectSizeLessThan") {
                if let Ok(size) = size_str.parse::<i64>() {
                    filter.object_size_less_than = Some(size);
                }
            }
        }
    }

    Ok(filter)
}

/// Parse the Expiration element
fn parse_expiration(content: &str) -> Option<LifecycleExpiration> {
    if let Some(exp_start) = content.find("<Expiration>") {
        let after_exp = &content[exp_start + 12..];
        if let Some(exp_end) = after_exp.find("</Expiration>") {
            let exp_content = &after_exp[..exp_end];

            let days = extract_xml_value(exp_content, "Days")
                .and_then(|s| s.parse::<i32>().ok());

            let date = extract_xml_value(exp_content, "Date");

            let expired_object_delete_marker = extract_xml_value(exp_content, "ExpiredObjectDeleteMarker")
                .and_then(|s| s.parse::<bool>().ok());

            // Only return Some if at least one field is present
            if days.is_some() || date.is_some() || expired_object_delete_marker.is_some() {
                return Some(LifecycleExpiration {
                    days,
                    date,
                    expired_object_delete_marker,
                });
            }
        }
    }
    None
}

/// Parse the NoncurrentVersionExpiration element
fn parse_noncurrent_version_expiration(content: &str) -> Option<NoncurrentVersionExpiration> {
    if let Some(nve_start) = content.find("<NoncurrentVersionExpiration>") {
        let after_nve = &content[nve_start + 29..];
        if let Some(nve_end) = after_nve.find("</NoncurrentVersionExpiration>") {
            let nve_content = &after_nve[..nve_end];

            let noncurrent_days = extract_xml_value(nve_content, "NoncurrentDays")
                .and_then(|s| s.parse::<i32>().ok());

            let newer_noncurrent_versions = extract_xml_value(nve_content, "NewerNoncurrentVersions")
                .and_then(|s| s.parse::<i32>().ok());

            // Only return Some if at least one field is present
            if noncurrent_days.is_some() || newer_noncurrent_versions.is_some() {
                return Some(NoncurrentVersionExpiration {
                    noncurrent_days,
                    newer_noncurrent_versions,
                });
            }
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
    fn test_parse_lifecycle_configuration_basic() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
        <LifecycleConfiguration>
            <Rule>
                <ID>expire-logs</ID>
                <Status>Enabled</Status>
                <Filter>
                    <Prefix>logs/</Prefix>
                </Filter>
                <Expiration>
                    <Days>30</Days>
                </Expiration>
            </Rule>
        </LifecycleConfiguration>"#;

        let lifecycle = parse_lifecycle_configuration(xml.as_bytes()).unwrap();

        assert_eq!(lifecycle.rules.len(), 1);
        assert_eq!(lifecycle.rules[0].id, Some("expire-logs".to_string()));
        assert_eq!(lifecycle.rules[0].status, LifecycleRuleStatus::Enabled);
        assert_eq!(lifecycle.rules[0].filter.prefix, Some("logs/".to_string()));
        assert_eq!(lifecycle.rules[0].expiration.as_ref().unwrap().days, Some(30));
    }

    #[test]
    fn test_parse_lifecycle_configuration_multiple_rules() {
        let xml = r#"<LifecycleConfiguration>
            <Rule>
                <ID>rule1</ID>
                <Status>Enabled</Status>
                <Filter>
                    <Prefix>logs/</Prefix>
                </Filter>
                <Expiration>
                    <Days>7</Days>
                </Expiration>
            </Rule>
            <Rule>
                <ID>rule2</ID>
                <Status>Disabled</Status>
                <Filter>
                    <Prefix>temp/</Prefix>
                </Filter>
                <Expiration>
                    <Days>1</Days>
                </Expiration>
            </Rule>
        </LifecycleConfiguration>"#;

        let lifecycle = parse_lifecycle_configuration(xml.as_bytes()).unwrap();

        assert_eq!(lifecycle.rules.len(), 2);
        assert_eq!(lifecycle.rules[0].id, Some("rule1".to_string()));
        assert_eq!(lifecycle.rules[0].status, LifecycleRuleStatus::Enabled);
        assert_eq!(lifecycle.rules[1].id, Some("rule2".to_string()));
        assert_eq!(lifecycle.rules[1].status, LifecycleRuleStatus::Disabled);
    }

    #[test]
    fn test_parse_lifecycle_configuration_noncurrent_version() {
        let xml = r#"<LifecycleConfiguration>
            <Rule>
                <ID>cleanup-versions</ID>
                <Status>Enabled</Status>
                <Filter>
                    <Prefix></Prefix>
                </Filter>
                <NoncurrentVersionExpiration>
                    <NoncurrentDays>30</NoncurrentDays>
                </NoncurrentVersionExpiration>
            </Rule>
        </LifecycleConfiguration>"#;

        let lifecycle = parse_lifecycle_configuration(xml.as_bytes()).unwrap();

        assert_eq!(lifecycle.rules.len(), 1);
        let nve = lifecycle.rules[0].noncurrent_version_expiration.as_ref().unwrap();
        assert_eq!(nve.noncurrent_days, Some(30));
    }

    #[test]
    fn test_parse_lifecycle_configuration_empty_prefix() {
        let xml = r#"<LifecycleConfiguration>
            <Rule>
                <Status>Enabled</Status>
                <Filter>
                    <Prefix></Prefix>
                </Filter>
                <Expiration>
                    <Days>30</Days>
                </Expiration>
            </Rule>
        </LifecycleConfiguration>"#;

        let lifecycle = parse_lifecycle_configuration(xml.as_bytes()).unwrap();

        assert_eq!(lifecycle.rules.len(), 1);
        assert_eq!(lifecycle.rules[0].filter.prefix, Some(String::new()));
    }

    #[test]
    fn test_parse_lifecycle_configuration_missing_status() {
        let xml = r#"<LifecycleConfiguration>
            <Rule>
                <ID>test</ID>
                <Filter>
                    <Prefix>logs/</Prefix>
                </Filter>
            </Rule>
        </LifecycleConfiguration>"#;

        let result = parse_lifecycle_configuration(xml.as_bytes());
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_lifecycle_configuration_empty() {
        let xml = r#"<LifecycleConfiguration></LifecycleConfiguration>"#;

        let result = parse_lifecycle_configuration(xml.as_bytes());
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_put_get_delete_bucket_lifecycle() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        // Put lifecycle configuration
        let xml = r#"<LifecycleConfiguration>
            <Rule>
                <ID>expire-logs</ID>
                <Status>Enabled</Status>
                <Filter>
                    <Prefix>logs/</Prefix>
                </Filter>
                <Expiration>
                    <Days>30</Days>
                </Expiration>
            </Rule>
        </LifecycleConfiguration>"#;

        let response = put_bucket_lifecycle_configuration(&storage, "test-bucket", Bytes::from(xml))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Get lifecycle configuration
        let response = get_bucket_lifecycle_configuration(&storage, "test-bucket").await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Delete lifecycle configuration
        let response = delete_bucket_lifecycle(&storage, "test-bucket")
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        // Get lifecycle should fail now
        let result = get_bucket_lifecycle_configuration(&storage, "test-bucket").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_bucket_lifecycle_no_config() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        // Should fail with NoSuchLifecycleConfiguration
        let result = get_bucket_lifecycle_configuration(&storage, "test-bucket").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_put_bucket_lifecycle_bucket_not_found() {
        let (storage, _temp) = create_test_storage().await;

        let xml = r#"<LifecycleConfiguration>
            <Rule>
                <Status>Enabled</Status>
                <Filter>
                    <Prefix>logs/</Prefix>
                </Filter>
                <Expiration>
                    <Days>30</Days>
                </Expiration>
            </Rule>
        </LifecycleConfiguration>"#;

        let result = put_bucket_lifecycle_configuration(&storage, "nonexistent", Bytes::from(xml)).await;
        assert!(result.is_err());
    }
}

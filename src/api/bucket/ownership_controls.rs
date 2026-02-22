//! Bucket Ownership Controls operations

use bytes::Bytes;
use http_body_util::Full;
use hyper::{Response, StatusCode};

use crate::storage::StorageEngine;
use crate::types::bucket::{ObjectOwnership, OwnershipControls, OwnershipControlsRule};
use crate::types::error::{S3Error, S3ErrorCode, S3Result};

/// Handle GetBucketOwnershipControls request
pub async fn get_bucket_ownership_controls(
    storage: &StorageEngine,
    bucket: &str,
) -> S3Result<Response<Full<Bytes>>> {
    let config = storage.get_ownership_controls(bucket).await?;

    let mut rules_xml = String::new();
    for rule in &config.rules {
        rules_xml.push_str(&format!(
            "  <Rule>\n    <ObjectOwnership>{}</ObjectOwnership>\n  </Rule>\n",
            rule.object_ownership.as_str()
        ));
    }

    let xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<OwnershipControls xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
{}
</OwnershipControls>"#,
        rules_xml.trim_end()
    );

    let response = Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/xml")
        .body(Full::new(Bytes::from(xml)))
        .unwrap();

    Ok(response)
}

/// Handle PutBucketOwnershipControls request
pub async fn put_bucket_ownership_controls(
    storage: &StorageEngine,
    bucket: &str,
    body: Bytes,
) -> S3Result<Response<Full<Bytes>>> {
    // Check bucket exists
    if !storage.bucket_exists(bucket).await {
        return Err(S3Error::no_such_bucket(bucket));
    }

    // Parse the request body XML
    let config = parse_ownership_controls(&body)?;

    storage.set_ownership_controls(bucket, config).await?;

    let response = Response::builder()
        .status(StatusCode::OK)
        .body(Full::new(Bytes::new()))
        .unwrap();

    Ok(response)
}

/// Handle DeleteBucketOwnershipControls request
pub async fn delete_bucket_ownership_controls(
    storage: &StorageEngine,
    bucket: &str,
) -> S3Result<Response<Full<Bytes>>> {
    // Check bucket exists
    if !storage.bucket_exists(bucket).await {
        return Err(S3Error::no_such_bucket(bucket));
    }

    storage.delete_ownership_controls(bucket).await?;

    let response = Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Full::new(Bytes::new()))
        .unwrap();

    Ok(response)
}

/// Parse the OwnershipControls request XML body
/// Format:
/// <OwnershipControls>
///   <Rule>
///     <ObjectOwnership>BucketOwnerEnforced</ObjectOwnership>
///   </Rule>
/// </OwnershipControls>
fn parse_ownership_controls(body: &[u8]) -> S3Result<OwnershipControls> {
    let body_str = std::str::from_utf8(body)
        .map_err(|_| S3Error::new(S3ErrorCode::MalformedXML, "Invalid UTF-8 in request body"))?;

    let mut config = OwnershipControls::new();

    // Find all <Rule>...</Rule> blocks
    let mut remaining = body_str;
    while let Some(rule_start) = remaining.find("<Rule>") {
        let after_rule_start = &remaining[rule_start + 6..];
        if let Some(rule_end) = after_rule_start.find("</Rule>") {
            let rule_content = &after_rule_start[..rule_end];

            // Extract ObjectOwnership value
            if let Some(ownership) = extract_xml_value(rule_content, "ObjectOwnership") {
                if let Some(obj_ownership) = ObjectOwnership::from_str(&ownership) {
                    config.rules.push(OwnershipControlsRule::new(obj_ownership));
                } else {
                    return Err(S3Error::new(
                        S3ErrorCode::MalformedXML,
                        format!("Invalid ObjectOwnership value: {}", ownership),
                    ));
                }
            }

            remaining = &after_rule_start[rule_end + 7..];
        } else {
            break;
        }
    }

    if config.rules.is_empty() {
        return Err(S3Error::new(
            S3ErrorCode::MalformedXML,
            "OwnershipControls must contain at least one Rule",
        ));
    }

    Ok(config)
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
    fn test_parse_ownership_controls_bucket_owner_enforced() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
        <OwnershipControls>
            <Rule>
                <ObjectOwnership>BucketOwnerEnforced</ObjectOwnership>
            </Rule>
        </OwnershipControls>"#;

        let config = parse_ownership_controls(xml.as_bytes()).unwrap();

        assert_eq!(config.rules.len(), 1);
        assert_eq!(
            config.rules[0].object_ownership,
            ObjectOwnership::BucketOwnerEnforced
        );
    }

    #[test]
    fn test_parse_ownership_controls_bucket_owner_preferred() {
        let xml = r#"<OwnershipControls>
            <Rule>
                <ObjectOwnership>BucketOwnerPreferred</ObjectOwnership>
            </Rule>
        </OwnershipControls>"#;

        let config = parse_ownership_controls(xml.as_bytes()).unwrap();

        assert_eq!(config.rules.len(), 1);
        assert_eq!(
            config.rules[0].object_ownership,
            ObjectOwnership::BucketOwnerPreferred
        );
    }

    #[test]
    fn test_parse_ownership_controls_object_writer() {
        let xml = r#"<OwnershipControls>
            <Rule>
                <ObjectOwnership>ObjectWriter</ObjectOwnership>
            </Rule>
        </OwnershipControls>"#;

        let config = parse_ownership_controls(xml.as_bytes()).unwrap();

        assert_eq!(config.rules.len(), 1);
        assert_eq!(
            config.rules[0].object_ownership,
            ObjectOwnership::ObjectWriter
        );
    }

    #[test]
    fn test_parse_ownership_controls_invalid_value() {
        let xml = r#"<OwnershipControls>
            <Rule>
                <ObjectOwnership>InvalidValue</ObjectOwnership>
            </Rule>
        </OwnershipControls>"#;

        let result = parse_ownership_controls(xml.as_bytes());
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_ownership_controls_empty_rules() {
        let xml = r#"<OwnershipControls></OwnershipControls>"#;

        let result = parse_ownership_controls(xml.as_bytes());
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_put_get_delete_ownership_controls() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        // Put ownership controls
        let xml = r#"<OwnershipControls>
            <Rule>
                <ObjectOwnership>BucketOwnerEnforced</ObjectOwnership>
            </Rule>
        </OwnershipControls>"#;

        let response =
            put_bucket_ownership_controls(&storage, "test-bucket", Bytes::from(xml))
                .await
                .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Get ownership controls
        let response = get_bucket_ownership_controls(&storage, "test-bucket")
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Delete ownership controls
        let response = delete_bucket_ownership_controls(&storage, "test-bucket")
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        // Get should fail now
        let result = get_bucket_ownership_controls(&storage, "test-bucket").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_ownership_controls_no_config() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        // Should fail with OwnershipControlsNotFoundError
        let result = get_bucket_ownership_controls(&storage, "test-bucket").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_put_ownership_controls_bucket_not_found() {
        let (storage, _temp) = create_test_storage().await;

        let xml = r#"<OwnershipControls>
            <Rule>
                <ObjectOwnership>BucketOwnerEnforced</ObjectOwnership>
            </Rule>
        </OwnershipControls>"#;

        let result =
            put_bucket_ownership_controls(&storage, "nonexistent", Bytes::from(xml)).await;
        assert!(result.is_err());
    }
}

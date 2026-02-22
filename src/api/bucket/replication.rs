//! Bucket Replication Configuration operations

use bytes::Bytes;
use http_body_util::Full;
use hyper::{Response, StatusCode};

use crate::storage::StorageEngine;
use crate::types::bucket::{
    ReplicationConfiguration, ReplicationDestination, ReplicationRule, ReplicationRuleFilter,
    ReplicationRuleStatus,
};
use crate::types::error::{S3Error, S3ErrorCode, S3Result};

/// Handle GetBucketReplication request
pub async fn get_bucket_replication(
    storage: &StorageEngine,
    bucket: &str,
) -> S3Result<Response<Full<Bytes>>> {
    // Check bucket exists
    if !storage.bucket_exists(bucket).await {
        return Err(S3Error::no_such_bucket(bucket));
    }

    let config = storage.get_bucket_replication(bucket).await?;

    // Generate XML response
    let xml = generate_replication_xml(&config);

    let response = Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/xml")
        .body(Full::new(Bytes::from(xml)))
        .unwrap();

    Ok(response)
}

/// Handle PutBucketReplication request
pub async fn put_bucket_replication(
    storage: &StorageEngine,
    bucket: &str,
    body: Bytes,
) -> S3Result<Response<Full<Bytes>>> {
    // Check bucket exists
    if !storage.bucket_exists(bucket).await {
        return Err(S3Error::no_such_bucket(bucket));
    }

    // Parse the request body XML
    let config = parse_replication_configuration(&body)?;

    storage.set_bucket_replication(bucket, config).await?;

    let response = Response::builder()
        .status(StatusCode::OK)
        .body(Full::new(Bytes::new()))
        .unwrap();

    Ok(response)
}

/// Handle DeleteBucketReplication request
pub async fn delete_bucket_replication(
    storage: &StorageEngine,
    bucket: &str,
) -> S3Result<Response<Full<Bytes>>> {
    // Check bucket exists
    if !storage.bucket_exists(bucket).await {
        return Err(S3Error::no_such_bucket(bucket));
    }

    storage.delete_bucket_replication(bucket).await?;

    let response = Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Full::new(Bytes::new()))
        .unwrap();

    Ok(response)
}

/// Generate XML for ReplicationConfiguration
fn generate_replication_xml(config: &ReplicationConfiguration) -> String {
    let mut xml = String::from(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ReplicationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">"#,
    );

    xml.push_str(&format!("\n    <Role>{}</Role>", escape_xml(&config.role)));

    for rule in &config.rules {
        xml.push_str("\n    <Rule>");

        if let Some(id) = &rule.id {
            xml.push_str(&format!("\n        <ID>{}</ID>", escape_xml(id)));
        }

        xml.push_str(&format!(
            "\n        <Status>{}</Status>",
            rule.status.as_str()
        ));

        if let Some(priority) = rule.priority {
            xml.push_str(&format!("\n        <Priority>{}</Priority>", priority));
        }

        if let Some(filter) = &rule.filter {
            xml.push_str("\n        <Filter>");
            if let Some(prefix) = &filter.prefix {
                xml.push_str(&format!("\n            <Prefix>{}</Prefix>", escape_xml(prefix)));
            }
            xml.push_str("\n        </Filter>");
        }

        xml.push_str("\n        <Destination>");
        xml.push_str(&format!(
            "\n            <Bucket>{}</Bucket>",
            escape_xml(&rule.destination.bucket)
        ));
        if let Some(storage_class) = &rule.destination.storage_class {
            xml.push_str(&format!(
                "\n            <StorageClass>{}</StorageClass>",
                escape_xml(storage_class)
            ));
        }
        xml.push_str("\n        </Destination>");

        xml.push_str("\n    </Rule>");
    }

    xml.push_str("\n</ReplicationConfiguration>");
    xml
}

/// Parse the ReplicationConfiguration request XML body
fn parse_replication_configuration(body: &[u8]) -> S3Result<ReplicationConfiguration> {
    let body_str = std::str::from_utf8(body)
        .map_err(|_| S3Error::new(S3ErrorCode::MalformedXML, "Invalid UTF-8 in request body"))?;

    let role = extract_xml_value(body_str, "Role")
        .ok_or_else(|| S3Error::new(S3ErrorCode::MalformedXML, "Missing Role element"))?;

    let mut rules = Vec::new();

    let mut remaining = body_str;
    while let Some(rule_content) = extract_xml_block(remaining, "Rule") {
        let id = extract_xml_value(&rule_content, "ID");
        let status_str = extract_xml_value(&rule_content, "Status").unwrap_or_default();
        let status = ReplicationRuleStatus::from_str(&status_str)
            .unwrap_or(ReplicationRuleStatus::Disabled);
        let priority = extract_xml_value(&rule_content, "Priority")
            .and_then(|p| p.parse().ok());

        // Parse filter
        let filter = if let Some(filter_content) = extract_xml_block(&rule_content, "Filter") {
            let prefix = extract_xml_value(&filter_content, "Prefix");
            Some(ReplicationRuleFilter {
                prefix,
                tag: None,
                and: None,
            })
        } else {
            None
        };

        // Parse destination
        let destination = if let Some(dest_content) = extract_xml_block(&rule_content, "Destination")
        {
            let bucket = extract_xml_value(&dest_content, "Bucket").unwrap_or_default();
            let storage_class = extract_xml_value(&dest_content, "StorageClass");
            let account = extract_xml_value(&dest_content, "Account");

            ReplicationDestination {
                bucket,
                account,
                storage_class,
            }
        } else {
            return Err(S3Error::new(
                S3ErrorCode::MalformedXML,
                "Missing Destination element in Rule",
            ));
        };

        rules.push(ReplicationRule {
            id,
            status,
            priority,
            filter,
            destination,
            delete_marker_replication: None,
        });

        // Move past this rule
        if let Some(end_idx) = remaining.find("</Rule>") {
            remaining = &remaining[end_idx + 7..];
        } else {
            break;
        }
    }

    if rules.is_empty() {
        return Err(S3Error::new(
            S3ErrorCode::MalformedXML,
            "At least one Rule is required",
        ));
    }

    Ok(ReplicationConfiguration { role, rules })
}

/// Extract a block of XML content between tags
fn extract_xml_block(content: &str, tag: &str) -> Option<String> {
    let open_tag = format!("<{}", tag);
    let close_tag = format!("</{}>", tag);

    if let Some(start) = content.find(&open_tag) {
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

/// Escape XML special characters
fn escape_xml(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
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
    fn test_parse_replication_configuration() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
        <ReplicationConfiguration>
            <Role>arn:aws:iam::123456789012:role/replication-role</Role>
            <Rule>
                <ID>rule-1</ID>
                <Status>Enabled</Status>
                <Priority>1</Priority>
                <Filter>
                    <Prefix>documents/</Prefix>
                </Filter>
                <Destination>
                    <Bucket>arn:aws:s3:::destination-bucket</Bucket>
                    <StorageClass>STANDARD</StorageClass>
                </Destination>
            </Rule>
        </ReplicationConfiguration>"#;

        let config = parse_replication_configuration(xml.as_bytes()).unwrap();

        assert_eq!(
            config.role,
            "arn:aws:iam::123456789012:role/replication-role"
        );
        assert_eq!(config.rules.len(), 1);
        assert_eq!(config.rules[0].id, Some("rule-1".to_string()));
        assert_eq!(config.rules[0].status, ReplicationRuleStatus::Enabled);
        assert_eq!(config.rules[0].priority, Some(1));
    }

    #[test]
    fn test_parse_replication_multiple_rules() {
        let xml = r#"<ReplicationConfiguration>
            <Role>arn:aws:iam::123:role/role</Role>
            <Rule>
                <ID>rule1</ID>
                <Status>Enabled</Status>
                <Destination>
                    <Bucket>arn:aws:s3:::dest1</Bucket>
                </Destination>
            </Rule>
            <Rule>
                <ID>rule2</ID>
                <Status>Disabled</Status>
                <Destination>
                    <Bucket>arn:aws:s3:::dest2</Bucket>
                </Destination>
            </Rule>
        </ReplicationConfiguration>"#;

        let config = parse_replication_configuration(xml.as_bytes()).unwrap();

        assert_eq!(config.rules.len(), 2);
        assert_eq!(config.rules[0].id, Some("rule1".to_string()));
        assert_eq!(config.rules[1].id, Some("rule2".to_string()));
    }

    #[tokio::test]
    async fn test_put_get_delete_bucket_replication() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        // Put replication configuration
        let xml = r#"<ReplicationConfiguration>
            <Role>arn:aws:iam::123:role/role</Role>
            <Rule>
                <ID>rule</ID>
                <Status>Enabled</Status>
                <Destination>
                    <Bucket>arn:aws:s3:::dest</Bucket>
                </Destination>
            </Rule>
        </ReplicationConfiguration>"#;

        let response = put_bucket_replication(&storage, "test-bucket", Bytes::from(xml))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Get replication configuration
        let response = get_bucket_replication(&storage, "test-bucket")
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Delete replication configuration
        let response = delete_bucket_replication(&storage, "test-bucket")
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        // Get should now fail
        let result = get_bucket_replication(&storage, "test-bucket").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_replication_not_configured() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        // Should return error when not configured
        let result = get_bucket_replication(&storage, "test-bucket").await;
        assert!(result.is_err());
    }
}

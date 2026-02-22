//! Bucket Website Configuration operations

use bytes::Bytes;
use http_body_util::Full;
use hyper::{Response, StatusCode};

use crate::storage::StorageEngine;
use crate::types::bucket::{
    ErrorDocument, IndexDocument, RedirectAllRequestsTo, RoutingRule, RoutingRuleCondition,
    RoutingRuleRedirect, WebsiteConfiguration,
};
use crate::types::error::{S3Error, S3ErrorCode, S3Result};
use crate::types::response::GetBucketWebsiteResponse;
use crate::utils::xml::to_xml;

/// Handle GetBucketWebsite request
pub async fn get_bucket_website(
    storage: &StorageEngine,
    bucket: &str,
) -> S3Result<Response<Full<Bytes>>> {
    let config = storage.get_bucket_website(bucket).await?;

    let response_body = GetBucketWebsiteResponse::from(&config);
    let xml = to_xml(&response_body)?;

    let response = Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/xml")
        .body(Full::new(Bytes::from(xml)))
        .unwrap();

    Ok(response)
}

/// Handle PutBucketWebsite request
pub async fn put_bucket_website(
    storage: &StorageEngine,
    bucket: &str,
    body: Bytes,
) -> S3Result<Response<Full<Bytes>>> {
    // Check bucket exists
    if !storage.bucket_exists(bucket).await {
        return Err(S3Error::no_such_bucket(bucket));
    }

    // Parse the request body XML
    let config = parse_website_configuration(&body)?;

    storage.set_bucket_website(bucket, config).await?;

    let response = Response::builder()
        .status(StatusCode::OK)
        .body(Full::new(Bytes::new()))
        .unwrap();

    Ok(response)
}

/// Handle DeleteBucketWebsite request
pub async fn delete_bucket_website(
    storage: &StorageEngine,
    bucket: &str,
) -> S3Result<Response<Full<Bytes>>> {
    // Check bucket exists
    if !storage.bucket_exists(bucket).await {
        return Err(S3Error::no_such_bucket(bucket));
    }

    storage.delete_bucket_website(bucket).await?;

    let response = Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Full::new(Bytes::new()))
        .unwrap();

    Ok(response)
}

/// Parse the WebsiteConfiguration request XML body
fn parse_website_configuration(body: &[u8]) -> S3Result<WebsiteConfiguration> {
    let body_str = std::str::from_utf8(body)
        .map_err(|_| S3Error::new(S3ErrorCode::MalformedXML, "Invalid UTF-8 in request body"))?;

    let mut config = WebsiteConfiguration::new();

    // Parse IndexDocument
    if let Some(index_content) = extract_xml_block(body_str, "IndexDocument") {
        if let Some(suffix) = extract_xml_value(&index_content, "Suffix") {
            config.index_document = Some(IndexDocument { suffix });
        }
    }

    // Parse ErrorDocument
    if let Some(error_content) = extract_xml_block(body_str, "ErrorDocument") {
        if let Some(key) = extract_xml_value(&error_content, "Key") {
            config.error_document = Some(ErrorDocument { key });
        }
    }

    // Parse RedirectAllRequestsTo
    if let Some(redirect_content) = extract_xml_block(body_str, "RedirectAllRequestsTo") {
        if let Some(host_name) = extract_xml_value(&redirect_content, "HostName") {
            let protocol = extract_xml_value(&redirect_content, "Protocol");
            config.redirect_all_requests_to = Some(RedirectAllRequestsTo {
                host_name,
                protocol,
            });
        }
    }

    // Parse RoutingRules
    if let Some(rules_content) = extract_xml_block(body_str, "RoutingRules") {
        config.routing_rules = parse_routing_rules(&rules_content);
    }

    // Validate: must have either IndexDocument or RedirectAllRequestsTo
    if config.index_document.is_none() && config.redirect_all_requests_to.is_none() {
        // AWS allows just routing rules, but typically index document is required
        // We'll be lenient and allow the configuration
    }

    Ok(config)
}

/// Parse routing rules from XML
fn parse_routing_rules(content: &str) -> Vec<RoutingRule> {
    let mut rules = Vec::new();
    let mut remaining = content;

    while let Some(rule_content) = extract_xml_block(remaining, "RoutingRule") {
        let mut rule = RoutingRule {
            condition: None,
            redirect: RoutingRuleRedirect::default(),
        };

        // Parse Condition (optional)
        if let Some(condition_content) = extract_xml_block(&rule_content, "Condition") {
            let mut condition = RoutingRuleCondition::default();
            condition.http_error_code_returned_equals =
                extract_xml_value(&condition_content, "HttpErrorCodeReturnedEquals");
            condition.key_prefix_equals = extract_xml_value(&condition_content, "KeyPrefixEquals");

            if condition.http_error_code_returned_equals.is_some()
                || condition.key_prefix_equals.is_some()
            {
                rule.condition = Some(condition);
            }
        }

        // Parse Redirect (required)
        if let Some(redirect_content) = extract_xml_block(&rule_content, "Redirect") {
            rule.redirect.host_name = extract_xml_value(&redirect_content, "HostName");
            rule.redirect.http_redirect_code =
                extract_xml_value(&redirect_content, "HttpRedirectCode");
            rule.redirect.protocol = extract_xml_value(&redirect_content, "Protocol");
            rule.redirect.replace_key_with = extract_xml_value(&redirect_content, "ReplaceKeyWith");
            rule.redirect.replace_key_prefix_with =
                extract_xml_value(&redirect_content, "ReplaceKeyPrefixWith");
        }

        rules.push(rule);

        // Move past this rule
        if let Some(end_idx) = remaining.find("</RoutingRule>") {
            remaining = &remaining[end_idx + 14..];
        } else {
            break;
        }
    }

    rules
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
    fn test_parse_website_configuration_basic() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
        <WebsiteConfiguration>
            <IndexDocument>
                <Suffix>index.html</Suffix>
            </IndexDocument>
        </WebsiteConfiguration>"#;

        let config = parse_website_configuration(xml.as_bytes()).unwrap();

        assert!(config.index_document.is_some());
        assert_eq!(config.index_document.unwrap().suffix, "index.html");
        assert!(config.error_document.is_none());
        assert!(config.redirect_all_requests_to.is_none());
        assert!(config.routing_rules.is_empty());
    }

    #[test]
    fn test_parse_website_configuration_full() {
        let xml = r#"<WebsiteConfiguration>
            <IndexDocument>
                <Suffix>index.html</Suffix>
            </IndexDocument>
            <ErrorDocument>
                <Key>error.html</Key>
            </ErrorDocument>
        </WebsiteConfiguration>"#;

        let config = parse_website_configuration(xml.as_bytes()).unwrap();

        assert!(config.index_document.is_some());
        assert_eq!(config.index_document.unwrap().suffix, "index.html");
        assert!(config.error_document.is_some());
        assert_eq!(config.error_document.unwrap().key, "error.html");
    }

    #[test]
    fn test_parse_website_configuration_redirect() {
        let xml = r#"<WebsiteConfiguration>
            <RedirectAllRequestsTo>
                <HostName>example.com</HostName>
                <Protocol>https</Protocol>
            </RedirectAllRequestsTo>
        </WebsiteConfiguration>"#;

        let config = parse_website_configuration(xml.as_bytes()).unwrap();

        assert!(config.redirect_all_requests_to.is_some());
        let redirect = config.redirect_all_requests_to.unwrap();
        assert_eq!(redirect.host_name, "example.com");
        assert_eq!(redirect.protocol, Some("https".to_string()));
    }

    #[test]
    fn test_parse_website_configuration_routing_rules() {
        let xml = r#"<WebsiteConfiguration>
            <IndexDocument>
                <Suffix>index.html</Suffix>
            </IndexDocument>
            <RoutingRules>
                <RoutingRule>
                    <Condition>
                        <KeyPrefixEquals>docs/</KeyPrefixEquals>
                    </Condition>
                    <Redirect>
                        <ReplaceKeyPrefixWith>documents/</ReplaceKeyPrefixWith>
                    </Redirect>
                </RoutingRule>
            </RoutingRules>
        </WebsiteConfiguration>"#;

        let config = parse_website_configuration(xml.as_bytes()).unwrap();

        assert_eq!(config.routing_rules.len(), 1);
        let rule = &config.routing_rules[0];
        assert!(rule.condition.is_some());
        let condition = rule.condition.as_ref().unwrap();
        assert_eq!(condition.key_prefix_equals, Some("docs/".to_string()));
        assert_eq!(
            rule.redirect.replace_key_prefix_with,
            Some("documents/".to_string())
        );
    }

    #[tokio::test]
    async fn test_put_get_delete_bucket_website() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        // Put website configuration
        let xml = r#"<WebsiteConfiguration>
            <IndexDocument>
                <Suffix>index.html</Suffix>
            </IndexDocument>
            <ErrorDocument>
                <Key>error.html</Key>
            </ErrorDocument>
        </WebsiteConfiguration>"#;

        let response = put_bucket_website(&storage, "test-bucket", Bytes::from(xml))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Get website configuration
        let response = get_bucket_website(&storage, "test-bucket").await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Delete website configuration
        let response = delete_bucket_website(&storage, "test-bucket")
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        // Get should fail now
        let result = get_bucket_website(&storage, "test-bucket").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_bucket_website_no_config() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        // Should fail with NoSuchWebsiteConfiguration
        let result = get_bucket_website(&storage, "test-bucket").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_put_bucket_website_bucket_not_found() {
        let (storage, _temp) = create_test_storage().await;

        let xml = r#"<WebsiteConfiguration>
            <IndexDocument>
                <Suffix>index.html</Suffix>
            </IndexDocument>
        </WebsiteConfiguration>"#;

        let result = put_bucket_website(&storage, "nonexistent", Bytes::from(xml)).await;
        assert!(result.is_err());
    }
}

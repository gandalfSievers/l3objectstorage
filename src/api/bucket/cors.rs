//! Bucket CORS operations

use bytes::Bytes;
use http_body_util::Full;
use hyper::{Response, StatusCode};

use crate::storage::StorageEngine;
use crate::types::error::{S3Error, S3ErrorCode, S3Result};
use crate::types::response::{CorsConfiguration, CorsRuleXml, GetBucketCorsResponse};
use crate::utils::xml::to_xml;

/// Handle GetBucketCors request
pub async fn get_bucket_cors(
    storage: &StorageEngine,
    bucket: &str,
) -> S3Result<Response<Full<Bytes>>> {
    let cors = storage.get_bucket_cors(bucket).await?;

    let response_body = GetBucketCorsResponse {
        cors_rules: cors.rules,
    };

    let xml = to_xml(&response_body)?;

    let response = Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/xml")
        .body(Full::new(Bytes::from(xml)))
        .unwrap();

    Ok(response)
}

/// Handle PutBucketCors request
pub async fn put_bucket_cors(
    storage: &StorageEngine,
    bucket: &str,
    body: Bytes,
) -> S3Result<Response<Full<Bytes>>> {
    // Check bucket exists
    if !storage.bucket_exists(bucket).await {
        return Err(S3Error::no_such_bucket(bucket));
    }

    // Parse the request body XML
    let cors = parse_cors_configuration(&body)?;

    storage.set_bucket_cors(bucket, cors).await?;

    let response = Response::builder()
        .status(StatusCode::OK)
        .body(Full::new(Bytes::new()))
        .unwrap();

    Ok(response)
}

/// Handle DeleteBucketCors request
pub async fn delete_bucket_cors(
    storage: &StorageEngine,
    bucket: &str,
) -> S3Result<Response<Full<Bytes>>> {
    // Check bucket exists
    if !storage.bucket_exists(bucket).await {
        return Err(S3Error::no_such_bucket(bucket));
    }

    storage.delete_bucket_cors(bucket).await?;

    let response = Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Full::new(Bytes::new()))
        .unwrap();

    Ok(response)
}

/// Parse the CORS configuration request XML body
/// Format:
/// <CORSConfiguration>
///   <CORSRule>
///     <AllowedOrigin>*</AllowedOrigin>
///     <AllowedMethod>GET</AllowedMethod>
///     <AllowedMethod>PUT</AllowedMethod>
///     <AllowedHeader>*</AllowedHeader>
///     <MaxAgeSeconds>3000</MaxAgeSeconds>
///     <ExposeHeader>x-amz-meta-custom</ExposeHeader>
///   </CORSRule>
/// </CORSConfiguration>
fn parse_cors_configuration(body: &[u8]) -> S3Result<CorsConfiguration> {
    let body_str = std::str::from_utf8(body)
        .map_err(|_| S3Error::new(S3ErrorCode::MalformedXML, "Invalid UTF-8 in request body"))?;

    let mut rules = Vec::new();
    let mut remaining = body_str;

    // Parse each CORSRule element
    while let Some(rule_start) = remaining.find("<CORSRule>") {
        let after_rule_start = &remaining[rule_start + 10..];

        if let Some(rule_end) = after_rule_start.find("</CORSRule>") {
            let rule_content = &after_rule_start[..rule_end];

            let mut cors_rule = CorsRuleXml::new();

            // Extract ID (optional)
            cors_rule.id = extract_xml_value(rule_content, "ID");

            // Extract AllowedOrigins (required, can be multiple)
            cors_rule.allowed_origins = extract_all_xml_values(rule_content, "AllowedOrigin");

            // Extract AllowedMethods (required, can be multiple)
            cors_rule.allowed_methods = extract_all_xml_values(rule_content, "AllowedMethod");

            // Extract AllowedHeaders (optional, can be multiple)
            cors_rule.allowed_headers = extract_all_xml_values(rule_content, "AllowedHeader");

            // Extract ExposeHeaders (optional, can be multiple)
            cors_rule.expose_headers = extract_all_xml_values(rule_content, "ExposeHeader");

            // Extract MaxAgeSeconds (optional)
            if let Some(max_age_str) = extract_xml_value(rule_content, "MaxAgeSeconds") {
                if let Ok(max_age) = max_age_str.parse::<i32>() {
                    cors_rule.max_age_seconds = Some(max_age);
                }
            }

            // Validate required fields
            if cors_rule.allowed_origins.is_empty() {
                return Err(S3Error::new(
                    S3ErrorCode::MalformedXML,
                    "CORSRule must have at least one AllowedOrigin",
                ));
            }
            if cors_rule.allowed_methods.is_empty() {
                return Err(S3Error::new(
                    S3ErrorCode::MalformedXML,
                    "CORSRule must have at least one AllowedMethod",
                ));
            }

            rules.push(cors_rule);
            remaining = &after_rule_start[rule_end + 11..];
        } else {
            break;
        }
    }

    if rules.is_empty() {
        return Err(S3Error::new(
            S3ErrorCode::MalformedXML,
            "CORSConfiguration must have at least one CORSRule",
        ));
    }

    Ok(CorsConfiguration { rules })
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

/// Extract all values from repeated XML elements
fn extract_all_xml_values(content: &str, tag: &str) -> Vec<String> {
    let open_tag = format!("<{}>", tag);
    let close_tag = format!("</{}>", tag);
    let mut values = Vec::new();
    let mut remaining = content;

    while let Some(start) = remaining.find(&open_tag) {
        let after_open = &remaining[start + open_tag.len()..];
        if let Some(end) = after_open.find(&close_tag) {
            let value = &after_open[..end];
            values.push(decode_xml_entities(value));
            remaining = &after_open[end + close_tag.len()..];
        } else {
            break;
        }
    }

    values
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
    fn test_parse_cors_configuration_basic() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
        <CORSConfiguration>
            <CORSRule>
                <AllowedOrigin>*</AllowedOrigin>
                <AllowedMethod>GET</AllowedMethod>
                <AllowedMethod>PUT</AllowedMethod>
            </CORSRule>
        </CORSConfiguration>"#;

        let cors = parse_cors_configuration(xml.as_bytes()).unwrap();

        assert_eq!(cors.rules.len(), 1);
        assert_eq!(cors.rules[0].allowed_origins, vec!["*"]);
        assert_eq!(cors.rules[0].allowed_methods, vec!["GET", "PUT"]);
    }

    #[test]
    fn test_parse_cors_configuration_full() {
        let xml = r#"<CORSConfiguration>
            <CORSRule>
                <ID>rule1</ID>
                <AllowedOrigin>https://example.com</AllowedOrigin>
                <AllowedOrigin>https://test.com</AllowedOrigin>
                <AllowedMethod>GET</AllowedMethod>
                <AllowedMethod>PUT</AllowedMethod>
                <AllowedMethod>POST</AllowedMethod>
                <AllowedHeader>*</AllowedHeader>
                <ExposeHeader>x-amz-meta-custom</ExposeHeader>
                <MaxAgeSeconds>3600</MaxAgeSeconds>
            </CORSRule>
        </CORSConfiguration>"#;

        let cors = parse_cors_configuration(xml.as_bytes()).unwrap();

        assert_eq!(cors.rules.len(), 1);
        let rule = &cors.rules[0];
        assert_eq!(rule.id, Some("rule1".to_string()));
        assert_eq!(rule.allowed_origins, vec!["https://example.com", "https://test.com"]);
        assert_eq!(rule.allowed_methods, vec!["GET", "PUT", "POST"]);
        assert_eq!(rule.allowed_headers, vec!["*"]);
        assert_eq!(rule.expose_headers, vec!["x-amz-meta-custom"]);
        assert_eq!(rule.max_age_seconds, Some(3600));
    }

    #[test]
    fn test_parse_cors_configuration_multiple_rules() {
        let xml = r#"<CORSConfiguration>
            <CORSRule>
                <AllowedOrigin>https://example.com</AllowedOrigin>
                <AllowedMethod>GET</AllowedMethod>
            </CORSRule>
            <CORSRule>
                <AllowedOrigin>https://test.com</AllowedOrigin>
                <AllowedMethod>PUT</AllowedMethod>
            </CORSRule>
        </CORSConfiguration>"#;

        let cors = parse_cors_configuration(xml.as_bytes()).unwrap();

        assert_eq!(cors.rules.len(), 2);
        assert_eq!(cors.rules[0].allowed_origins, vec!["https://example.com"]);
        assert_eq!(cors.rules[1].allowed_origins, vec!["https://test.com"]);
    }

    #[test]
    fn test_parse_cors_configuration_missing_origin() {
        let xml = r#"<CORSConfiguration>
            <CORSRule>
                <AllowedMethod>GET</AllowedMethod>
            </CORSRule>
        </CORSConfiguration>"#;

        let result = parse_cors_configuration(xml.as_bytes());
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_cors_configuration_missing_method() {
        let xml = r#"<CORSConfiguration>
            <CORSRule>
                <AllowedOrigin>*</AllowedOrigin>
            </CORSRule>
        </CORSConfiguration>"#;

        let result = parse_cors_configuration(xml.as_bytes());
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_cors_configuration_empty() {
        let xml = r#"<CORSConfiguration></CORSConfiguration>"#;

        let result = parse_cors_configuration(xml.as_bytes());
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_put_get_delete_bucket_cors() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        // Put CORS
        let xml = r#"<CORSConfiguration>
            <CORSRule>
                <AllowedOrigin>*</AllowedOrigin>
                <AllowedMethod>GET</AllowedMethod>
                <MaxAgeSeconds>3000</MaxAgeSeconds>
            </CORSRule>
        </CORSConfiguration>"#;

        let response = put_bucket_cors(&storage, "test-bucket", Bytes::from(xml))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Get CORS
        let response = get_bucket_cors(&storage, "test-bucket").await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Delete CORS
        let response = delete_bucket_cors(&storage, "test-bucket")
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        // Get CORS should fail now
        let result = get_bucket_cors(&storage, "test-bucket").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_bucket_cors_no_cors() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        // Should fail with NoSuchCORSConfiguration
        let result = get_bucket_cors(&storage, "test-bucket").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_put_bucket_cors_bucket_not_found() {
        let (storage, _temp) = create_test_storage().await;

        let xml = r#"<CORSConfiguration>
            <CORSRule>
                <AllowedOrigin>*</AllowedOrigin>
                <AllowedMethod>GET</AllowedMethod>
            </CORSRule>
        </CORSConfiguration>"#;

        let result = put_bucket_cors(&storage, "nonexistent", Bytes::from(xml)).await;
        assert!(result.is_err());
    }
}

//! Object Lock Configuration operations

use bytes::Bytes;
use http_body_util::Full;
use hyper::{Response, StatusCode};

use crate::storage::StorageEngine;
use crate::types::bucket::{
    DefaultRetention, ObjectLockConfiguration, ObjectLockEnabled, ObjectLockRetentionMode,
    ObjectLockRule,
};
use crate::types::error::{S3Error, S3ErrorCode, S3Result};

/// Handle GetObjectLockConfiguration request
pub async fn get_object_lock_configuration(
    storage: &StorageEngine,
    bucket: &str,
) -> S3Result<Response<Full<Bytes>>> {
    let config = storage.get_object_lock_configuration(bucket).await?;

    let xml = build_object_lock_configuration_xml(&config);

    let response = Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/xml")
        .body(Full::new(Bytes::from(xml)))
        .unwrap();

    Ok(response)
}

/// Handle PutObjectLockConfiguration request
pub async fn put_object_lock_configuration(
    storage: &StorageEngine,
    bucket: &str,
    body: Bytes,
) -> S3Result<Response<Full<Bytes>>> {
    // Check bucket exists and has object lock enabled
    if !storage.bucket_exists(bucket).await {
        return Err(S3Error::no_such_bucket(bucket));
    }

    if !storage.is_object_lock_enabled(bucket).await? {
        return Err(S3Error::new(
            S3ErrorCode::InvalidRequest,
            "Object Lock configuration can only be applied to buckets with Object Lock enabled",
        ));
    }

    // Parse the request body XML
    let config = parse_object_lock_configuration(&body)?;

    storage.set_object_lock_configuration(bucket, config).await?;

    let response = Response::builder()
        .status(StatusCode::OK)
        .body(Full::new(Bytes::new()))
        .unwrap();

    Ok(response)
}

/// Build XML response for Object Lock Configuration
fn build_object_lock_configuration_xml(config: &ObjectLockConfiguration) -> String {
    let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    xml.push_str("<ObjectLockConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n");
    xml.push_str(&format!(
        "  <ObjectLockEnabled>{}</ObjectLockEnabled>\n",
        config.object_lock_enabled.as_str()
    ));

    if let Some(ref rule) = config.rule {
        xml.push_str("  <Rule>\n");
        xml.push_str("    <DefaultRetention>\n");
        xml.push_str(&format!(
            "      <Mode>{}</Mode>\n",
            rule.default_retention.mode.as_str()
        ));
        if let Some(days) = rule.default_retention.days {
            xml.push_str(&format!("      <Days>{}</Days>\n", days));
        }
        if let Some(years) = rule.default_retention.years {
            xml.push_str(&format!("      <Years>{}</Years>\n", years));
        }
        xml.push_str("    </DefaultRetention>\n");
        xml.push_str("  </Rule>\n");
    }

    xml.push_str("</ObjectLockConfiguration>");
    xml
}

/// Parse Object Lock Configuration XML
fn parse_object_lock_configuration(body: &[u8]) -> S3Result<ObjectLockConfiguration> {
    let body_str = std::str::from_utf8(body)
        .map_err(|_| S3Error::new(S3ErrorCode::MalformedXML, "Invalid UTF-8 in request body"))?;

    let mut config = ObjectLockConfiguration {
        object_lock_enabled: ObjectLockEnabled::Enabled,
        rule: None,
    };

    // Check for ObjectLockEnabled
    if body_str.contains("<ObjectLockEnabled>Enabled</ObjectLockEnabled>") {
        config.object_lock_enabled = ObjectLockEnabled::Enabled;
    }

    // Parse Rule if present
    if let Some(rule_start) = body_str.find("<Rule>") {
        let after_rule = &body_str[rule_start..];
        if let Some(rule_end) = after_rule.find("</Rule>") {
            let rule_content = &after_rule[..rule_end];

            // Parse DefaultRetention
            if let Some(retention_start) = rule_content.find("<DefaultRetention>") {
                let after_retention = &rule_content[retention_start..];
                if let Some(retention_end) = after_retention.find("</DefaultRetention>") {
                    let retention_content = &after_retention[..retention_end];

                    // Parse Mode
                    let mode = if retention_content.contains("<Mode>GOVERNANCE</Mode>") {
                        ObjectLockRetentionMode::Governance
                    } else if retention_content.contains("<Mode>COMPLIANCE</Mode>") {
                        ObjectLockRetentionMode::Compliance
                    } else {
                        return Err(S3Error::new(
                            S3ErrorCode::MalformedXML,
                            "Invalid or missing Mode in DefaultRetention",
                        ));
                    };

                    // Parse Days
                    let days = extract_xml_value(retention_content, "Days")
                        .and_then(|s| s.parse::<i32>().ok());

                    // Parse Years
                    let years = extract_xml_value(retention_content, "Years")
                        .and_then(|s| s.parse::<i32>().ok());

                    // Must have either days or years
                    if days.is_none() && years.is_none() {
                        return Err(S3Error::new(
                            S3ErrorCode::MalformedXML,
                            "DefaultRetention must specify either Days or Years",
                        ));
                    }

                    config.rule = Some(ObjectLockRule {
                        default_retention: DefaultRetention { mode, days, years },
                    });
                }
            }
        }
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
            return Some(value.to_string());
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
    fn test_parse_object_lock_configuration_governance() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
        <ObjectLockConfiguration>
            <ObjectLockEnabled>Enabled</ObjectLockEnabled>
            <Rule>
                <DefaultRetention>
                    <Mode>GOVERNANCE</Mode>
                    <Days>30</Days>
                </DefaultRetention>
            </Rule>
        </ObjectLockConfiguration>"#;

        let config = parse_object_lock_configuration(xml.as_bytes()).unwrap();

        assert_eq!(config.object_lock_enabled, ObjectLockEnabled::Enabled);
        assert!(config.rule.is_some());
        let rule = config.rule.unwrap();
        assert_eq!(rule.default_retention.mode, ObjectLockRetentionMode::Governance);
        assert_eq!(rule.default_retention.days, Some(30));
        assert_eq!(rule.default_retention.years, None);
    }

    #[test]
    fn test_parse_object_lock_configuration_compliance_years() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
        <ObjectLockConfiguration>
            <ObjectLockEnabled>Enabled</ObjectLockEnabled>
            <Rule>
                <DefaultRetention>
                    <Mode>COMPLIANCE</Mode>
                    <Years>1</Years>
                </DefaultRetention>
            </Rule>
        </ObjectLockConfiguration>"#;

        let config = parse_object_lock_configuration(xml.as_bytes()).unwrap();

        assert!(config.rule.is_some());
        let rule = config.rule.unwrap();
        assert_eq!(rule.default_retention.mode, ObjectLockRetentionMode::Compliance);
        assert_eq!(rule.default_retention.days, None);
        assert_eq!(rule.default_retention.years, Some(1));
    }

    #[test]
    fn test_parse_object_lock_configuration_no_rule() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
        <ObjectLockConfiguration>
            <ObjectLockEnabled>Enabled</ObjectLockEnabled>
        </ObjectLockConfiguration>"#;

        let config = parse_object_lock_configuration(xml.as_bytes()).unwrap();

        assert_eq!(config.object_lock_enabled, ObjectLockEnabled::Enabled);
        assert!(config.rule.is_none());
    }

    #[test]
    fn test_build_object_lock_configuration_xml() {
        let config = ObjectLockConfiguration {
            object_lock_enabled: ObjectLockEnabled::Enabled,
            rule: Some(ObjectLockRule {
                default_retention: DefaultRetention {
                    mode: ObjectLockRetentionMode::Governance,
                    days: Some(30),
                    years: None,
                },
            }),
        };

        let xml = build_object_lock_configuration_xml(&config);

        assert!(xml.contains("<ObjectLockEnabled>Enabled</ObjectLockEnabled>"));
        assert!(xml.contains("<Mode>GOVERNANCE</Mode>"));
        assert!(xml.contains("<Days>30</Days>"));
    }

    #[tokio::test]
    async fn test_put_get_object_lock_configuration() {
        let (storage, _temp) = create_test_storage().await;

        // Create bucket with object lock enabled
        storage.create_bucket_with_object_lock("test-bucket").await.unwrap();

        // Put configuration
        let xml = r#"<ObjectLockConfiguration>
            <ObjectLockEnabled>Enabled</ObjectLockEnabled>
            <Rule>
                <DefaultRetention>
                    <Mode>GOVERNANCE</Mode>
                    <Days>30</Days>
                </DefaultRetention>
            </Rule>
        </ObjectLockConfiguration>"#;

        let response = put_object_lock_configuration(&storage, "test-bucket", Bytes::from(xml))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Get configuration
        let response = get_object_lock_configuration(&storage, "test-bucket")
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
}

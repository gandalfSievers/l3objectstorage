//! Bucket Notification Configuration operations

use bytes::Bytes;
use http_body_util::Full;
use hyper::{Response, StatusCode};

use crate::storage::StorageEngine;
use crate::types::bucket::{
    LambdaFunctionConfiguration, NotificationConfiguration, QueueConfiguration, TopicConfiguration,
};
use crate::types::error::{S3Error, S3ErrorCode, S3Result};

/// Handle GetBucketNotificationConfiguration request
pub async fn get_bucket_notification_configuration(
    storage: &StorageEngine,
    bucket: &str,
) -> S3Result<Response<Full<Bytes>>> {
    // Check bucket exists
    if !storage.bucket_exists(bucket).await {
        return Err(S3Error::no_such_bucket(bucket));
    }

    let config = storage.get_bucket_notification(bucket).await?;

    // Generate XML response
    let xml = generate_notification_xml(&config);

    let response = Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/xml")
        .body(Full::new(Bytes::from(xml)))
        .unwrap();

    Ok(response)
}

/// Handle PutBucketNotificationConfiguration request
pub async fn put_bucket_notification_configuration(
    storage: &StorageEngine,
    bucket: &str,
    body: Bytes,
) -> S3Result<Response<Full<Bytes>>> {
    // Check bucket exists
    if !storage.bucket_exists(bucket).await {
        return Err(S3Error::no_such_bucket(bucket));
    }

    // Parse the request body XML
    let config = parse_notification_configuration(&body)?;

    storage.set_bucket_notification(bucket, config).await?;

    let response = Response::builder()
        .status(StatusCode::OK)
        .body(Full::new(Bytes::new()))
        .unwrap();

    Ok(response)
}

/// Generate XML for NotificationConfiguration
fn generate_notification_xml(config: &NotificationConfiguration) -> String {
    let mut xml = String::from(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">"#,
    );

    // Topic configurations
    for topic in &config.topic_configurations {
        xml.push_str("\n    <TopicConfiguration>");
        if let Some(id) = &topic.id {
            xml.push_str(&format!("\n        <Id>{}</Id>", escape_xml(id)));
        }
        xml.push_str(&format!(
            "\n        <Topic>{}</Topic>",
            escape_xml(&topic.topic_arn)
        ));
        for event in &topic.events {
            xml.push_str(&format!("\n        <Event>{}</Event>", escape_xml(event)));
        }
        xml.push_str("\n    </TopicConfiguration>");
    }

    // Queue configurations
    for queue in &config.queue_configurations {
        xml.push_str("\n    <QueueConfiguration>");
        if let Some(id) = &queue.id {
            xml.push_str(&format!("\n        <Id>{}</Id>", escape_xml(id)));
        }
        xml.push_str(&format!(
            "\n        <Queue>{}</Queue>",
            escape_xml(&queue.queue_arn)
        ));
        for event in &queue.events {
            xml.push_str(&format!("\n        <Event>{}</Event>", escape_xml(event)));
        }
        xml.push_str("\n    </QueueConfiguration>");
    }

    // Lambda configurations
    for lambda in &config.lambda_function_configurations {
        xml.push_str("\n    <CloudFunctionConfiguration>");
        if let Some(id) = &lambda.id {
            xml.push_str(&format!("\n        <Id>{}</Id>", escape_xml(id)));
        }
        xml.push_str(&format!(
            "\n        <CloudFunction>{}</CloudFunction>",
            escape_xml(&lambda.lambda_function_arn)
        ));
        for event in &lambda.events {
            xml.push_str(&format!("\n        <Event>{}</Event>", escape_xml(event)));
        }
        xml.push_str("\n    </CloudFunctionConfiguration>");
    }

    // EventBridge configuration
    if config.event_bridge_configuration.is_some() {
        xml.push_str("\n    <EventBridgeConfiguration></EventBridgeConfiguration>");
    }

    xml.push_str("\n</NotificationConfiguration>");
    xml
}

/// Parse the NotificationConfiguration request XML body
fn parse_notification_configuration(body: &[u8]) -> S3Result<NotificationConfiguration> {
    let body_str = std::str::from_utf8(body)
        .map_err(|_| S3Error::new(S3ErrorCode::MalformedXML, "Invalid UTF-8 in request body"))?;

    let mut config = NotificationConfiguration::new();

    // Parse TopicConfiguration blocks
    let mut remaining = body_str;
    while let Some(topic_content) = extract_xml_block(remaining, "TopicConfiguration") {
        let id = extract_xml_value(&topic_content, "Id");
        let topic_arn = extract_xml_value(&topic_content, "Topic")
            .or_else(|| extract_xml_value(&topic_content, "TopicArn"))
            .unwrap_or_default();
        let events = extract_all_xml_values(&topic_content, "Event");

        if !topic_arn.is_empty() {
            config.topic_configurations.push(TopicConfiguration {
                id,
                topic_arn,
                events,
                filter: None,
            });
        }

        // Move past this block
        if let Some(end_idx) = remaining.find("</TopicConfiguration>") {
            remaining = &remaining[end_idx + 21..];
        } else {
            break;
        }
    }

    // Parse QueueConfiguration blocks
    remaining = body_str;
    while let Some(queue_content) = extract_xml_block(remaining, "QueueConfiguration") {
        let id = extract_xml_value(&queue_content, "Id");
        let queue_arn = extract_xml_value(&queue_content, "Queue")
            .or_else(|| extract_xml_value(&queue_content, "QueueArn"))
            .unwrap_or_default();
        let events = extract_all_xml_values(&queue_content, "Event");

        if !queue_arn.is_empty() {
            config.queue_configurations.push(QueueConfiguration {
                id,
                queue_arn,
                events,
                filter: None,
            });
        }

        // Move past this block
        if let Some(end_idx) = remaining.find("</QueueConfiguration>") {
            remaining = &remaining[end_idx + 21..];
        } else {
            break;
        }
    }

    // Parse CloudFunctionConfiguration (Lambda) blocks
    remaining = body_str;
    while let Some(lambda_content) = extract_xml_block(remaining, "CloudFunctionConfiguration") {
        let id = extract_xml_value(&lambda_content, "Id");
        let lambda_arn = extract_xml_value(&lambda_content, "CloudFunction")
            .or_else(|| extract_xml_value(&lambda_content, "LambdaFunctionArn"))
            .unwrap_or_default();
        let events = extract_all_xml_values(&lambda_content, "Event");

        if !lambda_arn.is_empty() {
            config
                .lambda_function_configurations
                .push(LambdaFunctionConfiguration {
                    id,
                    lambda_function_arn: lambda_arn,
                    events,
                    filter: None,
                });
        }

        // Move past this block
        if let Some(end_idx) = remaining.find("</CloudFunctionConfiguration>") {
            remaining = &remaining[end_idx + 30..];
        } else {
            break;
        }
    }

    // Also check for LambdaFunctionConfiguration blocks (alternative naming)
    remaining = body_str;
    while let Some(lambda_content) = extract_xml_block(remaining, "LambdaFunctionConfiguration") {
        let id = extract_xml_value(&lambda_content, "Id");
        let lambda_arn = extract_xml_value(&lambda_content, "LambdaFunctionArn")
            .or_else(|| extract_xml_value(&lambda_content, "CloudFunction"))
            .unwrap_or_default();
        let events = extract_all_xml_values(&lambda_content, "Event");

        if !lambda_arn.is_empty() {
            config
                .lambda_function_configurations
                .push(LambdaFunctionConfiguration {
                    id,
                    lambda_function_arn: lambda_arn,
                    events,
                    filter: None,
                });
        }

        // Move past this block
        if let Some(end_idx) = remaining.find("</LambdaFunctionConfiguration>") {
            remaining = &remaining[end_idx + 31..];
        } else {
            break;
        }
    }

    // Check for EventBridgeConfiguration
    if body_str.contains("<EventBridgeConfiguration") {
        config.event_bridge_configuration =
            Some(crate::types::bucket::EventBridgeConfiguration {});
    }

    Ok(config)
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

/// Extract all values for a repeated XML element
fn extract_all_xml_values(content: &str, tag: &str) -> Vec<String> {
    let open_tag = format!("<{}>", tag);
    let close_tag = format!("</{}>", tag);
    let mut values = Vec::new();
    let mut remaining = content;

    while let Some(start) = remaining.find(&open_tag) {
        let after_open = &remaining[start + open_tag.len()..];
        if let Some(end) = after_open.find(&close_tag) {
            let value = &after_open[..end];
            values.push(decode_xml_entities(value.trim()));
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
    fn test_parse_notification_configuration_topic() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
        <NotificationConfiguration>
            <TopicConfiguration>
                <Id>test-notification</Id>
                <Topic>arn:aws:sns:us-east-1:123456789012:my-topic</Topic>
                <Event>s3:ObjectCreated:*</Event>
            </TopicConfiguration>
        </NotificationConfiguration>"#;

        let config = parse_notification_configuration(xml.as_bytes()).unwrap();

        assert_eq!(config.topic_configurations.len(), 1);
        assert_eq!(
            config.topic_configurations[0].id,
            Some("test-notification".to_string())
        );
        assert_eq!(
            config.topic_configurations[0].topic_arn,
            "arn:aws:sns:us-east-1:123456789012:my-topic"
        );
        assert_eq!(config.topic_configurations[0].events.len(), 1);
    }

    #[test]
    fn test_parse_notification_configuration_queue() {
        let xml = r#"<NotificationConfiguration>
            <QueueConfiguration>
                <Id>queue-notification</Id>
                <Queue>arn:aws:sqs:us-east-1:123456789012:my-queue</Queue>
                <Event>s3:ObjectRemoved:*</Event>
            </QueueConfiguration>
        </NotificationConfiguration>"#;

        let config = parse_notification_configuration(xml.as_bytes()).unwrap();

        assert_eq!(config.queue_configurations.len(), 1);
        assert_eq!(
            config.queue_configurations[0].id,
            Some("queue-notification".to_string())
        );
        assert_eq!(
            config.queue_configurations[0].queue_arn,
            "arn:aws:sqs:us-east-1:123456789012:my-queue"
        );
    }

    #[test]
    fn test_parse_notification_configuration_lambda() {
        let xml = r#"<NotificationConfiguration>
            <CloudFunctionConfiguration>
                <Id>lambda-notification</Id>
                <CloudFunction>arn:aws:lambda:us-east-1:123456789012:function:my-func</CloudFunction>
                <Event>s3:ObjectCreated:Put</Event>
            </CloudFunctionConfiguration>
        </NotificationConfiguration>"#;

        let config = parse_notification_configuration(xml.as_bytes()).unwrap();

        assert_eq!(config.lambda_function_configurations.len(), 1);
        assert_eq!(
            config.lambda_function_configurations[0].id,
            Some("lambda-notification".to_string())
        );
    }

    #[test]
    fn test_parse_notification_configuration_empty() {
        let xml = r#"<NotificationConfiguration></NotificationConfiguration>"#;

        let config = parse_notification_configuration(xml.as_bytes()).unwrap();

        assert!(!config.is_configured());
    }

    #[tokio::test]
    async fn test_put_get_notification_configuration() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        // Put notification configuration
        let xml = r#"<NotificationConfiguration>
            <TopicConfiguration>
                <Id>test</Id>
                <Topic>arn:aws:sns:us-east-1:123:topic</Topic>
                <Event>s3:ObjectCreated:*</Event>
            </TopicConfiguration>
        </NotificationConfiguration>"#;

        let response =
            put_bucket_notification_configuration(&storage, "test-bucket", Bytes::from(xml))
                .await
                .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Get notification configuration
        let response = get_bucket_notification_configuration(&storage, "test-bucket")
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_notification_not_configured() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        // Should return empty configuration (not an error)
        let response = get_bucket_notification_configuration(&storage, "test-bucket")
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
}

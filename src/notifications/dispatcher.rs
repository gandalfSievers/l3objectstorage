use std::sync::Arc;
use tracing::warn;

use crate::types::bucket::NotificationConfiguration;

use super::event::build_event;
use super::matcher::{event_matches, filter_matches};
use super::sender::NotificationSender;

/// Orchestrates notification dispatch by matching bucket notification configurations
/// against events and sending messages to the appropriate SNS/SQS endpoints.
pub struct NotificationDispatcher {
    sender: Arc<NotificationSender>,
    region: String,
}

impl NotificationDispatcher {
    pub fn new(sender: NotificationSender, region: String) -> Self {
        Self {
            sender: Arc::new(sender),
            region,
        }
    }

    /// Dispatch notifications for an S3 event.
    ///
    /// Iterates topic and queue configurations, checks event and filter matching,
    /// and spawns fire-and-forget tasks for each match.
    pub fn dispatch(
        &self,
        config: NotificationConfiguration,
        event_name: &str,
        bucket: &str,
        key: &str,
        size: u64,
        etag: &str,
        version_id: Option<&str>,
    ) {
        // Process SNS topic configurations
        for topic_config in &config.topic_configurations {
            let matched = topic_config
                .events
                .iter()
                .any(|e| event_matches(e, event_name));

            if matched && filter_matches(&topic_config.filter, key) {
                let event = build_event(&self.region, event_name, bucket, key, size, etag, version_id);
                let message = match serde_json::to_string(&event) {
                    Ok(m) => m,
                    Err(e) => {
                        warn!("Failed to serialize notification event: {}", e);
                        continue;
                    }
                };

                let sender = Arc::clone(&self.sender);
                let topic_arn = topic_config.topic_arn.clone();

                tokio::spawn(async move {
                    let result = tokio::time::timeout(
                        std::time::Duration::from_secs(5),
                        sender.publish_to_sns(&topic_arn, &message),
                    )
                    .await;

                    match result {
                        Ok(Ok(())) => {}
                        Ok(Err(e)) => warn!("SNS notification failed for {}: {}", topic_arn, e),
                        Err(_) => warn!("SNS notification timed out for {}", topic_arn),
                    }
                });
            }
        }

        // Process SQS queue configurations
        for queue_config in &config.queue_configurations {
            let matched = queue_config
                .events
                .iter()
                .any(|e| event_matches(e, event_name));

            if matched && filter_matches(&queue_config.filter, key) {
                let event = build_event(&self.region, event_name, bucket, key, size, etag, version_id);
                let message = match serde_json::to_string(&event) {
                    Ok(m) => m,
                    Err(e) => {
                        warn!("Failed to serialize notification event: {}", e);
                        continue;
                    }
                };

                let sender = Arc::clone(&self.sender);
                let queue_arn = queue_config.queue_arn.clone();

                tokio::spawn(async move {
                    let result = tokio::time::timeout(
                        std::time::Duration::from_secs(5),
                        sender.send_to_sqs(&queue_arn, &message),
                    )
                    .await;

                    match result {
                        Ok(Ok(())) => {}
                        Ok(Err(e)) => warn!("SQS notification failed for {}: {}", queue_arn, e),
                        Err(_) => warn!("SQS notification timed out for {}", queue_arn),
                    }
                });
            }
        }

        // Log unsupported Lambda configurations
        if !config.lambda_function_configurations.is_empty() {
            warn!(
                "Lambda function notifications are not supported (bucket has {} configured)",
                config.lambda_function_configurations.len()
            );
        }

        // Log unsupported EventBridge configuration
        if config.event_bridge_configuration.is_some() {
            warn!("EventBridge notifications are not supported");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::bucket::{
        FilterRule, NotificationFilter, NotificationFilterKey, QueueConfiguration,
        TopicConfiguration,
    };

    fn make_dispatcher() -> NotificationDispatcher {
        let sender = NotificationSender::new(None, None);
        NotificationDispatcher::new(sender, "us-east-1".to_string())
    }

    #[test]
    fn dispatch_with_empty_config_does_not_panic() {
        let dispatcher = make_dispatcher();
        let config = NotificationConfiguration::new();
        // Should complete without panic even with no configurations
        dispatcher.dispatch(config, "s3:ObjectCreated:Put", "bucket", "key", 100, "etag", None);
    }

    #[test]
    fn dispatch_logs_unsupported_lambda() {
        use crate::types::bucket::LambdaFunctionConfiguration;

        let dispatcher = make_dispatcher();
        let mut config = NotificationConfiguration::new();
        config.lambda_function_configurations.push(LambdaFunctionConfiguration {
            id: None,
            lambda_function_arn: "arn:aws:lambda:us-east-1:000:function:test".to_string(),
            events: vec!["s3:ObjectCreated:*".to_string()],
            filter: None,
        });
        // Should not panic, just logs warning
        dispatcher.dispatch(config, "s3:ObjectCreated:Put", "bucket", "key", 100, "etag", None);
    }

    #[tokio::test]
    async fn matching_filters_correctly() {
        // Verify that the dispatcher's matching logic integrates event + filter matching
        let config = NotificationConfiguration {
            topic_configurations: vec![TopicConfiguration {
                id: None,
                topic_arn: "arn:aws:sns:us-east-1:000:topic".to_string(),
                events: vec!["s3:ObjectCreated:*".to_string()],
                filter: Some(NotificationFilter {
                    key: Some(NotificationFilterKey {
                        filter_rules: vec![FilterRule {
                            name: "prefix".to_string(),
                            value: "images/".to_string(),
                        }],
                    }),
                }),
            }],
            queue_configurations: vec![QueueConfiguration {
                id: None,
                queue_arn: "arn:aws:sqs:us-east-1:000:queue".to_string(),
                events: vec!["s3:ObjectRemoved:*".to_string()],
                filter: None,
            }],
            lambda_function_configurations: vec![],
            event_bridge_configuration: None,
        };

        let dispatcher = make_dispatcher();
        // This tests the full dispatch path — tasks will fail (no endpoint) but shouldn't panic
        dispatcher.dispatch(
            config,
            "s3:ObjectCreated:Put",
            "bucket",
            "images/photo.jpg",
            1024,
            "abc",
            None,
        );
    }
}

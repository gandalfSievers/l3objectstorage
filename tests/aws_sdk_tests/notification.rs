//! Bucket Notification Configuration integration tests

use super::*;

#[tokio::test]
#[ignore]
async fn test_put_get_notification_configuration() {
    use aws_sdk_s3::types::{Event, NotificationConfiguration, TopicConfiguration};

    let client = create_s3_client().await;
    let bucket = "sdk-notification-test";

    // Create bucket
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Create notification configuration with topic
    let topic_config = TopicConfiguration::builder()
        .id("test-notification")
        .topic_arn("arn:aws:sns:us-east-1:123456789012:my-topic")
        .events(Event::S3ObjectCreated)
        .build()
        .expect("Failed to build topic config");

    let notification_config = NotificationConfiguration::builder()
        .topic_configurations(topic_config)
        .build();

    // Put notification configuration
    client
        .put_bucket_notification_configuration()
        .bucket(bucket)
        .notification_configuration(notification_config)
        .send()
        .await
        .expect("Failed to put notification configuration");

    // Get notification configuration
    let result = client
        .get_bucket_notification_configuration()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to get notification configuration");

    // Verify topic configuration
    let topics = result.topic_configurations();
    assert_eq!(topics.len(), 1);
    assert_eq!(topics[0].id(), Some("test-notification"));
    assert_eq!(
        topics[0].topic_arn(),
        "arn:aws:sns:us-east-1:123456789012:my-topic"
    );

    // Cleanup
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_get_notification_not_configured() {
    let client = create_s3_client().await;
    let bucket = "sdk-no-notification-test";

    // Create bucket
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Get notification - should return empty configuration (not an error)
    let result = client
        .get_bucket_notification_configuration()
        .bucket(bucket)
        .send()
        .await
        .expect("GetBucketNotificationConfiguration should succeed");

    // Should have no configurations
    assert!(
        result.topic_configurations().is_empty(),
        "Should have no topic configurations"
    );
    assert!(
        result.queue_configurations().is_empty(),
        "Should have no queue configurations"
    );
    assert!(
        result.lambda_function_configurations().is_empty(),
        "Should have no lambda configurations"
    );

    // Cleanup
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_put_notification_with_queue() {
    use aws_sdk_s3::types::{Event, NotificationConfiguration, QueueConfiguration};

    let client = create_s3_client().await;
    let bucket = "sdk-notification-queue-test";

    // Create bucket
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Create notification configuration with queue
    let queue_config = QueueConfiguration::builder()
        .id("queue-notification")
        .queue_arn("arn:aws:sqs:us-east-1:123456789012:my-queue")
        .events(Event::S3ObjectRemoved)
        .build()
        .expect("Failed to build queue config");

    let notification_config = NotificationConfiguration::builder()
        .queue_configurations(queue_config)
        .build();

    // Put notification configuration
    client
        .put_bucket_notification_configuration()
        .bucket(bucket)
        .notification_configuration(notification_config)
        .send()
        .await
        .expect("Failed to put notification configuration");

    // Get notification configuration
    let result = client
        .get_bucket_notification_configuration()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to get notification configuration");

    // Verify queue configuration
    let queues = result.queue_configurations();
    assert_eq!(queues.len(), 1);
    assert_eq!(queues[0].id(), Some("queue-notification"));

    // Cleanup
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_put_notification_with_lambda() {
    use aws_sdk_s3::types::{Event, LambdaFunctionConfiguration, NotificationConfiguration};

    let client = create_s3_client().await;
    let bucket = "sdk-notification-lambda-test";

    // Create bucket
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Create notification configuration with lambda
    let lambda_config = LambdaFunctionConfiguration::builder()
        .id("lambda-notification")
        .lambda_function_arn("arn:aws:lambda:us-east-1:123456789012:function:my-func")
        .events(Event::S3ObjectCreatedPut)
        .build()
        .expect("Failed to build lambda config");

    let notification_config = NotificationConfiguration::builder()
        .lambda_function_configurations(lambda_config)
        .build();

    // Put notification configuration
    client
        .put_bucket_notification_configuration()
        .bucket(bucket)
        .notification_configuration(notification_config)
        .send()
        .await
        .expect("Failed to put notification configuration");

    // Get notification configuration
    let result = client
        .get_bucket_notification_configuration()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to get notification configuration");

    // Verify lambda configuration
    let lambdas = result.lambda_function_configurations();
    assert_eq!(lambdas.len(), 1);
    assert_eq!(lambdas[0].id(), Some("lambda-notification"));

    // Cleanup
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_put_empty_notification_clears_config() {
    use aws_sdk_s3::types::{Event, NotificationConfiguration, TopicConfiguration};

    let client = create_s3_client().await;
    let bucket = "sdk-notification-clear-test";

    // Create bucket
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Set initial notification
    let topic_config = TopicConfiguration::builder()
        .id("initial")
        .topic_arn("arn:aws:sns:us-east-1:123456789012:topic")
        .events(Event::S3ObjectCreated)
        .build()
        .expect("Failed to build topic config");

    let notification_config = NotificationConfiguration::builder()
        .topic_configurations(topic_config)
        .build();

    client
        .put_bucket_notification_configuration()
        .bucket(bucket)
        .notification_configuration(notification_config)
        .send()
        .await
        .expect("Failed to put notification configuration");

    // Clear notification by sending empty config
    let empty_config = NotificationConfiguration::builder().build();

    client
        .put_bucket_notification_configuration()
        .bucket(bucket)
        .notification_configuration(empty_config)
        .send()
        .await
        .expect("Failed to clear notification configuration");

    // Verify cleared
    let result = client
        .get_bucket_notification_configuration()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to get notification configuration");

    assert!(
        result.topic_configurations().is_empty(),
        "Should have no topic configurations after clear"
    );

    // Cleanup
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

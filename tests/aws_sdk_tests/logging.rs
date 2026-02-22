//! Bucket Logging integration tests

use super::*;

#[tokio::test]
#[ignore]
async fn test_put_get_bucket_logging() {
    use aws_sdk_s3::types::{BucketLoggingStatus, LoggingEnabled};

    let client = create_s3_client().await;
    let bucket = "sdk-logging-test";
    let log_bucket = "sdk-logging-target";

    // Create buckets
    let _ = client.create_bucket().bucket(bucket).send().await;
    let _ = client.create_bucket().bucket(log_bucket).send().await;

    // Create logging configuration
    let logging_enabled = LoggingEnabled::builder()
        .target_bucket(log_bucket)
        .target_prefix("logs/")
        .build()
        .expect("Failed to build LoggingEnabled");

    let logging_status = BucketLoggingStatus::builder()
        .logging_enabled(logging_enabled)
        .build();

    // Put bucket logging
    client
        .put_bucket_logging()
        .bucket(bucket)
        .bucket_logging_status(logging_status)
        .send()
        .await
        .expect("Failed to put bucket logging");

    // Get bucket logging
    let result = client
        .get_bucket_logging()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to get bucket logging");

    // Verify logging configuration
    let logging = result.logging_enabled().expect("Should have logging enabled");
    assert_eq!(logging.target_bucket(), log_bucket);
    assert_eq!(logging.target_prefix(), "logs/");

    // Cleanup
    let _ = client.delete_bucket().bucket(bucket).send().await;
    let _ = client.delete_bucket().bucket(log_bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_get_bucket_logging_not_configured() {
    let client = create_s3_client().await;
    let bucket = "sdk-no-logging-test";

    // Create bucket
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Get logging - should return empty (no LoggingEnabled)
    let result = client
        .get_bucket_logging()
        .bucket(bucket)
        .send()
        .await
        .expect("GetBucketLogging should succeed even when not configured");

    // Should have no logging_enabled
    assert!(
        result.logging_enabled().is_none(),
        "Should have no logging enabled when not configured"
    );

    // Cleanup
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_put_bucket_logging_disable() {
    use aws_sdk_s3::types::{BucketLoggingStatus, LoggingEnabled};

    let client = create_s3_client().await;
    let bucket = "sdk-disable-logging-test";
    let log_bucket = "sdk-disable-logging-target";

    // Create buckets
    let _ = client.create_bucket().bucket(bucket).send().await;
    let _ = client.create_bucket().bucket(log_bucket).send().await;

    // Enable logging first
    let logging_enabled = LoggingEnabled::builder()
        .target_bucket(log_bucket)
        .target_prefix("logs/")
        .build()
        .expect("Failed to build LoggingEnabled");

    let logging_status = BucketLoggingStatus::builder()
        .logging_enabled(logging_enabled)
        .build();

    client
        .put_bucket_logging()
        .bucket(bucket)
        .bucket_logging_status(logging_status)
        .send()
        .await
        .expect("Failed to put bucket logging");

    // Disable logging by sending empty BucketLoggingStatus
    let empty_status = BucketLoggingStatus::builder().build();

    client
        .put_bucket_logging()
        .bucket(bucket)
        .bucket_logging_status(empty_status)
        .send()
        .await
        .expect("Failed to disable bucket logging");

    // Verify logging is disabled
    let result = client
        .get_bucket_logging()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to get bucket logging");

    assert!(
        result.logging_enabled().is_none(),
        "Logging should be disabled"
    );

    // Cleanup
    let _ = client.delete_bucket().bucket(bucket).send().await;
    let _ = client.delete_bucket().bucket(log_bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_put_bucket_logging_bucket_not_found() {
    use aws_sdk_s3::types::{BucketLoggingStatus, LoggingEnabled};

    let client = create_s3_client().await;

    let logging_enabled = LoggingEnabled::builder()
        .target_bucket("some-target")
        .target_prefix("logs/")
        .build()
        .expect("Failed to build LoggingEnabled");

    let logging_status = BucketLoggingStatus::builder()
        .logging_enabled(logging_enabled)
        .build();

    // Should fail for non-existent bucket
    let result = client
        .put_bucket_logging()
        .bucket("nonexistent-bucket-12345")
        .bucket_logging_status(logging_status)
        .send()
        .await;

    assert!(result.is_err(), "Should fail for non-existent bucket");
}

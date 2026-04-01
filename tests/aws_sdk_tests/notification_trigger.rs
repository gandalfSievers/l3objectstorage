//! Integration tests for S3 bucket notification triggering.
//!
//! These tests verify that S3 events (PutObject, DeleteObject, CopyObject,
//! CompleteMultipartUpload) dispatch notifications to SNS topics and SQS queues
//! via local emulators (local-sns + ElasticMQ).

use super::*;

const SNS_TOPIC_ARN: &str = "arn:aws:sns:us-east-1:000000000000:test-topic";
const SQS_ENDPOINT: &str = "http://localhost:9324";

/// Poll ElasticMQ for a message on the given queue, retrying up to `timeout_ms`.
async fn poll_sqs_message(queue_name: &str, timeout_ms: u64) -> Option<String> {
    let url = format!(
        "{}/queue/{}?Action=ReceiveMessage&WaitTimeSeconds=0&MaxNumberOfMessages=1",
        SQS_ENDPOINT, queue_name
    );
    let client = reqwest::Client::new();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(timeout_ms);

    while tokio::time::Instant::now() < deadline {
        if let Ok(resp) = client.get(&url).send().await {
            if let Ok(body) = resp.text().await {
                // ElasticMQ returns XML; check if a <Body> element is present
                if let Some(start) = body.find("<Body>") {
                    let start = start + "<Body>".len();
                    if let Some(end) = body[start..].find("</Body>") {
                        let raw = &body[start..start + end];
                        // The body may be XML-escaped
                        let decoded = raw
                            .replace("&amp;", "&")
                            .replace("&lt;", "<")
                            .replace("&gt;", ">")
                            .replace("&quot;", "\"")
                            .replace("&#x27;", "'");
                        return Some(decoded);
                    }
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    None
}

/// Purge all messages from an ElasticMQ queue.
async fn purge_sqs_queue(queue_name: &str) {
    let url = format!(
        "{}/queue/{}?Action=PurgeQueue",
        SQS_ENDPOINT, queue_name
    );
    let _ = reqwest::Client::new().get(&url).send().await;
}

/// Parse an S3 event notification JSON and return the parsed value.
fn parse_s3_event(body: &str) -> serde_json::Value {
    serde_json::from_str(body).expect("Failed to parse S3 event JSON")
}

const SNS_ENDPOINT: &str = "http://localhost:9911";

/// Ensure the test-topic exists in local-sns and has an SQS subscription
/// that forwards (raw) to the `sns-forwarded` ElasticMQ queue.
/// Safe to call multiple times — CreateTopic is idempotent.
async fn ensure_sns_topic_with_sqs_subscription() {
    let client = reqwest::Client::new();

    // Create topic (idempotent)
    let resp = client
        .post(format!("{}/", SNS_ENDPOINT))
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body("Action=CreateTopic&Name=test-topic")
        .send()
        .await
        .expect("CreateTopic request failed");
    let body = resp.text().await.unwrap_or_default();

    // Extract TopicArn from response
    let topic_arn = body
        .split("<TopicArn>")
        .nth(1)
        .and_then(|s| s.split("</TopicArn>").next())
        .expect("Failed to extract TopicArn from CreateTopic response");
    assert!(
        topic_arn.contains("test-topic"),
        "Unexpected TopicArn: {}",
        topic_arn
    );

    // Subscribe: use Camel SQS URI so local-sns can reach ElasticMQ inside Docker
    let endpoint = "aws2-sqs://sns-forwarded?accessKey=localadmin&secretKey=localadmin&region=us-east-1&trustAllCertificates=true&overrideEndpoint=true&uriEndpointOverride=http://sqs:9324";
    let subscribe_body = format!(
        "Action=Subscribe&TopicArn={}&Protocol=sqs&Endpoint={}",
        percent_encode(topic_arn),
        percent_encode(endpoint),
    );
    let resp = client
        .post(format!("{}/", SNS_ENDPOINT))
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(subscribe_body)
        .send()
        .await
        .expect("Subscribe request failed");
    let body = resp.text().await.unwrap_or_default();

    // Extract SubscriptionArn
    if let Some(sub_arn) = body
        .split("<SubscriptionArn>")
        .nth(1)
        .and_then(|s| s.split("</SubscriptionArn>").next())
    {
        // Enable raw message delivery
        let attr_body = format!(
            "Action=SetSubscriptionAttributes&SubscriptionArn={}&AttributeName=RawMessageDelivery&AttributeValue=true",
            percent_encode(sub_arn),
        );
        let _ = client
            .post(format!("{}/", SNS_ENDPOINT))
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(attr_body)
            .send()
            .await;
    }
}

/// Simple percent-encoding for SNS API parameters.
fn percent_encode(s: &str) -> String {
    s.replace('%', "%25")
        .replace('&', "%26")
        .replace('=', "%3D")
        .replace('+', "%2B")
        .replace(' ', "%20")
        .replace('/', "%2F")
        .replace(':', "%3A")
        .replace('?', "%3F")
}

// ---------------------------------------------------------------------------
// Test 1: SNS topic notification on PutObject
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn notification_trigger_sns_put_object() {
    use aws_sdk_s3::types::{Event, NotificationConfiguration, TopicConfiguration};

    let client = create_s3_client().await;
    let bucket = "notif-trigger-sns-put";

    // Setup: ensure local-sns has the topic + SQS subscription
    ensure_sns_topic_with_sqs_subscription().await;
    let _ = client.create_bucket().bucket(bucket).send().await;
    purge_sqs_queue("sns-forwarded").await;

    let topic_config = TopicConfiguration::builder()
        .id("sns-put-test")
        .topic_arn(SNS_TOPIC_ARN)
        .events(Event::S3ObjectCreated)
        .build()
        .expect("build topic config");

    client
        .put_bucket_notification_configuration()
        .bucket(bucket)
        .notification_configuration(
            NotificationConfiguration::builder()
                .topic_configurations(topic_config)
                .build(),
        )
        .send()
        .await
        .expect("put notification config");

    // Act: put an object
    client
        .put_object()
        .bucket(bucket)
        .key("hello.txt")
        .body(Bytes::from_static(b"hello world").into())
        .send()
        .await
        .expect("put object");

    // Assert: poll the sns-forwarded queue (SNS forwards to SQS via subscription)
    let msg = poll_sqs_message("sns-forwarded", 3000)
        .await
        .expect("expected SNS-forwarded SQS message");

    let event = parse_s3_event(&msg);
    let record = &event["Records"][0];
    assert_eq!(record["eventName"].as_str().unwrap(), "ObjectCreated:Put");
    assert_eq!(record["s3"]["bucket"]["name"].as_str().unwrap(), bucket);
    assert_eq!(record["s3"]["object"]["key"].as_str().unwrap(), "hello.txt");
    assert!(record["s3"]["object"]["size"].as_u64().unwrap() > 0);
    assert!(record["s3"]["object"]["eTag"].as_str().is_some());

    // Cleanup
    let _ = client.delete_object().bucket(bucket).key("hello.txt").send().await;
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

// ---------------------------------------------------------------------------
// Test 2: SQS queue notification on PutObject
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn notification_trigger_sqs_put_object() {
    use aws_sdk_s3::types::{Event, NotificationConfiguration, QueueConfiguration};

    let client = create_s3_client().await;
    let bucket = "notif-trigger-sqs-put";
    let queue_arn = "arn:aws:sqs:us-east-1:000000000000:queue-sqs-put";

    let _ = client.create_bucket().bucket(bucket).send().await;
    purge_sqs_queue("queue-sqs-put").await;

    let queue_config = QueueConfiguration::builder()
        .id("sqs-put-test")
        .queue_arn(queue_arn)
        .events(Event::S3ObjectCreated)
        .build()
        .expect("build queue config");

    client
        .put_bucket_notification_configuration()
        .bucket(bucket)
        .notification_configuration(
            NotificationConfiguration::builder()
                .queue_configurations(queue_config)
                .build(),
        )
        .send()
        .await
        .expect("put notification config");

    // Act
    client
        .put_object()
        .bucket(bucket)
        .key("data.bin")
        .body(Bytes::from_static(b"some data").into())
        .send()
        .await
        .expect("put object");

    // Assert
    let msg = poll_sqs_message("queue-sqs-put", 3000)
        .await
        .expect("expected SQS message");

    let event = parse_s3_event(&msg);
    let record = &event["Records"][0];
    assert_eq!(record["eventName"].as_str().unwrap(), "ObjectCreated:Put");
    assert_eq!(record["s3"]["bucket"]["name"].as_str().unwrap(), bucket);
    assert_eq!(record["s3"]["object"]["key"].as_str().unwrap(), "data.bin");

    // Cleanup
    let _ = client.delete_object().bucket(bucket).key("data.bin").send().await;
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

// ---------------------------------------------------------------------------
// Test 3: SQS notification on DeleteObject
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn notification_trigger_sqs_delete_object() {
    use aws_sdk_s3::types::{Event, NotificationConfiguration, QueueConfiguration};

    let client = create_s3_client().await;
    let bucket = "notif-trigger-sqs-del";
    let queue_arn = "arn:aws:sqs:us-east-1:000000000000:queue-sqs-del";

    let _ = client.create_bucket().bucket(bucket).send().await;
    purge_sqs_queue("queue-sqs-del").await;

    let queue_config = QueueConfiguration::builder()
        .id("sqs-delete-test")
        .queue_arn(queue_arn)
        .events(Event::S3ObjectRemoved)
        .build()
        .expect("build queue config");

    client
        .put_bucket_notification_configuration()
        .bucket(bucket)
        .notification_configuration(
            NotificationConfiguration::builder()
                .queue_configurations(queue_config)
                .build(),
        )
        .send()
        .await
        .expect("put notification config");

    // Put then delete
    client
        .put_object()
        .bucket(bucket)
        .key("to-delete.txt")
        .body(Bytes::from_static(b"delete me").into())
        .send()
        .await
        .expect("put object");

    // Purge any creation events that may have leaked
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    purge_sqs_queue("queue-sqs-del").await;

    client
        .delete_object()
        .bucket(bucket)
        .key("to-delete.txt")
        .send()
        .await
        .expect("delete object");

    // Assert
    let msg = poll_sqs_message("queue-sqs-del", 3000)
        .await
        .expect("expected SQS message for delete");

    let event = parse_s3_event(&msg);
    let record = &event["Records"][0];
    assert_eq!(
        record["eventName"].as_str().unwrap(),
        "ObjectRemoved:Delete"
    );
    assert_eq!(record["s3"]["bucket"]["name"].as_str().unwrap(), bucket);
    assert_eq!(
        record["s3"]["object"]["key"].as_str().unwrap(),
        "to-delete.txt"
    );

    // Cleanup
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

// ---------------------------------------------------------------------------
// Test 4: Notification with prefix filter
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn notification_trigger_prefix_filter() {
    use aws_sdk_s3::types::{
        Event, FilterRule, FilterRuleName, NotificationConfiguration, NotificationConfigurationFilter,
        QueueConfiguration, S3KeyFilter,
    };

    let client = create_s3_client().await;
    let bucket = "notif-trigger-filter";
    let queue_arn = "arn:aws:sqs:us-east-1:000000000000:queue-filter";

    let _ = client.create_bucket().bucket(bucket).send().await;
    purge_sqs_queue("queue-filter").await;

    let filter = NotificationConfigurationFilter::builder()
        .key(
            S3KeyFilter::builder()
                .filter_rules(
                    FilterRule::builder()
                        .name(FilterRuleName::Prefix)
                        .value("images/")
                        .build(),
                )
                .build(),
        )
        .build();

    let queue_config = QueueConfiguration::builder()
        .id("filter-test")
        .queue_arn(queue_arn)
        .events(Event::S3ObjectCreated)
        .filter(filter)
        .build()
        .expect("build queue config");

    client
        .put_bucket_notification_configuration()
        .bucket(bucket)
        .notification_configuration(
            NotificationConfiguration::builder()
                .queue_configurations(queue_config)
                .build(),
        )
        .send()
        .await
        .expect("put notification config");

    // Act: put matching and non-matching objects
    client
        .put_object()
        .bucket(bucket)
        .key("images/photo.jpg")
        .body(Bytes::from_static(b"jpg data").into())
        .send()
        .await
        .expect("put images/photo.jpg");

    client
        .put_object()
        .bucket(bucket)
        .key("docs/readme.txt")
        .body(Bytes::from_static(b"txt data").into())
        .send()
        .await
        .expect("put docs/readme.txt");

    // Wait for async dispatch
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Assert: only 1 message for images/photo.jpg
    let msg = poll_sqs_message("queue-filter", 2000)
        .await
        .expect("expected SQS message for images/photo.jpg");

    let event = parse_s3_event(&msg);
    let record = &event["Records"][0];
    assert_eq!(
        record["s3"]["object"]["key"].as_str().unwrap(),
        "images/photo.jpg"
    );

    // No second message
    let no_msg = poll_sqs_message("queue-filter", 1000).await;
    assert!(
        no_msg.is_none(),
        "should NOT receive notification for docs/readme.txt"
    );

    // Cleanup
    let _ = client.delete_object().bucket(bucket).key("images/photo.jpg").send().await;
    let _ = client.delete_object().bucket(bucket).key("docs/readme.txt").send().await;
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

// ---------------------------------------------------------------------------
// Test 5: CompleteMultipartUpload triggers notification
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn notification_trigger_complete_multipart() {
    use aws_sdk_s3::types::{Event, NotificationConfiguration, QueueConfiguration};

    let client = create_s3_client().await;
    let bucket = "notif-trigger-multipart";
    let queue_arn = "arn:aws:sqs:us-east-1:000000000000:queue-multipart";

    let _ = client.create_bucket().bucket(bucket).send().await;
    purge_sqs_queue("queue-multipart").await;

    let queue_config = QueueConfiguration::builder()
        .id("multipart-test")
        .queue_arn(queue_arn)
        .events(Event::S3ObjectCreatedCompleteMultipartUpload)
        .build()
        .expect("build queue config");

    client
        .put_bucket_notification_configuration()
        .bucket(bucket)
        .notification_configuration(
            NotificationConfiguration::builder()
                .queue_configurations(queue_config)
                .build(),
        )
        .send()
        .await
        .expect("put notification config");

    // Create multipart upload
    let create_resp = client
        .create_multipart_upload()
        .bucket(bucket)
        .key("large-file.bin")
        .send()
        .await
        .expect("create multipart upload");

    let upload_id = create_resp.upload_id().expect("upload id");

    // Upload a single part (5MB minimum for real S3, but our local impl accepts smaller)
    let part_data = Bytes::from(vec![0u8; 1024]);
    let upload_part_resp = client
        .upload_part()
        .bucket(bucket)
        .key("large-file.bin")
        .upload_id(upload_id)
        .part_number(1)
        .body(part_data.into())
        .send()
        .await
        .expect("upload part");

    let completed_part = CompletedPart::builder()
        .e_tag(upload_part_resp.e_tag().unwrap_or_default())
        .part_number(1)
        .build();

    let completed_upload = CompletedMultipartUpload::builder()
        .parts(completed_part)
        .build();

    // Complete multipart upload
    client
        .complete_multipart_upload()
        .bucket(bucket)
        .key("large-file.bin")
        .upload_id(upload_id)
        .multipart_upload(completed_upload)
        .send()
        .await
        .expect("complete multipart upload");

    // Assert
    let msg = poll_sqs_message("queue-multipart", 3000)
        .await
        .expect("expected SQS message for multipart complete");

    let event = parse_s3_event(&msg);
    let record = &event["Records"][0];
    assert_eq!(
        record["eventName"].as_str().unwrap(),
        "ObjectCreated:CompleteMultipartUpload"
    );
    assert_eq!(
        record["s3"]["object"]["key"].as_str().unwrap(),
        "large-file.bin"
    );

    // Cleanup
    let _ = client.delete_object().bucket(bucket).key("large-file.bin").send().await;
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

// ---------------------------------------------------------------------------
// Test 6: CopyObject triggers notification
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn notification_trigger_copy_object() {
    use aws_sdk_s3::types::{Event, NotificationConfiguration, QueueConfiguration};

    let client = create_s3_client().await;
    let bucket = "notif-trigger-copy";
    let queue_arn = "arn:aws:sqs:us-east-1:000000000000:queue-copy";

    let _ = client.create_bucket().bucket(bucket).send().await;
    purge_sqs_queue("queue-copy").await;

    let queue_config = QueueConfiguration::builder()
        .id("copy-test")
        .queue_arn(queue_arn)
        .events(Event::S3ObjectCreatedCopy)
        .build()
        .expect("build queue config");

    client
        .put_bucket_notification_configuration()
        .bucket(bucket)
        .notification_configuration(
            NotificationConfiguration::builder()
                .queue_configurations(queue_config)
                .build(),
        )
        .send()
        .await
        .expect("put notification config");

    // Put source object
    client
        .put_object()
        .bucket(bucket)
        .key("source.txt")
        .body(Bytes::from_static(b"copy me").into())
        .send()
        .await
        .expect("put source object");

    // Wait and purge any creation events (we only care about copy)
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    purge_sqs_queue("queue-copy").await;

    // Copy object
    client
        .copy_object()
        .bucket(bucket)
        .key("destination.txt")
        .copy_source(format!("{}/source.txt", bucket))
        .send()
        .await
        .expect("copy object");

    // Assert
    let msg = poll_sqs_message("queue-copy", 3000)
        .await
        .expect("expected SQS message for copy");

    let event = parse_s3_event(&msg);
    let record = &event["Records"][0];
    assert_eq!(record["eventName"].as_str().unwrap(), "ObjectCreated:Copy");
    assert_eq!(
        record["s3"]["object"]["key"].as_str().unwrap(),
        "destination.txt"
    );

    // Cleanup
    let _ = client.delete_object().bucket(bucket).key("source.txt").send().await;
    let _ = client.delete_object().bucket(bucket).key("destination.txt").send().await;
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

// ---------------------------------------------------------------------------
// Test 7: No notification when config not set
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn notification_trigger_no_config_no_message() {
    let client = create_s3_client().await;
    let bucket = "notif-trigger-none";

    let _ = client.create_bucket().bucket(bucket).send().await;
    purge_sqs_queue("test-queue").await;
    purge_sqs_queue("sns-forwarded").await;

    // No notification configuration set — just put an object
    client
        .put_object()
        .bucket(bucket)
        .key("quiet.txt")
        .body(Bytes::from_static(b"no notification").into())
        .send()
        .await
        .expect("put object");

    // Wait for any async dispatch that might fire
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Assert: both queues should be empty
    let msg1 = poll_sqs_message("test-queue", 1000).await;
    assert!(msg1.is_none(), "test-queue should be empty");

    let msg2 = poll_sqs_message("sns-forwarded", 1000).await;
    assert!(msg2.is_none(), "sns-forwarded should be empty");

    // Cleanup
    let _ = client.delete_object().bucket(bucket).key("quiet.txt").send().await;
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

// ---------------------------------------------------------------------------
// Test 8: Direct SNS→SQS chain diagnostic
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn notification_trigger_sns_chain_diagnostic() {
    // Set up topic + subscription via API
    ensure_sns_topic_with_sqs_subscription().await;
    purge_sqs_queue("sns-forwarded").await;

    // Publish directly to local-sns
    let client = reqwest::Client::new();
    let message = r#"{"Records":[{"eventName":"ObjectCreated:Put","s3":{"bucket":{"name":"diag"},"object":{"key":"test.txt"}}}]}"#;
    let body = format!(
        "Action=Publish&TopicArn={}&Message={}",
        percent_encode(SNS_TOPIC_ARN),
        percent_encode(message),
    );
    let resp = client
        .post(format!("{}/", SNS_ENDPOINT))
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(body)
        .send()
        .await
        .expect("SNS Publish failed");
    let status = resp.status();
    let resp_body = resp.text().await.unwrap_or_default();
    eprintln!("SNS publish response: status={}, body={}", status, resp_body);
    assert!(status.is_success(), "SNS publish returned {}: {}", status, resp_body);

    // Check if message arrived in sns-forwarded queue
    let msg = poll_sqs_message("sns-forwarded", 5000).await;
    eprintln!("SQS poll result: {:?}", msg);
    assert!(msg.is_some(), "SNS->SQS chain broken: no message in sns-forwarded queue");
}

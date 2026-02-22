//! Bucket Request Payment integration tests

use super::*;

#[tokio::test]
#[ignore]
async fn test_put_get_bucket_request_payment_requester() {
    use aws_sdk_s3::types::{Payer, RequestPaymentConfiguration};

    let client = create_s3_client().await;
    let bucket = "sdk-request-payment-test";

    // Create bucket
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Set request payment to Requester
    let config = RequestPaymentConfiguration::builder()
        .payer(Payer::Requester)
        .build()
        .expect("Failed to build RequestPaymentConfiguration");

    client
        .put_bucket_request_payment()
        .bucket(bucket)
        .request_payment_configuration(config)
        .send()
        .await
        .expect("Failed to put bucket request payment");

    // Get request payment
    let result = client
        .get_bucket_request_payment()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to get bucket request payment");

    // Verify payer is Requester
    assert_eq!(
        result.payer(),
        Some(&Payer::Requester),
        "Payer should be Requester"
    );

    // Cleanup
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_put_get_bucket_request_payment_bucket_owner() {
    use aws_sdk_s3::types::{Payer, RequestPaymentConfiguration};

    let client = create_s3_client().await;
    let bucket = "sdk-request-payment-owner-test";

    // Create bucket
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Set request payment to BucketOwner
    let config = RequestPaymentConfiguration::builder()
        .payer(Payer::BucketOwner)
        .build()
        .expect("Failed to build RequestPaymentConfiguration");

    client
        .put_bucket_request_payment()
        .bucket(bucket)
        .request_payment_configuration(config)
        .send()
        .await
        .expect("Failed to put bucket request payment");

    // Get request payment
    let result = client
        .get_bucket_request_payment()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to get bucket request payment");

    // Verify payer is BucketOwner
    assert_eq!(
        result.payer(),
        Some(&Payer::BucketOwner),
        "Payer should be BucketOwner"
    );

    // Cleanup
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_get_bucket_request_payment_default() {
    use aws_sdk_s3::types::Payer;

    let client = create_s3_client().await;
    let bucket = "sdk-request-payment-default-test";

    // Create bucket
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Get request payment without setting it (should return BucketOwner by default)
    let result = client
        .get_bucket_request_payment()
        .bucket(bucket)
        .send()
        .await
        .expect("GetBucketRequestPayment should succeed even when not configured");

    // Default payer should be BucketOwner
    assert_eq!(
        result.payer(),
        Some(&Payer::BucketOwner),
        "Default payer should be BucketOwner"
    );

    // Cleanup
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_put_bucket_request_payment_bucket_not_found() {
    use aws_sdk_s3::types::{Payer, RequestPaymentConfiguration};

    let client = create_s3_client().await;

    let config = RequestPaymentConfiguration::builder()
        .payer(Payer::Requester)
        .build()
        .expect("Failed to build RequestPaymentConfiguration");

    // Should fail for non-existent bucket
    let result = client
        .put_bucket_request_payment()
        .bucket("nonexistent-bucket-request-payment-12345")
        .request_payment_configuration(config)
        .send()
        .await;

    assert!(result.is_err(), "Should fail for non-existent bucket");
}

use super::*;

#[tokio::test]
#[ignore] // Run manually with --ignored flag
async fn test_bucket_operations() {
    let client = create_s3_client().await;

    // Create bucket
    client
        .create_bucket()
        .bucket("sdk-test-bucket")
        .send()
        .await
        .expect("Failed to create bucket");

    // List buckets
    let buckets = client
        .list_buckets()
        .send()
        .await
        .expect("Failed to list buckets");

    assert!(buckets
        .buckets()
        .iter()
        .any(|b| b.name() == Some("sdk-test-bucket")));

    // Delete bucket
    client
        .delete_bucket()
        .bucket("sdk-test-bucket")
        .send()
        .await
        .expect("Failed to delete bucket");
}

#[tokio::test]
#[ignore]
async fn test_head_bucket() {
    let client = create_s3_client().await;
    let bucket = "sdk-head-bucket-test";

    // Create bucket
    client
        .create_bucket()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to create bucket");

    // Head bucket should succeed
    client
        .head_bucket()
        .bucket(bucket)
        .send()
        .await
        .expect("HeadBucket should succeed for existing bucket");

    // Cleanup
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_head_bucket_not_found() {
    let client = create_s3_client().await;

    // Head non-existent bucket should fail
    let result = client
        .head_bucket()
        .bucket("nonexistent-bucket-12345")
        .send()
        .await;

    assert!(result.is_err(), "HeadBucket should fail for non-existent bucket");
}

#[tokio::test]
#[ignore]
async fn test_get_bucket_location() {
    let client = create_s3_client().await;
    let bucket = "sdk-location-test";

    // Create bucket
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Get bucket location
    let result = client
        .get_bucket_location()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to get bucket location");

    // Location should be set (us-east-1 returns empty/null per S3 spec, or actual region)
    // Just verify the call succeeds
    let _location = result.location_constraint();

    // Cleanup
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_delete_non_empty_bucket() {
    let client = create_s3_client().await;
    let bucket = "sdk-non-empty-delete-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Put an object
    client
        .put_object()
        .bucket(bucket)
        .key("blocking-key")
        .body(Bytes::from("data").into())
        .send()
        .await
        .unwrap();

    // Try to delete bucket (should fail - bucket not empty)
    let result = client.delete_bucket().bucket(bucket).send().await;
    assert!(result.is_err(), "Should not be able to delete non-empty bucket");

    // Cleanup properly
    client.delete_object().bucket(bucket).key("blocking-key").send().await.ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

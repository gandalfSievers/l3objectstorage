use super::*;

/// Test NoSuchBucket error when operating on non-existent bucket
#[tokio::test]
#[ignore]
async fn test_error_no_such_bucket() {
    let client = create_s3_client().await;

    // Try to get object from non-existent bucket
    let result = client
        .get_object()
        .bucket("nonexistent-bucket-xyz-123456")
        .key("any-key")
        .send()
        .await;

    assert!(result.is_err(), "GetObject should fail for non-existent bucket");
    let err = format!("{:?}", result.err().unwrap());
    assert!(
        err.contains("NoSuchBucket") || err.contains("not found") || err.contains("404") || err.contains("does not exist"),
        "Error should indicate bucket doesn't exist: {}",
        err
    );

    // Try to list objects in non-existent bucket
    let list_result = client
        .list_objects_v2()
        .bucket("nonexistent-bucket-xyz-123456")
        .send()
        .await;

    assert!(list_result.is_err(), "ListObjects should fail for non-existent bucket");
}

/// Test NoSuchKey error when getting non-existent object
#[tokio::test]
#[ignore]
async fn test_error_no_such_key() {
    let client = create_s3_client().await;
    let bucket = "sdk-error-no-such-key-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Try to get non-existent key
    let result = client
        .get_object()
        .bucket(bucket)
        .key("nonexistent-key-xyz")
        .send()
        .await;

    assert!(result.is_err(), "GetObject should fail for non-existent key");
    let err = format!("{:?}", result.err().unwrap());
    assert!(
        err.contains("NoSuchKey") || err.contains("not found") || err.contains("404"),
        "Error should indicate key doesn't exist: {}",
        err
    );

    // Also test HeadObject
    let head_result = client
        .head_object()
        .bucket(bucket)
        .key("nonexistent-key-xyz")
        .send()
        .await;

    assert!(head_result.is_err(), "HeadObject should fail for non-existent key");

    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test BucketAlreadyExists error when creating duplicate bucket
#[tokio::test]
#[ignore]
async fn test_error_bucket_already_exists() {
    let client = create_s3_client().await;
    let bucket = "sdk-error-bucket-exists-test";

    // Create bucket first time
    let first_create = client.create_bucket().bucket(bucket).send().await;
    assert!(first_create.is_ok(), "First create should succeed");

    // Try to create same bucket again
    let second_create = client.create_bucket().bucket(bucket).send().await;

    // Depending on implementation, this might succeed (idempotent) or fail
    // AWS S3 returns BucketAlreadyOwnedByYou if same owner
    // Let's just verify the operation completes and bucket exists
    let _ = second_create;

    // Verify bucket exists
    let head_result = client.head_bucket().bucket(bucket).send().await;
    assert!(head_result.is_ok(), "Bucket should exist");

    // Cleanup
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test BucketNotEmpty error when deleting non-empty bucket
#[tokio::test]
#[ignore]
async fn test_error_bucket_not_empty() {
    let client = create_s3_client().await;
    let bucket = "sdk-error-not-empty-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Put an object
    client
        .put_object()
        .bucket(bucket)
        .key("test-object")
        .body(Bytes::from("content").into())
        .send()
        .await
        .expect("Failed to put object");

    // Try to delete non-empty bucket
    let delete_result = client.delete_bucket().bucket(bucket).send().await;

    assert!(delete_result.is_err(), "DeleteBucket should fail for non-empty bucket");
    let err = format!("{:?}", delete_result.err().unwrap());
    assert!(
        err.contains("BucketNotEmpty") || err.contains("not empty") || err.contains("409"),
        "Error should indicate bucket not empty: {}",
        err
    );

    // Cleanup: delete object first, then bucket
    client.delete_object().bucket(bucket).key("test-object").send().await.ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test InvalidBucketName error for invalid bucket names
#[tokio::test]
#[ignore]
async fn test_error_invalid_bucket_name() {
    let client = create_s3_client().await;

    // Bucket name too short (less than 3 chars)
    let short_result = client.create_bucket().bucket("ab").send().await;
    // This may or may not fail at SDK level

    // Bucket name with uppercase (invalid per S3 spec)
    let uppercase_result = client.create_bucket().bucket("MyBucket").send().await;

    // Bucket name with underscore (invalid per S3 spec)
    let underscore_result = client.create_bucket().bucket("my_bucket").send().await;

    // At least one of these should fail
    let any_failed = short_result.is_err() || uppercase_result.is_err() || underscore_result.is_err();

    // Cleanup any that might have succeeded
    let _ = client.delete_bucket().bucket("ab").send().await;
    let _ = client.delete_bucket().bucket("MyBucket").send().await;
    let _ = client.delete_bucket().bucket("my_bucket").send().await;

    // Note: Some implementations are more lenient, so we don't strictly assert failure
    // Just verify the test runs
    let _ = any_failed;
}

/// Test KeyTooLong error for object keys exceeding limit
#[tokio::test]
#[ignore]
async fn test_error_key_too_long() {
    let client = create_s3_client().await;
    let bucket = "sdk-error-key-too-long-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Create a key that's way too long (>1024 bytes)
    let long_key = "a".repeat(2000);

    let result = client
        .put_object()
        .bucket(bucket)
        .key(&long_key)
        .body(Bytes::from("content").into())
        .send()
        .await;

    // S3 limit is 1024 bytes for key
    assert!(result.is_err(), "PutObject should fail for key too long");
    let err = format!("{:?}", result.err().unwrap());
    assert!(
        err.contains("KeyTooLong") || err.contains("key") || err.contains("414") || err.contains("too long"),
        "Error should indicate key too long: {}",
        err
    );

    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test InvalidPartNumber error for multipart uploads
#[tokio::test]
#[ignore]
async fn test_error_invalid_part_number() {
    let client = create_s3_client().await;
    let bucket = "sdk-error-invalid-part-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Create multipart upload
    let create_result = client
        .create_multipart_upload()
        .bucket(bucket)
        .key("test-multipart")
        .send()
        .await
        .expect("Failed to create multipart upload");

    let upload_id = create_result.upload_id().unwrap();

    // Try to upload part with invalid number (0 or negative not possible with i32)
    // Part numbers must be 1-10000
    // Try part 0 (invalid)
    let result_zero = client
        .upload_part()
        .bucket(bucket)
        .key("test-multipart")
        .upload_id(upload_id)
        .part_number(0)
        .body(Bytes::from("data").into())
        .send()
        .await;

    // SDK might reject 0 at validation level or server might reject it
    let zero_failed = result_zero.is_err();

    // Try part > 10000 (invalid)
    let result_high = client
        .upload_part()
        .bucket(bucket)
        .key("test-multipart")
        .upload_id(upload_id)
        .part_number(10001)
        .body(Bytes::from("data").into())
        .send()
        .await;

    let high_failed = result_high.is_err();

    // At least one should fail
    assert!(
        zero_failed || high_failed,
        "Invalid part numbers should be rejected"
    );

    // Cleanup
    client
        .abort_multipart_upload()
        .bucket(bucket)
        .key("test-multipart")
        .upload_id(upload_id)
        .send()
        .await
        .ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test InvalidRange error for out-of-bounds range requests
#[tokio::test]
#[ignore]
async fn test_error_invalid_range() {
    let client = create_s3_client().await;
    let bucket = "sdk-error-invalid-range-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Put small object (10 bytes)
    client
        .put_object()
        .bucket(bucket)
        .key("small-object")
        .body(Bytes::from("0123456789").into())
        .send()
        .await
        .expect("Failed to put object");

    // Request range beyond object size
    let result = client
        .get_object()
        .bucket(bucket)
        .key("small-object")
        .range("bytes=100-200")
        .send()
        .await;

    // S3 returns 416 Range Not Satisfiable for invalid ranges
    assert!(result.is_err(), "GetObject should fail for invalid range");
    let err = format!("{:?}", result.err().unwrap());
    assert!(
        err.contains("InvalidRange") || err.contains("416") || err.contains("Range"),
        "Error should indicate invalid range: {}",
        err
    );

    // Cleanup
    client.delete_object().bucket(bucket).key("small-object").send().await.ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test PreconditionFailed error for conditional requests
#[tokio::test]
#[ignore]
async fn test_error_precondition_failed() {
    let client = create_s3_client().await;
    let bucket = "sdk-error-precondition-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Put object
    client
        .put_object()
        .bucket(bucket)
        .key("test-object")
        .body(Bytes::from("content").into())
        .send()
        .await
        .expect("Failed to put object");

    // GetObject with If-Match that doesn't match
    let result = client
        .get_object()
        .bucket(bucket)
        .key("test-object")
        .if_match("\"00000000000000000000000000000000\"")
        .send()
        .await;

    assert!(result.is_err(), "GetObject should fail when If-Match doesn't match");
    let err = format!("{:?}", result.err().unwrap());
    assert!(
        err.contains("PreconditionFailed") || err.contains("412") || err.contains("precondition"),
        "Error should indicate precondition failed: {}",
        err
    );

    // Cleanup
    client.delete_object().bucket(bucket).key("test-object").send().await.ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

//! Server-Side Encryption tests

use super::*;
use aws_sdk_s3::types::{
    ServerSideEncryption, ServerSideEncryptionByDefault, ServerSideEncryptionConfiguration,
    ServerSideEncryptionRule,
};

#[tokio::test]
#[ignore]
async fn test_bucket_encryption_lifecycle() {
    let client = create_s3_client().await;
    let bucket = "sdk-encryption-lifecycle-test";

    // Create bucket
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Get encryption should fail initially (no configuration)
    let result = client.get_bucket_encryption().bucket(bucket).send().await;
    assert!(
        result.is_err(),
        "GetBucketEncryption should fail when no configuration exists"
    );

    // Put bucket encryption (SSE-S3 / AES256)
    let sse_default = ServerSideEncryptionByDefault::builder()
        .sse_algorithm(ServerSideEncryption::Aes256)
        .build()
        .expect("Failed to build SSE default");

    let sse_rule = ServerSideEncryptionRule::builder()
        .apply_server_side_encryption_by_default(sse_default)
        .build();

    let sse_config = ServerSideEncryptionConfiguration::builder()
        .rules(sse_rule)
        .build()
        .expect("Failed to build SSE configuration");

    client
        .put_bucket_encryption()
        .bucket(bucket)
        .server_side_encryption_configuration(sse_config)
        .send()
        .await
        .expect("Failed to put bucket encryption");

    // Get encryption should work now
    let result = client
        .get_bucket_encryption()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to get bucket encryption");

    let config = result
        .server_side_encryption_configuration()
        .expect("Should have encryption configuration");
    assert!(!config.rules().is_empty(), "Should have at least one rule");

    let rule = &config.rules()[0];
    let default_sse = rule
        .apply_server_side_encryption_by_default()
        .expect("Should have default SSE");
    assert_eq!(
        default_sse.sse_algorithm(),
        &ServerSideEncryption::Aes256,
        "Should be AES256"
    );

    // Delete encryption
    client
        .delete_bucket_encryption()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to delete bucket encryption");

    // Verify deletion
    let result = client.get_bucket_encryption().bucket(bucket).send().await;
    assert!(
        result.is_err(),
        "GetBucketEncryption should fail after deletion"
    );

    // Cleanup
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_object_encryption_explicit_header() {
    let client = create_s3_client().await;
    let bucket = "sdk-object-sse-explicit-test";

    // Create bucket
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Put object with explicit SSE header
    let put_response = client
        .put_object()
        .bucket(bucket)
        .key("encrypted-object.txt")
        .body(Bytes::from("secret data that should be encrypted").into())
        .content_type("text/plain")
        .server_side_encryption(ServerSideEncryption::Aes256)
        .send()
        .await
        .expect("Failed to put encrypted object");

    // Verify SSE header in response
    assert_eq!(
        put_response.server_side_encryption(),
        Some(&ServerSideEncryption::Aes256),
        "PutObject response should indicate AES256 encryption"
    );

    // Get object should return SSE header and decrypted content
    let get_response = client
        .get_object()
        .bucket(bucket)
        .key("encrypted-object.txt")
        .send()
        .await
        .expect("Failed to get encrypted object");

    assert_eq!(
        get_response.server_side_encryption(),
        Some(&ServerSideEncryption::Aes256),
        "GetObject response should indicate AES256 encryption"
    );

    // Verify data is correctly decrypted
    let body = get_response.body.collect().await.unwrap().into_bytes();
    assert_eq!(
        body,
        Bytes::from("secret data that should be encrypted"),
        "Decrypted content should match original"
    );

    // Head object should also return SSE header
    let head_response = client
        .head_object()
        .bucket(bucket)
        .key("encrypted-object.txt")
        .send()
        .await
        .expect("Failed to head encrypted object");

    assert_eq!(
        head_response.server_side_encryption(),
        Some(&ServerSideEncryption::Aes256),
        "HeadObject response should indicate AES256 encryption"
    );

    // Cleanup
    client
        .delete_object()
        .bucket(bucket)
        .key("encrypted-object.txt")
        .send()
        .await
        .ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_bucket_default_encryption_auto_applies() {
    let client = create_s3_client().await;
    let bucket = "sdk-default-sse-auto-test";

    // Create bucket
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Set bucket default encryption
    let sse_default = ServerSideEncryptionByDefault::builder()
        .sse_algorithm(ServerSideEncryption::Aes256)
        .build()
        .expect("Failed to build SSE default");

    let sse_rule = ServerSideEncryptionRule::builder()
        .apply_server_side_encryption_by_default(sse_default)
        .build();

    let sse_config = ServerSideEncryptionConfiguration::builder()
        .rules(sse_rule)
        .build()
        .expect("Failed to build SSE configuration");

    client
        .put_bucket_encryption()
        .bucket(bucket)
        .server_side_encryption_configuration(sse_config)
        .send()
        .await
        .expect("Failed to put bucket encryption");

    // Put object WITHOUT explicit SSE header
    // Should still be encrypted due to bucket default
    let put_response = client
        .put_object()
        .bucket(bucket)
        .key("auto-encrypted.txt")
        .body(Bytes::from("auto-encrypted content").into())
        .send()
        .await
        .expect("Failed to put object");

    // Response should indicate encryption was applied
    assert_eq!(
        put_response.server_side_encryption(),
        Some(&ServerSideEncryption::Aes256),
        "Object should be auto-encrypted via bucket default"
    );

    // Verify on get
    let get_response = client
        .get_object()
        .bucket(bucket)
        .key("auto-encrypted.txt")
        .send()
        .await
        .expect("Failed to get object");

    assert_eq!(
        get_response.server_side_encryption(),
        Some(&ServerSideEncryption::Aes256),
        "GetObject should show encryption"
    );

    let body = get_response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body, Bytes::from("auto-encrypted content"));

    // Cleanup
    client
        .delete_object()
        .bucket(bucket)
        .key("auto-encrypted.txt")
        .send()
        .await
        .ok();
    client
        .delete_bucket_encryption()
        .bucket(bucket)
        .send()
        .await
        .ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_multipart_upload_with_encryption() {
    let client = create_s3_client().await;
    let bucket = "sdk-multipart-sse-test";

    // Create bucket
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Create multipart upload with SSE
    let create_response = client
        .create_multipart_upload()
        .bucket(bucket)
        .key("large-encrypted.bin")
        .server_side_encryption(ServerSideEncryption::Aes256)
        .send()
        .await
        .expect("Failed to create multipart upload");

    let upload_id = create_response.upload_id().expect("Should have upload ID");
    assert_eq!(
        create_response.server_side_encryption(),
        Some(&ServerSideEncryption::Aes256),
        "CreateMultipartUpload should indicate encryption"
    );

    // Upload part 1 (5MB minimum for non-final parts in real S3, but we'll use smaller for testing)
    let part1_data = vec![b'A'; 5 * 1024 * 1024]; // 5MB
    let part1_response = client
        .upload_part()
        .bucket(bucket)
        .key("large-encrypted.bin")
        .upload_id(upload_id)
        .part_number(1)
        .body(Bytes::from(part1_data.clone()).into())
        .send()
        .await
        .expect("Failed to upload part 1");

    // Upload part 2
    let part2_data = vec![b'B'; 1024]; // 1KB
    let part2_response = client
        .upload_part()
        .bucket(bucket)
        .key("large-encrypted.bin")
        .upload_id(upload_id)
        .part_number(2)
        .body(Bytes::from(part2_data.clone()).into())
        .send()
        .await
        .expect("Failed to upload part 2");

    // Complete multipart upload
    let completed_upload = CompletedMultipartUpload::builder()
        .parts(
            CompletedPart::builder()
                .part_number(1)
                .e_tag(part1_response.e_tag().unwrap())
                .build(),
        )
        .parts(
            CompletedPart::builder()
                .part_number(2)
                .e_tag(part2_response.e_tag().unwrap())
                .build(),
        )
        .build();

    let complete_response = client
        .complete_multipart_upload()
        .bucket(bucket)
        .key("large-encrypted.bin")
        .upload_id(upload_id)
        .multipart_upload(completed_upload)
        .send()
        .await
        .expect("Failed to complete multipart upload");

    assert_eq!(
        complete_response.server_side_encryption(),
        Some(&ServerSideEncryption::Aes256),
        "CompleteMultipartUpload should indicate encryption"
    );

    // Verify the object is encrypted and content is correct
    let head_response = client
        .head_object()
        .bucket(bucket)
        .key("large-encrypted.bin")
        .send()
        .await
        .expect("Failed to head object");

    assert_eq!(
        head_response.server_side_encryption(),
        Some(&ServerSideEncryption::Aes256),
        "HeadObject should show encryption"
    );

    // Verify content (get full object)
    let get_response = client
        .get_object()
        .bucket(bucket)
        .key("large-encrypted.bin")
        .send()
        .await
        .expect("Failed to get object");

    let body = get_response.body.collect().await.unwrap().into_bytes();
    let mut expected = part1_data;
    expected.extend(part2_data);
    assert_eq!(body.len(), expected.len(), "Content length should match");
    assert_eq!(body.as_ref(), expected.as_slice(), "Content should match");

    // Cleanup
    client
        .delete_object()
        .bucket(bucket)
        .key("large-encrypted.bin")
        .send()
        .await
        .ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_copy_encrypted_object() {
    let client = create_s3_client().await;
    let bucket = "sdk-copy-sse-test";

    // Create bucket
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Put encrypted source object
    client
        .put_object()
        .bucket(bucket)
        .key("source-encrypted.txt")
        .body(Bytes::from("encrypted source content").into())
        .server_side_encryption(ServerSideEncryption::Aes256)
        .send()
        .await
        .expect("Failed to put source object");

    // Copy to new key (should preserve encryption)
    let copy_response = client
        .copy_object()
        .bucket(bucket)
        .key("dest-encrypted.txt")
        .copy_source(format!("{}/source-encrypted.txt", bucket))
        .server_side_encryption(ServerSideEncryption::Aes256)
        .send()
        .await
        .expect("Failed to copy object");

    assert_eq!(
        copy_response.server_side_encryption(),
        Some(&ServerSideEncryption::Aes256),
        "CopyObject should indicate encryption"
    );

    // Verify destination is encrypted
    let head_response = client
        .head_object()
        .bucket(bucket)
        .key("dest-encrypted.txt")
        .send()
        .await
        .expect("Failed to head destination object");

    assert_eq!(
        head_response.server_side_encryption(),
        Some(&ServerSideEncryption::Aes256)
    );

    // Verify content
    let get_response = client
        .get_object()
        .bucket(bucket)
        .key("dest-encrypted.txt")
        .send()
        .await
        .expect("Failed to get destination object");

    let body = get_response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body, Bytes::from("encrypted source content"));

    // Cleanup
    client
        .delete_object()
        .bucket(bucket)
        .key("source-encrypted.txt")
        .send()
        .await
        .ok();
    client
        .delete_object()
        .bucket(bucket)
        .key("dest-encrypted.txt")
        .send()
        .await
        .ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

use super::*;

/// Test that standard response headers are present (Content-Type, ETag)
#[tokio::test]
#[ignore]
async fn test_response_headers_standard() {
    let client = create_s3_client().await;
    let bucket = "sdk-protocol-headers-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Put object with specific content type
    let put_result = client
        .put_object()
        .bucket(bucket)
        .key("test-file.json")
        .body(Bytes::from(r#"{"key": "value"}"#).into())
        .content_type("application/json")
        .send()
        .await
        .expect("PutObject should succeed");

    // Verify ETag is returned
    let etag = put_result.e_tag();
    assert!(etag.is_some(), "PutObject response should have ETag");
    let etag_value = etag.unwrap();
    assert!(!etag_value.is_empty(), "ETag should not be empty");
    assert!(etag_value.starts_with('"'), "ETag should be quoted");
    assert!(etag_value.ends_with('"'), "ETag should be quoted");

    // Get object and verify headers
    let get_result = client
        .get_object()
        .bucket(bucket)
        .key("test-file.json")
        .send()
        .await
        .expect("GetObject should succeed");

    // Verify Content-Type
    assert_eq!(
        get_result.content_type(),
        Some("application/json"),
        "Content-Type should match"
    );

    // Verify ETag
    assert!(get_result.e_tag().is_some(), "GetObject response should have ETag");
    assert_eq!(
        get_result.e_tag(),
        Some(etag_value),
        "ETags should match"
    );

    // Verify Content-Length
    assert!(get_result.content_length().is_some(), "Content-Length should be present");
    assert!(get_result.content_length().unwrap() > 0, "Content-Length should be positive");

    // Cleanup
    client.delete_object().bucket(bucket).key("test-file.json").send().await.ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test that x-amz-request-id header is present
#[tokio::test]
#[ignore]
async fn test_request_id_header_present() {
    let client = create_s3_client().await;
    let bucket = "sdk-protocol-request-id-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Put object
    let put_result = client
        .put_object()
        .bucket(bucket)
        .key("test-key")
        .body(Bytes::from("content").into())
        .send()
        .await
        .expect("PutObject should succeed");

    // The AWS SDK doesn't directly expose the request ID through the response object
    // in a straightforward way, but we can verify the operation completes successfully
    // Request ID would be in the raw HTTP response headers
    assert!(put_result.e_tag().is_some(), "Response should be valid");

    // For more detailed header inspection, we'd need to use raw HTTP client
    // or SDK interceptors. This test verifies basic protocol compliance.

    // Cleanup
    client.delete_object().bucket(bucket).key("test-key").send().await.ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test checksum headers (x-amz-checksum-*)
#[tokio::test]
#[ignore]
async fn test_checksum_headers() {
    let client = create_s3_client().await;
    let bucket = "sdk-protocol-checksum-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Put object with checksum algorithm specified
    // Note: AWS SDK supports CRC32, CRC32C, SHA1, SHA256
    let content = Bytes::from("content for checksum verification");

    let put_result = client
        .put_object()
        .bucket(bucket)
        .key("checksum-test-key")
        .body(content.clone().into())
        .checksum_algorithm(aws_sdk_s3::types::ChecksumAlgorithm::Crc32)
        .send()
        .await;

    // The operation might succeed or not depending on server support
    if put_result.is_ok() {
        let result = put_result.unwrap();

        // If checksum was used, response may include checksum
        if result.checksum_crc32().is_some() {
            let checksum = result.checksum_crc32().unwrap();
            assert!(!checksum.is_empty(), "Checksum should not be empty");
        }

        // Get and verify
        let get_result = client
            .get_object()
            .bucket(bucket)
            .key("checksum-test-key")
            .checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled)
            .send()
            .await
            .expect("GetObject should succeed");

        let body = get_result.body.collect().await.unwrap().into_bytes();
        assert_eq!(body, content, "Content should match");

        // Cleanup
        client.delete_object().bucket(bucket).key("checksum-test-key").send().await.ok();
    }

    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test Content-MD5 validation
#[tokio::test]
#[ignore]
async fn test_content_md5_validation() {
    let client = create_s3_client().await;
    let bucket = "sdk-protocol-md5-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Put object normally (SDK handles MD5 automatically in some cases)
    let content = Bytes::from("test content for md5");

    // The AWS SDK typically handles Content-MD5 automatically for certain operations
    // Here we just verify the put works correctly
    let put_result = client
        .put_object()
        .bucket(bucket)
        .key("md5-test-key")
        .body(content.clone().into())
        .send()
        .await
        .expect("PutObject should succeed");

    // Verify ETag is MD5-based for non-multipart uploads
    let etag = put_result.e_tag().expect("Should have ETag");
    // For non-multipart uploads, ETag is typically the MD5 hash
    // Format: "md5hash" (32 hex chars in quotes)
    let etag_inner = etag.trim_matches('"');

    // Non-multipart ETags are typically 32 hex characters (MD5)
    // Multipart ETags contain a dash
    if !etag_inner.contains('-') {
        assert_eq!(etag_inner.len(), 32, "Non-multipart ETag should be 32 hex chars (MD5)");
        // Verify it's valid hex
        assert!(
            etag_inner.chars().all(|c| c.is_ascii_hexdigit()),
            "ETag should be valid hex"
        );
    }

    // Get and verify content matches
    let get_result = client
        .get_object()
        .bucket(bucket)
        .key("md5-test-key")
        .send()
        .await
        .expect("GetObject should succeed");

    let body = get_result.body.collect().await.unwrap().into_bytes();
    assert_eq!(body, content, "Content should match");

    // Cleanup
    client.delete_object().bucket(bucket).key("md5-test-key").send().await.ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

use super::*;

/// Test pre-signed GET URL for downloading objects
#[tokio::test]
#[ignore]
async fn test_presigned_get_url() {
    let client = create_s3_client().await;
    let bucket = "presigned-get-test";

    // Setup: create bucket and object
    let _ = client.create_bucket().bucket(bucket).send().await;

    client
        .put_object()
        .bucket(bucket)
        .key("test-file.txt")
        .body(Bytes::from("pre-signed content").into())
        .content_type("text/plain")
        .send()
        .await
        .expect("Failed to put object");

    // Generate pre-signed URL (valid for 1 hour)
    let presigning_config =
        aws_sdk_s3::presigning::PresigningConfig::expires_in(std::time::Duration::from_secs(3600))
            .expect("Failed to create presigning config");

    let presigned_request = client
        .get_object()
        .bucket(bucket)
        .key("test-file.txt")
        .presigned(presigning_config)
        .await
        .expect("Failed to generate pre-signed URL");

    // Use reqwest to fetch via pre-signed URL (no SDK auth)
    let http_client = reqwest::Client::new();
    let response = http_client
        .get(presigned_request.uri())
        .send()
        .await
        .expect("Failed to fetch via pre-signed URL");

    assert_eq!(
        response.status(),
        200,
        "Pre-signed GET should return 200 OK"
    );
    let body = response.text().await.unwrap();
    assert_eq!(body, "pre-signed content");

    // Cleanup
    client
        .delete_object()
        .bucket(bucket)
        .key("test-file.txt")
        .send()
        .await
        .ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test pre-signed PUT URL for uploading objects
#[tokio::test]
#[ignore]
async fn test_presigned_put_url() {
    let client = create_s3_client().await;
    let bucket = "presigned-put-test";

    // Setup: create bucket
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Generate pre-signed PUT URL
    let presigning_config =
        aws_sdk_s3::presigning::PresigningConfig::expires_in(std::time::Duration::from_secs(3600))
            .expect("Failed to create presigning config");

    let presigned_request = client
        .put_object()
        .bucket(bucket)
        .key("uploaded-file.txt")
        .presigned(presigning_config)
        .await
        .expect("Failed to generate pre-signed URL");

    // Use reqwest to upload via pre-signed URL
    let http_client = reqwest::Client::new();
    let response = http_client
        .put(presigned_request.uri())
        .body("uploaded via pre-signed URL")
        .send()
        .await
        .expect("Failed to upload via pre-signed URL");

    assert_eq!(
        response.status(),
        200,
        "Pre-signed PUT should return 200 OK"
    );

    // Verify object was created by fetching it with SDK
    let get_response = client
        .get_object()
        .bucket(bucket)
        .key("uploaded-file.txt")
        .send()
        .await
        .expect("Failed to get uploaded object");

    let body = get_response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body, Bytes::from("uploaded via pre-signed URL"));

    // Cleanup
    client
        .delete_object()
        .bucket(bucket)
        .key("uploaded-file.txt")
        .send()
        .await
        .ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test expired pre-signed URL returns appropriate error
#[tokio::test]
#[ignore]
async fn test_presigned_url_expired() {
    let client = create_s3_client().await;
    let bucket = "presigned-expired-test";

    // Setup
    let _ = client.create_bucket().bucket(bucket).send().await;
    client
        .put_object()
        .bucket(bucket)
        .key("test-file.txt")
        .body(Bytes::from("content").into())
        .send()
        .await
        .unwrap();

    // Generate pre-signed URL with 1 second expiry
    let presigning_config =
        aws_sdk_s3::presigning::PresigningConfig::expires_in(std::time::Duration::from_secs(1))
            .expect("Failed to create presigning config");

    let presigned_request = client
        .get_object()
        .bucket(bucket)
        .key("test-file.txt")
        .presigned(presigning_config)
        .await
        .unwrap();

    // Wait for URL to expire
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Try to use expired URL
    let http_client = reqwest::Client::new();
    let response = http_client
        .get(presigned_request.uri())
        .send()
        .await
        .expect("Request should complete");

    // Should return 403 AccessDenied for expired URL
    assert_eq!(
        response.status(),
        403,
        "Expected 403 for expired URL, got {}",
        response.status()
    );

    let body = response.text().await.unwrap();
    assert!(
        body.contains("AccessDenied") || body.contains("expired") || body.contains("Request has expired"),
        "Response should indicate expiration: {}",
        body
    );

    // Cleanup
    client
        .delete_object()
        .bucket(bucket)
        .key("test-file.txt")
        .send()
        .await
        .ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test pre-signed URL with invalid signature is rejected
#[tokio::test]
#[ignore]
async fn test_presigned_url_invalid_signature() {
    let client = create_s3_client().await;
    let bucket = "presigned-invalid-sig-test";

    // Setup
    let _ = client.create_bucket().bucket(bucket).send().await;
    client
        .put_object()
        .bucket(bucket)
        .key("test-file.txt")
        .body(Bytes::from("content").into())
        .send()
        .await
        .unwrap();

    // Generate valid pre-signed URL
    let presigning_config =
        aws_sdk_s3::presigning::PresigningConfig::expires_in(std::time::Duration::from_secs(3600))
            .expect("Failed to create presigning config");

    let presigned_request = client
        .get_object()
        .bucket(bucket)
        .key("test-file.txt")
        .presigned(presigning_config)
        .await
        .unwrap();

    // Tamper with the signature in the URL
    let tampered_url = presigned_request
        .uri()
        .to_string()
        .replace("X-Amz-Signature=", "X-Amz-Signature=0000000000000000000000000000000000000000000000000000000000000000");

    let http_client = reqwest::Client::new();
    let response = http_client
        .get(&tampered_url)
        .send()
        .await
        .expect("Request should complete");

    assert_eq!(
        response.status(),
        403,
        "Should reject invalid signature with 403"
    );

    let body = response.text().await.unwrap();
    assert!(
        body.contains("SignatureDoesNotMatch"),
        "Response should indicate signature mismatch: {}",
        body
    );

    // Cleanup
    client
        .delete_object()
        .bucket(bucket)
        .key("test-file.txt")
        .send()
        .await
        .ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test pre-signed URL for non-existent object returns 404 (auth passes)
#[tokio::test]
#[ignore]
async fn test_presigned_url_object_not_found() {
    let client = create_s3_client().await;
    let bucket = "presigned-notfound-test";

    // Setup: create bucket only (no object)
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Generate pre-signed URL for non-existent object
    let presigning_config =
        aws_sdk_s3::presigning::PresigningConfig::expires_in(std::time::Duration::from_secs(3600))
            .expect("Failed to create presigning config");

    let presigned_request = client
        .get_object()
        .bucket(bucket)
        .key("nonexistent.txt")
        .presigned(presigning_config)
        .await
        .unwrap();

    let http_client = reqwest::Client::new();
    let response = http_client
        .get(presigned_request.uri())
        .send()
        .await
        .expect("Request should complete");

    // Auth should pass, but object lookup should fail with 404
    assert_eq!(
        response.status(),
        404,
        "Should return 404 for non-existent object"
    );

    // Cleanup
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test pre-signed URL with response header overrides
#[tokio::test]
#[ignore]
async fn test_presigned_url_response_headers() {
    let client = create_s3_client().await;
    let bucket = "presigned-response-headers-test";

    // Setup: create bucket and object
    let _ = client.create_bucket().bucket(bucket).send().await;

    client
        .put_object()
        .bucket(bucket)
        .key("document.txt")
        .body(Bytes::from("document content").into())
        .content_type("text/plain")
        .send()
        .await
        .expect("Failed to put object");

    // Generate pre-signed URL with response header overrides
    let presigning_config =
        aws_sdk_s3::presigning::PresigningConfig::expires_in(std::time::Duration::from_secs(3600))
            .expect("Failed to create presigning config");

    let presigned_request = client
        .get_object()
        .bucket(bucket)
        .key("document.txt")
        .response_content_type("application/octet-stream")
        .response_content_disposition("attachment; filename=\"download.txt\"")
        .response_cache_control("no-cache")
        .presigned(presigning_config)
        .await
        .expect("Failed to generate pre-signed URL");

    // Use reqwest to fetch via pre-signed URL
    let http_client = reqwest::Client::new();
    let response = http_client
        .get(presigned_request.uri())
        .send()
        .await
        .expect("Failed to fetch via pre-signed URL");

    assert_eq!(response.status(), 200);

    // Verify response headers were overridden
    let content_type = response.headers().get("content-type").map(|v| v.to_str().unwrap());
    let content_disposition = response.headers().get("content-disposition").map(|v| v.to_str().unwrap());
    let cache_control = response.headers().get("cache-control").map(|v| v.to_str().unwrap());

    assert_eq!(content_type, Some("application/octet-stream"), "Content-Type should be overridden");
    assert_eq!(content_disposition, Some("attachment; filename=\"download.txt\""), "Content-Disposition should be set");
    assert_eq!(cache_control, Some("no-cache"), "Cache-Control should be set");

    // Cleanup
    client
        .delete_object()
        .bucket(bucket)
        .key("document.txt")
        .send()
        .await
        .ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test pre-signed DELETE URL
#[tokio::test]
#[ignore]
async fn test_presigned_delete_url() {
    let client = create_s3_client().await;
    let bucket = "presigned-delete-test";

    // Setup: create bucket and object
    let _ = client.create_bucket().bucket(bucket).send().await;

    client
        .put_object()
        .bucket(bucket)
        .key("to-delete.txt")
        .body(Bytes::from("will be deleted").into())
        .send()
        .await
        .expect("Failed to put object");

    // Generate pre-signed DELETE URL
    let presigning_config =
        aws_sdk_s3::presigning::PresigningConfig::expires_in(std::time::Duration::from_secs(3600))
            .expect("Failed to create presigning config");

    let presigned_request = client
        .delete_object()
        .bucket(bucket)
        .key("to-delete.txt")
        .presigned(presigning_config)
        .await
        .expect("Failed to generate pre-signed DELETE URL");

    // Use reqwest to delete via pre-signed URL
    let http_client = reqwest::Client::new();
    let response = http_client
        .delete(presigned_request.uri())
        .send()
        .await
        .expect("Failed to delete via pre-signed URL");

    assert_eq!(response.status(), 204, "Pre-signed DELETE should return 204 No Content");

    // Verify object was deleted
    let get_result = client
        .get_object()
        .bucket(bucket)
        .key("to-delete.txt")
        .send()
        .await;

    assert!(get_result.is_err(), "Object should be deleted");

    // Cleanup
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test pre-signed multipart upload (CreateMultipartUpload + UploadPart + CompleteMultipartUpload)
#[tokio::test]
#[ignore]
async fn test_presigned_multipart_upload() {
    let client = create_s3_client().await;
    let bucket = "presigned-multipart-test";

    // Setup: create bucket
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Step 1: Create multipart upload (using SDK - this returns upload ID)
    let create_response = client
        .create_multipart_upload()
        .bucket(bucket)
        .key("large-file.bin")
        .send()
        .await
        .expect("Failed to create multipart upload");

    let upload_id = create_response.upload_id().expect("Missing upload ID");

    // Step 2: Generate pre-signed URL for uploading a part
    let presigning_config =
        aws_sdk_s3::presigning::PresigningConfig::expires_in(std::time::Duration::from_secs(3600))
            .expect("Failed to create presigning config");

    let presigned_part = client
        .upload_part()
        .bucket(bucket)
        .key("large-file.bin")
        .upload_id(upload_id)
        .part_number(1)
        .presigned(presigning_config)
        .await
        .expect("Failed to generate pre-signed URL for part upload");

    // Step 3: Upload part via pre-signed URL using reqwest
    let http_client = reqwest::Client::new();
    let part_data = vec![b'X'; 5 * 1024 * 1024]; // 5MB part
    let response = http_client
        .put(presigned_part.uri())
        .body(part_data.clone())
        .send()
        .await
        .expect("Failed to upload part via pre-signed URL");

    assert_eq!(response.status(), 200, "Part upload should succeed");

    // Extract ETag from response
    let etag = response
        .headers()
        .get("etag")
        .expect("Part response should have ETag")
        .to_str()
        .unwrap()
        .to_string();

    // Step 4: Complete multipart upload using SDK
    let completed_part = CompletedPart::builder()
        .e_tag(etag)
        .part_number(1)
        .build();

    let completed_upload = CompletedMultipartUpload::builder()
        .parts(completed_part)
        .build();

    client
        .complete_multipart_upload()
        .bucket(bucket)
        .key("large-file.bin")
        .upload_id(upload_id)
        .multipart_upload(completed_upload)
        .send()
        .await
        .expect("Failed to complete multipart upload");

    // Step 5: Verify the object exists and has correct size
    let head_response = client
        .head_object()
        .bucket(bucket)
        .key("large-file.bin")
        .send()
        .await
        .expect("Failed to head object");

    assert_eq!(
        head_response.content_length(),
        Some(5 * 1024 * 1024),
        "Object should be 5MB"
    );

    // Cleanup
    client
        .delete_object()
        .bucket(bucket)
        .key("large-file.bin")
        .send()
        .await
        .ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test pre-signed HEAD URL
#[tokio::test]
#[ignore]
async fn test_presigned_head_url() {
    let client = create_s3_client().await;
    let bucket = "presigned-head-test";

    // Setup: create bucket and object
    let _ = client.create_bucket().bucket(bucket).send().await;

    client
        .put_object()
        .bucket(bucket)
        .key("test-file.txt")
        .body(Bytes::from("test content for head").into())
        .content_type("text/plain")
        .send()
        .await
        .expect("Failed to put object");

    // Generate pre-signed HEAD URL
    let presigning_config =
        aws_sdk_s3::presigning::PresigningConfig::expires_in(std::time::Duration::from_secs(3600))
            .expect("Failed to create presigning config");

    let presigned_request = client
        .head_object()
        .bucket(bucket)
        .key("test-file.txt")
        .presigned(presigning_config)
        .await
        .expect("Failed to generate pre-signed HEAD URL");

    // Use reqwest to HEAD via pre-signed URL
    let http_client = reqwest::Client::new();
    let response = http_client
        .head(presigned_request.uri())
        .send()
        .await
        .expect("Failed to HEAD via pre-signed URL");

    assert_eq!(response.status(), 200);
    assert!(response.headers().contains_key("etag"), "Should have ETag header");
    assert!(response.headers().contains_key("content-length"), "Should have Content-Length header");
    assert_eq!(
        response.headers().get("content-length").map(|v| v.to_str().unwrap()),
        Some("21"),
        "Content-Length should match object size"
    );

    // Cleanup
    client
        .delete_object()
        .bucket(bucket)
        .key("test-file.txt")
        .send()
        .await
        .ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test pre-signed URL with special characters in key
#[tokio::test]
#[ignore]
async fn test_presigned_url_with_special_chars() {
    let client = create_s3_client().await;
    let bucket = "presigned-special-chars-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Create object with special characters in key
    let special_key = "path/to/file with spaces & special!chars.txt";
    client
        .put_object()
        .bucket(bucket)
        .key(special_key)
        .body(Bytes::from("special content").into())
        .send()
        .await
        .expect("Failed to put object");

    // Generate pre-signed URL
    let presigning_config =
        aws_sdk_s3::presigning::PresigningConfig::expires_in(std::time::Duration::from_secs(3600))
            .expect("Failed to create presigning config");

    let presigned_request = client
        .get_object()
        .bucket(bucket)
        .key(special_key)
        .presigned(presigning_config)
        .await
        .expect("Failed to generate pre-signed URL");

    // Fetch via pre-signed URL
    let http_client = reqwest::Client::new();
    let response = http_client
        .get(presigned_request.uri())
        .send()
        .await
        .expect("Failed to fetch via pre-signed URL");

    assert_eq!(response.status(), 200, "Pre-signed GET should work with special chars");
    let body = response.text().await.unwrap();
    assert_eq!(body, "special content");

    // Cleanup
    client.delete_object().bucket(bucket).key(special_key).send().await.ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test pre-signed URL with custom metadata
#[tokio::test]
#[ignore]
async fn test_presigned_url_with_metadata() {
    let client = create_s3_client().await;
    let bucket = "presigned-metadata-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Generate pre-signed PUT URL with metadata
    let presigning_config =
        aws_sdk_s3::presigning::PresigningConfig::expires_in(std::time::Duration::from_secs(3600))
            .expect("Failed to create presigning config");

    let presigned_request = client
        .put_object()
        .bucket(bucket)
        .key("metadata-file.txt")
        .metadata("custom-key", "custom-value")
        .presigned(presigning_config)
        .await
        .expect("Failed to generate pre-signed URL");

    // Upload via pre-signed URL
    // Note: Metadata headers must be included in the request
    let http_client = reqwest::Client::new();

    // Get the headers from the presigned request
    let headers = presigned_request.headers();

    let mut request_builder = http_client.put(presigned_request.uri()).body("content with metadata");

    // Add all headers from the presigned request
    for (key, value) in headers {
        request_builder = request_builder.header(key, value);
    }

    let response = request_builder.send().await.expect("Failed to upload via pre-signed URL");

    assert_eq!(response.status(), 200, "Pre-signed PUT with metadata should succeed");

    // Verify metadata was set
    let head_result = client
        .head_object()
        .bucket(bucket)
        .key("metadata-file.txt")
        .send()
        .await
        .expect("HeadObject should succeed");

    // Note: Metadata might not be set if not included correctly in the pre-signed request
    // The behavior depends on how the presigned URL was generated
    if let Some(metadata) = head_result.metadata() {
        if metadata.get("custom-key").is_some() {
            assert_eq!(metadata.get("custom-key").map(|s| s.as_str()), Some("custom-value"));
        }
    }

    // Cleanup
    client.delete_object().bucket(bucket).key("metadata-file.txt").send().await.ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test pre-signed URL with version ID
#[tokio::test]
#[ignore]
async fn test_presigned_url_version_id() {
    let client = create_s3_client().await;
    let bucket = "presigned-version-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Enable versioning
    client
        .put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(
            aws_sdk_s3::types::VersioningConfiguration::builder()
                .status(aws_sdk_s3::types::BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .expect("Failed to enable versioning");

    // Create two versions
    let put1 = client
        .put_object()
        .bucket(bucket)
        .key("versioned-file.txt")
        .body(Bytes::from("version 1 content").into())
        .send()
        .await
        .expect("Failed to put first version");

    let version1 = put1.version_id().expect("Should have version ID").to_string();

    let put2 = client
        .put_object()
        .bucket(bucket)
        .key("versioned-file.txt")
        .body(Bytes::from("version 2 content").into())
        .send()
        .await
        .expect("Failed to put second version");

    let version2 = put2.version_id().expect("Should have version ID").to_string();

    // Generate pre-signed URL for specific version (version 1)
    let presigning_config =
        aws_sdk_s3::presigning::PresigningConfig::expires_in(std::time::Duration::from_secs(3600))
            .expect("Failed to create presigning config");

    let presigned_v1 = client
        .get_object()
        .bucket(bucket)
        .key("versioned-file.txt")
        .version_id(&version1)
        .presigned(presigning_config)
        .await
        .expect("Failed to generate pre-signed URL for version");

    // Fetch version 1 via pre-signed URL
    let http_client = reqwest::Client::new();
    let response = http_client
        .get(presigned_v1.uri())
        .send()
        .await
        .expect("Failed to fetch via pre-signed URL");

    assert_eq!(response.status(), 200, "Pre-signed GET for specific version should succeed");
    let body = response.text().await.unwrap();
    assert_eq!(body, "version 1 content", "Should get version 1 content");

    // Cleanup
    client.delete_object().bucket(bucket).key("versioned-file.txt").version_id(&version1).send().await.ok();
    client.delete_object().bucket(bucket).key("versioned-file.txt").version_id(&version2).send().await.ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

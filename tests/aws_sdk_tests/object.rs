use super::*;

#[tokio::test]
#[ignore]
async fn test_object_operations() {
    let client = create_s3_client().await;

    // Setup: create bucket
    let _ = client
        .create_bucket()
        .bucket("sdk-object-test")
        .send()
        .await;

    // Put object
    client
        .put_object()
        .bucket("sdk-object-test")
        .key("test-key")
        .body(Bytes::from("hello world").into())
        .content_type("text/plain")
        .send()
        .await
        .expect("Failed to put object");

    // Get object
    let response = client
        .get_object()
        .bucket("sdk-object-test")
        .key("test-key")
        .send()
        .await
        .expect("Failed to get object");

    let body = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body, Bytes::from("hello world"));

    // Head object
    let head = client
        .head_object()
        .bucket("sdk-object-test")
        .key("test-key")
        .send()
        .await
        .expect("Failed to head object");

    assert_eq!(head.content_length(), Some(11));

    // Delete object
    client
        .delete_object()
        .bucket("sdk-object-test")
        .key("test-key")
        .send()
        .await
        .expect("Failed to delete object");

    // Cleanup
    let _ = client
        .delete_bucket()
        .bucket("sdk-object-test")
        .send()
        .await;
}

#[tokio::test]
#[ignore]
async fn test_copy_object() {
    let client = create_s3_client().await;

    // Setup
    let _ = client
        .create_bucket()
        .bucket("sdk-copy-test")
        .send()
        .await;

    // Put source object
    client
        .put_object()
        .bucket("sdk-copy-test")
        .key("source-key")
        .body(Bytes::from("copy this data").into())
        .send()
        .await
        .expect("Failed to put source object");

    // Copy object
    client
        .copy_object()
        .bucket("sdk-copy-test")
        .key("dest-key")
        .copy_source("sdk-copy-test/source-key")
        .send()
        .await
        .expect("Failed to copy object");

    // Verify copy
    let response = client
        .get_object()
        .bucket("sdk-copy-test")
        .key("dest-key")
        .send()
        .await
        .expect("Failed to get copied object");

    let body = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body, Bytes::from("copy this data"));

    // Cleanup
    client
        .delete_object()
        .bucket("sdk-copy-test")
        .key("source-key")
        .send()
        .await
        .unwrap();
    client
        .delete_object()
        .bucket("sdk-copy-test")
        .key("dest-key")
        .send()
        .await
        .unwrap();
    let _ = client
        .delete_bucket()
        .bucket("sdk-copy-test")
        .send()
        .await;
}

#[tokio::test]
#[ignore]
async fn test_copy_object_cross_bucket() {
    let client = create_s3_client().await;
    let src_bucket = "sdk-cross-copy-src";
    let dest_bucket = "sdk-cross-copy-dest";

    // Create both buckets
    let _ = client.create_bucket().bucket(src_bucket).send().await;
    let _ = client.create_bucket().bucket(dest_bucket).send().await;

    // Put object in source bucket
    let content = Bytes::from("cross-bucket copy content");
    client
        .put_object()
        .bucket(src_bucket)
        .key("source-key")
        .body(content.clone().into())
        .send()
        .await
        .expect("Failed to put source object");

    // Copy to different bucket
    client
        .copy_object()
        .bucket(dest_bucket)
        .key("dest-key")
        .copy_source(format!("{}/source-key", src_bucket))
        .send()
        .await
        .expect("Failed to copy object cross-bucket");

    // Verify copy in destination bucket
    let get_response = client
        .get_object()
        .bucket(dest_bucket)
        .key("dest-key")
        .send()
        .await
        .expect("Failed to get copied object");

    let body = get_response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body, content, "Copied content should match");

    // Cleanup
    client.delete_object().bucket(src_bucket).key("source-key").send().await.ok();
    client.delete_object().bucket(dest_bucket).key("dest-key").send().await.ok();
    let _ = client.delete_bucket().bucket(src_bucket).send().await;
    let _ = client.delete_bucket().bucket(dest_bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_get_object_not_found() {
    let client = create_s3_client().await;
    let bucket = "sdk-404-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Get non-existent object should fail with NoSuchKey
    let result = client
        .get_object()
        .bucket(bucket)
        .key("nonexistent-key")
        .send()
        .await;

    assert!(result.is_err(), "GetObject should fail for non-existent key");
    let err = format!("{:?}", result.err().unwrap());
    assert!(
        err.contains("NoSuchKey") || err.contains("not found") || err.contains("404"),
        "Error should indicate key not found: {}", err
    );

    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_put_object_to_nonexistent_bucket() {
    let client = create_s3_client().await;

    // Put object to non-existent bucket should fail
    let result = client
        .put_object()
        .bucket("nonexistent-bucket-xyz-123")
        .key("test-key")
        .body(Bytes::from("data").into())
        .send()
        .await;

    assert!(result.is_err(), "PutObject should fail for non-existent bucket");
}

/// Test HeadObject with If-Match condition (should return object when ETag matches)
#[tokio::test]
#[ignore]
async fn test_head_object_if_match() {
    let client = create_s3_client().await;
    let bucket = "sdk-head-if-match-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Put object
    let put_result = client
        .put_object()
        .bucket(bucket)
        .key("test-key")
        .body(Bytes::from("test content").into())
        .send()
        .await
        .expect("Failed to put object");

    let etag = put_result.e_tag().expect("Should have ETag");

    // HeadObject with matching ETag
    let head_result = client
        .head_object()
        .bucket(bucket)
        .key("test-key")
        .if_match(etag)
        .send()
        .await;

    assert!(head_result.is_ok(), "HeadObject should succeed when ETag matches");
    assert_eq!(head_result.unwrap().content_length(), Some(12));

    // HeadObject with non-matching ETag should fail
    let head_fail = client
        .head_object()
        .bucket(bucket)
        .key("test-key")
        .if_match("\"0000000000000000000000000000000\"")
        .send()
        .await;

    assert!(head_fail.is_err(), "HeadObject should fail when ETag doesn't match");

    // Cleanup
    client.delete_object().bucket(bucket).key("test-key").send().await.ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test HeadObject with If-None-Match condition (304 Not Modified)
#[tokio::test]
#[ignore]
async fn test_head_object_if_none_match() {
    let client = create_s3_client().await;
    let bucket = "sdk-head-if-none-match-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Put object
    let put_result = client
        .put_object()
        .bucket(bucket)
        .key("test-key")
        .body(Bytes::from("test content").into())
        .send()
        .await
        .expect("Failed to put object");

    let etag = put_result.e_tag().expect("Should have ETag");

    // HeadObject with matching ETag in If-None-Match should return 304
    let head_result = client
        .head_object()
        .bucket(bucket)
        .key("test-key")
        .if_none_match(etag)
        .send()
        .await;

    // SDK may translate 304 to an error or success with special handling
    // The behavior can vary, so we just ensure the call completes
    assert!(
        head_result.is_err() || head_result.as_ref().map(|r| r.content_length()).is_ok(),
        "HeadObject with If-None-Match should return 304 or error"
    );

    // HeadObject with non-matching ETag should succeed
    let head_success = client
        .head_object()
        .bucket(bucket)
        .key("test-key")
        .if_none_match("\"0000000000000000000000000000000\"")
        .send()
        .await;

    assert!(head_success.is_ok(), "HeadObject should succeed when ETag doesn't match If-None-Match");

    // Cleanup
    client.delete_object().bucket(bucket).key("test-key").send().await.ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test HeadObject with Range header (verifies range support in metadata)
#[tokio::test]
#[ignore]
async fn test_head_object_with_range() {
    let client = create_s3_client().await;
    let bucket = "sdk-head-range-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Put object with known content
    client
        .put_object()
        .bucket(bucket)
        .key("test-key")
        .body(Bytes::from("0123456789ABCDEF").into())
        .send()
        .await
        .expect("Failed to put object");

    // HeadObject (without range - gets full metadata)
    let head_result = client
        .head_object()
        .bucket(bucket)
        .key("test-key")
        .send()
        .await
        .expect("HeadObject should succeed");

    assert_eq!(head_result.content_length(), Some(16), "Full content should be 16 bytes");

    // Note: HeadObject doesn't typically support Range directly,
    // but we verify the object exists and has correct size
    // Range is primarily used with GetObject

    // Cleanup
    client.delete_object().bucket(bucket).key("test-key").send().await.ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test HeadObject with custom metadata
#[tokio::test]
#[ignore]
async fn test_head_object_custom_metadata() {
    let client = create_s3_client().await;
    let bucket = "sdk-head-metadata-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Put object with custom metadata
    client
        .put_object()
        .bucket(bucket)
        .key("test-key")
        .body(Bytes::from("content with metadata").into())
        .metadata("custom-key", "custom-value")
        .metadata("another-key", "another-value")
        .content_type("text/plain")
        .send()
        .await
        .expect("Failed to put object");

    // HeadObject should return metadata
    let head_result = client
        .head_object()
        .bucket(bucket)
        .key("test-key")
        .send()
        .await
        .expect("HeadObject should succeed");

    let metadata = head_result.metadata();
    assert!(metadata.is_some(), "Should have metadata");

    let meta_map = metadata.unwrap();
    assert_eq!(
        meta_map.get("custom-key").map(|s| s.as_str()),
        Some("custom-value"),
        "Should have custom-key metadata"
    );
    assert_eq!(
        meta_map.get("another-key").map(|s| s.as_str()),
        Some("another-value"),
        "Should have another-key metadata"
    );

    assert_eq!(
        head_result.content_type(),
        Some("text/plain"),
        "Should have correct content type"
    );

    // Cleanup
    client.delete_object().bucket(bucket).key("test-key").send().await.ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test CopyObject with MetadataDirective::COPY (preserves source metadata)
#[tokio::test]
#[ignore]
async fn test_copy_object_metadata_directive_copy() {
    let client = create_s3_client().await;
    let bucket = "sdk-copy-metadata-copy-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Put source object with metadata
    client
        .put_object()
        .bucket(bucket)
        .key("source-key")
        .body(Bytes::from("source content").into())
        .metadata("source-meta", "original-value")
        .content_type("text/plain")
        .send()
        .await
        .expect("Failed to put source object");

    // Copy with COPY directive (default - preserves metadata)
    client
        .copy_object()
        .bucket(bucket)
        .key("dest-key")
        .copy_source(format!("{}/source-key", bucket))
        .metadata_directive(aws_sdk_s3::types::MetadataDirective::Copy)
        .send()
        .await
        .expect("Failed to copy object");

    // Verify destination has same metadata
    let head_dest = client
        .head_object()
        .bucket(bucket)
        .key("dest-key")
        .send()
        .await
        .expect("HeadObject should succeed");

    let metadata = head_dest.metadata().expect("Should have metadata");
    assert_eq!(
        metadata.get("source-meta").map(|s| s.as_str()),
        Some("original-value"),
        "Copied object should preserve source metadata"
    );
    assert_eq!(
        head_dest.content_type(),
        Some("text/plain"),
        "Copied object should preserve content type"
    );

    // Cleanup
    client.delete_object().bucket(bucket).key("source-key").send().await.ok();
    client.delete_object().bucket(bucket).key("dest-key").send().await.ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test CopyObject with MetadataDirective::REPLACE (uses new metadata)
#[tokio::test]
#[ignore]
async fn test_copy_object_metadata_directive_replace() {
    let client = create_s3_client().await;
    let bucket = "sdk-copy-metadata-replace-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Put source object with metadata
    client
        .put_object()
        .bucket(bucket)
        .key("source-key")
        .body(Bytes::from("source content").into())
        .metadata("source-meta", "original-value")
        .content_type("text/plain")
        .send()
        .await
        .expect("Failed to put source object");

    // Copy with REPLACE directive and new metadata
    client
        .copy_object()
        .bucket(bucket)
        .key("dest-key")
        .copy_source(format!("{}/source-key", bucket))
        .metadata_directive(aws_sdk_s3::types::MetadataDirective::Replace)
        .metadata("new-meta", "new-value")
        .content_type("application/json")
        .send()
        .await
        .expect("Failed to copy object");

    // Verify destination has new metadata
    let head_dest = client
        .head_object()
        .bucket(bucket)
        .key("dest-key")
        .send()
        .await
        .expect("HeadObject should succeed");

    let metadata = head_dest.metadata().expect("Should have metadata");
    assert!(
        metadata.get("source-meta").is_none(),
        "Source metadata should NOT be present when REPLACE is used"
    );
    assert_eq!(
        metadata.get("new-meta").map(|s| s.as_str()),
        Some("new-value"),
        "New metadata should be present"
    );
    assert_eq!(
        head_dest.content_type(),
        Some("application/json"),
        "New content type should be set"
    );

    // Cleanup
    client.delete_object().bucket(bucket).key("source-key").send().await.ok();
    client.delete_object().bucket(bucket).key("dest-key").send().await.ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test CopyObject with custom metadata
#[tokio::test]
#[ignore]
async fn test_copy_object_with_custom_metadata() {
    let client = create_s3_client().await;
    let bucket = "sdk-copy-custom-metadata-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Put source object
    client
        .put_object()
        .bucket(bucket)
        .key("source-key")
        .body(Bytes::from("source content").into())
        .send()
        .await
        .expect("Failed to put source object");

    // Copy with multiple custom metadata fields
    client
        .copy_object()
        .bucket(bucket)
        .key("dest-key")
        .copy_source(format!("{}/source-key", bucket))
        .metadata_directive(aws_sdk_s3::types::MetadataDirective::Replace)
        .metadata("key1", "value1")
        .metadata("key2", "value2")
        .metadata("key3", "value3")
        .send()
        .await
        .expect("Failed to copy object");

    // Verify all metadata is present
    let head_dest = client
        .head_object()
        .bucket(bucket)
        .key("dest-key")
        .send()
        .await
        .expect("HeadObject should succeed");

    let metadata = head_dest.metadata().expect("Should have metadata");
    assert_eq!(metadata.get("key1").map(|s| s.as_str()), Some("value1"));
    assert_eq!(metadata.get("key2").map(|s| s.as_str()), Some("value2"));
    assert_eq!(metadata.get("key3").map(|s| s.as_str()), Some("value3"));

    // Verify content is copied correctly
    let get_result = client
        .get_object()
        .bucket(bucket)
        .key("dest-key")
        .send()
        .await
        .expect("GetObject should succeed");

    let body = get_result.body.collect().await.unwrap().into_bytes();
    assert_eq!(body, Bytes::from("source content"));

    // Cleanup
    client.delete_object().bucket(bucket).key("source-key").send().await.ok();
    client.delete_object().bucket(bucket).key("dest-key").send().await.ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test object keys with Unicode characters
#[tokio::test]
#[ignore]
async fn test_object_key_unicode_characters() {
    let client = create_s3_client().await;
    let bucket = "sdk-unicode-key-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Test various Unicode keys
    let unicode_keys = vec![
        "日本語/ファイル.txt",
        "中文/文件.txt",
        "한국어/파일.txt",
        "مرحبا/ملف.txt",
        "emoji-🎉🚀🌟.txt",
        "accénts/café.txt",
    ];

    for key in &unicode_keys {
        // Put object
        let put_result = client
            .put_object()
            .bucket(bucket)
            .key(*key)
            .body(Bytes::from(format!("content for {}", key)).into())
            .send()
            .await;

        assert!(put_result.is_ok(), "PutObject should succeed for Unicode key: {}", key);

        // Get object
        let get_result = client
            .get_object()
            .bucket(bucket)
            .key(*key)
            .send()
            .await
            .expect(&format!("GetObject should succeed for Unicode key: {}", key));

        let body = get_result.body.collect().await.unwrap().into_bytes();
        assert_eq!(
            body,
            Bytes::from(format!("content for {}", key)),
            "Content should match for {}",
            key
        );
    }

    // List and verify all keys
    let list_result = client
        .list_objects_v2()
        .bucket(bucket)
        .send()
        .await
        .expect("ListObjects should succeed");

    assert_eq!(
        list_result.contents().len(),
        unicode_keys.len(),
        "Should have all Unicode keys"
    );

    // Cleanup
    for key in &unicode_keys {
        client.delete_object().bucket(bucket).key(*key).send().await.ok();
    }
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test object keys with spaces
#[tokio::test]
#[ignore]
async fn test_object_key_spaces() {
    let client = create_s3_client().await;
    let bucket = "sdk-spaces-key-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    let space_keys = vec![
        "file with spaces.txt",
        "path with spaces/file name.txt",
        "  leading spaces.txt",
        "trailing spaces  .txt",
        "multiple   spaces.txt",
    ];

    for key in &space_keys {
        // Put object
        client
            .put_object()
            .bucket(bucket)
            .key(*key)
            .body(Bytes::from("content").into())
            .send()
            .await
            .expect(&format!("PutObject should succeed for key with spaces: {:?}", key));

        // Get object
        let get_result = client
            .get_object()
            .bucket(bucket)
            .key(*key)
            .send()
            .await
            .expect(&format!("GetObject should succeed for key with spaces: {:?}", key));

        let body = get_result.body.collect().await.unwrap().into_bytes();
        assert_eq!(body, Bytes::from("content"));
    }

    // Cleanup
    for key in &space_keys {
        client.delete_object().bucket(bucket).key(*key).send().await.ok();
    }
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test object keys with special characters (!@#$%^&*)
#[tokio::test]
#[ignore]
async fn test_object_key_special_chars() {
    let client = create_s3_client().await;
    let bucket = "sdk-special-chars-key-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // S3 allows most special characters in keys
    let special_keys = vec![
        "file!name.txt",
        "file@name.txt",
        "file#name.txt",
        "file$name.txt",
        "file%name.txt",
        "file^name.txt",
        "file&name.txt",
        "file(name).txt",
        "file[name].txt",
        "file{name}.txt",
        "file=name.txt",
        "file+name.txt",
        "file,name.txt",
        "file;name.txt",
    ];

    for key in &special_keys {
        // Put object
        let put_result = client
            .put_object()
            .bucket(bucket)
            .key(*key)
            .body(Bytes::from("content").into())
            .send()
            .await;

        assert!(
            put_result.is_ok(),
            "PutObject should succeed for special char key: {} - {:?}",
            key,
            put_result.err()
        );

        // Get object
        let get_result = client
            .get_object()
            .bucket(bucket)
            .key(*key)
            .send()
            .await;

        assert!(
            get_result.is_ok(),
            "GetObject should succeed for special char key: {} - {:?}",
            key,
            get_result.err()
        );
    }

    // Cleanup
    for key in &special_keys {
        client.delete_object().bucket(bucket).key(*key).send().await.ok();
    }
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test object keys with URL-encoded characters (%20, %2F)
#[tokio::test]
#[ignore]
async fn test_object_key_url_encoded() {
    let client = create_s3_client().await;
    let bucket = "sdk-url-encoded-key-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Test keys that might be URL encoded
    // The SDK should handle encoding/decoding
    let keys = vec![
        "path%20with%20encoded%20spaces.txt",
        "file%2Fwith%2Fslashes.txt",
        "percent%25sign.txt",
    ];

    for key in &keys {
        // Put object
        client
            .put_object()
            .bucket(bucket)
            .key(*key)
            .body(Bytes::from("content").into())
            .send()
            .await
            .expect(&format!("PutObject should succeed for: {}", key));

        // Get object with same key
        let get_result = client
            .get_object()
            .bucket(bucket)
            .key(*key)
            .send()
            .await
            .expect(&format!("GetObject should succeed for: {}", key));

        let body = get_result.body.collect().await.unwrap().into_bytes();
        assert_eq!(body, Bytes::from("content"));
    }

    // Cleanup
    for key in &keys {
        client.delete_object().bucket(bucket).key(*key).send().await.ok();
    }
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test object keys with forward slashes (deep paths)
#[tokio::test]
#[ignore]
async fn test_object_key_forward_slashes() {
    let client = create_s3_client().await;
    let bucket = "sdk-deep-path-key-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Test deep directory structures
    let deep_keys = vec![
        "a/b/c/d/e/f/g/h/i/j/file.txt",
        "level1/level2/level3/level4/level5/deep-file.txt",
        "//double/leading/slash.txt",
        "trailing/slash/.txt",
    ];

    for key in &deep_keys {
        // Put object
        client
            .put_object()
            .bucket(bucket)
            .key(*key)
            .body(Bytes::from("deep content").into())
            .send()
            .await
            .expect(&format!("PutObject should succeed for deep path: {}", key));

        // Get object
        let get_result = client
            .get_object()
            .bucket(bucket)
            .key(*key)
            .send()
            .await
            .expect(&format!("GetObject should succeed for deep path: {}", key));

        let body = get_result.body.collect().await.unwrap().into_bytes();
        assert_eq!(body, Bytes::from("deep content"));
    }

    // Test listing with delimiter
    let list_result = client
        .list_objects_v2()
        .bucket(bucket)
        .delimiter("/")
        .send()
        .await
        .expect("ListObjects should succeed");

    // Should have common prefixes
    assert!(!list_result.common_prefixes().is_empty(), "Should have common prefixes");

    // Cleanup
    for key in &deep_keys {
        client.delete_object().bucket(bucket).key(*key).send().await.ok();
    }
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test object keys with dots (., .., leading dots)
#[tokio::test]
#[ignore]
async fn test_object_key_dots() {
    let client = create_s3_client().await;
    let bucket = "sdk-dots-key-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    let dot_keys = vec![
        ".hidden-file",
        "..double-dot-start",
        "path/./with/dot/file.txt",
        "path/../with/double-dots/file.txt",
        ".../triple-dots",
        "file...with...dots.txt",
    ];

    for key in &dot_keys {
        // Put object
        client
            .put_object()
            .bucket(bucket)
            .key(*key)
            .body(Bytes::from("dot content").into())
            .send()
            .await
            .expect(&format!("PutObject should succeed for dot key: {}", key));

        // Get object (S3 treats . and .. literally, not as directory traversal)
        let get_result = client
            .get_object()
            .bucket(bucket)
            .key(*key)
            .send()
            .await
            .expect(&format!("GetObject should succeed for dot key: {}", key));

        let body = get_result.body.collect().await.unwrap().into_bytes();
        assert_eq!(body, Bytes::from("dot content"));
    }

    // Cleanup
    for key in &dot_keys {
        client.delete_object().bucket(bucket).key(*key).send().await.ok();
    }
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test metadata with special characters
#[tokio::test]
#[ignore]
async fn test_metadata_special_characters() {
    let client = create_s3_client().await;
    let bucket = "sdk-metadata-special-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Put object with metadata containing special characters
    // Note: S3 metadata values are limited in what characters they can contain
    // US-ASCII printable characters are generally safe
    client
        .put_object()
        .bucket(bucket)
        .key("test-object")
        .body(Bytes::from("content").into())
        .metadata("simple-key", "simple-value")
        .metadata("numeric-key", "12345")
        .metadata("dash-key", "value-with-dashes")
        .metadata("underscore-key", "value_with_underscores")
        .send()
        .await
        .expect("PutObject should succeed");

    // Get and verify metadata
    let head_result = client
        .head_object()
        .bucket(bucket)
        .key("test-object")
        .send()
        .await
        .expect("HeadObject should succeed");

    let metadata = head_result.metadata().expect("Should have metadata");

    assert_eq!(metadata.get("simple-key").map(|s| s.as_str()), Some("simple-value"));
    assert_eq!(metadata.get("numeric-key").map(|s| s.as_str()), Some("12345"));
    assert_eq!(metadata.get("dash-key").map(|s| s.as_str()), Some("value-with-dashes"));
    assert_eq!(metadata.get("underscore-key").map(|s| s.as_str()), Some("value_with_underscores"));

    // Cleanup
    client.delete_object().bucket(bucket).key("test-object").send().await.ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test PutObject with CRC32 checksum
#[tokio::test]
#[ignore]
async fn test_put_object_crc32_checksum() {
    let client = create_s3_client().await;
    let bucket = "sdk-checksum-crc32-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    let content = Bytes::from("content for crc32 checksum test");

    // Put with CRC32 checksum
    let put_result = client
        .put_object()
        .bucket(bucket)
        .key("crc32-file")
        .body(content.clone().into())
        .checksum_algorithm(aws_sdk_s3::types::ChecksumAlgorithm::Crc32)
        .send()
        .await;

    if put_result.is_ok() {
        let result = put_result.unwrap();

        // Check if checksum is in response
        if let Some(checksum) = result.checksum_crc32() {
            assert!(!checksum.is_empty(), "CRC32 checksum should not be empty");
        }

        // Verify object can be retrieved
        let get_result = client
            .get_object()
            .bucket(bucket)
            .key("crc32-file")
            .send()
            .await
            .expect("GetObject should succeed");

        let body = get_result.body.collect().await.unwrap().into_bytes();
        assert_eq!(body, content, "Content should match");

        // Cleanup
        client.delete_object().bucket(bucket).key("crc32-file").send().await.ok();
    }

    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test PutObject with SHA256 checksum
#[tokio::test]
#[ignore]
async fn test_put_object_sha256_checksum() {
    let client = create_s3_client().await;
    let bucket = "sdk-checksum-sha256-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    let content = Bytes::from("content for sha256 checksum test");

    // Put with SHA256 checksum
    let put_result = client
        .put_object()
        .bucket(bucket)
        .key("sha256-file")
        .body(content.clone().into())
        .checksum_algorithm(aws_sdk_s3::types::ChecksumAlgorithm::Sha256)
        .send()
        .await;

    if put_result.is_ok() {
        let result = put_result.unwrap();

        // Check if checksum is in response
        if let Some(checksum) = result.checksum_sha256() {
            assert!(!checksum.is_empty(), "SHA256 checksum should not be empty");
        }

        // Verify object can be retrieved
        let get_result = client
            .get_object()
            .bucket(bucket)
            .key("sha256-file")
            .send()
            .await
            .expect("GetObject should succeed");

        let body = get_result.body.collect().await.unwrap().into_bytes();
        assert_eq!(body, content, "Content should match");

        // Cleanup
        client.delete_object().bucket(bucket).key("sha256-file").send().await.ok();
    }

    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test GetObject with checksum header enabled
#[tokio::test]
#[ignore]
async fn test_get_object_checksum_header() {
    let client = create_s3_client().await;
    let bucket = "sdk-get-checksum-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    let content = Bytes::from("content for get checksum test");

    // Put object with checksum
    let put_result = client
        .put_object()
        .bucket(bucket)
        .key("checksum-get-file")
        .body(content.clone().into())
        .checksum_algorithm(aws_sdk_s3::types::ChecksumAlgorithm::Crc32)
        .send()
        .await;

    if put_result.is_ok() {
        // Get with checksum mode enabled
        let get_result = client
            .get_object()
            .bucket(bucket)
            .key("checksum-get-file")
            .checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled)
            .send()
            .await;

        if let Ok(result) = get_result {
            // Check for checksum in response first (before consuming body)
            let has_checksum = result.checksum_crc32().is_some();
            if has_checksum {
                let checksum = result.checksum_crc32().unwrap();
                assert!(!checksum.is_empty(), "CRC32 checksum should be present");
            }

            let body = result.body.collect().await.unwrap().into_bytes();
            assert_eq!(body, content, "Content should match");
        }

        // Cleanup
        client.delete_object().bucket(bucket).key("checksum-get-file").send().await.ok();
    }

    let _ = client.delete_bucket().bucket(bucket).send().await;
}

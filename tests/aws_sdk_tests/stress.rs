use super::*;

/// Test large object upload (100MB)
#[tokio::test]
#[ignore]
async fn test_large_object_100mb() {
    let client = create_s3_client().await;
    let bucket = "sdk-stress-large-object-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Create 100MB of data (pattern: repeating bytes 0-255)
    const SIZE: usize = 100 * 1024 * 1024; // 100MB
    let data: Vec<u8> = (0..SIZE).map(|i| (i % 256) as u8).collect();
    let data_bytes = Bytes::from(data.clone());

    // Upload large object
    let put_result = client
        .put_object()
        .bucket(bucket)
        .key("large-100mb-object")
        .body(data_bytes.clone().into())
        .content_type("application/octet-stream")
        .send()
        .await;

    assert!(put_result.is_ok(), "PutObject 100MB should succeed: {:?}", put_result.err());

    // Verify size via HeadObject
    let head_result = client
        .head_object()
        .bucket(bucket)
        .key("large-100mb-object")
        .send()
        .await
        .expect("HeadObject should succeed");

    assert_eq!(
        head_result.content_length(),
        Some(SIZE as i64),
        "Object size should be 100MB"
    );

    // Download and verify content matches
    let get_result = client
        .get_object()
        .bucket(bucket)
        .key("large-100mb-object")
        .send()
        .await
        .expect("GetObject should succeed");

    let downloaded = get_result.body.collect().await.unwrap().into_bytes();
    assert_eq!(downloaded.len(), SIZE, "Downloaded size should match");

    // Verify first and last chunks match
    assert_eq!(&downloaded[0..1000], &data[0..1000], "First chunk should match");
    assert_eq!(&downloaded[SIZE-1000..SIZE], &data[SIZE-1000..SIZE], "Last chunk should match");

    // Cleanup
    client.delete_object().bucket(bucket).key("large-100mb-object").send().await.ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test multipart upload with large parts (50MB parts)
#[tokio::test]
#[ignore]
async fn test_multipart_large_parts() {
    let client = create_s3_client().await;
    let bucket = "sdk-stress-large-parts-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Create multipart upload
    let create_result = client
        .create_multipart_upload()
        .bucket(bucket)
        .key("large-multipart-file")
        .send()
        .await
        .expect("CreateMultipartUpload should succeed");

    let upload_id = create_result.upload_id().unwrap();

    const PART_SIZE: usize = 50 * 1024 * 1024; // 50MB per part
    const NUM_PARTS: i32 = 3;

    let mut completed_parts = Vec::new();

    for part_num in 1..=NUM_PARTS {
        // Create part data (each part has different pattern)
        let data: Vec<u8> = (0..PART_SIZE).map(|i| ((i + part_num as usize) % 256) as u8).collect();

        let part_result = client
            .upload_part()
            .bucket(bucket)
            .key("large-multipart-file")
            .upload_id(upload_id)
            .part_number(part_num)
            .body(Bytes::from(data).into())
            .send()
            .await
            .expect(&format!("UploadPart {} should succeed", part_num));

        completed_parts.push(
            CompletedPart::builder()
                .part_number(part_num)
                .e_tag(part_result.e_tag().unwrap())
                .build(),
        );
    }

    // Complete the upload
    let completed_upload = CompletedMultipartUpload::builder()
        .set_parts(Some(completed_parts))
        .build();

    client
        .complete_multipart_upload()
        .bucket(bucket)
        .key("large-multipart-file")
        .upload_id(upload_id)
        .multipart_upload(completed_upload)
        .send()
        .await
        .expect("CompleteMultipartUpload should succeed");

    // Verify total size
    let expected_size = (PART_SIZE * NUM_PARTS as usize) as i64;
    let head_result = client
        .head_object()
        .bucket(bucket)
        .key("large-multipart-file")
        .send()
        .await
        .expect("HeadObject should succeed");

    assert_eq!(
        head_result.content_length(),
        Some(expected_size),
        "Total size should be {} bytes",
        expected_size
    );

    // Cleanup
    client.delete_object().bucket(bucket).key("large-multipart-file").send().await.ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test creating 1000 small objects
#[tokio::test]
#[ignore]
async fn test_many_small_objects_1000() {
    let client = create_s3_client().await;
    let bucket = "sdk-stress-many-objects-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    const NUM_OBJECTS: usize = 1000;

    // Create 1000 objects sequentially
    for i in 0..NUM_OBJECTS {
        client
            .put_object()
            .bucket(bucket)
            .key(format!("object-{:04}", i))
            .body(Bytes::from(format!("content-{}", i)).into())
            .send()
            .await
            .expect(&format!("PutObject {} should succeed", i));

        // Progress indicator every 100 objects
        if (i + 1) % 100 == 0 {
            eprintln!("Created {}/{} objects", i + 1, NUM_OBJECTS);
        }
    }

    // List and verify count
    let mut total_count = 0;
    let mut continuation_token: Option<String> = None;

    loop {
        let mut list_request = client.list_objects_v2().bucket(bucket).max_keys(1000);

        if let Some(token) = &continuation_token {
            list_request = list_request.continuation_token(token);
        }

        let list_result = list_request.send().await.expect("ListObjects should succeed");

        total_count += list_result.contents().len();

        if list_result.is_truncated().unwrap_or(false) {
            continuation_token = list_result.next_continuation_token().map(|s| s.to_string());
        } else {
            break;
        }
    }

    assert_eq!(total_count, NUM_OBJECTS, "Should have all {} objects", NUM_OBJECTS);

    // Cleanup using delete_objects (batch delete)
    let mut keys_to_delete = Vec::new();
    for i in 0..NUM_OBJECTS {
        keys_to_delete.push(
            ObjectIdentifier::builder()
                .key(format!("object-{:04}", i))
                .build()
                .unwrap(),
        );
    }

    // Delete in batches of 1000 (S3 limit)
    for chunk in keys_to_delete.chunks(1000) {
        let delete = Delete::builder()
            .set_objects(Some(chunk.to_vec()))
            .build()
            .unwrap();

        client
            .delete_objects()
            .bucket(bucket)
            .delete(delete)
            .send()
            .await
            .expect("DeleteObjects should succeed");
    }

    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test deep directory structure (100 levels)
#[tokio::test]
#[ignore]
async fn test_deep_directory_structure() {
    let client = create_s3_client().await;
    let bucket = "sdk-stress-deep-path-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Create a path with 100 directory levels
    let levels = 100;
    let path_parts: Vec<String> = (0..levels).map(|i| format!("level{:02}", i)).collect();
    let deep_key = format!("{}/file.txt", path_parts.join("/"));

    assert!(deep_key.len() < 1024, "Key should be within S3 limits");

    // Put object at deep path
    client
        .put_object()
        .bucket(bucket)
        .key(&deep_key)
        .body(Bytes::from("deep content").into())
        .send()
        .await
        .expect("PutObject at deep path should succeed");

    // Get object from deep path
    let get_result = client
        .get_object()
        .bucket(bucket)
        .key(&deep_key)
        .send()
        .await
        .expect("GetObject from deep path should succeed");

    let body = get_result.body.collect().await.unwrap().into_bytes();
    assert_eq!(body, Bytes::from("deep content"));

    // List with prefix to verify structure
    let list_result = client
        .list_objects_v2()
        .bucket(bucket)
        .prefix("level00/")
        .send()
        .await
        .expect("ListObjects should succeed");

    assert_eq!(list_result.contents().len(), 1, "Should find the deep object");

    // Cleanup
    client.delete_object().bucket(bucket).key(&deep_key).send().await.ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test wide directory structure (1000 files in one prefix)
#[tokio::test]
#[ignore]
async fn test_wide_directory_structure() {
    let client = create_s3_client().await;
    let bucket = "sdk-stress-wide-path-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    const NUM_FILES: usize = 1000;
    let prefix = "wide-dir/";

    // Create 1000 files in the same prefix
    for i in 0..NUM_FILES {
        client
            .put_object()
            .bucket(bucket)
            .key(format!("{}file-{:04}.txt", prefix, i))
            .body(Bytes::from(format!("content-{}", i)).into())
            .send()
            .await
            .expect(&format!("PutObject {} should succeed", i));

        if (i + 1) % 100 == 0 {
            eprintln!("Created {}/{} files in wide directory", i + 1, NUM_FILES);
        }
    }

    // List with prefix
    let mut total_count = 0;
    let mut continuation_token: Option<String> = None;

    loop {
        let mut list_request = client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(prefix)
            .max_keys(1000);

        if let Some(token) = &continuation_token {
            list_request = list_request.continuation_token(token);
        }

        let list_result = list_request.send().await.expect("ListObjects should succeed");

        total_count += list_result.contents().len();

        if list_result.is_truncated().unwrap_or(false) {
            continuation_token = list_result.next_continuation_token().map(|s| s.to_string());
        } else {
            break;
        }
    }

    assert_eq!(total_count, NUM_FILES, "Should have all {} files in prefix", NUM_FILES);

    // Test listing with delimiter
    let list_with_delimiter = client
        .list_objects_v2()
        .bucket(bucket)
        .delimiter("/")
        .send()
        .await
        .expect("ListObjects with delimiter should succeed");

    // Should have one common prefix
    assert_eq!(
        list_with_delimiter.common_prefixes().len(),
        1,
        "Should have one common prefix"
    );
    assert_eq!(
        list_with_delimiter.common_prefixes()[0].prefix(),
        Some(prefix),
        "Common prefix should be {}",
        prefix
    );

    // Cleanup using batch delete
    let mut keys_to_delete = Vec::new();
    for i in 0..NUM_FILES {
        keys_to_delete.push(
            ObjectIdentifier::builder()
                .key(format!("{}file-{:04}.txt", prefix, i))
                .build()
                .unwrap(),
        );
    }

    for chunk in keys_to_delete.chunks(1000) {
        let delete = Delete::builder()
            .set_objects(Some(chunk.to_vec()))
            .build()
            .unwrap();

        client
            .delete_objects()
            .bucket(bucket)
            .delete(delete)
            .send()
            .await
            .expect("DeleteObjects should succeed");
    }

    let _ = client.delete_bucket().bucket(bucket).send().await;
}

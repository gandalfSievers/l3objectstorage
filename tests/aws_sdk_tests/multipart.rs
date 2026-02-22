use super::*;

#[tokio::test]
#[ignore]
async fn test_create_multipart_upload() {
    let client = create_s3_client().await;
    let bucket = "sdk-multipart-create-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Initiate multipart upload
    let response = client
        .create_multipart_upload()
        .bucket(bucket)
        .key("large-file.bin")
        .content_type("application/octet-stream")
        .send()
        .await
        .expect("Failed to create multipart upload");

    assert_eq!(response.bucket(), Some(bucket));
    assert_eq!(response.key(), Some("large-file.bin"));
    assert!(response.upload_id().is_some());
    let upload_id = response.upload_id().unwrap();
    assert!(!upload_id.is_empty());

    // Cleanup: abort the upload
    client
        .abort_multipart_upload()
        .bucket(bucket)
        .key("large-file.bin")
        .upload_id(upload_id)
        .send()
        .await
        .ok();

    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_multipart_upload_complete() {
    let client = create_s3_client().await;
    let bucket = "sdk-multipart-complete-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Create multipart upload
    let create_response = client
        .create_multipart_upload()
        .bucket(bucket)
        .key("complete-file")
        .send()
        .await
        .unwrap();

    let upload_id = create_response.upload_id().unwrap();

    // Upload 2 parts
    let mut completed_parts = Vec::new();
    for i in 1..=2 {
        let data = Bytes::from(format!("Part {} data content here", i));
        let response = client
            .upload_part()
            .bucket(bucket)
            .key("complete-file")
            .upload_id(upload_id)
            .part_number(i)
            .body(data.into())
            .send()
            .await
            .expect("Failed to upload part");

        completed_parts.push(
            CompletedPart::builder()
                .part_number(i)
                .e_tag(response.e_tag().unwrap())
                .build(),
        );
    }

    // Complete multipart upload
    let completed_upload = CompletedMultipartUpload::builder()
        .set_parts(Some(completed_parts))
        .build();

    let complete_response = client
        .complete_multipart_upload()
        .bucket(bucket)
        .key("complete-file")
        .upload_id(upload_id)
        .multipart_upload(completed_upload)
        .send()
        .await
        .expect("Failed to complete multipart upload");

    assert_eq!(complete_response.bucket(), Some(bucket));
    assert_eq!(complete_response.key(), Some("complete-file"));
    assert!(complete_response.e_tag().is_some());

    // Verify the object exists and has correct content
    let get_response = client
        .get_object()
        .bucket(bucket)
        .key("complete-file")
        .send()
        .await
        .expect("Failed to get completed object");

    let body = get_response.body.collect().await.unwrap().into_bytes();
    assert!(body.starts_with(b"Part 1"));
    assert!(String::from_utf8_lossy(&body).contains("Part 2"));

    // Cleanup
    client
        .delete_object()
        .bucket(bucket)
        .key("complete-file")
        .send()
        .await
        .unwrap();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_list_parts() {
    let client = create_s3_client().await;
    let bucket = "sdk-list-parts-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Create multipart upload
    let create_response = client
        .create_multipart_upload()
        .bucket(bucket)
        .key("list-parts-file")
        .send()
        .await
        .unwrap();

    let upload_id = create_response.upload_id().unwrap();

    // Upload 3 parts
    for i in 1..=3 {
        let data = Bytes::from(vec![i as u8; 1024]);
        client
            .upload_part()
            .bucket(bucket)
            .key("list-parts-file")
            .upload_id(upload_id)
            .part_number(i)
            .body(data.into())
            .send()
            .await
            .unwrap();
    }

    // List parts
    let list_response = client
        .list_parts()
        .bucket(bucket)
        .key("list-parts-file")
        .upload_id(upload_id)
        .send()
        .await
        .expect("Failed to list parts");

    assert_eq!(list_response.parts().len(), 3);

    for (i, part) in list_response.parts().iter().enumerate() {
        assert_eq!(part.part_number(), Some((i + 1) as i32));
        assert_eq!(part.size(), Some(1024));
    }

    // Cleanup
    client
        .abort_multipart_upload()
        .bucket(bucket)
        .key("list-parts-file")
        .upload_id(upload_id)
        .send()
        .await
        .ok();

    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_abort_multipart_upload() {
    let client = create_s3_client().await;
    let bucket = "sdk-abort-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Create multipart upload
    let create_response = client
        .create_multipart_upload()
        .bucket(bucket)
        .key("abort-file")
        .send()
        .await
        .unwrap();

    let upload_id = create_response.upload_id().unwrap();

    // Upload a part
    client
        .upload_part()
        .bucket(bucket)
        .key("abort-file")
        .upload_id(upload_id)
        .part_number(1)
        .body(Bytes::from("part data").into())
        .send()
        .await
        .unwrap();

    // Abort the upload
    client
        .abort_multipart_upload()
        .bucket(bucket)
        .key("abort-file")
        .upload_id(upload_id)
        .send()
        .await
        .expect("Failed to abort multipart upload");

    // Verify the upload no longer exists (list parts should fail)
    let list_result = client
        .list_parts()
        .bucket(bucket)
        .key("abort-file")
        .upload_id(upload_id)
        .send()
        .await;

    assert!(list_result.is_err());

    // Verify no object was created
    let get_result = client
        .get_object()
        .bucket(bucket)
        .key("abort-file")
        .send()
        .await;

    assert!(get_result.is_err());

    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_list_multipart_uploads() {
    let client = create_s3_client().await;
    let bucket = "sdk-list-uploads-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Create multiple multipart uploads
    let mut upload_ids = Vec::new();
    for i in 0..3 {
        let response = client
            .create_multipart_upload()
            .bucket(bucket)
            .key(format!("file-{}", i))
            .send()
            .await
            .unwrap();
        upload_ids.push((format!("file-{}", i), response.upload_id().unwrap().to_string()));
    }

    // List multipart uploads
    let list_response = client
        .list_multipart_uploads()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to list multipart uploads");

    assert_eq!(list_response.uploads().len(), 3);

    for upload in list_response.uploads() {
        assert!(upload.key().is_some());
        assert!(upload.upload_id().is_some());
        assert!(upload.initiated().is_some());
    }

    // Cleanup: abort all uploads
    for (key, upload_id) in &upload_ids {
        client
            .abort_multipart_upload()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await
            .ok();
    }

    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_upload_part_copy() {
    let client = create_s3_client().await;
    let bucket = "sdk-upload-part-copy-test";

    // Create bucket
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Create source object
    let source_data = Bytes::from("This is source data for copy");
    client
        .put_object()
        .bucket(bucket)
        .key("source-object")
        .body(source_data.clone().into())
        .send()
        .await
        .expect("Failed to put source object");

    // Create multipart upload
    let create_response = client
        .create_multipart_upload()
        .bucket(bucket)
        .key("copied-multipart")
        .send()
        .await
        .expect("Failed to create multipart upload");

    let upload_id = create_response.upload_id().unwrap();

    // Upload part by copying from source
    let copy_result = client
        .upload_part_copy()
        .bucket(bucket)
        .key("copied-multipart")
        .upload_id(upload_id)
        .part_number(1)
        .copy_source(format!("{}/source-object", bucket))
        .send()
        .await
        .expect("Failed to upload part copy");

    let etag = copy_result
        .copy_part_result()
        .expect("Should have copy part result")
        .e_tag()
        .expect("Should have ETag");

    // Complete the multipart upload
    let completed_upload = CompletedMultipartUpload::builder()
        .parts(
            CompletedPart::builder()
                .part_number(1)
                .e_tag(etag)
                .build(),
        )
        .build();

    client
        .complete_multipart_upload()
        .bucket(bucket)
        .key("copied-multipart")
        .upload_id(upload_id)
        .multipart_upload(completed_upload)
        .send()
        .await
        .expect("Failed to complete multipart upload");

    // Verify the copied object
    let get_response = client
        .get_object()
        .bucket(bucket)
        .key("copied-multipart")
        .send()
        .await
        .expect("Failed to get copied object");

    let body = get_response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body, source_data);

    // Cleanup
    client
        .delete_object()
        .bucket(bucket)
        .key("source-object")
        .send()
        .await
        .ok();
    client
        .delete_object()
        .bucket(bucket)
        .key("copied-multipart")
        .send()
        .await
        .ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_upload_part_copy_with_range() {
    let client = create_s3_client().await;
    let bucket = "sdk-upload-part-copy-range-test";

    // Create bucket
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Create source object with known content
    let source_data = Bytes::from("0123456789ABCDEFGHIJ"); // 20 bytes
    client
        .put_object()
        .bucket(bucket)
        .key("range-source")
        .body(source_data.into())
        .send()
        .await
        .expect("Failed to put source object");

    // Create multipart upload
    let create_response = client
        .create_multipart_upload()
        .bucket(bucket)
        .key("range-copied")
        .send()
        .await
        .expect("Failed to create multipart upload");

    let upload_id = create_response.upload_id().unwrap();

    // Copy only bytes 5-14 (10 bytes: "56789ABCDE")
    let copy_result = client
        .upload_part_copy()
        .bucket(bucket)
        .key("range-copied")
        .upload_id(upload_id)
        .part_number(1)
        .copy_source(format!("{}/range-source", bucket))
        .copy_source_range("bytes=5-14")
        .send()
        .await
        .expect("Failed to upload part copy with range");

    let etag = copy_result
        .copy_part_result()
        .expect("Should have copy part result")
        .e_tag()
        .expect("Should have ETag");

    // Complete the multipart upload
    let completed_upload = CompletedMultipartUpload::builder()
        .parts(
            CompletedPart::builder()
                .part_number(1)
                .e_tag(etag)
                .build(),
        )
        .build();

    client
        .complete_multipart_upload()
        .bucket(bucket)
        .key("range-copied")
        .upload_id(upload_id)
        .multipart_upload(completed_upload)
        .send()
        .await
        .expect("Failed to complete multipart upload");

    // Verify the copied object contains only the range
    let get_response = client
        .get_object()
        .bucket(bucket)
        .key("range-copied")
        .send()
        .await
        .expect("Failed to get copied object");

    let body = get_response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body, Bytes::from("56789ABCDE"));

    // Cleanup
    client
        .delete_object()
        .bucket(bucket)
        .key("range-source")
        .send()
        .await
        .ok();
    client
        .delete_object()
        .bucket(bucket)
        .key("range-copied")
        .send()
        .await
        .ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_multipart_upload_realistic_part_sizes() {
    let client = create_s3_client().await;
    let bucket = "sdk-multipart-realistic-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Create multipart upload
    let create_response = client
        .create_multipart_upload()
        .bucket(bucket)
        .key("large-multipart-file")
        .content_type("application/octet-stream")
        .send()
        .await
        .expect("Failed to create multipart upload");

    let upload_id = create_response.upload_id().unwrap();

    // AWS S3 requires minimum 5MB parts (except last part)
    // 5MB = 5 * 1024 * 1024 = 5,242,880 bytes
    const PART_SIZE: usize = 5 * 1024 * 1024; // 5MB

    // Upload 2 parts of 5MB each + 1 smaller final part
    let mut completed_parts = Vec::new();

    // Part 1: 5MB of 'A' bytes
    let part1_data = Bytes::from(vec![b'A'; PART_SIZE]);
    let part1_response = client
        .upload_part()
        .bucket(bucket)
        .key("large-multipart-file")
        .upload_id(upload_id)
        .part_number(1)
        .body(part1_data.clone().into())
        .send()
        .await
        .expect("Failed to upload part 1");

    completed_parts.push(
        CompletedPart::builder()
            .part_number(1)
            .e_tag(part1_response.e_tag().unwrap())
            .build(),
    );

    // Part 2: 5MB of 'B' bytes
    let part2_data = Bytes::from(vec![b'B'; PART_SIZE]);
    let part2_response = client
        .upload_part()
        .bucket(bucket)
        .key("large-multipart-file")
        .upload_id(upload_id)
        .part_number(2)
        .body(part2_data.clone().into())
        .send()
        .await
        .expect("Failed to upload part 2");

    completed_parts.push(
        CompletedPart::builder()
            .part_number(2)
            .e_tag(part2_response.e_tag().unwrap())
            .build(),
    );

    // Part 3: Small final part (1KB) - allowed to be smaller than 5MB
    let part3_data = Bytes::from(vec![b'C'; 1024]);
    let part3_response = client
        .upload_part()
        .bucket(bucket)
        .key("large-multipart-file")
        .upload_id(upload_id)
        .part_number(3)
        .body(part3_data.clone().into())
        .send()
        .await
        .expect("Failed to upload part 3");

    completed_parts.push(
        CompletedPart::builder()
            .part_number(3)
            .e_tag(part3_response.e_tag().unwrap())
            .build(),
    );

    // Complete the multipart upload
    let completed_upload = CompletedMultipartUpload::builder()
        .set_parts(Some(completed_parts))
        .build();

    let complete_response = client
        .complete_multipart_upload()
        .bucket(bucket)
        .key("large-multipart-file")
        .upload_id(upload_id)
        .multipart_upload(completed_upload)
        .send()
        .await
        .expect("Failed to complete multipart upload");

    assert!(complete_response.e_tag().is_some(), "Should have ETag");

    // Verify total size: 5MB + 5MB + 1KB = 10,486,784 bytes
    let expected_size: i64 = (PART_SIZE * 2 + 1024) as i64;
    let head = client
        .head_object()
        .bucket(bucket)
        .key("large-multipart-file")
        .send()
        .await
        .expect("Failed to head object");

    assert_eq!(
        head.content_length(),
        Some(expected_size),
        "Object size should match sum of parts"
    );

    // Verify content by reading ranges
    // First 10 bytes should be 'A'
    let get_start = client
        .get_object()
        .bucket(bucket)
        .key("large-multipart-file")
        .range("bytes=0-9")
        .send()
        .await
        .expect("Failed to get start range");
    let start_bytes = get_start.body.collect().await.unwrap().into_bytes();
    assert!(start_bytes.iter().all(|&b| b == b'A'), "Start should be 'A' bytes");

    // Bytes at 5MB offset should be 'B'
    let get_middle = client
        .get_object()
        .bucket(bucket)
        .key("large-multipart-file")
        .range(format!("bytes={}-{}", PART_SIZE, PART_SIZE + 9))
        .send()
        .await
        .expect("Failed to get middle range");
    let middle_bytes = get_middle.body.collect().await.unwrap().into_bytes();
    assert!(middle_bytes.iter().all(|&b| b == b'B'), "Middle should be 'B' bytes");

    // Last 10 bytes should be 'C'
    let get_end = client
        .get_object()
        .bucket(bucket)
        .key("large-multipart-file")
        .range(format!("bytes={}-{}", expected_size - 10, expected_size - 1))
        .send()
        .await
        .expect("Failed to get end range");
    let end_bytes = get_end.body.collect().await.unwrap().into_bytes();
    assert!(end_bytes.iter().all(|&b| b == b'C'), "End should be 'C' bytes");

    // Cleanup
    client
        .delete_object()
        .bucket(bucket)
        .key("large-multipart-file")
        .send()
        .await
        .ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test ListParts pagination with part-number-marker
#[tokio::test]
#[ignore]
async fn test_list_parts_pagination() {
    let client = create_s3_client().await;
    let bucket = "sdk-list-parts-pagination-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Create multipart upload
    let create_response = client
        .create_multipart_upload()
        .bucket(bucket)
        .key("paginated-parts-file")
        .send()
        .await
        .unwrap();

    let upload_id = create_response.upload_id().unwrap();

    // Upload 5 parts
    for i in 1..=5 {
        let data = Bytes::from(vec![i as u8; 1024]);
        client
            .upload_part()
            .bucket(bucket)
            .key("paginated-parts-file")
            .upload_id(upload_id)
            .part_number(i)
            .body(data.into())
            .send()
            .await
            .unwrap();
    }

    // List with max-parts=2
    let list_response = client
        .list_parts()
        .bucket(bucket)
        .key("paginated-parts-file")
        .upload_id(upload_id)
        .max_parts(2)
        .send()
        .await
        .expect("Failed to list parts");

    assert_eq!(list_response.parts().len(), 2, "Should return 2 parts");
    assert!(list_response.is_truncated().unwrap_or(false), "Should be truncated");

    let next_part_marker = list_response.next_part_number_marker();
    assert!(next_part_marker.is_some(), "Should have next part number marker");

    // Get next page using part-number-marker
    let list_response_2 = client
        .list_parts()
        .bucket(bucket)
        .key("paginated-parts-file")
        .upload_id(upload_id)
        .max_parts(2)
        .part_number_marker(next_part_marker.unwrap().to_string())
        .send()
        .await
        .expect("Failed to list parts page 2");

    assert_eq!(list_response_2.parts().len(), 2, "Page 2 should have 2 parts");

    // Get final page
    if let Some(marker) = list_response_2.next_part_number_marker() {
        let list_response_3 = client
            .list_parts()
            .bucket(bucket)
            .key("paginated-parts-file")
            .upload_id(upload_id)
            .max_parts(2)
            .part_number_marker(marker.to_string())
            .send()
            .await
            .expect("Failed to list parts page 3");

        assert_eq!(list_response_3.parts().len(), 1, "Page 3 should have 1 part");
        assert!(!list_response_3.is_truncated().unwrap_or(true), "Page 3 should not be truncated");
    }

    // Cleanup
    client
        .abort_multipart_upload()
        .bucket(bucket)
        .key("paginated-parts-file")
        .upload_id(upload_id)
        .send()
        .await
        .ok();

    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test ListParts with max-parts parameter
#[tokio::test]
#[ignore]
async fn test_list_parts_max_parts() {
    let client = create_s3_client().await;
    let bucket = "sdk-list-parts-max-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Create multipart upload
    let create_response = client
        .create_multipart_upload()
        .bucket(bucket)
        .key("max-parts-file")
        .send()
        .await
        .unwrap();

    let upload_id = create_response.upload_id().unwrap();

    // Upload 10 parts
    for i in 1..=10 {
        let data = Bytes::from(vec![i as u8; 512]);
        client
            .upload_part()
            .bucket(bucket)
            .key("max-parts-file")
            .upload_id(upload_id)
            .part_number(i)
            .body(data.into())
            .send()
            .await
            .unwrap();
    }

    // Test various max-parts values
    for max in [1, 3, 5, 10] {
        let list_response = client
            .list_parts()
            .bucket(bucket)
            .key("max-parts-file")
            .upload_id(upload_id)
            .max_parts(max)
            .send()
            .await
            .expect("Failed to list parts");

        let expected = std::cmp::min(max, 10);
        assert_eq!(
            list_response.parts().len() as i32,
            expected,
            "Should return {} parts for max_parts={}",
            expected,
            max
        );
    }

    // Cleanup
    client
        .abort_multipart_upload()
        .bucket(bucket)
        .key("max-parts-file")
        .upload_id(upload_id)
        .send()
        .await
        .ok();

    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test ListMultipartUploads pagination
#[tokio::test]
#[ignore]
async fn test_list_multipart_uploads_pagination() {
    let client = create_s3_client().await;
    let bucket = "sdk-list-uploads-pagination-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Create 5 multipart uploads
    let mut upload_ids = Vec::new();
    for i in 0..5 {
        let response = client
            .create_multipart_upload()
            .bucket(bucket)
            .key(format!("file-{:02}", i))
            .send()
            .await
            .unwrap();
        upload_ids.push((format!("file-{:02}", i), response.upload_id().unwrap().to_string()));
    }

    // List with max-uploads=2
    let list_response = client
        .list_multipart_uploads()
        .bucket(bucket)
        .max_uploads(2)
        .send()
        .await
        .expect("Failed to list multipart uploads");

    assert_eq!(list_response.uploads().len(), 2, "Should return 2 uploads");
    assert!(list_response.is_truncated().unwrap_or(false), "Should be truncated");

    let next_key_marker = list_response.next_key_marker().expect("Should have next key marker");

    // Get next page
    let list_response_2 = client
        .list_multipart_uploads()
        .bucket(bucket)
        .max_uploads(2)
        .key_marker(next_key_marker)
        .send()
        .await
        .expect("Failed to list uploads page 2");

    assert_eq!(list_response_2.uploads().len(), 2, "Page 2 should have 2 uploads");

    // Get final page
    if let Some(marker) = list_response_2.next_key_marker() {
        let list_response_3 = client
            .list_multipart_uploads()
            .bucket(bucket)
            .max_uploads(2)
            .key_marker(marker)
            .send()
            .await
            .expect("Failed to list uploads page 3");

        assert_eq!(list_response_3.uploads().len(), 1, "Page 3 should have 1 upload");
        assert!(!list_response_3.is_truncated().unwrap_or(true), "Page 3 should not be truncated");
    }

    // Cleanup
    for (key, upload_id) in &upload_ids {
        client
            .abort_multipart_upload()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await
            .ok();
    }

    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test ListMultipartUploads with prefix and delimiter
#[tokio::test]
#[ignore]
async fn test_list_multipart_uploads_prefix_delimiter() {
    let client = create_s3_client().await;
    let bucket = "sdk-list-uploads-prefix-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Create uploads in different "directories"
    let mut upload_ids = Vec::new();
    for dir in ["dir-a/", "dir-b/"] {
        for i in 0..2 {
            let key = format!("{}file-{}", dir, i);
            let response = client
                .create_multipart_upload()
                .bucket(bucket)
                .key(&key)
                .send()
                .await
                .unwrap();
            upload_ids.push((key, response.upload_id().unwrap().to_string()));
        }
    }

    // Create a root-level upload
    let root_response = client
        .create_multipart_upload()
        .bucket(bucket)
        .key("root-file")
        .send()
        .await
        .unwrap();
    upload_ids.push(("root-file".to_string(), root_response.upload_id().unwrap().to_string()));

    // List with prefix filter
    let list_with_prefix = client
        .list_multipart_uploads()
        .bucket(bucket)
        .prefix("dir-a/")
        .send()
        .await
        .expect("Failed to list uploads with prefix");

    assert_eq!(list_with_prefix.uploads().len(), 2, "Should have 2 uploads with prefix dir-a/");
    for upload in list_with_prefix.uploads() {
        assert!(upload.key().unwrap().starts_with("dir-a/"), "All keys should have prefix");
    }

    // List with delimiter
    let list_with_delimiter = client
        .list_multipart_uploads()
        .bucket(bucket)
        .delimiter("/")
        .send()
        .await
        .expect("Failed to list uploads with delimiter");

    // Should have common prefixes for directories
    let common_prefixes = list_with_delimiter.common_prefixes();
    assert_eq!(common_prefixes.len(), 2, "Should have 2 common prefixes");

    // Should have 1 upload at root level
    assert_eq!(list_with_delimiter.uploads().len(), 1, "Should have 1 root upload");
    assert_eq!(list_with_delimiter.uploads()[0].key(), Some("root-file"));

    // Cleanup
    for (key, upload_id) in &upload_ids {
        client
            .abort_multipart_upload()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await
            .ok();
    }

    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test ListMultipartUploads on empty bucket
#[tokio::test]
#[ignore]
async fn test_list_multipart_uploads_empty() {
    let client = create_s3_client().await;
    let bucket = "sdk-list-uploads-empty-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // List on empty bucket
    let list_response = client
        .list_multipart_uploads()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to list multipart uploads");

    assert!(list_response.uploads().is_empty(), "Empty bucket should have no uploads");
    assert!(list_response.common_prefixes().is_empty(), "Empty bucket should have no common prefixes");
    assert!(!list_response.is_truncated().unwrap_or(true), "Should not be truncated");

    let _ = client.delete_bucket().bucket(bucket).send().await;
}

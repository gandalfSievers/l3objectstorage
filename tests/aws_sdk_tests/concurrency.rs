use super::*;
use std::sync::Arc;
use tokio::sync::Barrier;

/// Test concurrent PutObject operations on the same key
#[tokio::test]
#[ignore]
async fn test_concurrent_puts_same_key() {
    let client = Arc::new(create_s3_client().await);
    let bucket = "sdk-concurrent-puts-same-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    let num_concurrent = 10;
    let barrier = Arc::new(Barrier::new(num_concurrent));

    let mut handles = Vec::new();

    for i in 0..num_concurrent {
        let client = Arc::clone(&client);
        let barrier = Arc::clone(&barrier);
        let handle = tokio::spawn(async move {
            // Wait for all tasks to be ready
            barrier.wait().await;

            // All tasks write to the same key
            client
                .put_object()
                .bucket(bucket)
                .key("same-key")
                .body(Bytes::from(format!("content from task {}", i)).into())
                .send()
                .await
        });
        handles.push(handle);
    }

    // Wait for all to complete
    let results: Vec<_> = futures::future::join_all(handles).await;

    // All should succeed (last writer wins)
    let success_count = results.iter().filter(|r| r.as_ref().map(|r| r.is_ok()).unwrap_or(false)).count();
    assert_eq!(success_count, num_concurrent, "All concurrent puts should succeed");

    // Verify final state - should have one of the contents
    let get_result = client
        .get_object()
        .bucket(bucket)
        .key("same-key")
        .send()
        .await
        .expect("GetObject should succeed");

    let body = get_result.body.collect().await.unwrap().into_bytes();
    let body_str = String::from_utf8(body.to_vec()).unwrap();
    assert!(body_str.starts_with("content from task"), "Should have content from one of the tasks");

    // Cleanup
    client.delete_object().bucket(bucket).key("same-key").send().await.ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test concurrent PutObject operations on different keys
#[tokio::test]
#[ignore]
async fn test_concurrent_puts_different_keys() {
    let client = Arc::new(create_s3_client().await);
    let bucket = "sdk-concurrent-puts-diff-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    let num_concurrent = 20;
    let barrier = Arc::new(Barrier::new(num_concurrent));

    let mut handles = Vec::new();

    for i in 0..num_concurrent {
        let client = Arc::clone(&client);
        let barrier = Arc::clone(&barrier);
        let handle = tokio::spawn(async move {
            barrier.wait().await;

            client
                .put_object()
                .bucket(bucket)
                .key(format!("key-{}", i))
                .body(Bytes::from(format!("content-{}", i)).into())
                .send()
                .await
        });
        handles.push(handle);
    }

    let results: Vec<_> = futures::future::join_all(handles).await;

    // All should succeed
    let success_count = results.iter().filter(|r| r.as_ref().map(|r| r.is_ok()).unwrap_or(false)).count();
    assert_eq!(success_count, num_concurrent, "All concurrent puts to different keys should succeed");

    // Verify all objects exist
    let list_result = client
        .list_objects_v2()
        .bucket(bucket)
        .send()
        .await
        .expect("ListObjects should succeed");

    assert_eq!(list_result.contents().len(), num_concurrent, "Should have all objects");

    // Cleanup
    for i in 0..num_concurrent {
        client.delete_object().bucket(bucket).key(format!("key-{}", i)).send().await.ok();
    }
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test concurrent GetObject operations on the same key
#[tokio::test]
#[ignore]
async fn test_concurrent_gets_same_key() {
    let client = Arc::new(create_s3_client().await);
    let bucket = "sdk-concurrent-gets-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Create the object
    let content = "shared content for concurrent reads";
    client
        .put_object()
        .bucket(bucket)
        .key("shared-key")
        .body(Bytes::from(content).into())
        .send()
        .await
        .expect("Failed to put object");

    let num_concurrent = 20;
    let barrier = Arc::new(Barrier::new(num_concurrent));

    let mut handles = Vec::new();

    for _ in 0..num_concurrent {
        let client = Arc::clone(&client);
        let barrier = Arc::clone(&barrier);
        let handle = tokio::spawn(async move {
            barrier.wait().await;

            let result = client
                .get_object()
                .bucket(bucket)
                .key("shared-key")
                .send()
                .await?;

            let body = result.body.collect().await.unwrap().into_bytes();
            Ok::<_, aws_sdk_s3::Error>(body)
        });
        handles.push(handle);
    }

    let results: Vec<_> = futures::future::join_all(handles).await;

    // All should succeed with same content
    for result in results {
        let body = result.expect("Task should complete").expect("GetObject should succeed");
        assert_eq!(body, Bytes::from(content), "All reads should return same content");
    }

    // Cleanup
    client.delete_object().bucket(bucket).key("shared-key").send().await.ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test concurrent DeleteObject operations
#[tokio::test]
#[ignore]
async fn test_concurrent_deletes() {
    let client = Arc::new(create_s3_client().await);
    let bucket = "sdk-concurrent-deletes-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Create multiple objects
    let num_objects = 20;
    for i in 0..num_objects {
        client
            .put_object()
            .bucket(bucket)
            .key(format!("delete-key-{}", i))
            .body(Bytes::from("content").into())
            .send()
            .await
            .expect("Failed to put object");
    }

    let barrier = Arc::new(Barrier::new(num_objects));

    let mut handles = Vec::new();

    for i in 0..num_objects {
        let client = Arc::clone(&client);
        let barrier = Arc::clone(&barrier);
        let handle = tokio::spawn(async move {
            barrier.wait().await;

            client
                .delete_object()
                .bucket(bucket)
                .key(format!("delete-key-{}", i))
                .send()
                .await
        });
        handles.push(handle);
    }

    let results: Vec<_> = futures::future::join_all(handles).await;

    // All should succeed
    let success_count = results.iter().filter(|r| r.as_ref().map(|r| r.is_ok()).unwrap_or(false)).count();
    assert_eq!(success_count, num_objects, "All concurrent deletes should succeed");

    // Verify all objects are gone
    let list_result = client
        .list_objects_v2()
        .bucket(bucket)
        .send()
        .await
        .expect("ListObjects should succeed");

    assert!(list_result.contents().is_empty(), "All objects should be deleted");

    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test concurrent multipart uploads on the same key
#[tokio::test]
#[ignore]
async fn test_concurrent_multipart_same_key() {
    let client = Arc::new(create_s3_client().await);
    let bucket = "sdk-concurrent-multipart-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    let num_concurrent = 5;
    let barrier = Arc::new(Barrier::new(num_concurrent));

    let mut handles = Vec::new();

    for i in 0..num_concurrent {
        let client = Arc::clone(&client);
        let barrier = Arc::clone(&barrier);
        let handle = tokio::spawn(async move {
            barrier.wait().await;

            // Each task creates its own multipart upload
            let create_result = client
                .create_multipart_upload()
                .bucket(bucket)
                .key("concurrent-multipart-key")
                .send()
                .await?;

            let upload_id = create_result.upload_id().unwrap().to_string();

            // Upload one part
            let part_result = client
                .upload_part()
                .bucket(bucket)
                .key("concurrent-multipart-key")
                .upload_id(&upload_id)
                .part_number(1)
                .body(Bytes::from(format!("part data from task {}", i)).into())
                .send()
                .await?;

            // Complete the upload
            let completed_part = CompletedPart::builder()
                .part_number(1)
                .e_tag(part_result.e_tag().unwrap())
                .build();

            let completed_upload = CompletedMultipartUpload::builder()
                .parts(completed_part)
                .build();

            client
                .complete_multipart_upload()
                .bucket(bucket)
                .key("concurrent-multipart-key")
                .upload_id(&upload_id)
                .multipart_upload(completed_upload)
                .send()
                .await?;

            Ok::<_, aws_sdk_s3::Error>(i)
        });
        handles.push(handle);
    }

    let results: Vec<_> = futures::future::join_all(handles).await;

    // At least some should succeed (last one wins)
    let success_count = results.iter().filter(|r| r.as_ref().map(|r| r.is_ok()).unwrap_or(false)).count();
    assert!(success_count > 0, "At least one concurrent multipart should succeed");

    // Verify object exists with content from one of the tasks
    let get_result = client
        .get_object()
        .bucket(bucket)
        .key("concurrent-multipart-key")
        .send()
        .await
        .expect("GetObject should succeed");

    let body = get_result.body.collect().await.unwrap().into_bytes();
    let body_str = String::from_utf8(body.to_vec()).unwrap();
    assert!(body_str.starts_with("part data from task"), "Should have content from one task");

    // Cleanup
    client.delete_object().bucket(bucket).key("concurrent-multipart-key").send().await.ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test concurrent versioning writes
#[tokio::test]
#[ignore]
async fn test_concurrent_versioning_writes() {
    let client = Arc::new(create_s3_client().await);
    let bucket = "sdk-concurrent-versioning-test";

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

    let num_concurrent = 10;
    let barrier = Arc::new(Barrier::new(num_concurrent));

    let mut handles = Vec::new();

    for i in 0..num_concurrent {
        let client = Arc::clone(&client);
        let barrier = Arc::clone(&barrier);
        let handle = tokio::spawn(async move {
            barrier.wait().await;

            let result = client
                .put_object()
                .bucket(bucket)
                .key("versioned-key")
                .body(Bytes::from(format!("version from task {}", i)).into())
                .send()
                .await?;

            Ok::<_, aws_sdk_s3::Error>(result.version_id().map(|s| s.to_string()))
        });
        handles.push(handle);
    }

    let results: Vec<_> = futures::future::join_all(handles).await;

    // All should succeed and create unique versions
    let version_ids: Vec<String> = results
        .into_iter()
        .filter_map(|r| r.ok())
        .filter_map(|r| r.ok())
        .filter_map(|v| v)
        .collect();

    assert_eq!(version_ids.len(), num_concurrent, "All concurrent writes should create versions");

    // All version IDs should be unique
    let unique_versions: std::collections::HashSet<_> = version_ids.iter().collect();
    assert_eq!(unique_versions.len(), num_concurrent, "All version IDs should be unique");

    // List versions to verify
    let list_result = client
        .list_object_versions()
        .bucket(bucket)
        .send()
        .await
        .expect("ListObjectVersions should succeed");

    assert_eq!(list_result.versions().len(), num_concurrent, "Should have all versions");

    // Cleanup
    for version in list_result.versions() {
        if let (Some(key), Some(vid)) = (version.key(), version.version_id()) {
            client
                .delete_object()
                .bucket(bucket)
                .key(key)
                .version_id(vid)
                .send()
                .await
                .ok();
        }
    }
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

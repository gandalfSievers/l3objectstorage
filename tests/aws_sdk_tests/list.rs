use super::*;

#[tokio::test]
#[ignore]
async fn test_list_objects() {
    let client = create_s3_client().await;

    let _ = client
        .create_bucket()
        .bucket("sdk-list-test")
        .send()
        .await;

    // Create multiple objects
    for i in 0..5 {
        client
            .put_object()
            .bucket("sdk-list-test")
            .key(format!("prefix/key-{}", i))
            .body(Bytes::from("data").into())
            .send()
            .await
            .unwrap();
    }

    // List with prefix
    let response = client
        .list_objects_v2()
        .bucket("sdk-list-test")
        .prefix("prefix/")
        .send()
        .await
        .expect("Failed to list objects");

    assert_eq!(response.key_count(), Some(5));

    // Cleanup
    for i in 0..5 {
        client
            .delete_object()
            .bucket("sdk-list-test")
            .key(format!("prefix/key-{}", i))
            .send()
            .await
            .unwrap();
    }
    let _ = client
        .delete_bucket()
        .bucket("sdk-list-test")
        .send()
        .await;
}

#[tokio::test]
#[ignore]
async fn test_list_objects_v1() {
    let client = create_s3_client().await;
    let bucket = "sdk-list-v1-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Create objects
    for i in 0..3 {
        client
            .put_object()
            .bucket(bucket)
            .key(format!("key-{}", i))
            .body(Bytes::from("data").into())
            .send()
            .await
            .unwrap();
    }

    // List objects using v1 API
    let response = client
        .list_objects()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to list objects v1");

    let contents = response.contents();
    assert_eq!(contents.len(), 3, "Should have 3 objects");

    // Cleanup
    for i in 0..3 {
        client
            .delete_object()
            .bucket(bucket)
            .key(format!("key-{}", i))
            .send()
            .await
            .ok();
    }
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_list_objects_v2_pagination() {
    let client = create_s3_client().await;
    let bucket = "sdk-pagination-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Create 10 objects
    for i in 0..10 {
        client
            .put_object()
            .bucket(bucket)
            .key(format!("key-{:02}", i))
            .body(Bytes::from("data").into())
            .send()
            .await
            .unwrap();
    }

    // List with max_keys=3 (first page)
    let page1 = client
        .list_objects_v2()
        .bucket(bucket)
        .max_keys(3)
        .send()
        .await
        .expect("Failed to list first page");

    assert_eq!(page1.contents().len(), 3, "First page should have 3 objects");
    assert!(page1.is_truncated().unwrap_or(false), "Should be truncated");
    let token = page1.next_continuation_token().expect("Should have continuation token");

    // Get second page
    let page2 = client
        .list_objects_v2()
        .bucket(bucket)
        .max_keys(3)
        .continuation_token(token)
        .send()
        .await
        .expect("Failed to list second page");

    assert_eq!(page2.contents().len(), 3, "Second page should have 3 objects");
    assert!(page2.is_truncated().unwrap_or(false), "Should still be truncated");

    // Get remaining objects
    let token2 = page2.next_continuation_token().expect("Should have token");
    let page3 = client
        .list_objects_v2()
        .bucket(bucket)
        .max_keys(3)
        .continuation_token(token2)
        .send()
        .await
        .expect("Failed to list third page");

    assert_eq!(page3.contents().len(), 3, "Third page should have 3 objects");

    // Fourth page should have 1 remaining object
    let token3 = page3.next_continuation_token().expect("Should have token");
    let page4 = client
        .list_objects_v2()
        .bucket(bucket)
        .max_keys(3)
        .continuation_token(token3)
        .send()
        .await
        .expect("Failed to list fourth page");

    assert_eq!(page4.contents().len(), 1, "Fourth page should have 1 object");
    assert!(!page4.is_truncated().unwrap_or(true), "Should not be truncated");

    // Cleanup
    for i in 0..10 {
        client
            .delete_object()
            .bucket(bucket)
            .key(format!("key-{:02}", i))
            .send()
            .await
            .ok();
    }
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_list_objects_v2_delimiter() {
    let client = create_s3_client().await;
    let bucket = "sdk-delimiter-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Create folder-like structure
    let keys = vec![
        "folder1/file1.txt",
        "folder1/file2.txt",
        "folder1/subfolder/file3.txt",
        "folder2/file4.txt",
        "root-file.txt",
    ];

    for key in &keys {
        client
            .put_object()
            .bucket(bucket)
            .key(*key)
            .body(Bytes::from("data").into())
            .send()
            .await
            .unwrap();
    }

    // List with delimiter at root level
    let response = client
        .list_objects_v2()
        .bucket(bucket)
        .delimiter("/")
        .send()
        .await
        .expect("Failed to list with delimiter");

    // Should have 1 file at root and 2 common prefixes (folder1/, folder2/)
    assert_eq!(response.contents().len(), 1, "Should have 1 root-level file");
    assert_eq!(response.common_prefixes().len(), 2, "Should have 2 folder prefixes");

    // List folder1 contents
    let folder1_response = client
        .list_objects_v2()
        .bucket(bucket)
        .prefix("folder1/")
        .delimiter("/")
        .send()
        .await
        .expect("Failed to list folder1");

    assert_eq!(folder1_response.contents().len(), 2, "folder1 should have 2 direct files");
    assert_eq!(folder1_response.common_prefixes().len(), 1, "folder1 should have 1 subfolder");

    // Cleanup
    for key in &keys {
        client.delete_object().bucket(bucket).key(*key).send().await.ok();
    }
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

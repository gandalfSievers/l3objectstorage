//! Integration tests for object operations

use bytes::Bytes;
use std::collections::HashMap;

mod common {
    include!("common/mod.rs");
}

use common::create_test_storage;

#[tokio::test]
async fn test_object_lifecycle() {
    let (storage, _temp) = create_test_storage().await;

    storage.create_bucket("object-lifecycle").await.unwrap();

    // Put object
    let data = Bytes::from("hello world");
    let obj = storage
        .put_object("object-lifecycle", "test-key", data.clone(), Some("text/plain"), None)
        .await
        .unwrap();

    assert_eq!(obj.key, "test-key");
    assert_eq!(obj.size, 11);
    assert_eq!(obj.content_type, "text/plain");

    // Get object
    let (retrieved_obj, retrieved_data) = storage
        .get_object("object-lifecycle", "test-key")
        .await
        .unwrap();

    assert_eq!(retrieved_obj.key, "test-key");
    assert_eq!(retrieved_data, data);

    // Head object
    let head = storage
        .head_object("object-lifecycle", "test-key")
        .await
        .unwrap();

    assert_eq!(head.key, "test-key");
    assert_eq!(head.size, 11);

    // Delete object
    storage
        .delete_object("object-lifecycle", "test-key")
        .await
        .unwrap();

    assert!(!storage.object_exists("object-lifecycle", "test-key").await);
}

#[tokio::test]
async fn test_object_overwrite() {
    let (storage, _temp) = create_test_storage().await;

    storage.create_bucket("overwrite-test").await.unwrap();

    // First version
    storage
        .put_object(
            "overwrite-test",
            "key",
            Bytes::from("version1"),
            None,
            None,
        )
        .await
        .unwrap();

    // Second version
    storage
        .put_object(
            "overwrite-test",
            "key",
            Bytes::from("version2"),
            None,
            None,
        )
        .await
        .unwrap();

    let (_, data) = storage.get_object("overwrite-test", "key").await.unwrap();
    assert_eq!(data, Bytes::from("version2"));
}

#[tokio::test]
async fn test_list_objects() {
    let (storage, _temp) = create_test_storage().await;

    storage.create_bucket("list-test").await.unwrap();

    // Create objects with different prefixes
    storage
        .put_object("list-test", "dir1/file1.txt", Bytes::from("a"), None, None)
        .await
        .unwrap();
    storage
        .put_object("list-test", "dir1/file2.txt", Bytes::from("b"), None, None)
        .await
        .unwrap();
    storage
        .put_object("list-test", "dir2/file3.txt", Bytes::from("c"), None, None)
        .await
        .unwrap();
    storage
        .put_object("list-test", "root.txt", Bytes::from("d"), None, None)
        .await
        .unwrap();

    // List all
    let objects = storage.list_objects("list-test", None, None, 1000, None).await.unwrap();
    assert_eq!(objects.objects.len(), 4);

    // List with prefix
    let objects = storage
        .list_objects("list-test", Some("dir1/"), None, 1000, None)
        .await
        .unwrap();
    assert_eq!(objects.objects.len(), 2);

    // List with max keys
    let objects = storage.list_objects("list-test", None, None, 2, None).await.unwrap();
    assert_eq!(objects.objects.len(), 2);
}

#[tokio::test]
async fn test_copy_object() {
    let (storage, _temp) = create_test_storage().await;

    storage.create_bucket("source-bucket").await.unwrap();
    storage.create_bucket("dest-bucket").await.unwrap();

    // Create source object
    storage
        .put_object(
            "source-bucket",
            "source-key",
            Bytes::from("copy this data"),
            Some("text/plain"),
            None,
        )
        .await
        .unwrap();

    // Copy to different bucket
    let copied = storage
        .copy_object("source-bucket", "source-key", "dest-bucket", "dest-key")
        .await
        .unwrap();

    assert_eq!(copied.key, "dest-key");

    // Verify copy
    let (_, data) = storage.get_object("dest-bucket", "dest-key").await.unwrap();
    assert_eq!(data, Bytes::from("copy this data"));

    // Copy within same bucket
    storage
        .copy_object("source-bucket", "source-key", "source-bucket", "copied-key")
        .await
        .unwrap();

    assert!(storage.object_exists("source-bucket", "copied-key").await);
}

#[tokio::test]
async fn test_object_metadata() {
    let (storage, _temp) = create_test_storage().await;

    storage.create_bucket("metadata-test").await.unwrap();

    let mut metadata = HashMap::new();
    metadata.insert("x-amz-meta-author".to_string(), "test-author".to_string());
    metadata.insert("x-amz-meta-version".to_string(), "1.0".to_string());

    storage
        .put_object(
            "metadata-test",
            "with-metadata",
            Bytes::from("data"),
            Some("application/json"),
            Some(metadata),
        )
        .await
        .unwrap();

    let (obj, _) = storage
        .get_object("metadata-test", "with-metadata")
        .await
        .unwrap();

    assert_eq!(obj.content_type, "application/json");
    assert_eq!(
        obj.metadata.get("x-amz-meta-author"),
        Some(&"test-author".to_string())
    );
    assert_eq!(
        obj.metadata.get("x-amz-meta-version"),
        Some(&"1.0".to_string())
    );
}

#[tokio::test]
async fn test_large_object() {
    let (storage, _temp) = create_test_storage().await;

    storage.create_bucket("large-test").await.unwrap();

    // Create a 1MB object
    let large_data = Bytes::from(vec![0u8; 1024 * 1024]);

    storage
        .put_object("large-test", "large-key", large_data.clone(), None, None)
        .await
        .unwrap();

    let (obj, retrieved_data) = storage.get_object("large-test", "large-key").await.unwrap();

    assert_eq!(obj.size, 1024 * 1024);
    assert_eq!(retrieved_data.len(), 1024 * 1024);
}

#[tokio::test]
async fn test_special_characters_in_key() {
    let (storage, _temp) = create_test_storage().await;

    storage.create_bucket("special-chars").await.unwrap();

    let special_keys = vec![
        "key with spaces",
        "key/with/slashes",
        "key-with-dashes",
        "key_with_underscores",
        "key.with.dots",
        "key@with#special$chars",
    ];

    for key in special_keys {
        storage
            .put_object("special-chars", key, Bytes::from("data"), None, None)
            .await
            .unwrap();

        assert!(
            storage.object_exists("special-chars", key).await,
            "Key '{}' should exist",
            key
        );

        let (obj, _) = storage.get_object("special-chars", key).await.unwrap();
        assert_eq!(obj.key, key);
    }
}

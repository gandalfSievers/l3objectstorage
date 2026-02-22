//! Integration tests for bucket operations

use bytes::Bytes;

mod common {
    include!("common/mod.rs");
}

use common::create_test_storage;

#[tokio::test]
async fn test_bucket_lifecycle() {
    let (storage, _temp) = create_test_storage().await;

    // Create bucket
    let bucket = storage.create_bucket("lifecycle-test").await.unwrap();
    assert_eq!(bucket.name, "lifecycle-test");

    // Verify it exists
    assert!(storage.bucket_exists("lifecycle-test").await);

    // List buckets
    let buckets = storage.list_buckets().await;
    assert_eq!(buckets.len(), 1);
    assert_eq!(buckets[0].name, "lifecycle-test");

    // Delete bucket
    storage.delete_bucket("lifecycle-test").await.unwrap();
    assert!(!storage.bucket_exists("lifecycle-test").await);
}

#[tokio::test]
async fn test_multiple_buckets() {
    let (storage, _temp) = create_test_storage().await;

    // Create multiple buckets
    storage.create_bucket("bucket-a").await.unwrap();
    storage.create_bucket("bucket-b").await.unwrap();
    storage.create_bucket("bucket-c").await.unwrap();

    let buckets = storage.list_buckets().await;
    assert_eq!(buckets.len(), 3);

    // Should be sorted alphabetically
    assert_eq!(buckets[0].name, "bucket-a");
    assert_eq!(buckets[1].name, "bucket-b");
    assert_eq!(buckets[2].name, "bucket-c");
}

#[tokio::test]
async fn test_bucket_with_objects() {
    let (storage, _temp) = create_test_storage().await;

    storage.create_bucket("object-test").await.unwrap();

    // Add an object
    storage
        .put_object("object-test", "key1", Bytes::from("data"), None, None)
        .await
        .unwrap();

    // Cannot delete non-empty bucket
    let result = storage.delete_bucket("object-test").await;
    assert!(result.is_err());

    // Delete the object first
    storage.delete_object("object-test", "key1").await.unwrap();

    // Now we can delete the bucket
    storage.delete_bucket("object-test").await.unwrap();
}

#[tokio::test]
async fn test_bucket_name_validation() {
    let (storage, _temp) = create_test_storage().await;

    // Too short
    assert!(storage.create_bucket("ab").await.is_err());

    // Too long (65 chars)
    let long_name = "a".repeat(65);
    assert!(storage.create_bucket(&long_name).await.is_err());

    // Invalid characters
    assert!(storage.create_bucket("bucket_name").await.is_err());

    // Valid names
    assert!(storage.create_bucket("valid-bucket").await.is_ok());
    assert!(storage.create_bucket("bucket.with.dots").await.is_ok());
    assert!(storage.create_bucket("123numeric").await.is_ok());
}

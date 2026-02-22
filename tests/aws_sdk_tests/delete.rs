use super::*;

#[tokio::test]
#[ignore]
async fn test_delete_objects_batch() {
    let client = create_s3_client().await;
    let bucket = "sdk-delete-batch-test";

    // Setup: create bucket
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Create 5 objects
    for i in 0..5 {
        client
            .put_object()
            .bucket(bucket)
            .key(format!("key-{}", i))
            .body(Bytes::from("data").into())
            .send()
            .await
            .unwrap();
    }

    // Delete 3 of them in one batch request
    let objects_to_delete: Vec<ObjectIdentifier> = (0..3)
        .map(|i| {
            ObjectIdentifier::builder()
                .key(format!("key-{}", i))
                .build()
                .unwrap()
        })
        .collect();

    let delete = Delete::builder()
        .set_objects(Some(objects_to_delete))
        .build()
        .unwrap();

    let response = client
        .delete_objects()
        .bucket(bucket)
        .delete(delete)
        .send()
        .await
        .expect("Failed to delete objects");

    // Verify 3 objects were deleted
    assert_eq!(response.deleted().len(), 3);

    // Verify remaining objects (key-3 and key-4)
    let list = client
        .list_objects_v2()
        .bucket(bucket)
        .send()
        .await
        .unwrap();

    assert_eq!(list.key_count(), Some(2));

    // Cleanup remaining objects
    for i in 3..5 {
        client
            .delete_object()
            .bucket(bucket)
            .key(format!("key-{}", i))
            .send()
            .await
            .unwrap();
    }
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_delete_objects_with_nonexistent() {
    let client = create_s3_client().await;
    let bucket = "sdk-delete-nonexistent-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Create one object
    client
        .put_object()
        .bucket(bucket)
        .key("existing-key")
        .body(Bytes::from("data").into())
        .send()
        .await
        .unwrap();

    // Delete both existing and non-existing (S3 treats non-existent as success)
    let objects_to_delete = vec![
        ObjectIdentifier::builder()
            .key("existing-key")
            .build()
            .unwrap(),
        ObjectIdentifier::builder()
            .key("nonexistent-key")
            .build()
            .unwrap(),
    ];

    let delete = Delete::builder()
        .set_objects(Some(objects_to_delete))
        .build()
        .unwrap();

    let response = client
        .delete_objects()
        .bucket(bucket)
        .delete(delete)
        .send()
        .await
        .expect("Failed to delete objects");

    // S3 treats non-existent keys as successful deletes
    assert_eq!(response.deleted().len(), 2);

    let _ = client.delete_bucket().bucket(bucket).send().await;
}

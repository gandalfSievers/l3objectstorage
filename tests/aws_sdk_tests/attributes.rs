use super::*;

#[tokio::test]
#[ignore]
async fn test_get_object_attributes() {
    use aws_sdk_s3::types::ObjectAttributes;

    let client = create_s3_client().await;
    let bucket = "sdk-get-attrs-test";

    // Create bucket and object
    let _ = client.create_bucket().bucket(bucket).send().await;

    client
        .put_object()
        .bucket(bucket)
        .key("attrs-key")
        .body(Bytes::from("test content for attributes").into())
        .send()
        .await
        .expect("Failed to put object");

    // Get object attributes
    let result = client
        .get_object_attributes()
        .bucket(bucket)
        .key("attrs-key")
        .object_attributes(ObjectAttributes::Etag)
        .object_attributes(ObjectAttributes::ObjectSize)
        .object_attributes(ObjectAttributes::StorageClass)
        .send()
        .await
        .expect("Failed to get object attributes");

    // Verify attributes
    assert!(result.e_tag().is_some(), "Should have ETag");
    assert_eq!(result.object_size(), Some(27)); // "test content for attributes" = 27 bytes
    assert!(result.storage_class().is_some(), "Should have StorageClass");

    // Cleanup
    client
        .delete_object()
        .bucket(bucket)
        .key("attrs-key")
        .send()
        .await
        .ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_get_object_attributes_with_version() {
    use aws_sdk_s3::types::ObjectAttributes;

    let client = create_s3_client().await;
    let bucket = "sdk-get-attrs-version-test";

    // Create bucket
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

    // Put two versions with different sizes
    let put1 = client
        .put_object()
        .bucket(bucket)
        .key("attrs-version-key")
        .body(Bytes::from("v1").into())
        .send()
        .await
        .expect("Failed to put first version");
    let version1 = put1.version_id().unwrap().to_string();

    let put2 = client
        .put_object()
        .bucket(bucket)
        .key("attrs-version-key")
        .body(Bytes::from("version two is longer").into())
        .send()
        .await
        .expect("Failed to put second version");
    let version2 = put2.version_id().unwrap().to_string();

    // Get attributes for specific version
    let result = client
        .get_object_attributes()
        .bucket(bucket)
        .key("attrs-version-key")
        .version_id(&version1)
        .object_attributes(ObjectAttributes::ObjectSize)
        .send()
        .await
        .expect("Failed to get object attributes");

    assert_eq!(result.object_size(), Some(2)); // "v1"

    // Get attributes for current version
    let result2 = client
        .get_object_attributes()
        .bucket(bucket)
        .key("attrs-version-key")
        .object_attributes(ObjectAttributes::ObjectSize)
        .send()
        .await
        .expect("Failed to get object attributes");

    assert_eq!(result2.object_size(), Some(21)); // "version two is longer"

    // Cleanup
    client
        .delete_object()
        .bucket(bucket)
        .key("attrs-version-key")
        .version_id(&version1)
        .send()
        .await
        .ok();
    client
        .delete_object()
        .bucket(bucket)
        .key("attrs-version-key")
        .version_id(&version2)
        .send()
        .await
        .ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

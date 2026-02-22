use super::*;

#[tokio::test]
#[ignore]
async fn test_put_get_public_access_block() {
    use aws_sdk_s3::types::PublicAccessBlockConfiguration;

    let client = create_s3_client().await;
    let bucket = "sdk-public-access-block-test";

    // Create bucket
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Create public access block configuration - block everything
    let config = PublicAccessBlockConfiguration::builder()
        .block_public_acls(true)
        .ignore_public_acls(true)
        .block_public_policy(true)
        .restrict_public_buckets(true)
        .build();

    // Put public access block configuration
    client
        .put_public_access_block()
        .bucket(bucket)
        .public_access_block_configuration(config)
        .send()
        .await
        .expect("Failed to put public access block");

    // Get public access block configuration
    let result = client
        .get_public_access_block()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to get public access block");

    let config = result.public_access_block_configuration().expect("Should have config");
    assert_eq!(config.block_public_acls(), Some(true));
    assert_eq!(config.ignore_public_acls(), Some(true));
    assert_eq!(config.block_public_policy(), Some(true));
    assert_eq!(config.restrict_public_buckets(), Some(true));

    // Cleanup
    let _ = client.delete_public_access_block().bucket(bucket).send().await;
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_delete_public_access_block() {
    use aws_sdk_s3::types::PublicAccessBlockConfiguration;

    let client = create_s3_client().await;
    let bucket = "sdk-delete-public-access-block-test";

    // Create bucket
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Put public access block configuration
    let config = PublicAccessBlockConfiguration::builder()
        .block_public_acls(true)
        .build();

    client
        .put_public_access_block()
        .bucket(bucket)
        .public_access_block_configuration(config)
        .send()
        .await
        .expect("Failed to put public access block");

    // Delete public access block configuration
    client
        .delete_public_access_block()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to delete public access block");

    // Get should now fail with NoSuchPublicAccessBlockConfiguration
    let result = client
        .get_public_access_block()
        .bucket(bucket)
        .send()
        .await;

    assert!(
        result.is_err(),
        "GetPublicAccessBlock should fail after deletion"
    );

    // Cleanup
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_public_access_block_no_configuration() {
    let client = create_s3_client().await;
    let bucket = "sdk-no-public-access-block-test";

    // Create bucket
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Get public access block should fail on bucket without config
    let result = client
        .get_public_access_block()
        .bucket(bucket)
        .send()
        .await;

    assert!(
        result.is_err(),
        "GetPublicAccessBlock should fail when no configuration exists"
    );
    let err_str = format!("{:?}", result.err().unwrap());
    assert!(
        err_str.contains("NoSuchPublicAccessBlockConfiguration") || err_str.contains("404"),
        "Should indicate no public access block configuration: {}",
        err_str
    );

    // Cleanup
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_public_access_block_partial_config() {
    use aws_sdk_s3::types::PublicAccessBlockConfiguration;

    let client = create_s3_client().await;
    let bucket = "sdk-partial-public-access-block-test";

    // Create bucket
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Only set some fields
    let config = PublicAccessBlockConfiguration::builder()
        .block_public_acls(true)
        .block_public_policy(false)
        .build();

    client
        .put_public_access_block()
        .bucket(bucket)
        .public_access_block_configuration(config)
        .send()
        .await
        .expect("Failed to put public access block");

    // Get and verify
    let result = client
        .get_public_access_block()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to get public access block");

    let config = result.public_access_block_configuration().expect("Should have config");
    assert_eq!(config.block_public_acls(), Some(true));
    assert_eq!(config.block_public_policy(), Some(false));
    // Fields not explicitly set should default to false
    assert!(config.ignore_public_acls().is_none() || config.ignore_public_acls() == Some(false));
    assert!(config.restrict_public_buckets().is_none() || config.restrict_public_buckets() == Some(false));

    // Cleanup
    let _ = client.delete_public_access_block().bucket(bucket).send().await;
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

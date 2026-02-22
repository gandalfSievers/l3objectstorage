use super::*;
use aws_sdk_s3::types::{ObjectOwnership, OwnershipControls, OwnershipControlsRule};

#[tokio::test]
#[ignore]
async fn test_put_get_ownership_controls_bucket_owner_enforced() {
    let client = create_s3_client().await;
    let bucket = "sdk-ownership-controls-enforced-test";

    // Create bucket
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Create ownership controls with BucketOwnerEnforced
    let rule = OwnershipControlsRule::builder()
        .object_ownership(ObjectOwnership::BucketOwnerEnforced)
        .build()
        .expect("Failed to build rule");

    let ownership_controls = OwnershipControls::builder()
        .rules(rule)
        .build()
        .expect("Failed to build ownership controls");

    // Put ownership controls
    client
        .put_bucket_ownership_controls()
        .bucket(bucket)
        .ownership_controls(ownership_controls)
        .send()
        .await
        .expect("Failed to put ownership controls");

    // Get ownership controls
    let result = client
        .get_bucket_ownership_controls()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to get ownership controls");

    let controls = result.ownership_controls().expect("Should have controls");
    assert_eq!(controls.rules().len(), 1);
    assert_eq!(
        controls.rules()[0].object_ownership(),
        &ObjectOwnership::BucketOwnerEnforced
    );

    // Cleanup
    let _ = client
        .delete_bucket_ownership_controls()
        .bucket(bucket)
        .send()
        .await;
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_put_get_ownership_controls_bucket_owner_preferred() {
    let client = create_s3_client().await;
    let bucket = "sdk-ownership-controls-preferred-test";

    // Create bucket
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Create ownership controls with BucketOwnerPreferred
    let rule = OwnershipControlsRule::builder()
        .object_ownership(ObjectOwnership::BucketOwnerPreferred)
        .build()
        .expect("Failed to build rule");

    let ownership_controls = OwnershipControls::builder()
        .rules(rule)
        .build()
        .expect("Failed to build ownership controls");

    // Put ownership controls
    client
        .put_bucket_ownership_controls()
        .bucket(bucket)
        .ownership_controls(ownership_controls)
        .send()
        .await
        .expect("Failed to put ownership controls");

    // Get ownership controls
    let result = client
        .get_bucket_ownership_controls()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to get ownership controls");

    let controls = result.ownership_controls().expect("Should have controls");
    assert_eq!(controls.rules().len(), 1);
    assert_eq!(
        controls.rules()[0].object_ownership(),
        &ObjectOwnership::BucketOwnerPreferred
    );

    // Cleanup
    let _ = client
        .delete_bucket_ownership_controls()
        .bucket(bucket)
        .send()
        .await;
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_put_get_ownership_controls_object_writer() {
    let client = create_s3_client().await;
    let bucket = "sdk-ownership-controls-writer-test";

    // Create bucket
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Create ownership controls with ObjectWriter
    let rule = OwnershipControlsRule::builder()
        .object_ownership(ObjectOwnership::ObjectWriter)
        .build()
        .expect("Failed to build rule");

    let ownership_controls = OwnershipControls::builder()
        .rules(rule)
        .build()
        .expect("Failed to build ownership controls");

    // Put ownership controls
    client
        .put_bucket_ownership_controls()
        .bucket(bucket)
        .ownership_controls(ownership_controls)
        .send()
        .await
        .expect("Failed to put ownership controls");

    // Get ownership controls
    let result = client
        .get_bucket_ownership_controls()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to get ownership controls");

    let controls = result.ownership_controls().expect("Should have controls");
    assert_eq!(controls.rules().len(), 1);
    assert_eq!(
        controls.rules()[0].object_ownership(),
        &ObjectOwnership::ObjectWriter
    );

    // Cleanup
    let _ = client
        .delete_bucket_ownership_controls()
        .bucket(bucket)
        .send()
        .await;
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_delete_ownership_controls() {
    let client = create_s3_client().await;
    let bucket = "sdk-delete-ownership-controls-test";

    // Create bucket
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Put ownership controls
    let rule = OwnershipControlsRule::builder()
        .object_ownership(ObjectOwnership::BucketOwnerEnforced)
        .build()
        .expect("Failed to build rule");

    let ownership_controls = OwnershipControls::builder()
        .rules(rule)
        .build()
        .expect("Failed to build ownership controls");

    client
        .put_bucket_ownership_controls()
        .bucket(bucket)
        .ownership_controls(ownership_controls)
        .send()
        .await
        .expect("Failed to put ownership controls");

    // Delete ownership controls
    client
        .delete_bucket_ownership_controls()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to delete ownership controls");

    // Get should now fail with OwnershipControlsNotFoundError
    let result = client
        .get_bucket_ownership_controls()
        .bucket(bucket)
        .send()
        .await;

    assert!(
        result.is_err(),
        "GetBucketOwnershipControls should fail after deletion"
    );

    // Cleanup
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_get_ownership_controls_no_configuration() {
    let client = create_s3_client().await;
    let bucket = "sdk-no-ownership-controls-test";

    // Create bucket
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Get ownership controls should fail on bucket without config
    let result = client
        .get_bucket_ownership_controls()
        .bucket(bucket)
        .send()
        .await;

    assert!(
        result.is_err(),
        "GetBucketOwnershipControls should fail when no configuration exists"
    );
    let err_str = format!("{:?}", result.err().unwrap());
    assert!(
        err_str.contains("OwnershipControlsNotFoundError") || err_str.contains("404"),
        "Should indicate no ownership controls configuration: {}",
        err_str
    );

    // Cleanup
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

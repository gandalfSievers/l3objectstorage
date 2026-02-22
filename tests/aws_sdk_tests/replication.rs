//! Bucket Replication Configuration integration tests

use super::*;

#[tokio::test]
#[ignore]
async fn test_put_get_bucket_replication() {
    use aws_sdk_s3::types::{
        Destination, ReplicationConfiguration, ReplicationRule, ReplicationRuleFilter,
        ReplicationRuleStatus,
    };

    let client = create_s3_client().await;
    let bucket = "sdk-replication-test";

    // Create bucket and enable versioning (required for replication)
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Enable versioning
    use aws_sdk_s3::types::{BucketVersioningStatus, VersioningConfiguration};
    client
        .put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .expect("Failed to enable versioning");

    // Create replication configuration
    let destination = Destination::builder()
        .bucket("arn:aws:s3:::destination-bucket")
        .build()
        .expect("Failed to build destination");

    let rule = ReplicationRule::builder()
        .id("rule-1")
        .status(ReplicationRuleStatus::Enabled)
        .priority(1)
        .filter(ReplicationRuleFilter::builder().prefix("documents/").build())
        .destination(destination)
        .build()
        .expect("Failed to build rule");

    let replication_config = ReplicationConfiguration::builder()
        .role("arn:aws:iam::123456789012:role/replication-role")
        .rules(rule)
        .build()
        .expect("Failed to build replication config");

    // Put replication configuration
    client
        .put_bucket_replication()
        .bucket(bucket)
        .replication_configuration(replication_config)
        .send()
        .await
        .expect("Failed to put replication configuration");

    // Get replication configuration
    let result = client
        .get_bucket_replication()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to get replication configuration");

    // Verify configuration
    let config = result
        .replication_configuration()
        .expect("Should have replication config");
    assert_eq!(
        config.role(),
        "arn:aws:iam::123456789012:role/replication-role"
    );
    assert_eq!(config.rules().len(), 1);
    assert_eq!(config.rules()[0].id(), Some("rule-1"));

    // Cleanup
    let _ = client
        .delete_bucket_replication()
        .bucket(bucket)
        .send()
        .await;
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_delete_bucket_replication() {
    use aws_sdk_s3::types::{
        Destination, ReplicationConfiguration, ReplicationRule, ReplicationRuleStatus,
    };

    let client = create_s3_client().await;
    let bucket = "sdk-delete-replication-test";

    // Create bucket and enable versioning
    let _ = client.create_bucket().bucket(bucket).send().await;

    use aws_sdk_s3::types::{BucketVersioningStatus, VersioningConfiguration};
    client
        .put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .expect("Failed to enable versioning");

    // Set replication configuration
    let destination = Destination::builder()
        .bucket("arn:aws:s3:::dest-bucket")
        .build()
        .expect("Failed to build destination");

    let rule = ReplicationRule::builder()
        .id("rule")
        .status(ReplicationRuleStatus::Enabled)
        .destination(destination)
        .build()
        .expect("Failed to build rule");

    let config = ReplicationConfiguration::builder()
        .role("arn:aws:iam::123456789012:role/role")
        .rules(rule)
        .build()
        .expect("Failed to build config");

    client
        .put_bucket_replication()
        .bucket(bucket)
        .replication_configuration(config)
        .send()
        .await
        .expect("Failed to put replication");

    // Delete replication configuration
    client
        .delete_bucket_replication()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to delete replication");

    // Get should now fail
    let result = client.get_bucket_replication().bucket(bucket).send().await;
    assert!(
        result.is_err(),
        "GetBucketReplication should fail after deletion"
    );

    // Cleanup
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_get_replication_not_configured() {
    let client = create_s3_client().await;
    let bucket = "sdk-no-replication-test";

    // Create bucket
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Get replication should fail when not configured
    let result = client.get_bucket_replication().bucket(bucket).send().await;

    assert!(
        result.is_err(),
        "GetBucketReplication should fail when not configured"
    );
    let err_str = format!("{:?}", result.err().unwrap());
    assert!(
        err_str.contains("ReplicationConfigurationNotFound") || err_str.contains("404"),
        "Should indicate no replication configuration: {}",
        err_str
    );

    // Cleanup
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_put_replication_multiple_rules() {
    use aws_sdk_s3::types::{
        Destination, ReplicationConfiguration, ReplicationRule, ReplicationRuleFilter,
        ReplicationRuleStatus,
    };

    let client = create_s3_client().await;
    let bucket = "sdk-replication-multi-rules-test";

    // Create bucket and enable versioning
    let _ = client.create_bucket().bucket(bucket).send().await;

    use aws_sdk_s3::types::{BucketVersioningStatus, VersioningConfiguration};
    client
        .put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .expect("Failed to enable versioning");

    // Create multiple rules
    let dest1 = Destination::builder()
        .bucket("arn:aws:s3:::dest-bucket-1")
        .build()
        .expect("Failed to build destination");

    let rule1 = ReplicationRule::builder()
        .id("docs-rule")
        .status(ReplicationRuleStatus::Enabled)
        .priority(1)
        .filter(ReplicationRuleFilter::builder().prefix("docs/").build())
        .destination(dest1)
        .build()
        .expect("Failed to build rule");

    let dest2 = Destination::builder()
        .bucket("arn:aws:s3:::dest-bucket-2")
        .build()
        .expect("Failed to build destination");

    let rule2 = ReplicationRule::builder()
        .id("images-rule")
        .status(ReplicationRuleStatus::Enabled)
        .priority(2)
        .filter(ReplicationRuleFilter::builder().prefix("images/").build())
        .destination(dest2)
        .build()
        .expect("Failed to build rule");

    let config = ReplicationConfiguration::builder()
        .role("arn:aws:iam::123456789012:role/role")
        .rules(rule1)
        .rules(rule2)
        .build()
        .expect("Failed to build config");

    client
        .put_bucket_replication()
        .bucket(bucket)
        .replication_configuration(config)
        .send()
        .await
        .expect("Failed to put replication");

    // Get and verify
    let result = client
        .get_bucket_replication()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to get replication");

    let config = result
        .replication_configuration()
        .expect("Should have config");
    assert_eq!(config.rules().len(), 2);

    // Cleanup
    let _ = client
        .delete_bucket_replication()
        .bucket(bucket)
        .send()
        .await;
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

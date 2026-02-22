use super::*;

#[tokio::test]
#[ignore]
async fn test_bucket_lifecycle_configuration() {
    use aws_sdk_s3::types::{
        BucketLifecycleConfiguration, ExpirationStatus, LifecycleExpiration, LifecycleRule,
        LifecycleRuleFilter,
    };

    let client = create_s3_client().await;
    let bucket = "sdk-lifecycle-config-test";

    // Create bucket
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Create lifecycle rule: expire objects after 30 days
    let expiration = LifecycleExpiration::builder().days(30).build();

    let filter = LifecycleRuleFilter::builder()
        .prefix("logs/")
        .build();

    let rule = LifecycleRule::builder()
        .id("expire-after-30-days")
        .status(ExpirationStatus::Enabled)
        .filter(filter)
        .expiration(expiration)
        .build()
        .expect("Failed to build lifecycle rule");

    let lifecycle_config = BucketLifecycleConfiguration::builder()
        .rules(rule)
        .build()
        .expect("Failed to build lifecycle configuration");

    // Put lifecycle configuration
    client
        .put_bucket_lifecycle_configuration()
        .bucket(bucket)
        .lifecycle_configuration(lifecycle_config)
        .send()
        .await
        .expect("Failed to put lifecycle configuration");

    // Get lifecycle configuration
    let result = client
        .get_bucket_lifecycle_configuration()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to get lifecycle configuration");

    let rules = result.rules();
    assert_eq!(rules.len(), 1, "Should have 1 lifecycle rule");

    let rule = &rules[0];
    assert_eq!(rule.id(), Some("expire-after-30-days"));
    assert_eq!(*rule.status(), ExpirationStatus::Enabled);

    // Verify expiration days
    let expiration = rule.expiration().expect("Should have expiration");
    assert_eq!(expiration.days(), Some(30));

    // Delete lifecycle configuration
    client
        .delete_bucket_lifecycle()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to delete lifecycle configuration");

    // Verify lifecycle is deleted (should return error)
    let result = client
        .get_bucket_lifecycle_configuration()
        .bucket(bucket)
        .send()
        .await;

    assert!(
        result.is_err(),
        "GetBucketLifecycleConfiguration should fail after deletion"
    );

    // Cleanup
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_bucket_lifecycle_no_configuration() {
    let client = create_s3_client().await;
    let bucket = "sdk-lifecycle-no-config-test";

    // Create bucket
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Get lifecycle configuration on bucket without lifecycle should fail
    let result = client
        .get_bucket_lifecycle_configuration()
        .bucket(bucket)
        .send()
        .await;

    assert!(
        result.is_err(),
        "GetBucketLifecycleConfiguration should fail when no configuration exists"
    );
    let err_str = format!("{:?}", result.err().unwrap());
    assert!(
        err_str.contains("NoSuchLifecycleConfiguration") || err_str.contains("404"),
        "Should indicate no lifecycle configuration: {}",
        err_str
    );

    // Cleanup
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_bucket_lifecycle_multiple_rules() {
    use aws_sdk_s3::types::{
        BucketLifecycleConfiguration, ExpirationStatus, LifecycleExpiration, LifecycleRule,
        LifecycleRuleFilter, NoncurrentVersionExpiration,
    };

    let client = create_s3_client().await;
    let bucket = "sdk-lifecycle-multi-rules-test";

    // Create bucket
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Rule 1: Expire objects in logs/ after 7 days
    let rule1 = LifecycleRule::builder()
        .id("expire-logs-7-days")
        .status(ExpirationStatus::Enabled)
        .filter(LifecycleRuleFilter::builder().prefix("logs/").build())
        .expiration(LifecycleExpiration::builder().days(7).build())
        .build()
        .expect("Failed to build rule 1");

    // Rule 2: Expire objects in temp/ after 1 day
    let rule2 = LifecycleRule::builder()
        .id("expire-temp-1-day")
        .status(ExpirationStatus::Enabled)
        .filter(LifecycleRuleFilter::builder().prefix("temp/").build())
        .expiration(LifecycleExpiration::builder().days(1).build())
        .build()
        .expect("Failed to build rule 2");

    // Rule 3: Delete noncurrent versions after 30 days (disabled)
    let rule3 = LifecycleRule::builder()
        .id("cleanup-old-versions")
        .status(ExpirationStatus::Disabled)
        .filter(LifecycleRuleFilter::builder().prefix("").build())
        .noncurrent_version_expiration(
            NoncurrentVersionExpiration::builder()
                .noncurrent_days(30)
                .build(),
        )
        .build()
        .expect("Failed to build rule 3");

    let lifecycle_config = BucketLifecycleConfiguration::builder()
        .rules(rule1)
        .rules(rule2)
        .rules(rule3)
        .build()
        .expect("Failed to build lifecycle configuration");

    // Put lifecycle configuration
    client
        .put_bucket_lifecycle_configuration()
        .bucket(bucket)
        .lifecycle_configuration(lifecycle_config)
        .send()
        .await
        .expect("Failed to put lifecycle configuration");

    // Get and verify all rules
    let result = client
        .get_bucket_lifecycle_configuration()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to get lifecycle configuration");

    let rules = result.rules();
    assert_eq!(rules.len(), 3, "Should have 3 lifecycle rules");

    // Verify rule IDs
    let rule_ids: Vec<_> = rules.iter().filter_map(|r| r.id()).collect();
    assert!(rule_ids.contains(&"expire-logs-7-days"));
    assert!(rule_ids.contains(&"expire-temp-1-day"));
    assert!(rule_ids.contains(&"cleanup-old-versions"));

    // Verify logs rule
    let logs_rule = rules.iter().find(|r| r.id() == Some("expire-logs-7-days")).unwrap();
    assert_eq!(*logs_rule.status(), ExpirationStatus::Enabled);
    assert_eq!(logs_rule.expiration().and_then(|e| e.days()), Some(7));

    // Verify temp rule
    let temp_rule = rules.iter().find(|r| r.id() == Some("expire-temp-1-day")).unwrap();
    assert_eq!(*temp_rule.status(), ExpirationStatus::Enabled);
    assert_eq!(temp_rule.expiration().and_then(|e| e.days()), Some(1));

    // Verify disabled rule
    let disabled_rule = rules.iter().find(|r| r.id() == Some("cleanup-old-versions")).unwrap();
    assert_eq!(*disabled_rule.status(), ExpirationStatus::Disabled);

    // Cleanup
    client
        .delete_bucket_lifecycle()
        .bucket(bucket)
        .send()
        .await
        .ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

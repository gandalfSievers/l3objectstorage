use super::*;

#[tokio::test]
#[ignore]
async fn test_bucket_cors() {
    let client = create_s3_client().await;
    let bucket = "sdk-cors-test";

    // Create bucket
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Put CORS configuration
    let cors_rule = CorsRule::builder()
        .allowed_methods("GET")
        .allowed_methods("PUT")
        .allowed_origins("*")
        .allowed_headers("*")
        .max_age_seconds(3000)
        .build()
        .unwrap();

    let cors_config = CorsConfiguration::builder()
        .cors_rules(cors_rule)
        .build()
        .unwrap();

    client
        .put_bucket_cors()
        .bucket(bucket)
        .cors_configuration(cors_config)
        .send()
        .await
        .expect("Failed to put CORS");

    // Get CORS configuration
    let result = client
        .get_bucket_cors()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to get CORS");

    assert_eq!(result.cors_rules().len(), 1);

    let rule = &result.cors_rules()[0];
    assert!(rule.allowed_methods().contains(&"GET".to_string()));
    assert!(rule.allowed_methods().contains(&"PUT".to_string()));
    assert!(rule.allowed_origins().contains(&"*".to_string()));
    assert_eq!(rule.max_age_seconds(), Some(3000));

    // Delete CORS
    client
        .delete_bucket_cors()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to delete CORS");

    // Verify CORS is deleted (should error)
    let result = client
        .get_bucket_cors()
        .bucket(bucket)
        .send()
        .await;
    assert!(result.is_err());

    // Cleanup
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_bucket_policy() {
    let client = create_s3_client().await;
    let bucket = "sdk-policy-test";

    // Create bucket
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Get policy should fail initially (no policy set)
    let result = client.get_bucket_policy().bucket(bucket).send().await;
    assert!(result.is_err(), "Should fail when no policy exists");

    // Put a simple bucket policy
    let policy = r#"{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "PublicReadGetObject",
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": "arn:aws:s3:::sdk-policy-test/*"
            }
        ]
    }"#;

    client
        .put_bucket_policy()
        .bucket(bucket)
        .policy(policy)
        .send()
        .await
        .expect("Failed to put bucket policy");

    // Get policy and verify
    let result = client
        .get_bucket_policy()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to get bucket policy");

    let retrieved_policy = result.policy().unwrap();
    // Verify it's valid JSON and contains expected fields
    let parsed: serde_json::Value = serde_json::from_str(retrieved_policy)
        .expect("Policy should be valid JSON");
    assert_eq!(parsed["Version"], "2012-10-17");
    assert!(parsed["Statement"].is_array());

    // Delete policy
    client
        .delete_bucket_policy()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to delete bucket policy");

    // Verify policy is deleted (get should fail)
    let result = client.get_bucket_policy().bucket(bucket).send().await;
    assert!(result.is_err(), "Should fail after policy deleted");

    // Cleanup
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_bucket_policy_status() {
    let client = create_s3_client().await;
    let bucket = "sdk-policy-status-test";

    // Create bucket
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Get policy status should fail initially (no policy set)
    let result = client.get_bucket_policy_status().bucket(bucket).send().await;
    assert!(result.is_err(), "Should fail when no policy exists");

    // Put a public bucket policy (Principal: "*")
    let public_policy = r#"{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "PublicReadGetObject",
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": "arn:aws:s3:::sdk-policy-status-test/*"
            }
        ]
    }"#;

    client
        .put_bucket_policy()
        .bucket(bucket)
        .policy(public_policy)
        .send()
        .await
        .expect("Failed to put bucket policy");

    // Get policy status - should indicate public
    let result = client
        .get_bucket_policy_status()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to get bucket policy status");

    let status = result.policy_status().expect("Should have policy status");
    assert_eq!(status.is_public(), Some(true), "Policy with Principal:* should be public");

    // Now put a non-public policy (Principal is a specific account)
    let private_policy = r#"{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "PrivateAccess",
                "Effect": "Allow",
                "Principal": {"AWS": "arn:aws:iam::123456789012:root"},
                "Action": "s3:GetObject",
                "Resource": "arn:aws:s3:::sdk-policy-status-test/*"
            }
        ]
    }"#;

    client
        .put_bucket_policy()
        .bucket(bucket)
        .policy(private_policy)
        .send()
        .await
        .expect("Failed to put private bucket policy");

    // Get policy status - should indicate not public
    let result = client
        .get_bucket_policy_status()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to get bucket policy status");

    let status = result.policy_status().expect("Should have policy status");
    assert_eq!(status.is_public(), Some(false), "Policy with specific Principal should not be public");

    // Delete policy and verify status fails
    client
        .delete_bucket_policy()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to delete bucket policy");

    let result = client.get_bucket_policy_status().bucket(bucket).send().await;
    assert!(result.is_err(), "Should fail after policy deleted");

    // Cleanup
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

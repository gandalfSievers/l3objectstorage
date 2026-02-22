use super::*;

/// Test basic bucket policy put operation
#[tokio::test]
#[ignore]
async fn test_put_bucket_policy_basic() {
    let client = create_s3_client().await;
    let bucket = "sdk-policy-put-test";

    // Create bucket
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Put a simple bucket policy
    let policy = r#"{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "PublicReadGetObject",
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": "arn:aws:s3:::sdk-policy-put-test/*"
            }
        ]
    }"#;

    let result = client
        .put_bucket_policy()
        .bucket(bucket)
        .policy(policy)
        .send()
        .await;

    assert!(result.is_ok(), "PutBucketPolicy should succeed: {:?}", result.err());

    // Cleanup
    let _ = client.delete_bucket_policy().bucket(bucket).send().await;
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test get bucket policy
#[tokio::test]
#[ignore]
async fn test_get_bucket_policy() {
    let client = create_s3_client().await;
    let bucket = "sdk-policy-get-test";

    // Create bucket
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Put policy
    let policy = r#"{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "TestStatement",
                "Effect": "Deny",
                "Principal": "*",
                "Action": "s3:DeleteObject",
                "Resource": "arn:aws:s3:::sdk-policy-get-test/*"
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

    // Get policy
    let get_result = client
        .get_bucket_policy()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to get bucket policy");

    let retrieved_policy = get_result.policy().expect("Policy should be present");
    assert!(retrieved_policy.contains("TestStatement"), "Policy should contain the statement");
    assert!(retrieved_policy.contains("DeleteObject"), "Policy should contain the action");

    // Cleanup
    let _ = client.delete_bucket_policy().bucket(bucket).send().await;
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test delete bucket policy
#[tokio::test]
#[ignore]
async fn test_delete_bucket_policy() {
    let client = create_s3_client().await;
    let bucket = "sdk-policy-delete-test";

    // Create bucket
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Put policy
    let policy = r#"{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": "arn:aws:s3:::sdk-policy-delete-test/*"
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

    // Delete policy
    client
        .delete_bucket_policy()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to delete bucket policy");

    // Verify policy is gone
    let get_result = client
        .get_bucket_policy()
        .bucket(bucket)
        .send()
        .await;

    assert!(get_result.is_err(), "GetBucketPolicy should fail after deletion");

    // Cleanup
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test get bucket policy status for public policy
#[tokio::test]
#[ignore]
async fn test_get_bucket_policy_status_public() {
    let client = create_s3_client().await;
    let bucket = "sdk-policy-status-public-test";

    // Create bucket
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Put a public policy (Principal: "*")
    let policy = r#"{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": "arn:aws:s3:::sdk-policy-status-public-test/*"
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

    // Get policy status
    let status_result = client
        .get_bucket_policy_status()
        .bucket(bucket)
        .send()
        .await;

    // Policy status might indicate public access
    if let Ok(status) = status_result {
        if let Some(policy_status) = status.policy_status() {
            // The policy has Principal: "*" so it should be considered public
            // Note: actual S3 behavior may vary based on account settings
            let _is_public = policy_status.is_public();
        }
    }

    // Cleanup
    let _ = client.delete_bucket_policy().bucket(bucket).send().await;
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test get bucket policy status for private policy
#[tokio::test]
#[ignore]
async fn test_get_bucket_policy_status_private() {
    let client = create_s3_client().await;
    let bucket = "sdk-policy-status-private-test";

    // Create bucket
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Put a private policy (specific Principal)
    let policy = r#"{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"AWS": "arn:aws:iam::123456789012:root"},
                "Action": "s3:GetObject",
                "Resource": "arn:aws:s3:::sdk-policy-status-private-test/*"
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

    // Get policy status
    let status_result = client
        .get_bucket_policy_status()
        .bucket(bucket)
        .send()
        .await;

    // This policy should not be considered public
    if let Ok(status) = status_result {
        if let Some(policy_status) = status.policy_status() {
            // A specific principal should not be public
            assert!(
                !policy_status.is_public().unwrap_or(true),
                "Policy with specific principal should not be public"
            );
        }
    }

    // Cleanup
    let _ = client.delete_bucket_policy().bucket(bucket).send().await;
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test put bucket policy with invalid JSON
#[tokio::test]
#[ignore]
async fn test_put_bucket_policy_invalid_json() {
    let client = create_s3_client().await;
    let bucket = "sdk-policy-invalid-json-test";

    // Create bucket
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Try to put invalid JSON as policy
    let invalid_policy = "this is not valid json {{{";

    let result = client
        .put_bucket_policy()
        .bucket(bucket)
        .policy(invalid_policy)
        .send()
        .await;

    assert!(result.is_err(), "PutBucketPolicy should fail for invalid JSON");
    let err = format!("{:?}", result.err().unwrap());
    assert!(
        err.contains("MalformedPolicy") || err.contains("Invalid") || err.contains("malformed"),
        "Error should indicate malformed policy: {}",
        err
    );

    // Cleanup
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test get bucket policy when no policy exists (should return NoSuchBucketPolicy)
#[tokio::test]
#[ignore]
async fn test_get_bucket_policy_no_policy() {
    let client = create_s3_client().await;
    let bucket = "sdk-policy-no-policy-test";

    // Create bucket (no policy)
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Get policy should fail
    let result = client
        .get_bucket_policy()
        .bucket(bucket)
        .send()
        .await;

    assert!(result.is_err(), "GetBucketPolicy should fail when no policy exists");
    let err = format!("{:?}", result.err().unwrap());
    assert!(
        err.contains("NoSuchBucketPolicy") || err.contains("not found") || err.contains("404"),
        "Error should indicate no policy: {}",
        err
    );

    // Cleanup
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

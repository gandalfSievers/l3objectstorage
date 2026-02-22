use super::*;

#[tokio::test]
#[ignore]
async fn test_bucket_acl() {
    let client = create_s3_client().await;
    let bucket = "sdk-acl-test";

    // Create bucket
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Get default ACL - should return owner with FULL_CONTROL
    let result = client
        .get_bucket_acl()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to get bucket ACL");

    // Verify owner exists
    let owner = result.owner().expect("ACL should have an owner");
    assert!(owner.id().is_some(), "Owner should have an ID");

    // Verify default grants (owner has FULL_CONTROL)
    let grants = result.grants();
    assert!(!grants.is_empty(), "Should have at least one grant");

    let owner_grant = grants
        .iter()
        .find(|g| g.permission() == Some(&Permission::FullControl));
    assert!(owner_grant.is_some(), "Owner should have FULL_CONTROL");

    // Cleanup
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_bucket_acl_canned() {
    let client = create_s3_client().await;
    let bucket = "sdk-acl-canned-test";

    // Create bucket with public-read ACL
    let _ = client
        .create_bucket()
        .bucket(bucket)
        .acl(BucketCannedAcl::PublicRead)
        .send()
        .await;

    // Get ACL and verify public read grant exists
    let result = client
        .get_bucket_acl()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to get bucket ACL");

    let grants = result.grants();

    // Should have owner FULL_CONTROL and AllUsers READ
    assert!(
        grants.len() >= 2,
        "Should have at least 2 grants for public-read"
    );

    let has_public_read = grants.iter().any(|g| {
        g.permission() == Some(&Permission::Read)
            && g.grantee()
                .and_then(|grantee| grantee.uri())
                .map(|uri| uri.contains("AllUsers"))
                .unwrap_or(false)
    });
    assert!(has_public_read, "Should have public READ grant");

    // Put ACL to change it back to private
    client
        .put_bucket_acl()
        .bucket(bucket)
        .acl(BucketCannedAcl::Private)
        .send()
        .await
        .expect("Failed to put bucket ACL");

    // Verify it's now private (only owner grant)
    let result = client
        .get_bucket_acl()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to get bucket ACL");

    let grants = result.grants();
    // Private should only have owner with FULL_CONTROL
    assert_eq!(grants.len(), 1, "Private ACL should have only 1 grant");
    assert_eq!(
        grants[0].permission(),
        Some(&Permission::FullControl),
        "Should be FULL_CONTROL"
    );

    // Cleanup
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_object_acl_default() {
    let client = create_s3_client().await;
    let bucket = "sdk-object-acl-test";

    // Create bucket and object
    let _ = client.create_bucket().bucket(bucket).send().await;

    client
        .put_object()
        .bucket(bucket)
        .key("test-object")
        .body(Bytes::from("test content").into())
        .send()
        .await
        .expect("Failed to put object");

    // Get object ACL - should return owner with FULL_CONTROL
    let result = client
        .get_object_acl()
        .bucket(bucket)
        .key("test-object")
        .send()
        .await
        .expect("Failed to get object ACL");

    // Verify owner exists
    let owner = result.owner().expect("ACL should have an owner");
    assert!(owner.id().is_some(), "Owner should have an ID");

    // Verify default grants (owner has FULL_CONTROL)
    let grants = result.grants();
    assert!(!grants.is_empty(), "Should have at least one grant");

    let owner_grant = grants
        .iter()
        .find(|g| g.permission() == Some(&Permission::FullControl));
    assert!(owner_grant.is_some(), "Owner should have FULL_CONTROL");

    // Cleanup
    client
        .delete_object()
        .bucket(bucket)
        .key("test-object")
        .send()
        .await
        .ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_object_acl_canned() {
    use aws_sdk_s3::types::ObjectCannedAcl;

    let client = create_s3_client().await;
    let bucket = "sdk-object-acl-canned-test";

    // Create bucket
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Put object with public-read ACL
    client
        .put_object()
        .bucket(bucket)
        .key("public-object")
        .body(Bytes::from("public content").into())
        .acl(ObjectCannedAcl::PublicRead)
        .send()
        .await
        .expect("Failed to put object with ACL");

    // Get object ACL and verify public read grant exists
    let result = client
        .get_object_acl()
        .bucket(bucket)
        .key("public-object")
        .send()
        .await
        .expect("Failed to get object ACL");

    let grants = result.grants();

    // Should have owner FULL_CONTROL and AllUsers READ
    assert!(
        grants.len() >= 2,
        "Should have at least 2 grants for public-read"
    );

    let has_public_read = grants.iter().any(|g| {
        g.permission() == Some(&Permission::Read)
            && g.grantee()
                .and_then(|grantee| grantee.uri())
                .map(|uri| uri.contains("AllUsers"))
                .unwrap_or(false)
    });
    assert!(has_public_read, "Should have public READ grant");

    // Cleanup
    client
        .delete_object()
        .bucket(bucket)
        .key("public-object")
        .send()
        .await
        .ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_put_object_acl() {
    use aws_sdk_s3::types::ObjectCannedAcl;

    let client = create_s3_client().await;
    let bucket = "sdk-put-object-acl-test";

    // Create bucket and object
    let _ = client.create_bucket().bucket(bucket).send().await;

    client
        .put_object()
        .bucket(bucket)
        .key("acl-test-object")
        .body(Bytes::from("test content").into())
        .send()
        .await
        .expect("Failed to put object");

    // Initially should be private (only owner grant)
    let initial_result = client
        .get_object_acl()
        .bucket(bucket)
        .key("acl-test-object")
        .send()
        .await
        .expect("Failed to get object ACL");

    assert_eq!(
        initial_result.grants().len(),
        1,
        "Private object should have only 1 grant"
    );

    // Change ACL to public-read
    client
        .put_object_acl()
        .bucket(bucket)
        .key("acl-test-object")
        .acl(ObjectCannedAcl::PublicRead)
        .send()
        .await
        .expect("Failed to put object ACL");

    // Verify ACL changed
    let result = client
        .get_object_acl()
        .bucket(bucket)
        .key("acl-test-object")
        .send()
        .await
        .expect("Failed to get object ACL");

    let grants = result.grants();
    assert!(grants.len() >= 2, "Should have at least 2 grants after public-read");

    let has_public_read = grants.iter().any(|g| {
        g.permission() == Some(&Permission::Read)
            && g.grantee()
                .and_then(|grantee| grantee.uri())
                .map(|uri| uri.contains("AllUsers"))
                .unwrap_or(false)
    });
    assert!(has_public_read, "Should have public READ grant");

    // Change back to private
    client
        .put_object_acl()
        .bucket(bucket)
        .key("acl-test-object")
        .acl(ObjectCannedAcl::Private)
        .send()
        .await
        .expect("Failed to put object ACL");

    // Verify it's now private
    let result = client
        .get_object_acl()
        .bucket(bucket)
        .key("acl-test-object")
        .send()
        .await
        .expect("Failed to get object ACL");

    assert_eq!(
        result.grants().len(),
        1,
        "Private ACL should have only 1 grant"
    );

    // Cleanup
    client
        .delete_object()
        .bucket(bucket)
        .key("acl-test-object")
        .send()
        .await
        .ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

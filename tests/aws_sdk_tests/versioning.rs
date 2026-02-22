use super::*;

#[tokio::test]
#[ignore]
async fn test_bucket_versioning() {
    let client = create_s3_client().await;
    let bucket = "sdk-versioning-test";

    // Create bucket
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Get versioning status (should be disabled by default - returns empty/no status)
    let result = client
        .get_bucket_versioning()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to get bucket versioning");

    // Initially versioning is not set (status is None)
    assert!(
        result.status().is_none()
            || result.status() == Some(&aws_sdk_s3::types::BucketVersioningStatus::Suspended)
    );

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

    // Verify versioning is enabled
    let result = client
        .get_bucket_versioning()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to get bucket versioning");

    assert_eq!(
        result.status(),
        Some(&aws_sdk_s3::types::BucketVersioningStatus::Enabled)
    );

    // Suspend versioning
    client
        .put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(
            aws_sdk_s3::types::VersioningConfiguration::builder()
                .status(aws_sdk_s3::types::BucketVersioningStatus::Suspended)
                .build(),
        )
        .send()
        .await
        .expect("Failed to suspend versioning");

    // Verify versioning is suspended
    let result = client
        .get_bucket_versioning()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to get bucket versioning");

    assert_eq!(
        result.status(),
        Some(&aws_sdk_s3::types::BucketVersioningStatus::Suspended)
    );

    // Cleanup
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_versioning_put_creates_versions() {
    let client = create_s3_client().await;
    let bucket = "sdk-versioning-put-test";

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

    // Put first version
    let put1 = client
        .put_object()
        .bucket(bucket)
        .key("versioned-key")
        .body(Bytes::from("version 1").into())
        .send()
        .await
        .expect("Failed to put first version");

    let version1 = put1.version_id().expect("Should return version ID");
    assert!(!version1.is_empty(), "Version ID should not be empty");

    // Put second version
    let put2 = client
        .put_object()
        .bucket(bucket)
        .key("versioned-key")
        .body(Bytes::from("version 2").into())
        .send()
        .await
        .expect("Failed to put second version");

    let version2 = put2.version_id().expect("Should return version ID");
    assert!(!version2.is_empty(), "Version ID should not be empty");
    assert_ne!(version1, version2, "Version IDs should be different");

    // Get current (should be version 2)
    let get_current = client
        .get_object()
        .bucket(bucket)
        .key("versioned-key")
        .send()
        .await
        .expect("Failed to get current version");

    let current_version_id = get_current.version_id().map(|s| s.to_string());
    let body = get_current.body.collect().await.unwrap().into_bytes();
    assert_eq!(body, Bytes::from("version 2"));
    assert_eq!(current_version_id.as_deref(), Some(version2.as_ref()));

    // Get specific version (version 1)
    let get_v1 = client
        .get_object()
        .bucket(bucket)
        .key("versioned-key")
        .version_id(version1)
        .send()
        .await
        .expect("Failed to get specific version");

    let body = get_v1.body.collect().await.unwrap().into_bytes();
    assert_eq!(body, Bytes::from("version 1"));

    // Cleanup: delete all versions
    client
        .delete_object()
        .bucket(bucket)
        .key("versioned-key")
        .version_id(version1)
        .send()
        .await
        .ok();
    client
        .delete_object()
        .bucket(bucket)
        .key("versioned-key")
        .version_id(version2)
        .send()
        .await
        .ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_versioning_delete_creates_marker() {
    let client = create_s3_client().await;
    let bucket = "sdk-versioning-delete-test";

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

    // Put object
    let put_result = client
        .put_object()
        .bucket(bucket)
        .key("delete-marker-key")
        .body(Bytes::from("original content").into())
        .send()
        .await
        .expect("Failed to put object");

    let original_version = put_result.version_id().expect("Should have version ID").to_string();

    // Delete without version ID (should create delete marker)
    let delete_result = client
        .delete_object()
        .bucket(bucket)
        .key("delete-marker-key")
        .send()
        .await
        .expect("Failed to delete object");

    assert!(
        delete_result.delete_marker().unwrap_or(false),
        "Should indicate delete marker was created"
    );
    let delete_marker_version = delete_result
        .version_id()
        .expect("Should return delete marker version ID")
        .to_string();

    // Get object should now fail (404)
    let get_result = client
        .get_object()
        .bucket(bucket)
        .key("delete-marker-key")
        .send()
        .await;
    assert!(get_result.is_err(), "Get should fail after delete marker");

    // But getting the original version should still work
    let get_original = client
        .get_object()
        .bucket(bucket)
        .key("delete-marker-key")
        .version_id(&original_version)
        .send()
        .await
        .expect("Should be able to get original version");

    let body = get_original.body.collect().await.unwrap().into_bytes();
    assert_eq!(body, Bytes::from("original content"));

    // Cleanup: delete all versions including delete marker
    client
        .delete_object()
        .bucket(bucket)
        .key("delete-marker-key")
        .version_id(&delete_marker_version)
        .send()
        .await
        .ok();
    client
        .delete_object()
        .bucket(bucket)
        .key("delete-marker-key")
        .version_id(&original_version)
        .send()
        .await
        .ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_list_object_versions() {
    let client = create_s3_client().await;
    let bucket = "sdk-list-versions-test";

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

    // Create multiple versions of same key
    let mut version_ids = Vec::new();
    for i in 1..=3 {
        let result = client
            .put_object()
            .bucket(bucket)
            .key("multi-version-key")
            .body(Bytes::from(format!("content v{}", i)).into())
            .send()
            .await
            .expect("Failed to put object");
        version_ids.push(result.version_id().unwrap().to_string());
    }

    // Delete to create a delete marker
    let delete_result = client
        .delete_object()
        .bucket(bucket)
        .key("multi-version-key")
        .send()
        .await
        .expect("Failed to delete");
    let delete_marker_version = delete_result.version_id().unwrap().to_string();

    // List versions
    let list_result = client
        .list_object_versions()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to list object versions");

    // Should have 3 versions
    let versions = list_result.versions();
    assert_eq!(versions.len(), 3, "Should have 3 object versions");

    // Should have 1 delete marker
    let delete_markers = list_result.delete_markers();
    assert_eq!(delete_markers.len(), 1, "Should have 1 delete marker");

    // Verify first version is marked as latest (which is the delete marker)
    // The latest non-delete-marker version should have IsLatest=false now

    // Cleanup: delete all versions
    for vid in &version_ids {
        client
            .delete_object()
            .bucket(bucket)
            .key("multi-version-key")
            .version_id(vid)
            .send()
            .await
            .ok();
    }
    client
        .delete_object()
        .bucket(bucket)
        .key("multi-version-key")
        .version_id(&delete_marker_version)
        .send()
        .await
        .ok();

    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_list_versions_with_prefix() {
    let client = create_s3_client().await;
    let bucket = "sdk-list-versions-prefix-test";

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

    // Put objects with different prefixes
    let mut all_versions = Vec::new();

    for prefix in ["prefix-a/", "prefix-b/"] {
        for i in 1..=2 {
            let result = client
                .put_object()
                .bucket(bucket)
                .key(format!("{}key-{}", prefix, i))
                .body(Bytes::from("content").into())
                .send()
                .await
                .expect("Failed to put object");
            all_versions.push((
                format!("{}key-{}", prefix, i),
                result.version_id().unwrap().to_string(),
            ));
        }
    }

    // List versions with prefix filter
    let list_result = client
        .list_object_versions()
        .bucket(bucket)
        .prefix("prefix-a/")
        .send()
        .await
        .expect("Failed to list object versions");

    // Should only have prefix-a versions
    let versions = list_result.versions();
    assert_eq!(versions.len(), 2, "Should have 2 versions for prefix-a/");

    for v in versions {
        assert!(
            v.key().unwrap().starts_with("prefix-a/"),
            "All keys should start with prefix-a/"
        );
    }

    // Cleanup
    for (key, vid) in &all_versions {
        client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .version_id(vid)
            .send()
            .await
            .ok();
    }
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_head_object_with_version() {
    let client = create_s3_client().await;
    let bucket = "sdk-head-version-test";

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
        .key("head-test-key")
        .body(Bytes::from("short").into())
        .send()
        .await
        .expect("Failed to put first version");
    let version1 = put1.version_id().unwrap().to_string();

    let put2 = client
        .put_object()
        .bucket(bucket)
        .key("head-test-key")
        .body(Bytes::from("much longer content here").into())
        .send()
        .await
        .expect("Failed to put second version");
    let version2 = put2.version_id().unwrap().to_string();

    // Head current version
    let head_current = client
        .head_object()
        .bucket(bucket)
        .key("head-test-key")
        .send()
        .await
        .expect("Failed to head current version");

    assert_eq!(head_current.content_length(), Some(24)); // "much longer content here"
    assert_eq!(head_current.version_id(), Some(version2.as_str()));

    // Head specific version
    let head_v1 = client
        .head_object()
        .bucket(bucket)
        .key("head-test-key")
        .version_id(&version1)
        .send()
        .await
        .expect("Failed to head specific version");

    assert_eq!(head_v1.content_length(), Some(5)); // "short"
    assert_eq!(head_v1.version_id(), Some(version1.as_str()));

    // Cleanup
    client
        .delete_object()
        .bucket(bucket)
        .key("head-test-key")
        .version_id(&version1)
        .send()
        .await
        .ok();
    client
        .delete_object()
        .bucket(bucket)
        .key("head-test-key")
        .version_id(&version2)
        .send()
        .await
        .ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_delete_specific_version() {
    let client = create_s3_client().await;
    let bucket = "sdk-delete-version-test";

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

    // Create 3 versions
    let mut version_ids = Vec::new();
    for i in 1..=3 {
        let result = client
            .put_object()
            .bucket(bucket)
            .key("delete-version-key")
            .body(Bytes::from(format!("v{}", i)).into())
            .send()
            .await
            .expect("Failed to put object");
        version_ids.push(result.version_id().unwrap().to_string());
    }

    // Delete middle version permanently
    let delete_result = client
        .delete_object()
        .bucket(bucket)
        .key("delete-version-key")
        .version_id(&version_ids[1])
        .send()
        .await
        .expect("Failed to delete specific version");

    // Should NOT be a delete marker when deleting specific version
    assert!(
        !delete_result.delete_marker().unwrap_or(false),
        "Deleting specific version should not create delete marker"
    );

    // Verify version is gone
    let get_deleted = client
        .get_object()
        .bucket(bucket)
        .key("delete-version-key")
        .version_id(&version_ids[1])
        .send()
        .await;
    assert!(get_deleted.is_err(), "Deleted version should not exist");

    // Other versions should still exist
    let get_v1 = client
        .get_object()
        .bucket(bucket)
        .key("delete-version-key")
        .version_id(&version_ids[0])
        .send()
        .await;
    assert!(get_v1.is_ok(), "Version 1 should still exist");

    let get_v3 = client
        .get_object()
        .bucket(bucket)
        .key("delete-version-key")
        .version_id(&version_ids[2])
        .send()
        .await;
    assert!(get_v3.is_ok(), "Version 3 should still exist");

    // Cleanup remaining versions
    client
        .delete_object()
        .bucket(bucket)
        .key("delete-version-key")
        .version_id(&version_ids[0])
        .send()
        .await
        .ok();
    client
        .delete_object()
        .bucket(bucket)
        .key("delete-version-key")
        .version_id(&version_ids[2])
        .send()
        .await
        .ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_versioning_complete_workflow_with_fixtures() {
    use std::path::PathBuf;
    use tempfile::TempDir;

    let client = create_s3_client().await;
    let bucket = "sdk-versioning-workflow-test-2";

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

    // Fixture paths
    let fixtures_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("versioning");
    let v1_fixture = fixtures_dir.join("document_v1.txt");
    let v2_fixture = fixtures_dir.join("document_v2.txt");
    let v3_fixture = fixtures_dir.join("document_v3.txt");

    // Create temp directory for downloads
    let tmp_dir = TempDir::new().expect("Failed to create temp dir");

    // Read fixture files
    let v1_content = tokio::fs::read(&v1_fixture).await.expect("Failed to read v1 fixture");
    let v2_content = tokio::fs::read(&v2_fixture).await.expect("Failed to read v2 fixture");
    let v3_content = tokio::fs::read(&v3_fixture).await.expect("Failed to read v3 fixture");

    // ==========================================================================
    // Step 1: Upload first version with tags
    // ==========================================================================
    let put_v1 = client
        .put_object()
        .bucket(bucket)
        .key("document.txt")
        .body(Bytes::from(v1_content.clone()).into())
        .content_type("text/plain")
        .send()
        .await
        .expect("Failed to put first version");

    let version1_id = put_v1.version_id().expect("Should return version ID").to_string();
    assert!(!version1_id.is_empty(), "Version 1 ID should not be empty");

    // Add tags to the object
    client
        .put_object_tagging()
        .bucket(bucket)
        .key("document.txt")
        .tagging(
            aws_sdk_s3::types::Tagging::builder()
                .tag_set(
                    aws_sdk_s3::types::Tag::builder()
                        .key("Environment")
                        .value("Test")
                        .build()
                        .unwrap()
                )
                .tag_set(
                    aws_sdk_s3::types::Tag::builder()
                        .key("Version")
                        .value("1.0")
                        .build()
                        .unwrap()
                )
                .build()
                .unwrap()
        )
        .send()
        .await
        .expect("Failed to put tags");

    // ==========================================================================
    // Step 2: Read back tags and verify
    // ==========================================================================
    let tags_result = client
        .get_object_tagging()
        .bucket(bucket)
        .key("document.txt")
        .send()
        .await
        .expect("Failed to get tags");

    let tags: Vec<_> = tags_result.tag_set().iter().collect();
    assert_eq!(tags.len(), 2, "Should have 2 tags");

    // Verify tag values
    let env_tag = tags.iter().find(|t| t.key() == "Environment").expect("Should have Environment tag");
    assert_eq!(env_tag.value(), "Test");
    let version_tag = tags.iter().find(|t| t.key() == "Version").expect("Should have Version tag");
    assert_eq!(version_tag.value(), "1.0");

    // ==========================================================================
    // Step 3: Upload second version
    // ==========================================================================
    let put_v2 = client
        .put_object()
        .bucket(bucket)
        .key("document.txt")
        .body(Bytes::from(v2_content.clone()).into())
        .content_type("text/plain")
        .send()
        .await
        .expect("Failed to put second version");

    let version2_id = put_v2.version_id().expect("Should return version ID").to_string();
    assert_ne!(version1_id, version2_id, "Version IDs should be different");

    // ==========================================================================
    // Step 4: Download current version and compare with v2 fixture
    // ==========================================================================
    let get_v2 = client
        .get_object()
        .bucket(bucket)
        .key("document.txt")
        .send()
        .await
        .expect("Failed to get current version");

    let v2_downloaded = get_v2.body.collect().await.unwrap().into_bytes();
    let v2_download_path = tmp_dir.path().join("downloaded_v2.txt");
    tokio::fs::write(&v2_download_path, &v2_downloaded).await.expect("Failed to write downloaded v2");

    // Compare with fixture
    assert_eq!(v2_downloaded.as_ref(), v2_content.as_slice(), "Downloaded v2 should match fixture");

    // ==========================================================================
    // Step 5: Upload third version
    // ==========================================================================
    let put_v3 = client
        .put_object()
        .bucket(bucket)
        .key("document.txt")
        .body(Bytes::from(v3_content.clone()).into())
        .content_type("text/plain")
        .send()
        .await
        .expect("Failed to put third version");

    let version3_id = put_v3.version_id().expect("Should return version ID").to_string();

    // ==========================================================================
    // Step 6: Download current version and compare with v3 fixture
    // ==========================================================================
    let get_v3 = client
        .get_object()
        .bucket(bucket)
        .key("document.txt")
        .send()
        .await
        .expect("Failed to get v3");

    let v3_downloaded = get_v3.body.collect().await.unwrap().into_bytes();
    let v3_download_path = tmp_dir.path().join("downloaded_v3.txt");
    tokio::fs::write(&v3_download_path, &v3_downloaded).await.expect("Failed to write downloaded v3");

    // Compare with fixture
    assert_eq!(v3_downloaded.as_ref(), v3_content.as_slice(), "Downloaded v3 should match fixture");

    // ==========================================================================
    // Step 7: Access first version via versioning and compare with v1 fixture
    // ==========================================================================
    let get_v1 = client
        .get_object()
        .bucket(bucket)
        .key("document.txt")
        .version_id(&version1_id)
        .send()
        .await
        .expect("Failed to get v1 via version ID");

    let v1_downloaded = get_v1.body.collect().await.unwrap().into_bytes();
    let v1_download_path = tmp_dir.path().join("downloaded_v1.txt");
    tokio::fs::write(&v1_download_path, &v1_downloaded).await.expect("Failed to write downloaded v1");

    // Compare with fixture
    assert_eq!(v1_downloaded.as_ref(), v1_content.as_slice(), "Downloaded v1 should match fixture");

    // ==========================================================================
    // Step 8: Verify tags are still accessible on v1 (via version_id)
    // Note: In S3, tags are per-version. get_object_tagging without version_id
    // returns tags for the current version (v3), which has no tags.
    // To verify v1's tags are still accessible, we need to specify version_id.
    // ==========================================================================
    let tags_result_again = client
        .get_object_tagging()
        .bucket(bucket)
        .key("document.txt")
        .version_id(&version1_id)
        .send()
        .await
        .expect("Failed to get tags again");

    let tags_again: Vec<_> = tags_result_again.tag_set().iter().collect();
    assert_eq!(tags_again.len(), 2, "Should still have 2 tags on v1");

    // ==========================================================================
    // Step 9: List versions to verify all versions exist
    // ==========================================================================
    let list_versions = client
        .list_object_versions()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to list versions");

    let versions = list_versions.versions();
    assert_eq!(versions.len(), 3, "Should have 3 versions");

    // ==========================================================================
    // Step 10: Delete the object (creates delete marker)
    // ==========================================================================
    let delete_result = client
        .delete_object()
        .bucket(bucket)
        .key("document.txt")
        .send()
        .await
        .expect("Failed to delete object");

    assert!(
        delete_result.delete_marker().unwrap_or(false),
        "Should create a delete marker"
    );
    let delete_marker_version = delete_result.version_id().expect("Should have delete marker version ID").to_string();

    // ==========================================================================
    // Step 11: Verify object appears deleted (404)
    // ==========================================================================
    let get_after_delete = client
        .get_object()
        .bucket(bucket)
        .key("document.txt")
        .send()
        .await;
    assert!(get_after_delete.is_err(), "Object should appear deleted");

    // ==========================================================================
    // Step 12: Verify versions still accessible via version ID
    // ==========================================================================
    let get_v1_after_delete = client
        .get_object()
        .bucket(bucket)
        .key("document.txt")
        .version_id(&version1_id)
        .send()
        .await
        .expect("V1 should still be accessible via version ID");

    let v1_content_after_delete = get_v1_after_delete.body.collect().await.unwrap().into_bytes();
    assert_eq!(v1_content_after_delete.as_ref(), v1_content.as_slice(), "V1 content should match after delete");

    // ==========================================================================
    // Step 13: List versions to verify delete marker exists
    // ==========================================================================
    let list_after_delete = client
        .list_object_versions()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to list versions after delete");

    let delete_markers = list_after_delete.delete_markers();
    assert_eq!(delete_markers.len(), 1, "Should have 1 delete marker");
    assert_eq!(list_after_delete.versions().len(), 3, "Should still have 3 versions");

    // ==========================================================================
    // Cleanup: Delete all versions and delete marker
    // ==========================================================================
    for vid in [&version1_id, &version2_id, &version3_id, &delete_marker_version] {
        client
            .delete_object()
            .bucket(bucket)
            .key("document.txt")
            .version_id(vid)
            .send()
            .await
            .ok();
    }
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test ListObjectVersions on an empty versioned bucket
#[tokio::test]
#[ignore]
async fn test_list_object_versions_empty_bucket() {
    let client = create_s3_client().await;
    let bucket = "sdk-list-versions-empty-test";

    // Create bucket and enable versioning
    let _ = client.create_bucket().bucket(bucket).send().await;
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

    // List versions on empty bucket
    let list_result = client
        .list_object_versions()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to list object versions");

    // Should have empty lists
    assert!(list_result.versions().is_empty(), "Empty bucket should have no versions");
    assert!(list_result.delete_markers().is_empty(), "Empty bucket should have no delete markers");
    assert!(list_result.common_prefixes().is_empty(), "Empty bucket should have no common prefixes");

    // Cleanup
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test ListObjectVersions pagination with max-keys and key-marker
#[tokio::test]
#[ignore]
async fn test_list_object_versions_pagination() {
    let client = create_s3_client().await;
    let bucket = "sdk-list-versions-pagination-test";

    // Create bucket and enable versioning
    let _ = client.create_bucket().bucket(bucket).send().await;
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

    // Create 5 different objects
    let mut all_keys_versions: Vec<(String, String)> = Vec::new();
    for i in 0..5 {
        let key = format!("object-{:02}", i);
        let result = client
            .put_object()
            .bucket(bucket)
            .key(&key)
            .body(Bytes::from(format!("content-{}", i)).into())
            .send()
            .await
            .expect("Failed to put object");
        all_keys_versions.push((key, result.version_id().unwrap().to_string()));
    }

    // List with max-keys=2
    let list_result = client
        .list_object_versions()
        .bucket(bucket)
        .max_keys(2)
        .send()
        .await
        .expect("Failed to list versions");

    assert_eq!(list_result.versions().len(), 2, "Should return only 2 versions");
    assert!(list_result.is_truncated().unwrap_or(false), "Should be truncated");

    let next_key_marker = list_result.next_key_marker().expect("Should have next key marker");

    // Get next page using key-marker
    let list_result_2 = client
        .list_object_versions()
        .bucket(bucket)
        .max_keys(2)
        .key_marker(next_key_marker)
        .send()
        .await
        .expect("Failed to list versions page 2");

    assert_eq!(list_result_2.versions().len(), 2, "Page 2 should have 2 versions");

    // Get final page
    let next_key_marker_2 = list_result_2.next_key_marker();
    if let Some(marker) = next_key_marker_2 {
        let list_result_3 = client
            .list_object_versions()
            .bucket(bucket)
            .max_keys(2)
            .key_marker(marker)
            .send()
            .await
            .expect("Failed to list versions page 3");

        assert_eq!(list_result_3.versions().len(), 1, "Page 3 should have 1 version");
        assert!(!list_result_3.is_truncated().unwrap_or(true), "Page 3 should not be truncated");
    }

    // Cleanup
    for (key, version_id) in &all_keys_versions {
        client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .version_id(version_id)
            .send()
            .await
            .ok();
    }
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test ListObjectVersions with version-id-marker for mid-version pagination
#[tokio::test]
#[ignore]
async fn test_list_object_versions_version_id_marker() {
    let client = create_s3_client().await;
    let bucket = "sdk-list-versions-vid-marker-test";

    // Create bucket and enable versioning
    let _ = client.create_bucket().bucket(bucket).send().await;
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

    // Create multiple versions of the same key
    let key = "multi-version-key";
    let mut version_ids: Vec<String> = Vec::new();
    for i in 0..5 {
        let result = client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(Bytes::from(format!("version-{}", i)).into())
            .send()
            .await
            .expect("Failed to put object");
        version_ids.push(result.version_id().unwrap().to_string());
    }

    // List with max-keys=2
    let list_result = client
        .list_object_versions()
        .bucket(bucket)
        .max_keys(2)
        .send()
        .await
        .expect("Failed to list versions");

    assert_eq!(list_result.versions().len(), 2, "Should return 2 versions");

    // Use both key-marker and version-id-marker for pagination
    if list_result.is_truncated().unwrap_or(false) {
        let next_key = list_result.next_key_marker();
        let next_vid = list_result.next_version_id_marker();

        if let (Some(key_marker), Some(vid_marker)) = (next_key, next_vid) {
            let list_result_2 = client
                .list_object_versions()
                .bucket(bucket)
                .max_keys(2)
                .key_marker(key_marker)
                .version_id_marker(vid_marker)
                .send()
                .await
                .expect("Failed to list versions with markers");

            // Should get next batch of versions
            assert!(!list_result_2.versions().is_empty(), "Should have more versions");
        }
    }

    // Cleanup
    for vid in &version_ids {
        client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .version_id(vid)
            .send()
            .await
            .ok();
    }
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test ListObjectVersions with delimiter for common prefixes
#[tokio::test]
#[ignore]
async fn test_list_object_versions_delimiter() {
    let client = create_s3_client().await;
    let bucket = "sdk-list-versions-delimiter-test";

    // Create bucket and enable versioning
    let _ = client.create_bucket().bucket(bucket).send().await;
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

    // Create objects in different "directories"
    let mut all_versions: Vec<(String, String)> = Vec::new();

    for dir in ["dir-a/", "dir-b/", "dir-c/"] {
        for i in 0..2 {
            let key = format!("{}file-{}", dir, i);
            let result = client
                .put_object()
                .bucket(bucket)
                .key(&key)
                .body(Bytes::from("content").into())
                .send()
                .await
                .expect("Failed to put object");
            all_versions.push((key, result.version_id().unwrap().to_string()));
        }
    }

    // Also create a root-level object
    let root_result = client
        .put_object()
        .bucket(bucket)
        .key("root-file.txt")
        .body(Bytes::from("root content").into())
        .send()
        .await
        .expect("Failed to put root object");
    all_versions.push(("root-file.txt".to_string(), root_result.version_id().unwrap().to_string()));

    // List with delimiter "/"
    let list_result = client
        .list_object_versions()
        .bucket(bucket)
        .delimiter("/")
        .send()
        .await
        .expect("Failed to list versions with delimiter");

    // Should have common prefixes for the directories
    let common_prefixes = list_result.common_prefixes();
    assert_eq!(common_prefixes.len(), 3, "Should have 3 common prefixes (dir-a/, dir-b/, dir-c/)");

    // Should have 1 version for root-level object
    let versions = list_result.versions();
    assert_eq!(versions.len(), 1, "Should have 1 root-level version");
    assert_eq!(versions[0].key(), Some("root-file.txt"));

    // Verify common prefix values
    let prefix_values: Vec<&str> = common_prefixes
        .iter()
        .filter_map(|p| p.prefix())
        .collect();
    assert!(prefix_values.contains(&"dir-a/"), "Should contain dir-a/");
    assert!(prefix_values.contains(&"dir-b/"), "Should contain dir-b/");
    assert!(prefix_values.contains(&"dir-c/"), "Should contain dir-c/");

    // Cleanup
    for (key, vid) in &all_versions {
        client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .version_id(vid)
            .send()
            .await
            .ok();
    }
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

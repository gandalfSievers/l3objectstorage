use super::*;

#[tokio::test]
#[ignore]
async fn test_workflow_multipart_with_versioning() {
    let client = create_s3_client().await;
    let bucket = "sdk-multipart-versioning-workflow";

    // Setup: Create bucket and enable versioning
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

    // Create first version via multipart upload
    let create1 = client
        .create_multipart_upload()
        .bucket(bucket)
        .key("multipart-versioned")
        .send()
        .await
        .unwrap();
    let upload_id1 = create1.upload_id().unwrap();

    let part1 = client
        .upload_part()
        .bucket(bucket)
        .key("multipart-versioned")
        .upload_id(upload_id1)
        .part_number(1)
        .body(Bytes::from("version 1 content from multipart").into())
        .send()
        .await
        .unwrap();

    let complete1 = client
        .complete_multipart_upload()
        .bucket(bucket)
        .key("multipart-versioned")
        .upload_id(upload_id1)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .parts(
                    CompletedPart::builder()
                        .part_number(1)
                        .e_tag(part1.e_tag().unwrap())
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .expect("Failed to complete first multipart");

    let version1_id = complete1.version_id().expect("Should have version ID").to_string();

    // Create second version via multipart
    let create2 = client
        .create_multipart_upload()
        .bucket(bucket)
        .key("multipart-versioned")
        .send()
        .await
        .unwrap();
    let upload_id2 = create2.upload_id().unwrap();

    let part2 = client
        .upload_part()
        .bucket(bucket)
        .key("multipart-versioned")
        .upload_id(upload_id2)
        .part_number(1)
        .body(Bytes::from("version 2 content from multipart - longer").into())
        .send()
        .await
        .unwrap();

    let complete2 = client
        .complete_multipart_upload()
        .bucket(bucket)
        .key("multipart-versioned")
        .upload_id(upload_id2)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .parts(
                    CompletedPart::builder()
                        .part_number(1)
                        .e_tag(part2.e_tag().unwrap())
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .expect("Failed to complete second multipart");

    let version2_id = complete2.version_id().expect("Should have version ID").to_string();
    assert_ne!(version1_id, version2_id, "Versions should be different");

    // Verify both versions accessible
    let get_v1 = client
        .get_object()
        .bucket(bucket)
        .key("multipart-versioned")
        .version_id(&version1_id)
        .send()
        .await
        .expect("Failed to get v1");
    let body_v1 = get_v1.body.collect().await.unwrap().into_bytes();
    assert!(body_v1.starts_with(b"version 1"));

    let get_v2 = client
        .get_object()
        .bucket(bucket)
        .key("multipart-versioned")
        .version_id(&version2_id)
        .send()
        .await
        .expect("Failed to get v2");
    let body_v2 = get_v2.body.collect().await.unwrap().into_bytes();
    assert!(body_v2.starts_with(b"version 2"));

    // List versions
    let versions = client
        .list_object_versions()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to list versions");
    assert_eq!(versions.versions().len(), 2, "Should have 2 versions");

    // Cleanup
    client.delete_object().bucket(bucket).key("multipart-versioned").version_id(&version1_id).send().await.ok();
    client.delete_object().bucket(bucket).key("multipart-versioned").version_id(&version2_id).send().await.ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_workflow_full_object_lifecycle() {
    use aws_sdk_s3::types::ObjectCannedAcl;

    let client = create_s3_client().await;
    let bucket = "sdk-lifecycle-workflow";

    // Step 1: Create bucket with tags
    let _ = client.create_bucket().bucket(bucket).send().await;

    let bucket_tag = Tag::builder().key("purpose").value("testing").build().unwrap();
    client
        .put_bucket_tagging()
        .bucket(bucket)
        .tagging(Tagging::builder().tag_set(bucket_tag).build().unwrap())
        .send()
        .await
        .expect("Failed to tag bucket");

    // Step 2: Upload object with ACL
    client
        .put_object()
        .bucket(bucket)
        .key("lifecycle-object")
        .body(Bytes::from("initial content").into())
        .content_type("text/plain")
        .acl(ObjectCannedAcl::Private)
        .send()
        .await
        .expect("Failed to put object");

    // Step 3: Add object tags
    let obj_tag1 = Tag::builder().key("status").value("draft").build().unwrap();
    let obj_tag2 = Tag::builder().key("owner").value("test-user").build().unwrap();
    client
        .put_object_tagging()
        .bucket(bucket)
        .key("lifecycle-object")
        .tagging(Tagging::builder().tag_set(obj_tag1).tag_set(obj_tag2).build().unwrap())
        .send()
        .await
        .expect("Failed to tag object");

    // Step 4: Verify object metadata
    let head = client
        .head_object()
        .bucket(bucket)
        .key("lifecycle-object")
        .send()
        .await
        .expect("Failed to head object");
    assert_eq!(head.content_type(), Some("text/plain"));

    // Step 5: Update object (overwrite)
    client
        .put_object()
        .bucket(bucket)
        .key("lifecycle-object")
        .body(Bytes::from("updated content - version 2").into())
        .content_type("text/plain")
        .send()
        .await
        .expect("Failed to update object");

    // Step 6: Verify updated content
    let get_response = client
        .get_object()
        .bucket(bucket)
        .key("lifecycle-object")
        .send()
        .await
        .expect("Failed to get object");
    let body = get_response.body.collect().await.unwrap().into_bytes();
    assert!(String::from_utf8_lossy(&body).contains("version 2"));

    // Step 7: Copy object to new key
    client
        .copy_object()
        .bucket(bucket)
        .key("lifecycle-object-copy")
        .copy_source(format!("{}/lifecycle-object", bucket))
        .send()
        .await
        .expect("Failed to copy object");

    // Step 8: Verify copy exists and has same content as updated object
    let copy_head = client
        .head_object()
        .bucket(bucket)
        .key("lifecycle-object-copy")
        .send()
        .await
        .expect("Failed to head copy");
    // Copy should have same length as updated object (27 bytes: "updated content - version 2")
    assert_eq!(copy_head.content_length(), Some(27), "Copy should match updated object size");

    // Step 9: Update tags (change status)
    let updated_tag = Tag::builder().key("status").value("published").build().unwrap();
    client
        .put_object_tagging()
        .bucket(bucket)
        .key("lifecycle-object")
        .tagging(Tagging::builder().tag_set(updated_tag).build().unwrap())
        .send()
        .await
        .expect("Failed to update tags");

    // Step 10: Verify updated tags
    let tags = client
        .get_object_tagging()
        .bucket(bucket)
        .key("lifecycle-object")
        .send()
        .await
        .expect("Failed to get tags");
    let status_tag = tags.tag_set().iter().find(|t| t.key() == "status");
    assert_eq!(status_tag.map(|t| t.value()), Some("published"));

    // Step 11: Change ACL to public-read
    client
        .put_object_acl()
        .bucket(bucket)
        .key("lifecycle-object")
        .acl(ObjectCannedAcl::PublicRead)
        .send()
        .await
        .expect("Failed to update ACL");

    // Step 12: Verify ACL
    let acl = client
        .get_object_acl()
        .bucket(bucket)
        .key("lifecycle-object")
        .send()
        .await
        .expect("Failed to get ACL");
    let has_public = acl.grants().iter().any(|g| {
        g.grantee()
            .and_then(|grantee| grantee.uri())
            .map(|uri| uri.contains("AllUsers"))
            .unwrap_or(false)
    });
    assert!(has_public, "Should have public read grant");

    // Step 13: Delete original, verify copy still exists
    client
        .delete_object()
        .bucket(bucket)
        .key("lifecycle-object")
        .send()
        .await
        .expect("Failed to delete original");

    let copy_still_exists = client
        .head_object()
        .bucket(bucket)
        .key("lifecycle-object-copy")
        .send()
        .await;
    assert!(copy_still_exists.is_ok(), "Copy should still exist");

    // Cleanup
    client.delete_object().bucket(bucket).key("lifecycle-object-copy").send().await.ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_workflow_concurrent_multipart_uploads() {
    let client = create_s3_client().await;
    let bucket = "sdk-concurrent-multipart";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Start multiple multipart uploads for same key
    let upload1 = client
        .create_multipart_upload()
        .bucket(bucket)
        .key("concurrent-key")
        .send()
        .await
        .unwrap();
    let upload_id1 = upload1.upload_id().unwrap().to_string();

    let upload2 = client
        .create_multipart_upload()
        .bucket(bucket)
        .key("concurrent-key")
        .send()
        .await
        .unwrap();
    let upload_id2 = upload2.upload_id().unwrap().to_string();

    // Both uploads should be listed
    let uploads = client
        .list_multipart_uploads()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to list uploads");
    assert_eq!(uploads.uploads().len(), 2, "Should have 2 in-progress uploads");

    // Upload parts to both
    let _part1_resp = client
        .upload_part()
        .bucket(bucket)
        .key("concurrent-key")
        .upload_id(&upload_id1)
        .part_number(1)
        .body(Bytes::from("upload 1 data").into())
        .send()
        .await
        .unwrap();

    let part2_resp = client
        .upload_part()
        .bucket(bucket)
        .key("concurrent-key")
        .upload_id(&upload_id2)
        .part_number(1)
        .body(Bytes::from("upload 2 data - longer").into())
        .send()
        .await
        .unwrap();

    // Complete upload 2 (should become the object)
    client
        .complete_multipart_upload()
        .bucket(bucket)
        .key("concurrent-key")
        .upload_id(&upload_id2)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .parts(
                    CompletedPart::builder()
                        .part_number(1)
                        .e_tag(part2_resp.e_tag().unwrap())
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .expect("Failed to complete upload 2");

    // Verify object has upload 2's content
    let get = client
        .get_object()
        .bucket(bucket)
        .key("concurrent-key")
        .send()
        .await
        .expect("Failed to get object");
    let body = get.body.collect().await.unwrap().into_bytes();
    assert!(String::from_utf8_lossy(&body).contains("upload 2"));

    // Abort upload 1
    client
        .abort_multipart_upload()
        .bucket(bucket)
        .key("concurrent-key")
        .upload_id(&upload_id1)
        .send()
        .await
        .expect("Failed to abort upload 1");

    // No uploads should remain
    let remaining = client
        .list_multipart_uploads()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to list");
    assert!(remaining.uploads().is_empty(), "No uploads should remain");

    // Cleanup
    client.delete_object().bucket(bucket).key("concurrent-key").send().await.ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_workflow_tagging_with_versioning() {
    let client = create_s3_client().await;
    let bucket = "sdk-tagging-versioning-workflow";

    // Setup
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
        .unwrap();

    // Create v1 with tags
    let put1 = client
        .put_object()
        .bucket(bucket)
        .key("tagged-versioned")
        .body(Bytes::from("version 1").into())
        .send()
        .await
        .unwrap();
    let v1_id = put1.version_id().unwrap().to_string();

    // Tag v1
    client
        .put_object_tagging()
        .bucket(bucket)
        .key("tagged-versioned")
        .version_id(&v1_id)
        .tagging(
            Tagging::builder()
                .tag_set(Tag::builder().key("version").value("1.0").build().unwrap())
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // Create v2 with different tags
    let put2 = client
        .put_object()
        .bucket(bucket)
        .key("tagged-versioned")
        .body(Bytes::from("version 2").into())
        .send()
        .await
        .unwrap();
    let v2_id = put2.version_id().unwrap().to_string();

    // Tag v2
    client
        .put_object_tagging()
        .bucket(bucket)
        .key("tagged-versioned")
        .version_id(&v2_id)
        .tagging(
            Tagging::builder()
                .tag_set(Tag::builder().key("version").value("2.0").build().unwrap())
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // Verify v1 tags
    let v1_tags = client
        .get_object_tagging()
        .bucket(bucket)
        .key("tagged-versioned")
        .version_id(&v1_id)
        .send()
        .await
        .unwrap();
    let v1_version_tag = v1_tags.tag_set().iter().find(|t| t.key() == "version");
    assert_eq!(v1_version_tag.map(|t| t.value()), Some("1.0"));

    // Verify v2 tags
    let v2_tags = client
        .get_object_tagging()
        .bucket(bucket)
        .key("tagged-versioned")
        .version_id(&v2_id)
        .send()
        .await
        .unwrap();
    let v2_version_tag = v2_tags.tag_set().iter().find(|t| t.key() == "version");
    assert_eq!(v2_version_tag.map(|t| t.value()), Some("2.0"));

    // Verify current (no version_id) returns v2's tags
    let current_tags = client
        .get_object_tagging()
        .bucket(bucket)
        .key("tagged-versioned")
        .send()
        .await
        .unwrap();
    let current_version_tag = current_tags.tag_set().iter().find(|t| t.key() == "version");
    assert_eq!(current_version_tag.map(|t| t.value()), Some("2.0"));

    // Cleanup
    client.delete_object().bucket(bucket).key("tagged-versioned").version_id(&v1_id).send().await.ok();
    client.delete_object().bucket(bucket).key("tagged-versioned").version_id(&v2_id).send().await.ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_workflow_batch_delete_versioned() {
    let client = create_s3_client().await;
    let bucket = "sdk-batch-delete-versioned";

    // Setup
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
        .unwrap();

    // Create multiple objects with versions
    let mut all_versions = Vec::new();
    for i in 0..3 {
        let put = client
            .put_object()
            .bucket(bucket)
            .key(format!("obj-{}", i))
            .body(Bytes::from("data").into())
            .send()
            .await
            .unwrap();
        all_versions.push((format!("obj-{}", i), put.version_id().unwrap().to_string()));
    }

    // Batch delete without version IDs (creates delete markers)
    let objects_to_delete: Vec<ObjectIdentifier> = (0..3)
        .map(|i| {
            ObjectIdentifier::builder()
                .key(format!("obj-{}", i))
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
        .expect("Failed to batch delete");

    // All should be marked as deleted (delete markers created)
    assert_eq!(response.deleted().len(), 3);

    // Collect delete marker version IDs
    let delete_markers: Vec<String> = response
        .deleted()
        .iter()
        .filter_map(|d| d.delete_marker_version_id().map(|s| s.to_string()))
        .collect();
    assert_eq!(delete_markers.len(), 3, "Should have 3 delete markers");

    // Verify objects appear deleted
    for i in 0..3 {
        let get = client
            .get_object()
            .bucket(bucket)
            .key(format!("obj-{}", i))
            .send()
            .await;
        assert!(get.is_err(), "Object {} should appear deleted", i);
    }

    // But original versions still accessible
    for (key, vid) in &all_versions {
        let get = client
            .get_object()
            .bucket(bucket)
            .key(key)
            .version_id(vid)
            .send()
            .await;
        assert!(get.is_ok(), "Original version of {} should still exist", key);
    }

    // Cleanup: delete all versions and delete markers
    for (key, vid) in &all_versions {
        client.delete_object().bucket(bucket).key(key).version_id(vid).send().await.ok();
    }
    for (i, dm_vid) in delete_markers.iter().enumerate() {
        client.delete_object().bucket(bucket).key(format!("obj-{}", i)).version_id(dm_vid).send().await.ok();
    }
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

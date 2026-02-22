use super::*;

#[tokio::test]
#[ignore]
async fn test_bucket_tagging() {
    let client = create_s3_client().await;
    let bucket = "sdk-tagging-test";

    // Create bucket
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Put tags
    let tag1 = Tag::builder()
        .key("env")
        .value("dev")
        .build()
        .unwrap();
    let tag2 = Tag::builder()
        .key("team")
        .value("platform")
        .build()
        .unwrap();
    let tagging = Tagging::builder()
        .tag_set(tag1)
        .tag_set(tag2)
        .build()
        .unwrap();

    client
        .put_bucket_tagging()
        .bucket(bucket)
        .tagging(tagging)
        .send()
        .await
        .expect("Failed to put bucket tagging");

    // Get tags and verify
    let result = client
        .get_bucket_tagging()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to get bucket tagging");

    assert_eq!(result.tag_set().len(), 2);

    // Verify tag values
    let tags: Vec<_> = result.tag_set().iter().collect();
    let env_tag = tags.iter().find(|t| t.key() == "env");
    let team_tag = tags.iter().find(|t| t.key() == "team");
    assert!(env_tag.is_some());
    assert!(team_tag.is_some());
    assert_eq!(env_tag.unwrap().value(), "dev");
    assert_eq!(team_tag.unwrap().value(), "platform");

    // Delete tags
    client
        .delete_bucket_tagging()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to delete bucket tagging");

    // Verify tags deleted (should error with NoSuchTagSet)
    let result = client.get_bucket_tagging().bucket(bucket).send().await;
    assert!(result.is_err());

    // Cleanup
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_object_tagging() {
    let client = create_s3_client().await;
    let bucket = "sdk-object-tagging-test";

    // Setup: create bucket and object
    let _ = client.create_bucket().bucket(bucket).send().await;

    client
        .put_object()
        .bucket(bucket)
        .key("tagged-object")
        .body(Bytes::from("test content").into())
        .send()
        .await
        .expect("Failed to put object");

    // Put object tags
    let tag1 = Tag::builder()
        .key("environment")
        .value("testing")
        .build()
        .unwrap();
    let tag2 = Tag::builder()
        .key("project")
        .value("local-s3")
        .build()
        .unwrap();
    let tagging = Tagging::builder()
        .tag_set(tag1)
        .tag_set(tag2)
        .build()
        .unwrap();

    client
        .put_object_tagging()
        .bucket(bucket)
        .key("tagged-object")
        .tagging(tagging)
        .send()
        .await
        .expect("Failed to put object tagging");

    // Get tags and verify
    let result = client
        .get_object_tagging()
        .bucket(bucket)
        .key("tagged-object")
        .send()
        .await
        .expect("Failed to get object tagging");

    assert_eq!(result.tag_set().len(), 2);

    // Verify tag values
    let tags: Vec<_> = result.tag_set().iter().collect();
    let env_tag = tags.iter().find(|t| t.key() == "environment");
    let project_tag = tags.iter().find(|t| t.key() == "project");
    assert!(env_tag.is_some());
    assert!(project_tag.is_some());
    assert_eq!(env_tag.unwrap().value(), "testing");
    assert_eq!(project_tag.unwrap().value(), "local-s3");

    // Delete tags
    client
        .delete_object_tagging()
        .bucket(bucket)
        .key("tagged-object")
        .send()
        .await
        .expect("Failed to delete object tagging");

    // Verify tags deleted (should error with NoSuchTagSet or return empty)
    let result = client
        .get_object_tagging()
        .bucket(bucket)
        .key("tagged-object")
        .send()
        .await;

    // After deletion, either returns empty tag set or error
    if let Ok(response) = result {
        assert!(response.tag_set().is_empty(), "Tags should be empty after deletion");
    }

    // Cleanup
    client
        .delete_object()
        .bucket(bucket)
        .key("tagged-object")
        .send()
        .await
        .ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

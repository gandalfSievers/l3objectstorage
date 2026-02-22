use super::*;

#[tokio::test]
#[ignore]
async fn test_get_object_if_match() {
    let client = create_s3_client().await;
    let bucket = "sdk-if-match-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Put object and get its ETag
    client
        .put_object()
        .bucket(bucket)
        .key("etag-key")
        .body(Bytes::from("test content").into())
        .send()
        .await
        .expect("Failed to put object");

    let head = client
        .head_object()
        .bucket(bucket)
        .key("etag-key")
        .send()
        .await
        .expect("Failed to head object");

    let etag = head.e_tag().expect("Should have ETag").to_string();

    // Get with matching ETag should succeed
    let response = client
        .get_object()
        .bucket(bucket)
        .key("etag-key")
        .if_match(&etag)
        .send()
        .await
        .expect("If-Match with correct ETag should succeed");

    let body = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body, Bytes::from("test content"));

    // Get with non-matching ETag should fail (PreconditionFailed)
    let wrong_etag = "\"wrongetag123456789\"";
    let result = client
        .get_object()
        .bucket(bucket)
        .key("etag-key")
        .if_match(wrong_etag)
        .send()
        .await;

    assert!(result.is_err(), "If-Match with wrong ETag should fail");
    let err_str = format!("{:?}", result.err().unwrap());
    assert!(
        err_str.contains("PreconditionFailed") || err_str.contains("412"),
        "Should be PreconditionFailed error: {}", err_str
    );

    // Cleanup
    client
        .delete_object()
        .bucket(bucket)
        .key("etag-key")
        .send()
        .await
        .ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_get_object_if_none_match() {
    let client = create_s3_client().await;
    let bucket = "sdk-if-none-match-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Put object and get its ETag
    client
        .put_object()
        .bucket(bucket)
        .key("etag-key")
        .body(Bytes::from("test content").into())
        .send()
        .await
        .expect("Failed to put object");

    let head = client
        .head_object()
        .bucket(bucket)
        .key("etag-key")
        .send()
        .await
        .expect("Failed to head object");

    let etag = head.e_tag().expect("Should have ETag").to_string();

    // Get with non-matching ETag should succeed (returns content)
    let wrong_etag = "\"differentetag\"";
    let response = client
        .get_object()
        .bucket(bucket)
        .key("etag-key")
        .if_none_match(wrong_etag)
        .send()
        .await
        .expect("If-None-Match with different ETag should succeed");

    let body = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body, Bytes::from("test content"));

    // Get with matching ETag should return 304 Not Modified (or error in SDK)
    let result = client
        .get_object()
        .bucket(bucket)
        .key("etag-key")
        .if_none_match(&etag)
        .send()
        .await;

    // When ETag matches, S3 returns 304 Not Modified
    // The SDK typically surfaces this as an error
    assert!(
        result.is_err(),
        "If-None-Match with matching ETag should indicate not modified"
    );
    let err_str = format!("{:?}", result.err().unwrap());
    assert!(
        err_str.contains("304") || err_str.contains("NotModified"),
        "Should indicate not modified: {}", err_str
    );

    // Cleanup
    client
        .delete_object()
        .bucket(bucket)
        .key("etag-key")
        .send()
        .await
        .ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_put_object_if_none_match_for_create_only() {
    let client = create_s3_client().await;
    let bucket = "sdk-put-if-none-match-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Put object with If-None-Match: * (create only if doesn't exist)
    let result = client
        .put_object()
        .bucket(bucket)
        .key("new-object")
        .body(Bytes::from("initial content").into())
        .if_none_match("*")
        .send()
        .await;

    // First put should succeed (object doesn't exist)
    assert!(result.is_ok(), "First put with If-None-Match: * should succeed");

    // Second put with same condition should fail (object exists)
    let result = client
        .put_object()
        .bucket(bucket)
        .key("new-object")
        .body(Bytes::from("updated content").into())
        .if_none_match("*")
        .send()
        .await;

    assert!(
        result.is_err(),
        "Second put with If-None-Match: * should fail (object exists)"
    );
    let err_str = format!("{:?}", result.err().unwrap());
    assert!(
        err_str.contains("PreconditionFailed") || err_str.contains("412"),
        "Should be PreconditionFailed: {}", err_str
    );

    // Verify original content is unchanged
    let get = client
        .get_object()
        .bucket(bucket)
        .key("new-object")
        .send()
        .await
        .expect("Failed to get object");
    let body = get.body.collect().await.unwrap().into_bytes();
    assert_eq!(body, Bytes::from("initial content"), "Content should be unchanged");

    // Cleanup
    client
        .delete_object()
        .bucket(bucket)
        .key("new-object")
        .send()
        .await
        .ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

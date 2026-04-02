use super::*;

/// Create an S3 client configured for virtual hosted-style addressing.
///
/// Uses `TEST_VHOST_ENDPOINT_URL` env var (default: `http://s3.local:9000`).
/// The AWS SDK prepends the bucket name as a subdomain, so for bucket "vhost-test-bucket"
/// it will connect to `http://vhost-test-bucket.s3.local:9000`.
/// In Docker, this is achieved via a network alias on the s3 service.
async fn create_vhost_client() -> Client {
    let endpoint = std::env::var("TEST_VHOST_ENDPOINT_URL")
        .unwrap_or_else(|_| "http://s3.local:9000".to_string());
    let config = aws_config::defaults(BehaviorVersion::latest())
        .endpoint_url(&endpoint)
        .region(aws_config::Region::new("us-east-1"))
        .credentials_provider(aws_credential_types::Credentials::new(
            "localadmin",
            "localadmin",
            None,
            None,
            "test",
        ))
        .load()
        .await;

    let s3_config = aws_sdk_s3::config::Builder::from(&config)
        .force_path_style(false)
        .build();

    Client::from_conf(s3_config)
}

#[tokio::test]
#[ignore]
async fn test_virtual_hosted_style_put_get_delete() {
    let bucket = "vhost-test-bucket";

    // Use a path-style client to create the bucket first
    let path_client = create_s3_client().await;
    path_client
        .create_bucket()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to create bucket for vhost test");

    // Use the virtual hosted-style client for object operations
    let client = create_vhost_client().await;

    // PUT object via virtual hosted-style
    client
        .put_object()
        .bucket(bucket)
        .key("vhost-test-key")
        .body(Bytes::from("virtual hosted content").into())
        .content_type("text/plain")
        .send()
        .await
        .expect("Failed to put object via virtual hosted-style");

    // GET object via virtual hosted-style
    let response = client
        .get_object()
        .bucket(bucket)
        .key("vhost-test-key")
        .send()
        .await
        .expect("Failed to get object via virtual hosted-style");

    let body = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body, Bytes::from("virtual hosted content"));

    // HEAD object via virtual hosted-style
    let head = client
        .head_object()
        .bucket(bucket)
        .key("vhost-test-key")
        .send()
        .await
        .expect("Failed to head object via virtual hosted-style");

    assert_eq!(head.content_length(), Some(22)); // "virtual hosted content".len()

    // LIST objects via virtual hosted-style
    let list = client
        .list_objects_v2()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to list objects via virtual hosted-style");

    let keys: Vec<_> = list
        .contents()
        .iter()
        .filter_map(|obj| obj.key())
        .collect();
    assert!(
        keys.contains(&"vhost-test-key"),
        "Expected vhost-test-key in listing, got: {:?}",
        keys
    );

    // DELETE object via virtual hosted-style
    client
        .delete_object()
        .bucket(bucket)
        .key("vhost-test-key")
        .send()
        .await
        .expect("Failed to delete object via virtual hosted-style");

    // Verify object is gone
    let result = client
        .get_object()
        .bucket(bucket)
        .key("vhost-test-key")
        .send()
        .await;
    assert!(result.is_err(), "Object should not exist after deletion");

    // Cleanup
    let _ = path_client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_virtual_hosted_style_nested_key() {
    let bucket = "vhost-test-bucket";

    let path_client = create_s3_client().await;
    path_client
        .create_bucket()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to create bucket for vhost test");

    let client = create_vhost_client().await;

    // PUT object with nested key path
    client
        .put_object()
        .bucket(bucket)
        .key("path/to/nested/object.txt")
        .body(Bytes::from("nested content").into())
        .send()
        .await
        .expect("Failed to put nested key via virtual hosted-style");

    // GET the nested key
    let response = client
        .get_object()
        .bucket(bucket)
        .key("path/to/nested/object.txt")
        .send()
        .await
        .expect("Failed to get nested key via virtual hosted-style");

    let body = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body, Bytes::from("nested content"));

    // Cleanup
    client
        .delete_object()
        .bucket(bucket)
        .key("path/to/nested/object.txt")
        .send()
        .await
        .expect("Failed to delete nested key");
    let _ = path_client.delete_bucket().bucket(bucket).send().await;
}

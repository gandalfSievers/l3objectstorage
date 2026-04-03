use super::*;

/// Create an S3 client configured for AWS-style virtual hosted addressing.
///
/// Uses `TEST_AWSSTYLE_ENDPOINT_URL` env var (default: `http://s3.us-east-1.amazonaws.com:9000`).
/// The AWS SDK prepends the bucket name as a subdomain, so for bucket "awsstyle-test-bucket"
/// it will connect to `http://awsstyle-test-bucket.s3.us-east-1.amazonaws.com:9000`.
/// In Docker, this is achieved via network aliases on the s3 service.
async fn create_awsstyle_client() -> Client {
    let endpoint = std::env::var("TEST_AWSSTYLE_ENDPOINT_URL")
        .unwrap_or_else(|_| "http://s3.us-east-1.amazonaws.com:9000".to_string());
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
async fn test_aws_style_vhost_put_get_delete() {
    let bucket = "awsstyle-test-bucket";

    // Use a path-style client to create the bucket first
    let path_client = create_s3_client().await;
    let _ = path_client
        .create_bucket()
        .bucket(bucket)
        .send()
        .await;

    // Use the AWS-style virtual hosted client for object operations
    let client = create_awsstyle_client().await;

    // PUT object via AWS-style virtual hosted addressing
    client
        .put_object()
        .bucket(bucket)
        .key("awsstyle-test-key")
        .body(Bytes::from("aws style virtual hosted content").into())
        .content_type("text/plain")
        .send()
        .await
        .expect("Failed to put object via AWS-style vhost");

    // GET object
    let response = client
        .get_object()
        .bucket(bucket)
        .key("awsstyle-test-key")
        .send()
        .await
        .expect("Failed to get object via AWS-style vhost");

    let body = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body, Bytes::from("aws style virtual hosted content"));

    // HEAD object
    let head = client
        .head_object()
        .bucket(bucket)
        .key("awsstyle-test-key")
        .send()
        .await
        .expect("Failed to head object via AWS-style vhost");

    assert_eq!(head.content_length(), Some(32)); // "aws style virtual hosted content".len()

    // LIST objects
    let list = client
        .list_objects_v2()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to list objects via AWS-style vhost");

    let keys: Vec<_> = list
        .contents()
        .iter()
        .filter_map(|obj| obj.key())
        .collect();
    assert!(
        keys.contains(&"awsstyle-test-key"),
        "Expected awsstyle-test-key in listing, got: {:?}",
        keys
    );

    // DELETE object
    client
        .delete_object()
        .bucket(bucket)
        .key("awsstyle-test-key")
        .send()
        .await
        .expect("Failed to delete object via AWS-style vhost");

    // Verify object is gone
    let result = client
        .get_object()
        .bucket(bucket)
        .key("awsstyle-test-key")
        .send()
        .await;
    assert!(result.is_err(), "Object should not exist after deletion");

    // Cleanup
    let _ = path_client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_aws_style_vhost_nested_key() {
    let bucket = "awsstyle-test-bucket";

    let path_client = create_s3_client().await;
    let _ = path_client
        .create_bucket()
        .bucket(bucket)
        .send()
        .await;

    let client = create_awsstyle_client().await;

    // PUT object with nested key path
    client
        .put_object()
        .bucket(bucket)
        .key("path/to/nested/object.txt")
        .body(Bytes::from("nested content").into())
        .send()
        .await
        .expect("Failed to put nested key via AWS-style vhost");

    // GET the nested key
    let response = client
        .get_object()
        .bucket(bucket)
        .key("path/to/nested/object.txt")
        .send()
        .await
        .expect("Failed to get nested key via AWS-style vhost");

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

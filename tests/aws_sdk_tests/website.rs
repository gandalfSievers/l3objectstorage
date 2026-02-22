use super::*;

#[tokio::test]
#[ignore]
async fn test_put_get_bucket_website() {
    use aws_sdk_s3::types::{ErrorDocument, IndexDocument, WebsiteConfiguration};

    let client = create_s3_client().await;
    let bucket = "sdk-website-test";

    // Create bucket
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Create website configuration
    let config = WebsiteConfiguration::builder()
        .index_document(
            IndexDocument::builder()
                .suffix("index.html")
                .build()
                .unwrap(),
        )
        .error_document(ErrorDocument::builder().key("error.html").build().unwrap())
        .build();

    // Put website configuration
    client
        .put_bucket_website()
        .bucket(bucket)
        .website_configuration(config)
        .send()
        .await
        .expect("Failed to put bucket website");

    // Get website configuration
    let result = client
        .get_bucket_website()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to get bucket website");

    // Verify index document
    let index_doc = result.index_document().expect("Should have index document");
    assert_eq!(index_doc.suffix(), "index.html");

    // Verify error document
    let error_doc = result.error_document().expect("Should have error document");
    assert_eq!(error_doc.key(), "error.html");

    // Cleanup
    let _ = client.delete_bucket_website().bucket(bucket).send().await;
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_delete_bucket_website() {
    use aws_sdk_s3::types::{IndexDocument, WebsiteConfiguration};

    let client = create_s3_client().await;
    let bucket = "sdk-delete-website-test";

    // Create bucket
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Put website configuration
    let config = WebsiteConfiguration::builder()
        .index_document(
            IndexDocument::builder()
                .suffix("index.html")
                .build()
                .unwrap(),
        )
        .build();

    client
        .put_bucket_website()
        .bucket(bucket)
        .website_configuration(config)
        .send()
        .await
        .expect("Failed to put bucket website");

    // Delete website configuration
    client
        .delete_bucket_website()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to delete bucket website");

    // Get should now fail with NoSuchWebsiteConfiguration
    let result = client.get_bucket_website().bucket(bucket).send().await;

    assert!(
        result.is_err(),
        "GetBucketWebsite should fail after deletion"
    );

    // Cleanup
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_bucket_website_no_configuration() {
    let client = create_s3_client().await;
    let bucket = "sdk-no-website-test";

    // Create bucket
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Get website should fail on bucket without config
    let result = client.get_bucket_website().bucket(bucket).send().await;

    assert!(
        result.is_err(),
        "GetBucketWebsite should fail when no configuration exists"
    );
    let err_str = format!("{:?}", result.err().unwrap());
    assert!(
        err_str.contains("NoSuchWebsiteConfiguration") || err_str.contains("404"),
        "Should indicate no website configuration: {}",
        err_str
    );

    // Cleanup
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_bucket_website_with_redirect() {
    use aws_sdk_s3::types::{RedirectAllRequestsTo, WebsiteConfiguration};

    let client = create_s3_client().await;
    let bucket = "sdk-website-redirect-test";

    // Create bucket
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Create website configuration with redirect
    let config = WebsiteConfiguration::builder()
        .redirect_all_requests_to(
            RedirectAllRequestsTo::builder()
                .host_name("example.com")
                .protocol(aws_sdk_s3::types::Protocol::Https)
                .build()
                .unwrap(),
        )
        .build();

    // Put website configuration
    client
        .put_bucket_website()
        .bucket(bucket)
        .website_configuration(config)
        .send()
        .await
        .expect("Failed to put bucket website with redirect");

    // Get website configuration
    let result = client
        .get_bucket_website()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to get bucket website");

    // Verify redirect
    let redirect = result
        .redirect_all_requests_to()
        .expect("Should have redirect");
    assert_eq!(redirect.host_name(), "example.com");
    assert_eq!(
        redirect.protocol(),
        Some(&aws_sdk_s3::types::Protocol::Https)
    );

    // Cleanup
    let _ = client.delete_bucket_website().bucket(bucket).send().await;
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_bucket_website_with_routing_rules() {
    use aws_sdk_s3::types::{
        Condition, IndexDocument, Redirect, RoutingRule, WebsiteConfiguration,
    };

    let client = create_s3_client().await;
    let bucket = "sdk-website-routing-test";

    // Create bucket
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Create website configuration with routing rules
    let routing_rule = RoutingRule::builder()
        .condition(
            Condition::builder()
                .key_prefix_equals("docs/")
                .build(),
        )
        .redirect(
            Redirect::builder()
                .replace_key_prefix_with("documents/")
                .build(),
        )
        .build();

    let config = WebsiteConfiguration::builder()
        .index_document(
            IndexDocument::builder()
                .suffix("index.html")
                .build()
                .unwrap(),
        )
        .routing_rules(routing_rule)
        .build();

    // Put website configuration
    client
        .put_bucket_website()
        .bucket(bucket)
        .website_configuration(config)
        .send()
        .await
        .expect("Failed to put bucket website with routing rules");

    // Get website configuration
    let result = client
        .get_bucket_website()
        .bucket(bucket)
        .send()
        .await
        .expect("Failed to get bucket website");

    // Verify routing rules
    let rules = result.routing_rules();
    assert_eq!(rules.len(), 1);

    let rule = &rules[0];
    let condition = rule.condition().expect("Should have condition");
    assert_eq!(condition.key_prefix_equals(), Some("docs/"));

    let redirect = rule.redirect().expect("Should have redirect");
    assert_eq!(redirect.replace_key_prefix_with(), Some("documents/"));

    // Cleanup
    let _ = client.delete_bucket_website().bucket(bucket).send().await;
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

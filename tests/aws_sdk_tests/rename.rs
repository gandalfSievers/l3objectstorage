use super::*;
use aws_credential_types::Credentials;
use aws_sigv4::http_request::{sign, SignableBody, SignableRequest, SigningSettings};
use aws_sigv4::sign::v4;
use aws_smithy_runtime_api::client::identity::Identity;
use std::time::SystemTime;

// ============================================================================
// Test endpoint configuration
// ============================================================================

fn get_test_base_url() -> String {
    std::env::var("TEST_ENDPOINT_URL").unwrap_or_else(|_| "http://localhost:9999".to_string())
}

fn get_test_host() -> String {
    get_test_base_url()
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .to_string()
}

// ============================================================================
// Helper function for signing HTTP requests with AWS SigV4
// ============================================================================

/// Send a signed rename request using AWS SigV4 authentication
async fn send_signed_rename_request(
    bucket: &str,
    dest_key: &str,
    source_key: &str,
) -> reqwest::Response {
    let base_url = get_test_base_url();
    let host = get_test_host();
    let url = format!("{}/{}/{}?renameObject", base_url, bucket, dest_key);

    // Create credentials and identity
    let credentials = Credentials::new("localadmin", "localadmin", None, None, "test");
    let identity = Identity::new(credentials, None);

    // Create signing parameters
    let signing_settings = SigningSettings::default();
    let signing_params = v4::SigningParams::builder()
        .identity(&identity)
        .region("us-east-1")
        .name("s3")
        .time(SystemTime::now())
        .settings(signing_settings)
        .build()
        .expect("Failed to build signing params");

    // Build the signable request
    let signable_request = SignableRequest::new(
        "PUT",
        &url,
        vec![
            ("host", host.as_str()),
            ("x-amz-rename-source", source_key),
            ("x-amz-content-sha256", "UNSIGNED-PAYLOAD"),
        ]
        .into_iter(),
        SignableBody::UnsignedPayload,
    )
    .expect("Failed to create signable request");

    // Sign the request
    let (signing_instructions, _signature) = sign(signable_request, &signing_params.into())
        .expect("Failed to sign request")
        .into_parts();

    // Build the actual HTTP request with signed headers
    let http_client = reqwest::Client::new();
    let mut request_builder = http_client
        .put(&url)
        .header("x-amz-rename-source", source_key)
        .header("x-amz-content-sha256", "UNSIGNED-PAYLOAD");

    // Apply signing instructions (adds Authorization, X-Amz-Date, etc.)
    for (name, value) in signing_instructions.headers() {
        request_builder = request_builder.header(name, value);
    }

    request_builder
        .send()
        .await
        .expect("Failed to send rename request")
}

/// Send a signed rename request without the source header (for error testing)
async fn send_signed_rename_request_without_source(
    bucket: &str,
    dest_key: &str,
) -> reqwest::Response {
    let base_url = get_test_base_url();
    let host = get_test_host();
    let url = format!("{}/{}/{}?renameObject", base_url, bucket, dest_key);

    let credentials = Credentials::new("localadmin", "localadmin", None, None, "test");
    let identity = Identity::new(credentials, None);

    let signing_settings = SigningSettings::default();
    let signing_params = v4::SigningParams::builder()
        .identity(&identity)
        .region("us-east-1")
        .name("s3")
        .time(SystemTime::now())
        .settings(signing_settings)
        .build()
        .expect("Failed to build signing params");

    let signable_request = SignableRequest::new(
        "PUT",
        &url,
        vec![
            ("host", host.as_str()),
            ("x-amz-content-sha256", "UNSIGNED-PAYLOAD"),
        ]
        .into_iter(),
        SignableBody::UnsignedPayload,
    )
    .expect("Failed to create signable request");

    let (signing_instructions, _signature) = sign(signable_request, &signing_params.into())
        .expect("Failed to sign request")
        .into_parts();

    let http_client = reqwest::Client::new();
    let mut request_builder = http_client
        .put(&url)
        .header("x-amz-content-sha256", "UNSIGNED-PAYLOAD");

    for (name, value) in signing_instructions.headers() {
        request_builder = request_builder.header(name, value);
    }

    request_builder
        .send()
        .await
        .expect("Failed to send rename request")
}

// ============================================================================
// Authenticated rename tests (run with auth enabled)
// ============================================================================

/// Test basic rename object functionality with authentication
#[tokio::test]
#[ignore]
async fn test_rename_object_basic_authenticated() {
    let client = create_s3_client().await;
    let bucket = "sdk-rename-auth-test";

    // Setup: create bucket and put an object
    let _ = client.create_bucket().bucket(bucket).send().await;

    let content = Bytes::from("rename me content");
    client
        .put_object()
        .bucket(bucket)
        .key("original-key")
        .body(content.clone().into())
        .content_type("text/plain")
        .send()
        .await
        .expect("Failed to put object");

    // Rename using signed HTTP request
    let response = send_signed_rename_request(bucket, "new-key", "original-key").await;

    assert_eq!(
        response.status(),
        200,
        "Rename should return 200: {:?}",
        response.text().await
    );

    // Verify old key no longer exists
    let old_result = client
        .get_object()
        .bucket(bucket)
        .key("original-key")
        .send()
        .await;
    assert!(
        old_result.is_err(),
        "Original key should not exist after rename"
    );

    // Verify new key exists with correct content
    let new_response = client
        .get_object()
        .bucket(bucket)
        .key("new-key")
        .send()
        .await
        .expect("Failed to get renamed object");

    let body = new_response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body, content, "Renamed object content should match");

    // Cleanup
    client
        .delete_object()
        .bucket(bucket)
        .key("new-key")
        .send()
        .await
        .ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test rename to existing key (should overwrite) with authentication
#[tokio::test]
#[ignore]
async fn test_rename_object_overwrites_existing_authenticated() {
    let client = create_s3_client().await;
    let bucket = "sdk-rename-overwrite-auth-test";

    // Setup
    let _ = client.create_bucket().bucket(bucket).send().await;

    let source_content = Bytes::from("source content");
    let dest_content = Bytes::from("destination content");

    client
        .put_object()
        .bucket(bucket)
        .key("source-key")
        .body(source_content.clone().into())
        .send()
        .await
        .expect("Failed to put source object");

    client
        .put_object()
        .bucket(bucket)
        .key("dest-key")
        .body(dest_content.into())
        .send()
        .await
        .expect("Failed to put destination object");

    // Rename source to dest (should overwrite)
    let response = send_signed_rename_request(bucket, "dest-key", "source-key").await;

    assert_eq!(response.status(), 200);

    // Verify dest-key now has source content
    let get_response = client
        .get_object()
        .bucket(bucket)
        .key("dest-key")
        .send()
        .await
        .expect("Failed to get object");

    let body = get_response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body, source_content, "Content should be from source");

    // Verify source-key no longer exists
    let old_result = client
        .get_object()
        .bucket(bucket)
        .key("source-key")
        .send()
        .await;
    assert!(old_result.is_err());

    // Cleanup
    client
        .delete_object()
        .bucket(bucket)
        .key("dest-key")
        .send()
        .await
        .ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test rename non-existent source key with authentication
#[tokio::test]
#[ignore]
async fn test_rename_object_source_not_found_authenticated() {
    let client = create_s3_client().await;
    let bucket = "sdk-rename-404-auth-test";

    // Setup
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Rename non-existent source
    let response = send_signed_rename_request(bucket, "new-key", "nonexistent-key").await;

    assert_eq!(
        response.status(),
        404,
        "Should return 404 for non-existent source"
    );

    // Cleanup
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test rename without x-amz-rename-source header with authentication
#[tokio::test]
#[ignore]
async fn test_rename_object_missing_source_header_authenticated() {
    let client = create_s3_client().await;
    let bucket = "sdk-rename-missing-header-auth-test";

    // Setup
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Rename without source header
    let response = send_signed_rename_request_without_source(bucket, "new-key").await;

    assert_eq!(
        response.status(),
        400,
        "Should return 400 for missing source header"
    );

    // Cleanup
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test rename in non-existent bucket with authentication
#[tokio::test]
#[ignore]
async fn test_rename_object_bucket_not_found_authenticated() {
    let response = send_signed_rename_request(
        "nonexistent-bucket-xyz-auth",
        "new-key",
        "old-key",
    )
    .await;

    assert_eq!(
        response.status(),
        404,
        "Should return 404 for non-existent bucket"
    );
}

/// Test rename preserves content-type with authentication
#[tokio::test]
#[ignore]
async fn test_rename_object_preserves_content_type_authenticated() {
    let client = create_s3_client().await;
    let bucket = "sdk-rename-metadata-auth-test";

    // Setup
    let _ = client.create_bucket().bucket(bucket).send().await;

    let content = Bytes::from(r#"{"key": "value"}"#);
    client
        .put_object()
        .bucket(bucket)
        .key("original-key")
        .body(content.clone().into())
        .content_type("application/json")
        .send()
        .await
        .expect("Failed to put object");

    // Verify original content-type
    let original_head = client
        .head_object()
        .bucket(bucket)
        .key("original-key")
        .send()
        .await
        .expect("Failed to head original object");
    assert_eq!(
        original_head.content_type(),
        Some("application/json"),
        "Original Content-Type should be application/json"
    );

    // Rename
    let response = send_signed_rename_request(bucket, "renamed-key", "original-key").await;

    assert_eq!(response.status(), 200);

    // Verify content-type preserved
    let head = client
        .head_object()
        .bucket(bucket)
        .key("renamed-key")
        .send()
        .await
        .expect("Failed to head object");

    assert_eq!(
        head.content_type(),
        Some("application/json"),
        "Content-Type should be preserved after rename"
    );

    // Verify content is preserved
    let get_response = client
        .get_object()
        .bucket(bucket)
        .key("renamed-key")
        .send()
        .await
        .expect("Failed to get renamed object");

    let body = get_response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body, content, "Content should be preserved after rename");

    // Cleanup
    client
        .delete_object()
        .bucket(bucket)
        .key("renamed-key")
        .send()
        .await
        .ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

// ============================================================================
// Unauthenticated rename tests (run with auth disabled)
// These tests use raw HTTP requests without AWS SigV4 signing.
// Run with: make test-integration-noauth
// Or: LOCAL_S3_REQUIRE_AUTH=false cargo test --test aws_sdk_tests --features noauth_tests -- --ignored
// ============================================================================

/// Test basic rename object functionality
/// RenameObject uses PUT /{bucket}/{new-key}?renameObject with x-amz-rename-source header
#[tokio::test]
#[ignore]
#[cfg(feature = "noauth_tests")]
async fn test_rename_object_basic() {
    let client = create_s3_client().await;
    let bucket = "sdk-rename-test";

    // Setup: create bucket and put an object
    let _ = client.create_bucket().bucket(bucket).send().await;

    let content = Bytes::from("rename me content");
    client
        .put_object()
        .bucket(bucket)
        .key("original-key")
        .body(content.clone().into())
        .content_type("text/plain")
        .send()
        .await
        .expect("Failed to put object");

    // Rename using raw HTTP request (AWS SDK doesn't have native RenameObject)
    let http_client = reqwest::Client::new();
    let response = http_client
        .put(&format!(
            "{}/{}/new-key?renameObject", get_test_base_url(),
            bucket
        ))
        .header("x-amz-rename-source", "original-key")
        .send()
        .await
        .expect("Failed to send rename request");

    assert_eq!(
        response.status(),
        200,
        "Rename should return 200: {:?}",
        response.text().await
    );

    // Verify old key no longer exists
    let old_result = client
        .get_object()
        .bucket(bucket)
        .key("original-key")
        .send()
        .await;
    assert!(
        old_result.is_err(),
        "Original key should not exist after rename"
    );

    // Verify new key exists with correct content
    let new_response = client
        .get_object()
        .bucket(bucket)
        .key("new-key")
        .send()
        .await
        .expect("Failed to get renamed object");

    let body = new_response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body, content, "Renamed object content should match");

    // Cleanup
    client
        .delete_object()
        .bucket(bucket)
        .key("new-key")
        .send()
        .await
        .ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test rename object with URL-encoded source key (simple case with spaces)
#[tokio::test]
#[ignore]
#[cfg(feature = "noauth_tests")]
async fn test_rename_object_url_encoded_source() {
    let client = create_s3_client().await;
    let bucket = "sdk-rename-encoded-test";

    // Setup
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Use a key with spaces that needs URL encoding
    let original_key = "file with spaces.txt";
    let content = Bytes::from("special chars content");
    client
        .put_object()
        .bucket(bucket)
        .key(original_key)
        .body(content.clone().into())
        .send()
        .await
        .expect("Failed to put object");

    // Rename with URL-encoded source (spaces become %20)
    let http_client = reqwest::Client::new();
    let response = http_client
        .put(&format!(
            "{}/{}/renamed-file.txt?renameObject", get_test_base_url(),
            bucket
        ))
        .header("x-amz-rename-source", "file%20with%20spaces.txt")
        .send()
        .await
        .expect("Failed to send rename request");

    assert_eq!(response.status(), 200);

    // Verify new key exists
    let new_response = client
        .get_object()
        .bucket(bucket)
        .key("renamed-file.txt")
        .send()
        .await
        .expect("Failed to get renamed object");

    let body = new_response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body, content);

    // Verify original key no longer exists
    let old_result = client
        .get_object()
        .bucket(bucket)
        .key(original_key)
        .send()
        .await;
    assert!(old_result.is_err(), "Original key should not exist");

    // Cleanup
    client
        .delete_object()
        .bucket(bucket)
        .key("renamed-file.txt")
        .send()
        .await
        .ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test rename to existing key (should overwrite by default)
#[tokio::test]
#[ignore]
#[cfg(feature = "noauth_tests")]
async fn test_rename_object_overwrites_existing() {
    let client = create_s3_client().await;
    let bucket = "sdk-rename-overwrite-test";

    // Setup
    let _ = client.create_bucket().bucket(bucket).send().await;

    let source_content = Bytes::from("source content");
    let dest_content = Bytes::from("destination content");

    client
        .put_object()
        .bucket(bucket)
        .key("source-key")
        .body(source_content.clone().into())
        .send()
        .await
        .expect("Failed to put source object");

    client
        .put_object()
        .bucket(bucket)
        .key("dest-key")
        .body(dest_content.into())
        .send()
        .await
        .expect("Failed to put destination object");

    // Rename source to dest (should overwrite)
    let http_client = reqwest::Client::new();
    let response = http_client
        .put(&format!(
            "{}/{}/dest-key?renameObject", get_test_base_url(),
            bucket
        ))
        .header("x-amz-rename-source", "source-key")
        .send()
        .await
        .expect("Failed to send rename request");

    assert_eq!(response.status(), 200);

    // Verify dest-key now has source content
    let get_response = client
        .get_object()
        .bucket(bucket)
        .key("dest-key")
        .send()
        .await
        .expect("Failed to get object");

    let body = get_response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body, source_content, "Content should be from source");

    // Verify source-key no longer exists
    let old_result = client
        .get_object()
        .bucket(bucket)
        .key("source-key")
        .send()
        .await;
    assert!(old_result.is_err());

    // Cleanup
    client
        .delete_object()
        .bucket(bucket)
        .key("dest-key")
        .send()
        .await
        .ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test rename non-existent source key
#[tokio::test]
#[ignore]
#[cfg(feature = "noauth_tests")]
async fn test_rename_object_source_not_found() {
    let client = create_s3_client().await;
    let bucket = "sdk-rename-404-test";

    // Setup
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Rename non-existent source
    let http_client = reqwest::Client::new();
    let response = http_client
        .put(&format!(
            "{}/{}/new-key?renameObject", get_test_base_url(),
            bucket
        ))
        .header("x-amz-rename-source", "nonexistent-key")
        .send()
        .await
        .expect("Failed to send rename request");

    assert_eq!(
        response.status(),
        404,
        "Should return 404 for non-existent source"
    );

    // Cleanup
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test rename without x-amz-rename-source header
#[tokio::test]
#[ignore]
#[cfg(feature = "noauth_tests")]
async fn test_rename_object_missing_source_header() {
    let client = create_s3_client().await;
    let bucket = "sdk-rename-missing-header-test";

    // Setup
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Rename without source header
    let http_client = reqwest::Client::new();
    let response = http_client
        .put(&format!(
            "{}/{}/new-key?renameObject", get_test_base_url(),
            bucket
        ))
        .send()
        .await
        .expect("Failed to send rename request");

    assert_eq!(
        response.status(),
        400,
        "Should return 400 for missing source header"
    );

    // Cleanup
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test rename in non-existent bucket
#[tokio::test]
#[ignore]
#[cfg(feature = "noauth_tests")]
async fn test_rename_object_bucket_not_found() {
    let http_client = reqwest::Client::new();
    let response = http_client
        .put(&format!("{}/nonexistent-bucket-xyz/new-key?renameObject", get_test_base_url()))
        .header("x-amz-rename-source", "old-key")
        .send()
        .await
        .expect("Failed to send rename request");

    assert_eq!(
        response.status(),
        404,
        "Should return 404 for non-existent bucket"
    );
}

/// Test rename preserves content-type
#[tokio::test]
#[ignore]
#[cfg(feature = "noauth_tests")]
async fn test_rename_object_preserves_content_type() {
    let client = create_s3_client().await;
    let bucket = "sdk-rename-metadata-test";

    // Setup
    let _ = client.create_bucket().bucket(bucket).send().await;

    let content = Bytes::from(r#"{"key": "value"}"#);
    client
        .put_object()
        .bucket(bucket)
        .key("original-key")
        .body(content.clone().into())
        .content_type("application/json")
        .send()
        .await
        .expect("Failed to put object");

    // Verify original content-type
    let original_head = client
        .head_object()
        .bucket(bucket)
        .key("original-key")
        .send()
        .await
        .expect("Failed to head original object");
    assert_eq!(
        original_head.content_type(),
        Some("application/json"),
        "Original Content-Type should be application/json"
    );

    // Rename
    let http_client = reqwest::Client::new();
    let response = http_client
        .put(&format!(
            "{}/{}/renamed-key?renameObject", get_test_base_url(),
            bucket
        ))
        .header("x-amz-rename-source", "original-key")
        .send()
        .await
        .expect("Failed to send rename request");

    assert_eq!(response.status(), 200);

    // Verify content-type preserved
    let head = client
        .head_object()
        .bucket(bucket)
        .key("renamed-key")
        .send()
        .await
        .expect("Failed to head object");

    assert_eq!(
        head.content_type(),
        Some("application/json"),
        "Content-Type should be preserved after rename"
    );

    // Verify content is preserved
    let get_response = client
        .get_object()
        .bucket(bucket)
        .key("renamed-key")
        .send()
        .await
        .expect("Failed to get renamed object");

    let body = get_response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body, content, "Content should be preserved after rename");

    // Cleanup
    client
        .delete_object()
        .bucket(bucket)
        .key("renamed-key")
        .send()
        .await
        .ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

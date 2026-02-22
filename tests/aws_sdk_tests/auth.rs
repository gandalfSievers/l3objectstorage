use super::*;

/// Create a client with invalid credentials for testing auth rejection
async fn create_s3_client_with_invalid_creds() -> Client {
    let endpoint = std::env::var("TEST_ENDPOINT_URL")
        .unwrap_or_else(|_| "http://localhost:9999".to_string());
    let config = aws_config::defaults(BehaviorVersion::latest())
        .endpoint_url(&endpoint)
        .region(aws_config::Region::new("us-east-1"))
        .credentials_provider(aws_credential_types::Credentials::new(
            "invalidkey",
            "invalidsecret",
            None,
            None,
            "test",
        ))
        .load()
        .await;

    let s3_config = aws_sdk_s3::config::Builder::from(&config)
        .force_path_style(true)
        .build();

    Client::from_conf(s3_config)
}

#[tokio::test]
#[ignore]
async fn test_auth_valid_signature() {
    // This test verifies that valid credentials work correctly
    // Uses the default localadmin/localadmin credentials
    let client = create_s3_client().await;

    // Simple operation that should succeed with valid credentials
    let result = client.list_buckets().send().await;
    assert!(result.is_ok(), "Valid credentials should be accepted");
}

/// Tests that invalid credentials are rejected when server has auth enabled.
///
/// **IMPORTANT**: This test MUST be run with auth enabled on the server:
///   `LOCAL_S3_REQUIRE_AUTH=true cargo run`
///
/// If this test passes when auth is disabled, it indicates a false positive.
/// Run `test_anonymous_access_when_auth_disabled` instead for that scenario.
///
/// This test is disabled when running with `--features noauth_tests`.
#[tokio::test]
#[ignore]
#[cfg(not(feature = "noauth_tests"))]
async fn test_auth_rejects_invalid_credentials() {
    let client = create_s3_client_with_invalid_creds().await;

    // This MUST fail with an authentication error when server has auth enabled
    let result = client.list_buckets().send().await;

    // We explicitly expect an error - if this succeeds, either:
    // 1. The server doesn't have auth enabled (test is running in wrong mode)
    // 2. The server has a bug allowing invalid credentials
    assert!(
        result.is_err(),
        "Invalid credentials MUST be rejected. If this fails, ensure server is running with LOCAL_S3_REQUIRE_AUTH=true"
    );

    let err = result.err().unwrap();
    let err_str = format!("{:?}", err);
    assert!(
        err_str.contains("SignatureDoesNotMatch")
            || err_str.contains("InvalidAccessKeyId")
            || err_str.contains("AccessDenied")
            || err_str.contains("403"),
        "Expected auth error (SignatureDoesNotMatch, InvalidAccessKeyId, or AccessDenied), got: {}",
        err_str
    );
}

/// Tests that anonymous access works when server auth is disabled.
///
/// **IMPORTANT**: This test MUST be run with auth disabled on the server:
///   `cargo run` (without LOCAL_S3_REQUIRE_AUTH)
///
/// This verifies the server correctly allows unauthenticated access when configured to do so.
///
/// This test is disabled by default. Run with `make test-integration-noauth` or:
///   `cargo test --test aws_sdk_tests --features noauth_tests -- --ignored`
#[tokio::test]
#[ignore]
#[cfg(feature = "noauth_tests")]
async fn test_anonymous_access_when_auth_disabled() {
    let client = create_s3_client_with_invalid_creds().await;

    // When auth is disabled, even invalid/no credentials should work
    let result = client.list_buckets().send().await;

    // We explicitly expect success - if this fails, either:
    // 1. The server has auth enabled (test is running in wrong mode)
    // 2. The server has a bug rejecting valid anonymous requests
    assert!(
        result.is_ok(),
        "Anonymous access should be allowed when auth is disabled. If this fails, ensure server is running WITHOUT LOCAL_S3_REQUIRE_AUTH"
    );
}

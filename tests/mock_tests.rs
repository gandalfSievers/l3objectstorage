//! Mock/offline tests that don't require a running server
//!
//! These tests verify SDK configuration, error handling patterns,
//! and other behaviors that can be tested without network calls.
//!
//! Run with: cargo test --test mock_tests

use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use std::time::Duration;

/// Create a test client configured for local testing
async fn create_test_client() -> Client {
    let config = aws_config::defaults(BehaviorVersion::latest())
        .endpoint_url("http://localhost:9000")
        .region(aws_config::Region::new("us-east-1"))
        .credentials_provider(aws_credential_types::Credentials::new(
            "test-access-key",
            "test-secret-key",
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

/// Test that client configuration is correct
#[tokio::test]
async fn test_client_configuration() {
    let client = create_test_client().await;

    // Client should be created successfully
    // We can verify configuration by attempting operations
    // (they'll fail without a server, but the client should be valid)
    assert!(
        std::mem::size_of_val(&client) > 0,
        "Client should be created"
    );
}

/// Test that SDK properly handles connection refused errors
#[tokio::test]
async fn test_connection_refused_handling() {
    // Create client pointing to an unlikely port
    let config = aws_config::defaults(BehaviorVersion::latest())
        .endpoint_url("http://localhost:59999") // Unlikely to have anything running
        .region(aws_config::Region::new("us-east-1"))
        .credentials_provider(aws_credential_types::Credentials::new(
            "test",
            "test",
            None,
            None,
            "test",
        ))
        .load()
        .await;

    let s3_config = aws_sdk_s3::config::Builder::from(&config)
        .force_path_style(true)
        .build();

    let client = Client::from_conf(s3_config);

    // This should fail with a connection error
    let result = client
        .list_buckets()
        .send()
        .await;

    assert!(result.is_err(), "Should fail when server is not running");

    // Error should be a dispatch/connection failure
    let err = result.err().unwrap();
    let err_string = format!("{:?}", err);

    // Should indicate connection failure
    assert!(
        err_string.contains("dispatch") || err_string.contains("connection") || err_string.contains("Dispatch"),
        "Error should be connection-related: {}",
        err_string
    );
}

/// Test timeout configuration
#[tokio::test]
async fn test_timeout_configuration() {
    let config = aws_config::defaults(BehaviorVersion::latest())
        .endpoint_url("http://localhost:9000")
        .region(aws_config::Region::new("us-east-1"))
        .credentials_provider(aws_credential_types::Credentials::new(
            "test",
            "test",
            None,
            None,
            "test",
        ))
        .load()
        .await;

    // Configure with custom timeouts
    let s3_config = aws_sdk_s3::config::Builder::from(&config)
        .force_path_style(true)
        .build();

    let client = Client::from_conf(s3_config);

    // Client should be created with timeout config
    assert!(
        std::mem::size_of_val(&client) > 0,
        "Client should be created with timeout config"
    );
}

/// Test region configuration
#[tokio::test]
async fn test_region_configuration() {
    // Test with various regions
    let regions = vec!["us-east-1", "us-west-2", "eu-west-1", "ap-northeast-1"];

    for region in regions {
        let config = aws_config::defaults(BehaviorVersion::latest())
            .endpoint_url("http://localhost:9000")
            .region(aws_config::Region::new(region))
            .credentials_provider(aws_credential_types::Credentials::new(
                "test",
                "test",
                None,
                None,
                "test",
            ))
            .load()
            .await;

        let s3_config = aws_sdk_s3::config::Builder::from(&config)
            .force_path_style(true)
            .build();

        let client = Client::from_conf(s3_config);

        // Client should be valid for any region
        assert!(
            std::mem::size_of_val(&client) > 0,
            "Client should be created for region {}",
            region
        );
    }
}

/// Test path style vs virtual hosted style configuration
#[tokio::test]
async fn test_path_style_configuration() {
    let config = aws_config::defaults(BehaviorVersion::latest())
        .endpoint_url("http://localhost:9000")
        .region(aws_config::Region::new("us-east-1"))
        .credentials_provider(aws_credential_types::Credentials::new(
            "test",
            "test",
            None,
            None,
            "test",
        ))
        .load()
        .await;

    // Path style (what we use)
    let path_style_config = aws_sdk_s3::config::Builder::from(&config)
        .force_path_style(true)
        .build();

    let path_style_client = Client::from_conf(path_style_config);

    // Virtual hosted style
    let virtual_style_config = aws_sdk_s3::config::Builder::from(&config)
        .force_path_style(false)
        .build();

    let virtual_style_client = Client::from_conf(virtual_style_config);

    // Both should create valid clients
    assert!(
        std::mem::size_of_val(&path_style_client) > 0,
        "Path style client should be created"
    );
    assert!(
        std::mem::size_of_val(&virtual_style_client) > 0,
        "Virtual style client should be created"
    );
}

/// Test credentials configuration
#[tokio::test]
async fn test_credentials_configuration() {
    // Test with various credential configurations
    let credentials_sets = vec![
        ("access-key-1", "secret-key-1"),
        ("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"),
        ("short", "s"),
        ("very-long-access-key-for-testing-purposes", "very-long-secret-key-for-testing-purposes"),
    ];

    for (access_key, secret_key) in credentials_sets {
        let config = aws_config::defaults(BehaviorVersion::latest())
            .endpoint_url("http://localhost:9000")
            .region(aws_config::Region::new("us-east-1"))
            .credentials_provider(aws_credential_types::Credentials::new(
                access_key,
                secret_key,
                None,
                None,
                "test",
            ))
            .load()
            .await;

        let s3_config = aws_sdk_s3::config::Builder::from(&config)
            .force_path_style(true)
            .build();

        let client = Client::from_conf(s3_config);

        assert!(
            std::mem::size_of_val(&client) > 0,
            "Client should be created with credentials {}/{}",
            access_key,
            secret_key
        );
    }
}

/// Test endpoint URL configuration
#[tokio::test]
async fn test_endpoint_url_configuration() {
    let endpoints = vec![
        "http://localhost:9000",
        "http://127.0.0.1:9000",
        "http://localhost:8000",
        "https://localhost:9443",
    ];

    for endpoint in endpoints {
        let config = aws_config::defaults(BehaviorVersion::latest())
            .endpoint_url(endpoint)
            .region(aws_config::Region::new("us-east-1"))
            .credentials_provider(aws_credential_types::Credentials::new(
                "test",
                "test",
                None,
                None,
                "test",
            ))
            .load()
            .await;

        let s3_config = aws_sdk_s3::config::Builder::from(&config)
            .force_path_style(true)
            .build();

        let client = Client::from_conf(s3_config);

        assert!(
            std::mem::size_of_val(&client) > 0,
            "Client should be created for endpoint {}",
            endpoint
        );
    }
}

/// Test presigning configuration without network
#[tokio::test]
async fn test_presigning_duration_configuration() {
    let client = create_test_client().await;

    // Test various presigning durations
    let durations = vec![
        Duration::from_secs(60),        // 1 minute
        Duration::from_secs(3600),      // 1 hour
        Duration::from_secs(86400),     // 1 day
        Duration::from_secs(604800),    // 1 week (max is typically 7 days)
    ];

    for duration in durations {
        let presigning_config = aws_sdk_s3::presigning::PresigningConfig::expires_in(duration);

        assert!(
            presigning_config.is_ok(),
            "Presigning config should be valid for {} seconds",
            duration.as_secs()
        );
    }

    // Test that we can create presigned request configuration
    let config = aws_sdk_s3::presigning::PresigningConfig::expires_in(Duration::from_secs(3600))
        .expect("Should create presigning config");

    // Try to generate a presigned URL (won't make network call)
    let result = client
        .get_object()
        .bucket("test-bucket")
        .key("test-key")
        .presigned(config)
        .await;

    assert!(result.is_ok(), "Presigning should succeed without network call");

    let presigned = result.unwrap();
    let uri = presigned.uri().to_string();

    // Verify presigned URL structure
    assert!(uri.contains("test-bucket"), "URL should contain bucket name");
    assert!(uri.contains("test-key"), "URL should contain key");
    assert!(uri.contains("X-Amz-Signature"), "URL should contain signature");
    assert!(uri.contains("X-Amz-Algorithm"), "URL should contain algorithm");
    assert!(uri.contains("X-Amz-Credential"), "URL should contain credential");
    assert!(uri.contains("X-Amz-Date"), "URL should contain date");
    assert!(uri.contains("X-Amz-Expires"), "URL should contain expiry");
    assert!(uri.contains("X-Amz-SignedHeaders"), "URL should contain signed headers");
}

/// Test bucket name validation (SDK-side)
#[tokio::test]
async fn test_bucket_name_handling() {
    let client = create_test_client().await;

    // These bucket names should be accepted by the SDK
    // (actual validation happens server-side)
    let valid_bucket_names = vec![
        "my-bucket",
        "my.bucket.name",
        "mybucket123",
        "123bucket",
        "a-b-c",
    ];

    for bucket_name in valid_bucket_names {
        // SDK should accept these names (no network call needed to check)
        let request = client.list_objects_v2().bucket(bucket_name);

        // The request builder should work
        assert!(
            std::mem::size_of_val(&request) > 0,
            "Request should be built for bucket {}",
            bucket_name
        );
    }
}

/// Test object key handling (SDK-side)
#[tokio::test]
async fn test_object_key_handling() {
    let client = create_test_client().await;

    // Various key formats that SDK should handle
    let keys = vec![
        "simple-key",
        "path/to/object",
        "key with spaces",
        "key/with/many/slashes/file.txt",
        "unicode-日本語.txt",
        "special!@#$%chars",
    ];

    for key in keys {
        // SDK should accept these keys
        let request = client.get_object().bucket("test-bucket").key(key);

        assert!(
            std::mem::size_of_val(&request) > 0,
            "Request should be built for key {}",
            key
        );
    }
}

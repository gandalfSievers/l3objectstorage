//! External integration tests using AWS SDK
//!
//! Run with: cargo test --test aws_sdk_tests -- --ignored
//! Requires: Server running on localhost (port configurable via TEST_ENDPOINT_URL env var)

pub use aws_config::BehaviorVersion;
pub use aws_sdk_s3::types::{
    BucketCannedAcl, CompletedMultipartUpload, CompletedPart, CorsConfiguration, CorsRule,
    DefaultRetention, Delete, ObjectIdentifier, ObjectLockConfiguration, ObjectLockEnabled,
    ObjectLockLegalHold, ObjectLockLegalHoldStatus, ObjectLockRetention, ObjectLockRetentionMode,
    ObjectLockRule, Permission, Tag, Tagging,
};
pub use aws_sdk_s3::Client;
pub use bytes::Bytes;

pub async fn create_s3_client() -> Client {
    let endpoint = std::env::var("TEST_ENDPOINT_URL")
        .unwrap_or_else(|_| "http://localhost:9999".to_string());
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
        .force_path_style(true)
        .build();

    Client::from_conf(s3_config)
}

mod acl;
mod attributes;
mod auth;
mod bucket;
mod conditional;
mod cors_policy;
mod delete;
mod encryption;
mod lifecycle;
mod list;
mod multipart;
mod object;
mod object_lock;
mod ownership_controls;
mod presigned;
mod public_access_block;
mod range;
mod tagging;
mod versioning;
mod website;
mod workflow;
mod rename;
mod logging;
mod notification;
mod replication;
mod request_payment;
mod select;
mod policy;
mod errors;
mod concurrency;
mod stress;
mod protocol;
mod performance;
mod notification_trigger;
mod virtual_host;

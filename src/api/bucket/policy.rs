//! Bucket policy operations

use bytes::Bytes;
use http_body_util::Full;
use hyper::{Response, StatusCode};

use crate::storage::StorageEngine;
use crate::types::error::{S3Error, S3Result};

/// Handle GetBucketPolicy request
pub async fn get_bucket_policy(
    storage: &StorageEngine,
    bucket: &str,
) -> S3Result<Response<Full<Bytes>>> {
    let policy = storage.get_bucket_policy(bucket).await?;

    // Return the policy as JSON (not XML)
    let response = Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/json")
        .body(Full::new(Bytes::from(policy)))
        .unwrap();

    Ok(response)
}

/// Handle PutBucketPolicy request
pub async fn put_bucket_policy(
    storage: &StorageEngine,
    bucket: &str,
    body: Bytes,
) -> S3Result<Response<Full<Bytes>>> {
    // Check bucket exists
    if !storage.bucket_exists(bucket).await {
        return Err(S3Error::no_such_bucket(bucket));
    }

    // Parse the request body as JSON to validate it
    let policy_str = std::str::from_utf8(&body)
        .map_err(|_| S3Error::new(
            crate::types::error::S3ErrorCode::MalformedXML,
            "Invalid UTF-8 in request body",
        ))?;

    // Validate that the policy is valid JSON
    let _: serde_json::Value = serde_json::from_str(policy_str)
        .map_err(|e| S3Error::new(
            crate::types::error::S3ErrorCode::MalformedXML,
            format!("Invalid JSON in policy: {}", e),
        ))?;

    storage.set_bucket_policy(bucket, policy_str.to_string()).await?;

    let response = Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Full::new(Bytes::new()))
        .unwrap();

    Ok(response)
}

/// Handle GetBucketPolicyStatus request
/// Returns whether the bucket policy allows public access
pub async fn get_bucket_policy_status(
    storage: &StorageEngine,
    bucket: &str,
) -> S3Result<Response<Full<Bytes>>> {
    // Get the bucket policy - this will fail if no policy exists
    let policy = storage.get_bucket_policy(bucket).await?;

    // Parse the policy and check if it's public
    let is_public = is_policy_public(&policy);

    // Build XML response
    let xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
        <PolicyStatus xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  \
        <IsPublic>{}</IsPublic>\n\
        </PolicyStatus>",
        is_public
    );

    let response = Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/xml")
        .body(Full::new(Bytes::from(xml)))
        .unwrap();

    Ok(response)
}

/// Check if a bucket policy allows public access
/// A policy is considered public if it has any statement with:
/// - Effect: Allow
/// - Principal: "*" or {"AWS": "*"}
/// without restrictive conditions
fn is_policy_public(policy: &str) -> bool {
    let parsed: Result<serde_json::Value, _> = serde_json::from_str(policy);

    match parsed {
        Ok(policy_value) => {
            if let Some(statements) = policy_value.get("Statement").and_then(|s| s.as_array()) {
                for statement in statements {
                    if is_statement_public(statement) {
                        return true;
                    }
                }
            }
            false
        }
        Err(_) => false,
    }
}

/// Check if a single policy statement grants public access
fn is_statement_public(statement: &serde_json::Value) -> bool {
    // Check Effect is Allow
    let effect = statement.get("Effect").and_then(|e| e.as_str());
    if effect != Some("Allow") {
        return false;
    }

    // Check Principal
    let principal = statement.get("Principal");
    if let Some(principal) = principal {
        // Check for "*" principal (everyone)
        if principal.as_str() == Some("*") {
            // Check for restrictive conditions
            if !has_restrictive_conditions(statement) {
                return true;
            }
        }

        // Check for {"AWS": "*"}
        if let Some(aws_principal) = principal.get("AWS") {
            if aws_principal.as_str() == Some("*") {
                if !has_restrictive_conditions(statement) {
                    return true;
                }
            }
            // Also check if AWS is an array containing "*"
            if let Some(arr) = aws_principal.as_array() {
                if arr.iter().any(|v| v.as_str() == Some("*")) {
                    if !has_restrictive_conditions(statement) {
                        return true;
                    }
                }
            }
        }
    }

    false
}

/// Check if a statement has restrictive conditions that would make it effectively non-public
/// This is a simplified check - AWS has more complex logic for this
fn has_restrictive_conditions(statement: &serde_json::Value) -> bool {
    if let Some(condition) = statement.get("Condition") {
        // If there's an IpAddress, StringEquals for aws:SourceArn, aws:SourceVpc, etc.
        // these are typically restrictive conditions
        let restrictive_keys = [
            "aws:SourceIp",
            "aws:SourceVpc",
            "aws:SourceVpce",
            "aws:SourceArn",
            "aws:SourceAccount",
            "aws:PrincipalArn",
            "aws:PrincipalOrgID",
        ];

        // Check common condition operators
        for operator in ["StringEquals", "StringLike", "ArnEquals", "ArnLike", "IpAddress"] {
            if let Some(op_conditions) = condition.get(operator) {
                for key in &restrictive_keys {
                    if op_conditions.get(*key).is_some() {
                        return true;
                    }
                }
            }
        }
    }
    false
}

/// Handle DeleteBucketPolicy request
pub async fn delete_bucket_policy(
    storage: &StorageEngine,
    bucket: &str,
) -> S3Result<Response<Full<Bytes>>> {
    // Check bucket exists
    if !storage.bucket_exists(bucket).await {
        return Err(S3Error::no_such_bucket(bucket));
    }

    storage.delete_bucket_policy(bucket).await?;

    let response = Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Full::new(Bytes::new()))
        .unwrap();

    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use tempfile::TempDir;

    async fn create_test_storage() -> (StorageEngine, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::new().with_data_dir(temp_dir.path());
        let storage = StorageEngine::new(config).await.unwrap();
        (storage, temp_dir)
    }

    #[tokio::test]
    async fn test_put_get_delete_bucket_policy() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        // Put policy
        let policy = r#"{"Version":"2012-10-17","Statement":[]}"#;
        let response = put_bucket_policy(&storage, "test-bucket", Bytes::from(policy))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        // Get policy
        let response = get_bucket_policy(&storage, "test-bucket").await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Delete policy
        let response = delete_bucket_policy(&storage, "test-bucket")
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        // Get policy should fail now
        let result = get_bucket_policy(&storage, "test-bucket").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_bucket_policy_no_policy() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        // Should fail with NoSuchBucketPolicy
        let result = get_bucket_policy(&storage, "test-bucket").await;
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert_eq!(err.code, crate::types::error::S3ErrorCode::NoSuchBucketPolicy);
        assert_eq!(err.http_status(), 404);
    }

    #[tokio::test]
    async fn test_put_bucket_policy_bucket_not_found() {
        let (storage, _temp) = create_test_storage().await;

        let policy = r#"{"Version":"2012-10-17","Statement":[]}"#;
        let result = put_bucket_policy(&storage, "nonexistent", Bytes::from(policy)).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_put_bucket_policy_invalid_json() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        // Invalid JSON
        let policy = r#"not valid json"#;
        let result = put_bucket_policy(&storage, "test-bucket", Bytes::from(policy)).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_bucket_policy_status_public() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        // Put a public policy (Principal: "*")
        let public_policy = r#"{
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "PublicRead",
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:GetObject",
                    "Resource": "arn:aws:s3:::test-bucket/*"
                }
            ]
        }"#;
        put_bucket_policy(&storage, "test-bucket", Bytes::from(public_policy))
            .await
            .unwrap();

        // Get policy status
        let response = get_bucket_policy_status(&storage, "test-bucket")
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Parse XML response
        let body = response.into_body();
        let body_bytes = http_body_util::BodyExt::collect(body).await.unwrap().to_bytes();
        let body_str = String::from_utf8(body_bytes.to_vec()).unwrap();
        assert!(body_str.contains("<IsPublic>true</IsPublic>"));
    }

    #[tokio::test]
    async fn test_get_bucket_policy_status_private() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        // Put a private policy (specific Principal)
        let private_policy = r#"{
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "PrivateAccess",
                    "Effect": "Allow",
                    "Principal": {"AWS": "arn:aws:iam::123456789012:root"},
                    "Action": "s3:GetObject",
                    "Resource": "arn:aws:s3:::test-bucket/*"
                }
            ]
        }"#;
        put_bucket_policy(&storage, "test-bucket", Bytes::from(private_policy))
            .await
            .unwrap();

        // Get policy status
        let response = get_bucket_policy_status(&storage, "test-bucket")
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Parse XML response
        let body = response.into_body();
        let body_bytes = http_body_util::BodyExt::collect(body).await.unwrap().to_bytes();
        let body_str = String::from_utf8(body_bytes.to_vec()).unwrap();
        assert!(body_str.contains("<IsPublic>false</IsPublic>"));
    }

    #[tokio::test]
    async fn test_get_bucket_policy_status_no_policy() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        // Should fail when no policy exists
        let result = get_bucket_policy_status(&storage, "test-bucket").await;
        assert!(result.is_err());
    }

    #[test]
    fn test_is_policy_public_star_principal() {
        let policy = r#"{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": "*"
            }]
        }"#;
        assert!(is_policy_public(policy));
    }

    #[test]
    fn test_is_policy_public_aws_star_principal() {
        let policy = r#"{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"AWS": "*"},
                "Action": "s3:GetObject",
                "Resource": "*"
            }]
        }"#;
        assert!(is_policy_public(policy));
    }

    #[test]
    fn test_is_policy_public_aws_star_in_array() {
        let policy = r#"{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"AWS": ["*"]},
                "Action": "s3:GetObject",
                "Resource": "*"
            }]
        }"#;
        assert!(is_policy_public(policy));
    }

    #[test]
    fn test_is_policy_not_public_specific_principal() {
        let policy = r#"{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"AWS": "arn:aws:iam::123456789012:root"},
                "Action": "s3:GetObject",
                "Resource": "*"
            }]
        }"#;
        assert!(!is_policy_public(policy));
    }

    #[test]
    fn test_is_policy_not_public_deny_effect() {
        let policy = r#"{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Deny",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": "*"
            }]
        }"#;
        assert!(!is_policy_public(policy));
    }

    #[test]
    fn test_is_policy_not_public_with_restrictive_condition() {
        let policy = r#"{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": "*",
                "Condition": {
                    "IpAddress": {
                        "aws:SourceIp": "192.168.1.0/24"
                    }
                }
            }]
        }"#;
        assert!(!is_policy_public(policy));
    }

    #[test]
    fn test_is_policy_not_public_empty_statements() {
        let policy = r#"{
            "Version": "2012-10-17",
            "Statement": []
        }"#;
        assert!(!is_policy_public(policy));
    }
}

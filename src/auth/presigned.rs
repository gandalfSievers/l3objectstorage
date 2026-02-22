//! Pre-signed URL parameter parsing and validation

use std::collections::HashMap;

use chrono::{Duration, Utc};

use crate::types::error::{S3Error, S3ErrorCode, S3Result};

/// Parsed pre-signed URL query parameters
#[derive(Debug, Clone)]
pub struct PresignedUrlParams {
    /// X-Amz-Algorithm (should be AWS4-HMAC-SHA256)
    pub algorithm: String,
    /// X-Amz-Credential (access-key/date/region/s3/aws4_request)
    pub credential: String,
    /// X-Amz-Date (ISO8601: 20130524T000000Z)
    pub date: String,
    /// X-Amz-Expires (seconds)
    pub expires: u64,
    /// X-Amz-SignedHeaders (semicolon-separated)
    pub signed_headers: Vec<String>,
    /// X-Amz-Signature (hex-encoded)
    pub signature: String,
}

impl PresignedUrlParams {
    /// Parse pre-signed URL parameters from query string HashMap
    pub fn from_query_params(params: &HashMap<String, String>) -> S3Result<Self> {
        let algorithm = params
            .get("X-Amz-Algorithm")
            .ok_or_else(|| {
                S3Error::new(
                    S3ErrorCode::InvalidArgument,
                    "Missing X-Amz-Algorithm parameter",
                )
            })?
            .clone();

        let credential = params
            .get("X-Amz-Credential")
            .ok_or_else(|| {
                S3Error::new(
                    S3ErrorCode::InvalidArgument,
                    "Missing X-Amz-Credential parameter",
                )
            })?
            .clone();

        let date = params
            .get("X-Amz-Date")
            .ok_or_else(|| {
                S3Error::new(
                    S3ErrorCode::InvalidArgument,
                    "Missing X-Amz-Date parameter",
                )
            })?
            .clone();

        let expires_str = params.get("X-Amz-Expires").ok_or_else(|| {
            S3Error::new(
                S3ErrorCode::InvalidArgument,
                "Missing X-Amz-Expires parameter",
            )
        })?;

        let expires = expires_str.parse::<u64>().map_err(|_| {
            S3Error::new(
                S3ErrorCode::InvalidArgument,
                "Invalid X-Amz-Expires value",
            )
        })?;

        let signed_headers_str = params.get("X-Amz-SignedHeaders").ok_or_else(|| {
            S3Error::new(
                S3ErrorCode::InvalidArgument,
                "Missing X-Amz-SignedHeaders parameter",
            )
        })?;

        let signed_headers: Vec<String> =
            signed_headers_str.split(';').map(String::from).collect();

        let signature = params
            .get("X-Amz-Signature")
            .ok_or_else(|| {
                S3Error::new(
                    S3ErrorCode::InvalidArgument,
                    "Missing X-Amz-Signature parameter",
                )
            })?
            .clone();

        Ok(Self {
            algorithm,
            credential,
            date,
            expires,
            signed_headers,
            signature,
        })
    }

    /// Check if the pre-signed URL has expired
    pub fn is_expired(&self) -> bool {
        use chrono::NaiveDateTime;

        // Parse X-Amz-Date (format: 20130524T000000Z)
        // The 'Z' suffix indicates UTC but we parse it separately
        let date_str = self.date.trim_end_matches('Z');
        let sign_time = match NaiveDateTime::parse_from_str(date_str, "%Y%m%dT%H%M%S") {
            Ok(dt) => dt.and_utc(),
            Err(e) => {
                // If we can't parse the date, consider it expired
                tracing::warn!("Failed to parse X-Amz-Date '{}': {}", self.date, e);
                return true;
            }
        };

        let expiry_time = sign_time + Duration::seconds(self.expires as i64);
        let now = Utc::now();

        tracing::debug!(
            "Pre-signed URL expiration check: signed={}, expires_in={}s, expiry={}, now={}",
            sign_time,
            self.expires,
            expiry_time,
            now
        );

        now > expiry_time
    }

    /// Extract access key ID from credential string
    /// Credential format: AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request
    pub fn access_key_id(&self) -> S3Result<&str> {
        self.credential
            .split('/')
            .next()
            .ok_or_else(|| {
                S3Error::new(
                    S3ErrorCode::InvalidArgument,
                    "Invalid X-Amz-Credential format",
                )
            })
    }

    /// Get credential scope (date/region/service/aws4_request)
    /// Returns everything after the access key ID
    pub fn credential_scope(&self) -> &str {
        match self.credential.find('/') {
            Some(idx) => &self.credential[idx + 1..],
            None => "",
        }
    }

    /// Get the date portion only (YYYYMMDD) from the full date
    pub fn date_stamp(&self) -> &str {
        if self.date.len() >= 8 {
            &self.date[..8]
        } else {
            &self.date
        }
    }
}

/// Check if query params contain pre-signed URL authentication parameters
pub fn has_presigned_params(query_params: &HashMap<String, String>) -> bool {
    query_params.contains_key("X-Amz-Signature")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_valid_params() -> HashMap<String, String> {
        let mut params = HashMap::new();
        params.insert(
            "X-Amz-Algorithm".to_string(),
            "AWS4-HMAC-SHA256".to_string(),
        );
        params.insert(
            "X-Amz-Credential".to_string(),
            "AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request".to_string(),
        );
        params.insert("X-Amz-Date".to_string(), "20130524T000000Z".to_string());
        params.insert("X-Amz-Expires".to_string(), "86400".to_string());
        params.insert("X-Amz-SignedHeaders".to_string(), "host".to_string());
        params.insert("X-Amz-Signature".to_string(), "abc123def456".to_string());
        params
    }

    #[test]
    fn test_parse_presigned_params() {
        let params = create_valid_params();
        let parsed = PresignedUrlParams::from_query_params(&params).unwrap();

        assert_eq!(parsed.algorithm, "AWS4-HMAC-SHA256");
        assert_eq!(parsed.expires, 86400);
        assert_eq!(parsed.signed_headers, vec!["host"]);
        assert_eq!(parsed.signature, "abc123def456");
    }

    #[test]
    fn test_missing_required_param() {
        let params = HashMap::new();
        let result = PresignedUrlParams::from_query_params(&params);
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_access_key_id() {
        let params = create_valid_params();
        let parsed = PresignedUrlParams::from_query_params(&params).unwrap();

        assert_eq!(parsed.access_key_id().unwrap(), "AKIAIOSFODNN7EXAMPLE");
    }

    #[test]
    fn test_credential_scope() {
        let params = create_valid_params();
        let parsed = PresignedUrlParams::from_query_params(&params).unwrap();

        assert_eq!(
            parsed.credential_scope(),
            "20130524/us-east-1/s3/aws4_request"
        );
    }

    #[test]
    fn test_date_stamp() {
        let params = create_valid_params();
        let parsed = PresignedUrlParams::from_query_params(&params).unwrap();

        assert_eq!(parsed.date_stamp(), "20130524");
    }

    #[test]
    fn test_expiration_past_date() {
        // Test with a date far in the past (should be expired)
        let mut params = create_valid_params();
        params.insert("X-Amz-Date".to_string(), "20200101T000000Z".to_string());
        params.insert("X-Amz-Expires".to_string(), "3600".to_string());

        let parsed = PresignedUrlParams::from_query_params(&params).unwrap();
        assert!(parsed.is_expired());
    }

    #[test]
    fn test_has_presigned_params() {
        let mut params = HashMap::new();
        assert!(!has_presigned_params(&params));

        params.insert("X-Amz-Signature".to_string(), "abc".to_string());
        assert!(has_presigned_params(&params));
    }

    #[test]
    fn test_multiple_signed_headers() {
        let mut params = create_valid_params();
        params.insert(
            "X-Amz-SignedHeaders".to_string(),
            "host;x-amz-content-sha256".to_string(),
        );

        let parsed = PresignedUrlParams::from_query_params(&params).unwrap();
        assert_eq!(
            parsed.signed_headers,
            vec!["host", "x-amz-content-sha256"]
        );
    }

    #[test]
    fn test_invalid_expires_value() {
        let mut params = create_valid_params();
        params.insert("X-Amz-Expires".to_string(), "not-a-number".to_string());

        let result = PresignedUrlParams::from_query_params(&params);
        assert!(result.is_err());
    }
}

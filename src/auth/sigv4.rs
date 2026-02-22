//! AWS Signature Version 4 verification

use ring::hmac;

use super::presigned::PresignedUrlParams;
use super::Credentials;
use crate::types::error::{S3Error, S3ErrorCode, S3Result};

/// Verifies AWS Signature Version 4 signatures
pub struct SigV4Verifier {
    credentials: Credentials,
    region: String,
    service: String,
}

impl SigV4Verifier {
    pub fn new(credentials: Credentials, region: impl Into<String>) -> Self {
        Self {
            credentials,
            region: region.into(),
            service: "s3".to_string(),
        }
    }

    /// Verify a request signature
    /// Returns Ok(()) if valid, Err if invalid
    pub fn verify(
        &self,
        method: &str,
        uri: &str,
        headers: &[(String, String)],
        payload_hash: &str,
        authorization_header: &str,
    ) -> S3Result<()> {
        // Parse the authorization header
        let auth_parts = self.parse_authorization_header(authorization_header)?;

        // Extract date from headers
        let date = headers
            .iter()
            .find(|(k, _)| k.to_lowercase() == "x-amz-date")
            .map(|(_, v)| v.as_str())
            .ok_or_else(|| {
                S3Error::new(S3ErrorCode::MissingSecurityHeader, "Missing X-Amz-Date header")
            })?;

        // Extract query string from URI if present
        let (path, query_string) = match uri.find('?') {
            Some(idx) => (&uri[..idx], &uri[idx + 1..]),
            None => (uri, ""),
        };

        // Build canonical query string (sorted and URL-encoded)
        let canonical_query = self.build_canonical_query_string(query_string);

        // Build the canonical URI (URI-encode path, preserving /)
        let canonical_uri = self.build_canonical_uri(path);

        // Build the canonical request
        let canonical_request = self.build_canonical_request(
            method,
            &canonical_uri,
            &canonical_query,
            headers,
            &auth_parts.signed_headers,
            payload_hash,
        )?;

        // Build the string to sign
        let string_to_sign = self.build_string_to_sign(
            date,
            &auth_parts.credential_scope,
            &canonical_request,
        );

        // Calculate the expected signature
        let date_only = &date[..8];
        let expected_signature = self.calculate_signature(
            &self.credentials.secret_access_key,
            date_only,
            &self.region,
            &self.service,
            &string_to_sign,
        );

        // Debug logging for signature verification
        tracing::debug!("SigV4 verification:");
        tracing::debug!("  Signed headers from auth: {:?}", auth_parts.signed_headers);
        tracing::debug!("  Credential scope: {}", auth_parts.credential_scope);
        tracing::debug!("  Canonical request:\n{}", canonical_request);
        tracing::debug!("  String to sign:\n{}", string_to_sign);
        tracing::debug!("  Expected signature: {}", expected_signature);
        tracing::debug!("  Received signature: {}", auth_parts.signature);

        // Compare signatures
        if auth_parts.signature != expected_signature {
            tracing::warn!("=== SIGNATURE MISMATCH DEBUG ===");
            tracing::warn!("Expected signature: {}", expected_signature);
            tracing::warn!("Received signature: {}", auth_parts.signature);
            tracing::warn!("Signed headers: {:?}", auth_parts.signed_headers);
            tracing::warn!("Canonical request (escaped newlines for clarity):");
            for (i, line) in canonical_request.lines().enumerate() {
                tracing::warn!("  Line {}: {:?}", i, line);
            }
            tracing::warn!("String to sign:");
            for (i, line) in string_to_sign.lines().enumerate() {
                tracing::warn!("  Line {}: {:?}", i, line);
            }
            return Err(S3Error::new(
                S3ErrorCode::SignatureDoesNotMatch,
                "The request signature we calculated does not match the signature you provided",
            ));
        }

        tracing::debug!("Signature verified successfully");
        Ok(())
    }

    /// Verify a pre-signed URL request
    /// Key differences from header-based auth:
    /// 1. Auth params come from query string, not headers
    /// 2. Canonical query string EXCLUDES X-Amz-Signature
    /// 3. Payload hash is typically UNSIGNED-PAYLOAD
    pub fn verify_presigned(
        &self,
        method: &str,
        path: &str,
        query_string: &str,
        headers: &[(String, String)],
        payload_hash: &str,
        params: &PresignedUrlParams,
    ) -> S3Result<()> {
        // Build canonical query string (excluding X-Amz-Signature)
        let canonical_query = self.build_presigned_canonical_query_string(query_string);

        // Build the canonical URI
        let canonical_uri = self.build_canonical_uri(path);

        // Build the canonical request
        let canonical_request = self.build_canonical_request(
            method,
            &canonical_uri,
            &canonical_query,
            headers,
            &params.signed_headers,
            payload_hash,
        )?;

        // Build the string to sign
        let string_to_sign = self.build_string_to_sign(
            &params.date,
            params.credential_scope(),
            &canonical_request,
        );

        // Calculate the expected signature
        let date_only = params.date_stamp();
        let expected_signature = self.calculate_signature(
            &self.credentials.secret_access_key,
            date_only,
            &self.region,
            &self.service,
            &string_to_sign,
        );

        // Debug logging for signature verification
        tracing::debug!("Pre-signed URL SigV4 verification:");
        tracing::debug!("  Path: {}", path);
        tracing::debug!("  Query string: {}", query_string);
        tracing::debug!("  Canonical query: {}", canonical_query);
        tracing::debug!("  Canonical request:\n{}", canonical_request);
        tracing::debug!("  String to sign:\n{}", string_to_sign);
        tracing::debug!("  Expected signature: {}", expected_signature);
        tracing::debug!("  Received signature: {}", params.signature);

        // Compare signatures
        if params.signature != expected_signature {
            tracing::warn!(
                "Pre-signed URL signature mismatch: expected={}, received={}",
                expected_signature,
                params.signature
            );
            return Err(S3Error::new(
                S3ErrorCode::SignatureDoesNotMatch,
                "The request signature we calculated does not match the signature you provided",
            ));
        }

        tracing::debug!("Pre-signed URL signature verified successfully");
        Ok(())
    }

    /// Build canonical query string for pre-signed URLs
    /// MUST exclude X-Amz-Signature but include all other parameters
    fn build_presigned_canonical_query_string(&self, query_string: &str) -> String {
        if query_string.is_empty() {
            return String::new();
        }

        let mut params: Vec<(String, String)> = query_string
            .split('&')
            .filter(|s| !s.is_empty())
            // CRITICAL: Exclude X-Amz-Signature from canonical query string
            .filter(|s| !s.starts_with("X-Amz-Signature="))
            .map(|pair| {
                if let Some((key, value)) = pair.split_once('=') {
                    // URL-decode first to handle already-encoded values, then re-encode
                    // This ensures consistent encoding without double-encoding
                    let decoded_key = percent_decode(key);
                    let decoded_value = percent_decode(value);
                    (url_encode(&decoded_key), url_encode(&decoded_value))
                } else {
                    let decoded = percent_decode(pair);
                    (url_encode(&decoded), String::new())
                }
            })
            .collect();

        // Sort by parameter name (then by value for same name)
        params.sort_by(|a, b| {
            let key_cmp = a.0.cmp(&b.0);
            if key_cmp == std::cmp::Ordering::Equal {
                a.1.cmp(&b.1)
            } else {
                key_cmp
            }
        });

        params
            .into_iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join("&")
    }

    fn parse_authorization_header(&self, header: &str) -> S3Result<AuthorizationParts> {
        // Format: AWS4-HMAC-SHA256 Credential=.../..., SignedHeaders=..., Signature=...
        let header = header.strip_prefix("AWS4-HMAC-SHA256 ").ok_or_else(|| {
            S3Error::new(S3ErrorCode::InvalidSecurity, "Invalid authorization header format")
        })?;

        let mut credential = String::new();
        let mut credential_scope = String::new();
        let mut signed_headers = Vec::new();
        let mut signature = String::new();

        for part in header.split(", ") {
            if let Some(value) = part.strip_prefix("Credential=") {
                credential = value.to_string();
                // Extract credential scope (everything after the access key ID)
                if let Some(idx) = value.find('/') {
                    credential_scope = value[idx + 1..].to_string();
                }
            } else if let Some(value) = part.strip_prefix("SignedHeaders=") {
                signed_headers = value.split(';').map(String::from).collect();
            } else if let Some(value) = part.strip_prefix("Signature=") {
                signature = value.to_string();
            }
        }

        Ok(AuthorizationParts {
            credential,
            credential_scope,
            signed_headers,
            signature,
        })
    }

    /// Build canonical query string according to AWS SigV4 spec
    /// Query parameters must be sorted by name, then URI-encoded
    /// The incoming query string is already URL-encoded, so we decode first then re-encode
    /// to ensure consistent encoding (avoiding double-encoding issues)
    fn build_canonical_query_string(&self, query_string: &str) -> String {
        if query_string.is_empty() {
            return String::new();
        }

        let mut params: Vec<(String, String)> = query_string
            .split('&')
            .filter(|s| !s.is_empty())
            .map(|pair| {
                if let Some((key, value)) = pair.split_once('=') {
                    // URL-decode first to handle already-encoded values, then re-encode
                    // This ensures consistent encoding without double-encoding
                    let decoded_key = percent_decode(key);
                    let decoded_value = percent_decode(value);
                    (url_encode(&decoded_key), url_encode(&decoded_value))
                } else {
                    let decoded = percent_decode(pair);
                    (url_encode(&decoded), String::new())
                }
            })
            .collect();

        // Sort by parameter name (then by value for same name)
        params.sort_by(|a, b| {
            let key_cmp = a.0.cmp(&b.0);
            if key_cmp == std::cmp::Ordering::Equal {
                a.1.cmp(&b.1)
            } else {
                key_cmp
            }
        });

        params
            .into_iter()
            .map(|(k, v)| {
                // AWS SigV4 spec: query parameters ALWAYS have '=' even for empty values
                // e.g., "tagging" becomes "tagging=" in the canonical query string
                format!("{}={}", k, v)
            })
            .collect::<Vec<_>>()
            .join("&")
    }

    /// Build canonical URI by URI-encoding each path segment while preserving slashes
    fn build_canonical_uri(&self, path: &str) -> String {
        if path.is_empty() {
            return "/".to_string();
        }
        // First decode the path (it may already be URL-encoded from the HTTP request),
        // then re-encode according to AWS SigV4 rules
        path.split('/')
            .map(|segment| {
                // Decode first (handles %20, %26, etc.)
                let decoded = percent_encoding::percent_decode_str(segment)
                    .decode_utf8_lossy()
                    .to_string();
                // Then re-encode according to AWS rules
                url_encode(&decoded)
            })
            .collect::<Vec<_>>()
            .join("/")
    }

    fn build_canonical_request(
        &self,
        method: &str,
        uri: &str,
        query_string: &str,
        headers: &[(String, String)],
        signed_headers: &[String],
        payload_hash: &str,
    ) -> S3Result<String> {
        // Build canonical headers - must be lowercase and use trimall
        // AWS SigV4 spec: if there are multiple values for the same header,
        // they must be combined into a comma-separated list, preserving order
        let mut canonical_headers = String::new();
        for signed_header in signed_headers {
            // Collect ALL values for this header (there may be multiple)
            let values: Vec<&str> = headers
                .iter()
                .filter(|(k, _)| k.to_lowercase() == *signed_header)
                .map(|(_, v)| v.as_str())
                .collect();

            if values.is_empty() {
                return Err(S3Error::new(
                    S3ErrorCode::InvalidSecurity,
                    &format!("Signed header '{}' not found in request", signed_header),
                ));
            }

            // Combine multiple values with comma, applying trimall to each
            let combined_value = values
                .iter()
                .map(|v| trimall(v))
                .collect::<Vec<_>>()
                .join(",");

            // AWS spec: lowercase header name, trimall for value
            canonical_headers.push_str(&format!(
                "{}:{}\n",
                signed_header.to_lowercase(),
                combined_value
            ));
        }

        let signed_headers_str = signed_headers.join(";");

        Ok(format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            method,
            uri,
            query_string,
            canonical_headers,
            signed_headers_str,
            payload_hash
        ))
    }

    fn build_string_to_sign(
        &self,
        date: &str,
        credential_scope: &str,
        canonical_request: &str,
    ) -> String {
        use ring::digest::{digest, SHA256};
        let canonical_request_hash = hex::encode(digest(&SHA256, canonical_request.as_bytes()));

        format!(
            "AWS4-HMAC-SHA256\n{}\n{}\n{}",
            date, credential_scope, canonical_request_hash
        )
    }

    fn calculate_signature(
        &self,
        secret_key: &str,
        date: &str,
        region: &str,
        service: &str,
        string_to_sign: &str,
    ) -> String {
        let k_secret = format!("AWS4{}", secret_key);
        let k_date = self.hmac_sha256(k_secret.as_bytes(), date.as_bytes());
        let k_region = self.hmac_sha256(&k_date, region.as_bytes());
        let k_service = self.hmac_sha256(&k_region, service.as_bytes());
        let k_signing = self.hmac_sha256(&k_service, b"aws4_request");

        hex::encode(self.hmac_sha256(&k_signing, string_to_sign.as_bytes()))
    }

    fn hmac_sha256(&self, key: &[u8], data: &[u8]) -> Vec<u8> {
        let key = hmac::Key::new(hmac::HMAC_SHA256, key);
        hmac::sign(&key, data).as_ref().to_vec()
    }
}

struct AuthorizationParts {
    #[allow(dead_code)]
    credential: String,
    credential_scope: String,
    signed_headers: Vec<String>,
    signature: String,
}

/// Trim whitespace and collapse multiple spaces to single space (AWS "trimall")
/// This is required for canonical header values per AWS SigV4 spec
fn trimall(s: &str) -> String {
    s.trim()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// URL-decode a string (percent-decode)
fn percent_decode(s: &str) -> String {
    let mut result = String::new();
    let mut chars = s.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '%' {
            let hex: String = chars.by_ref().take(2).collect();
            if hex.len() == 2 {
                if let Ok(byte) = u8::from_str_radix(&hex, 16) {
                    result.push(byte as char);
                } else {
                    result.push('%');
                    result.push_str(&hex);
                }
            } else {
                result.push('%');
                result.push_str(&hex);
            }
        } else {
            result.push(c);
        }
    }

    result
}

/// URL-encode a string according to AWS SigV4 rules
/// Only unreserved characters are left unencoded: A-Z, a-z, 0-9, '-', '.', '_', '~'
fn url_encode(s: &str) -> String {
    let mut result = String::new();
    for byte in s.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                result.push(byte as char);
            }
            _ => {
                result.push_str(&format!("%{:02X}", byte));
            }
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sigv4_verifier_creation() {
        let creds = Credentials::new("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        let verifier = SigV4Verifier::new(creds, "us-east-1");

        assert_eq!(verifier.region, "us-east-1");
        assert_eq!(verifier.service, "s3");
    }

    #[test]
    fn test_hmac_sha256() {
        let creds = Credentials::new("test", "secret");
        let verifier = SigV4Verifier::new(creds, "us-east-1");

        let result = verifier.hmac_sha256(b"key", b"data");

        // Should produce consistent output
        let result2 = verifier.hmac_sha256(b"key", b"data");
        assert_eq!(result, result2);
    }

    #[test]
    fn test_parse_authorization_header() {
        let creds = Credentials::new("AKIAIOSFODNN7EXAMPLE", "secret");
        let verifier = SigV4Verifier::new(creds, "us-east-1");

        let header = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=abc123";

        let parts = verifier.parse_authorization_header(header).unwrap();

        assert!(parts.credential.starts_with("AKIAIOSFODNN7EXAMPLE"));
        assert_eq!(parts.signed_headers, vec!["host", "x-amz-date"]);
        assert_eq!(parts.signature, "abc123");
    }

    #[test]
    fn test_invalid_authorization_header() {
        let creds = Credentials::new("test", "secret");
        let verifier = SigV4Verifier::new(creds, "us-east-1");

        let result = verifier.parse_authorization_header("InvalidHeader");

        assert!(result.is_err());
    }
}

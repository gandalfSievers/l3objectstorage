//! GetObject operation

use bytes::Bytes;
use http_body_util::Full;
use hyper::{Response, StatusCode};

use crate::storage::StorageEngine;
use crate::types::error::{S3Error, S3ErrorCode, S3Result};
use crate::utils::time::format_http_date;

/// Response header overrides for GetObject
/// These can be specified via query parameters in pre-signed URLs
#[derive(Debug, Clone, Default)]
pub struct ResponseHeaderOverrides {
    pub content_type: Option<String>,
    pub content_disposition: Option<String>,
    pub content_encoding: Option<String>,
    pub content_language: Option<String>,
    pub cache_control: Option<String>,
    pub expires: Option<String>,
}

/// Parsed range request
#[derive(Debug, Clone)]
pub struct ParsedRange {
    pub start: u64,
    pub end: u64, // inclusive
}

/// Parse a Range header value
/// Supports: bytes=0-100, bytes=100-, bytes=-100
fn parse_range(range_header: &str, content_length: u64) -> Result<ParsedRange, S3Error> {
    let range_header = range_header.trim();

    if !range_header.starts_with("bytes=") {
        return Err(S3Error::new(
            S3ErrorCode::InvalidRange,
            "Invalid range format: must start with 'bytes='",
        ));
    }

    let range_spec = &range_header[6..]; // Skip "bytes="

    // Handle suffix-byte-range-spec: bytes=-N (last N bytes)
    if range_spec.starts_with('-') {
        let suffix_length: u64 = range_spec[1..]
            .parse()
            .map_err(|_| S3Error::new(S3ErrorCode::InvalidRange, "Invalid suffix length"))?;

        if suffix_length == 0 {
            return Err(S3Error::new(
                S3ErrorCode::InvalidRange,
                "Suffix length must be greater than 0",
            ));
        }

        let start = if suffix_length >= content_length {
            0
        } else {
            content_length - suffix_length
        };
        let end = content_length - 1;

        return Ok(ParsedRange { start, end });
    }

    // Handle byte-range-spec: bytes=START-END or bytes=START-
    let parts: Vec<&str> = range_spec.split('-').collect();
    if parts.len() != 2 {
        return Err(S3Error::new(
            S3ErrorCode::InvalidRange,
            "Invalid range format",
        ));
    }

    let start: u64 = parts[0]
        .parse()
        .map_err(|_| S3Error::new(S3ErrorCode::InvalidRange, "Invalid range start"))?;

    // Check if start is beyond content
    if start >= content_length {
        return Err(S3Error::new(
            S3ErrorCode::InvalidRange,
            "Range start is beyond content length",
        ));
    }

    let end: u64 = if parts[1].is_empty() {
        // bytes=START- (from start to end)
        content_length - 1
    } else {
        let requested_end: u64 = parts[1]
            .parse()
            .map_err(|_| S3Error::new(S3ErrorCode::InvalidRange, "Invalid range end"))?;
        // Clamp end to content length - 1
        std::cmp::min(requested_end, content_length - 1)
    };

    if start > end {
        return Err(S3Error::new(
            S3ErrorCode::InvalidRange,
            "Range start is greater than end",
        ));
    }

    Ok(ParsedRange { start, end })
}

/// Check If-Match condition
/// Returns true if the request should proceed, false if precondition failed
fn check_if_match(if_match: &str, etag: &str) -> bool {
    let if_match = if_match.trim();

    // Wildcard matches any ETag
    if if_match == "*" {
        return true;
    }

    // Compare ETags (handle with or without quotes)
    let normalized_if_match = if_match.trim_matches('"');
    let normalized_etag = etag.trim_matches('"');

    normalized_if_match == normalized_etag
}

/// Check If-None-Match condition
/// Returns Some(true) if content should be returned
/// Returns Some(false) if 304 Not Modified should be returned
/// Returns None if wildcard and object doesn't exist (for PUT)
fn check_if_none_match(if_none_match: &str, etag: &str) -> bool {
    let if_none_match = if_none_match.trim();

    // Wildcard: if object exists, return 304/412
    if if_none_match == "*" {
        return false; // Object exists, so condition fails
    }

    // Compare ETags
    let normalized_if_none_match = if_none_match.trim_matches('"');
    let normalized_etag = etag.trim_matches('"');

    // If ETags match, return 304
    normalized_if_none_match != normalized_etag
}

/// Handle GetObject request
pub async fn get_object(
    storage: &StorageEngine,
    bucket: &str,
    key: &str,
) -> S3Result<Response<Full<Bytes>>> {
    get_object_with_conditionals(storage, bucket, key, None, None, None, None).await
}

/// Handle GetObject request with optional version ID
pub async fn get_object_versioned(
    storage: &StorageEngine,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
) -> S3Result<Response<Full<Bytes>>> {
    get_object_with_conditionals(storage, bucket, key, version_id, None, None, None).await
}

/// Handle GetObject request with full conditional and range support
pub async fn get_object_with_conditionals(
    storage: &StorageEngine,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
    range: Option<&str>,
    if_match: Option<&str>,
    if_none_match: Option<&str>,
) -> S3Result<Response<Full<Bytes>>> {
    get_object_full(
        storage,
        bucket,
        key,
        version_id,
        range,
        if_match,
        if_none_match,
        None,
    )
    .await
}

/// Handle GetObject request with full conditional, range, and response header override support
pub async fn get_object_full(
    storage: &StorageEngine,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
    range: Option<&str>,
    if_match: Option<&str>,
    if_none_match: Option<&str>,
    response_overrides: Option<&ResponseHeaderOverrides>,
) -> S3Result<Response<Full<Bytes>>> {
    let (object, data) = storage.get_object_versioned(bucket, key, version_id).await?;
    let content_length = data.len() as u64;

    // Check If-Match condition (must match for request to proceed)
    if let Some(if_match_value) = if_match {
        if !check_if_match(if_match_value, &object.etag) {
            return Err(S3Error::new(
                S3ErrorCode::PreconditionFailed,
                "At least one of the pre-conditions you specified did not hold",
            ));
        }
    }

    // Check If-None-Match condition
    if let Some(if_none_match_value) = if_none_match {
        if !check_if_none_match(if_none_match_value, &object.etag) {
            // Return 304 Not Modified
            let response = Response::builder()
                .status(StatusCode::NOT_MODIFIED)
                .header("ETag", &object.etag)
                .header("Last-Modified", format_http_date(&object.last_modified))
                .body(Full::new(Bytes::new()))
                .unwrap();
            return Ok(response);
        }
    }

    // Handle range request
    let (status, response_data, content_range) = if let Some(range_header) = range {
        let parsed_range = parse_range(range_header, content_length)?;
        let start = parsed_range.start as usize;
        let end = parsed_range.end as usize;

        let partial_data = data.slice(start..=end);
        let content_range = format!(
            "bytes {}-{}/{}",
            parsed_range.start, parsed_range.end, content_length
        );

        (StatusCode::PARTIAL_CONTENT, partial_data, Some(content_range))
    } else {
        (StatusCode::OK, data, None)
    };

    // Determine Content-Type (use override if provided, otherwise use object's content type)
    let content_type = response_overrides
        .and_then(|o| o.content_type.as_ref())
        .unwrap_or(&object.content_type);

    let mut response_builder = Response::builder()
        .status(status)
        .header("ETag", &object.etag)
        .header("Content-Type", content_type)
        .header("Content-Length", response_data.len())
        .header("Last-Modified", format_http_date(&object.last_modified))
        .header("Accept-Ranges", "bytes");

    // Add Content-Range header for partial content
    if let Some(range_value) = content_range {
        response_builder = response_builder.header("Content-Range", range_value);
    }

    // Add version ID header if present
    if let Some(ref vid) = object.version_id {
        response_builder = response_builder.header("x-amz-version-id", vid);
    }

    // Add SSE header if encryption was applied
    if let Some(ref sse) = object.sse_algorithm {
        response_builder = response_builder.header("x-amz-server-side-encryption", sse);
    }

    // Add custom metadata headers
    for (key, value) in &object.metadata {
        if key.starts_with("x-amz-meta-") {
            response_builder = response_builder.header(key, value);
        }
    }

    // Apply response header overrides
    if let Some(overrides) = response_overrides {
        if let Some(ref disposition) = overrides.content_disposition {
            response_builder = response_builder.header("Content-Disposition", disposition);
        }
        if let Some(ref encoding) = overrides.content_encoding {
            response_builder = response_builder.header("Content-Encoding", encoding);
        }
        if let Some(ref language) = overrides.content_language {
            response_builder = response_builder.header("Content-Language", language);
        }
        if let Some(ref cache_control) = overrides.cache_control {
            response_builder = response_builder.header("Cache-Control", cache_control);
        }
        if let Some(ref expires) = overrides.expires {
            response_builder = response_builder.header("Expires", expires);
        }
    }

    let response = response_builder.body(Full::new(response_data)).unwrap();

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
    async fn test_get_object_success() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();
        storage
            .put_object(
                "test-bucket",
                "test-key",
                Bytes::from("hello world"),
                Some("text/plain"),
                None,
            )
            .await
            .unwrap();

        let response = get_object(&storage, "test-bucket", "test-key")
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().contains_key("etag"));
        assert_eq!(
            response.headers().get("content-type").unwrap(),
            "text/plain"
        );
        assert_eq!(response.headers().get("content-length").unwrap(), "11");

        // Check body
        let body = response.into_body();
        let bytes = http_body_util::BodyExt::collect(body)
            .await
            .unwrap()
            .to_bytes();
        assert_eq!(bytes, Bytes::from("hello world"));
    }

    #[tokio::test]
    async fn test_get_object_not_found() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        let result = get_object(&storage, "test-bucket", "nonexistent").await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_object_bucket_not_found() {
        let (storage, _temp) = create_test_storage().await;

        let result = get_object(&storage, "nonexistent", "key").await;

        assert!(result.is_err());
    }

    // Range parsing tests
    #[test]
    fn test_parse_range_basic() {
        // bytes=0-4 for 20-byte content
        let range = parse_range("bytes=0-4", 20).unwrap();
        assert_eq!(range.start, 0);
        assert_eq!(range.end, 4);
    }

    #[test]
    fn test_parse_range_middle() {
        // bytes=5-9 for 20-byte content
        let range = parse_range("bytes=5-9", 20).unwrap();
        assert_eq!(range.start, 5);
        assert_eq!(range.end, 9);
    }

    #[test]
    fn test_parse_range_open_ended() {
        // bytes=10- for 20-byte content (from 10 to end)
        let range = parse_range("bytes=10-", 20).unwrap();
        assert_eq!(range.start, 10);
        assert_eq!(range.end, 19);
    }

    #[test]
    fn test_parse_range_suffix() {
        // bytes=-3 for 20-byte content (last 3 bytes)
        let range = parse_range("bytes=-3", 20).unwrap();
        assert_eq!(range.start, 17);
        assert_eq!(range.end, 19);
    }

    #[test]
    fn test_parse_range_beyond_content() {
        // bytes=5-100 for 10-byte content (should clamp to 9)
        let range = parse_range("bytes=5-100", 10).unwrap();
        assert_eq!(range.start, 5);
        assert_eq!(range.end, 9);
    }

    #[test]
    fn test_parse_range_invalid_start_beyond() {
        // bytes=100-200 for 20-byte content (start beyond content)
        let result = parse_range("bytes=100-200", 20);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_range_invalid_format() {
        // Invalid format
        let result = parse_range("invalid", 20);
        assert!(result.is_err());
    }

    // Conditional request tests
    #[test]
    fn test_check_if_match_exact() {
        assert!(check_if_match("\"abc123\"", "\"abc123\""));
        assert!(check_if_match("abc123", "abc123"));
        assert!(check_if_match("\"abc123\"", "abc123"));
        assert!(check_if_match("abc123", "\"abc123\""));
    }

    #[test]
    fn test_check_if_match_wildcard() {
        assert!(check_if_match("*", "\"anyetag\""));
    }

    #[test]
    fn test_check_if_match_mismatch() {
        assert!(!check_if_match("\"abc123\"", "\"xyz789\""));
    }

    #[test]
    fn test_check_if_none_match_different() {
        // Different ETags: content should be returned
        assert!(check_if_none_match("\"different\"", "\"abc123\""));
    }

    #[test]
    fn test_check_if_none_match_same() {
        // Same ETags: 304 should be returned
        assert!(!check_if_none_match("\"abc123\"", "\"abc123\""));
    }

    #[test]
    fn test_check_if_none_match_wildcard() {
        // Wildcard with existing object: 304/412 should be returned
        assert!(!check_if_none_match("*", "\"anyetag\""));
    }

    #[tokio::test]
    async fn test_get_object_with_range() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();
        storage
            .put_object(
                "test-bucket",
                "range-key",
                Bytes::from("0123456789ABCDEFGHIJ"),
                Some("text/plain"),
                None,
            )
            .await
            .unwrap();

        // Test bytes=0-4
        let response = get_object_with_conditionals(
            &storage,
            "test-bucket",
            "range-key",
            None,
            Some("bytes=0-4"),
            None,
            None,
        )
        .await
        .unwrap();

        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert!(response.headers().contains_key("content-range"));

        let body = response.into_body();
        let bytes = http_body_util::BodyExt::collect(body)
            .await
            .unwrap()
            .to_bytes();
        assert_eq!(bytes, Bytes::from("01234"));
    }

    #[tokio::test]
    async fn test_get_object_if_match_success() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();
        let obj = storage
            .put_object(
                "test-bucket",
                "conditional-key",
                Bytes::from("test content"),
                Some("text/plain"),
                None,
            )
            .await
            .unwrap();

        // Get with matching ETag should succeed
        let response = get_object_with_conditionals(
            &storage,
            "test-bucket",
            "conditional-key",
            None,
            None,
            Some(&obj.etag),
            None,
        )
        .await
        .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_object_if_match_failure() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();
        storage
            .put_object(
                "test-bucket",
                "conditional-key",
                Bytes::from("test content"),
                Some("text/plain"),
                None,
            )
            .await
            .unwrap();

        // Get with wrong ETag should fail
        let result = get_object_with_conditionals(
            &storage,
            "test-bucket",
            "conditional-key",
            None,
            None,
            Some("\"wrongetag\""),
            None,
        )
        .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, S3ErrorCode::PreconditionFailed);
    }

    #[tokio::test]
    async fn test_get_object_if_none_match_304() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();
        let obj = storage
            .put_object(
                "test-bucket",
                "conditional-key",
                Bytes::from("test content"),
                Some("text/plain"),
                None,
            )
            .await
            .unwrap();

        // Get with matching ETag should return 304
        let response = get_object_with_conditionals(
            &storage,
            "test-bucket",
            "conditional-key",
            None,
            None,
            None,
            Some(&obj.etag),
        )
        .await
        .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
    }
}

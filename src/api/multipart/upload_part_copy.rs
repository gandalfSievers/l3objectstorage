//! UploadPartCopy operation

use bytes::Bytes;
use http_body_util::Full;
use hyper::{Response, StatusCode};

use crate::storage::StorageEngine;
use crate::types::error::{S3Error, S3ErrorCode, S3Result};
use crate::types::response::CopyPartResult;
use crate::utils::xml::to_xml;

/// Handle UploadPartCopy request
pub async fn upload_part_copy(
    storage: &StorageEngine,
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number: i32,
    copy_source: &str,
    copy_source_range: Option<&str>,
) -> S3Result<Response<Full<Bytes>>> {
    // Parse source bucket and key from the copy source header
    // Format: /<bucket>/<key> or <bucket>/<key>
    let source = copy_source.trim_start_matches('/');
    let (src_bucket, src_key) = source
        .split_once('/')
        .ok_or_else(|| S3Error::new(S3ErrorCode::InvalidArgument, "Invalid x-amz-copy-source"))?;

    // URL decode the source key
    let src_key = percent_encoding::percent_decode_str(src_key)
        .decode_utf8_lossy()
        .to_string();

    // Get the source object data
    let (source_obj, mut data) = storage.get_object(src_bucket, &src_key).await?;

    // Handle byte range if specified
    if let Some(range) = copy_source_range {
        data = parse_and_apply_range(range, data)?;
    }

    // Upload the data as a part
    let part = storage
        .upload_part(bucket, key, upload_id, part_number, data)
        .await?;

    // Build the response
    let response_body = CopyPartResult::new(
        part.etag.clone(),
        source_obj.last_modified.to_rfc3339(),
    );

    let xml = to_xml(&response_body)?;

    let response = Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/xml")
        .body(Full::new(Bytes::from(xml)))
        .unwrap();

    Ok(response)
}

/// Parse a byte range header and extract the specified portion of data
/// Format: "bytes=start-end"
fn parse_and_apply_range(range: &str, data: Bytes) -> S3Result<Bytes> {
    let range = range
        .strip_prefix("bytes=")
        .ok_or_else(|| S3Error::new(S3ErrorCode::InvalidArgument, "Invalid range format"))?;

    let parts: Vec<&str> = range.split('-').collect();
    if parts.len() != 2 {
        return Err(S3Error::new(S3ErrorCode::InvalidArgument, "Invalid range format"));
    }

    let start: usize = parts[0]
        .parse()
        .map_err(|_| S3Error::new(S3ErrorCode::InvalidArgument, "Invalid range start"))?;
    let end: usize = parts[1]
        .parse()
        .map_err(|_| S3Error::new(S3ErrorCode::InvalidArgument, "Invalid range end"))?;

    // Validate range
    if start > end || end >= data.len() {
        return Err(S3Error::new(
            S3ErrorCode::InvalidRange,
            "Range out of bounds",
        ));
    }

    // Extract the range (inclusive of end)
    Ok(data.slice(start..=end))
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
    async fn test_upload_part_copy() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        // Create source object
        storage
            .put_object(
                "test-bucket",
                "source-key",
                Bytes::from("source data to copy"),
                None,
                None,
            )
            .await
            .unwrap();

        // Create multipart upload
        let upload_id = storage
            .create_multipart_upload("test-bucket", "dest-key")
            .await
            .unwrap();

        // Upload part by copy
        let response = upload_part_copy(
            &storage,
            "test-bucket",
            "dest-key",
            &upload_id,
            1,
            "test-bucket/source-key",
            None,
        )
        .await
        .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body();
        let bytes = http_body_util::BodyExt::collect(body)
            .await
            .unwrap()
            .to_bytes();
        let body_str = String::from_utf8_lossy(&bytes);

        assert!(body_str.contains("<CopyPartResult>"), "Should be CopyPartResult");
        assert!(body_str.contains("<ETag>"), "Should contain ETag");
        assert!(body_str.contains("<LastModified>"), "Should contain LastModified");
    }

    #[tokio::test]
    async fn test_upload_part_copy_with_range() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        // Create source object
        storage
            .put_object(
                "test-bucket",
                "source-key",
                Bytes::from("0123456789ABCDEF"),
                None,
                None,
            )
            .await
            .unwrap();

        // Create multipart upload
        let upload_id = storage
            .create_multipart_upload("test-bucket", "dest-key")
            .await
            .unwrap();

        // Upload part by copy with range
        let response = upload_part_copy(
            &storage,
            "test-bucket",
            "dest-key",
            &upload_id,
            1,
            "test-bucket/source-key",
            Some("bytes=5-9"),
        )
        .await
        .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        // Verify the part was created (we can check the parts list)
        let parts = storage.list_parts("test-bucket", &upload_id).await.unwrap();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].size, 5); // "56789" is 5 bytes
    }

    #[test]
    fn test_parse_and_apply_range() {
        let data = Bytes::from("0123456789");

        // Test normal range
        let result = parse_and_apply_range("bytes=0-4", data.clone()).unwrap();
        assert_eq!(result, Bytes::from("01234"));

        // Test middle range
        let result = parse_and_apply_range("bytes=3-6", data.clone()).unwrap();
        assert_eq!(result, Bytes::from("3456"));

        // Test end range
        let result = parse_and_apply_range("bytes=7-9", data.clone()).unwrap();
        assert_eq!(result, Bytes::from("789"));
    }

    #[test]
    fn test_parse_and_apply_range_invalid() {
        let data = Bytes::from("0123456789");

        // Invalid format
        assert!(parse_and_apply_range("0-4", data.clone()).is_err());

        // Out of bounds
        assert!(parse_and_apply_range("bytes=0-100", data.clone()).is_err());

        // Start > end
        assert!(parse_and_apply_range("bytes=5-3", data.clone()).is_err());
    }
}

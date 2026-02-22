//! CompleteMultipartUpload operation

use bytes::Bytes;
use http_body_util::Full;
use hyper::{Response, StatusCode};

use crate::storage::StorageEngine;
use crate::types::error::{S3Error, S3ErrorCode, S3Result};
use crate::types::response::CompleteMultipartUploadResponse;
use crate::utils::xml::to_xml;

/// Handle CompleteMultipartUpload request
pub async fn complete_multipart_upload(
    storage: &StorageEngine,
    bucket: &str,
    key: &str,
    upload_id: &str,
    body: Bytes,
    _sse_header: Option<&str>,
) -> S3Result<Response<Full<Bytes>>> {
    // Parse the request body to get the part list
    let parts = parse_complete_request(&body)?;

    // Get the multipart upload to retrieve the stored SSE algorithm
    // (SSE is specified at CreateMultipartUpload time and stored with the upload)
    let upload = storage.get_multipart_upload(bucket, upload_id).await?;
    let sse_algorithm = upload.sse_algorithm.as_ref().and_then(|s| {
        crate::types::bucket::SseAlgorithm::from_str(s)
    });

    let obj = storage
        .complete_multipart_upload_with_sse(bucket, key, upload_id, parts, sse_algorithm.as_ref())
        .await?;

    let response_body = CompleteMultipartUploadResponse {
        location: format!("/{}/{}", bucket, key),
        bucket: bucket.to_string(),
        key: key.to_string(),
        etag: obj.etag.clone(),
    };

    let xml = to_xml(&response_body)?;

    let mut response = Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/xml")
        .header("ETag", &obj.etag);

    // Add version ID header if versioning is enabled
    if let Some(ref version_id) = obj.version_id {
        response = response.header("x-amz-version-id", version_id);
    }

    // Add SSE header if encryption was applied
    if let Some(ref sse) = obj.sse_algorithm {
        response = response.header("x-amz-server-side-encryption", sse);
    }

    let response = response.body(Full::new(Bytes::from(xml))).unwrap();

    Ok(response)
}

/// Parse the CompleteMultipartUpload request XML body
fn parse_complete_request(body: &[u8]) -> S3Result<Vec<(i32, String)>> {
    let body_str = std::str::from_utf8(body)
        .map_err(|_| S3Error::new(S3ErrorCode::MalformedXML, "Invalid UTF-8 in request body"))?;

    let mut parts = Vec::new();

    // Parse <CompleteMultipartUpload><Part><PartNumber>N</PartNumber><ETag>...</ETag></Part>...</CompleteMultipartUpload>
    let mut remaining = body_str;

    while let Some(part_start) = remaining.find("<Part>") {
        let after_part = &remaining[part_start + 6..];

        if let Some(part_end) = after_part.find("</Part>") {
            let part_content = &after_part[..part_end];

            // Extract PartNumber
            let part_number = extract_xml_value(part_content, "PartNumber")
                .and_then(|s| s.parse::<i32>().ok());

            // Extract ETag
            let etag = extract_xml_value(part_content, "ETag");

            if let (Some(pn), Some(et)) = (part_number, etag) {
                parts.push((pn, et));
            }

            remaining = &after_part[part_end + 7..];
        } else {
            break;
        }
    }

    if parts.is_empty() {
        return Err(S3Error::new(
            S3ErrorCode::MalformedXML,
            "No parts specified in complete request",
        ));
    }

    // Sort by part number
    parts.sort_by_key(|(pn, _)| *pn);

    // Verify parts are in order and no duplicates
    for i in 1..parts.len() {
        if parts[i].0 <= parts[i - 1].0 {
            return Err(S3Error::new(
                S3ErrorCode::InvalidPartOrder,
                "Parts must be in ascending order",
            ));
        }
    }

    Ok(parts)
}

/// Extract a value from an XML element
fn extract_xml_value(content: &str, tag: &str) -> Option<String> {
    let open_tag = format!("<{}>", tag);
    let close_tag = format!("</{}>", tag);

    if let Some(start) = content.find(&open_tag) {
        let after_open = &content[start + open_tag.len()..];
        if let Some(end) = after_open.find(&close_tag) {
            let value = &after_open[..end];
            return Some(value.to_string());
        }
    }
    None
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

    #[test]
    fn test_parse_complete_request() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
        <CompleteMultipartUpload>
            <Part><PartNumber>1</PartNumber><ETag>"abc123"</ETag></Part>
            <Part><PartNumber>2</PartNumber><ETag>"def456"</ETag></Part>
        </CompleteMultipartUpload>"#;

        let parts = parse_complete_request(xml.as_bytes()).unwrap();

        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0], (1, "\"abc123\"".to_string()));
        assert_eq!(parts[1], (2, "\"def456\"".to_string()));
    }

    #[test]
    fn test_parse_complete_request_empty() {
        let xml = r#"<CompleteMultipartUpload></CompleteMultipartUpload>"#;

        let result = parse_complete_request(xml.as_bytes());

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_complete_multipart_upload() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();
        let upload_id = storage
            .create_multipart_upload("test-bucket", "test-key")
            .await
            .unwrap();

        // Upload parts
        let part1 = storage
            .upload_part("test-bucket", "test-key", &upload_id, 1, Bytes::from("part1"))
            .await
            .unwrap();
        let part2 = storage
            .upload_part("test-bucket", "test-key", &upload_id, 2, Bytes::from("part2"))
            .await
            .unwrap();

        let xml = format!(
            r#"<CompleteMultipartUpload>
                <Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part>
                <Part><PartNumber>2</PartNumber><ETag>{}</ETag></Part>
            </CompleteMultipartUpload>"#,
            part1.etag, part2.etag
        );

        let response = complete_multipart_upload(
            &storage,
            "test-bucket",
            "test-key",
            &upload_id,
            Bytes::from(xml),
            None,
        )
        .await
        .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        // Verify object was created
        let (obj, data) = storage.get_object("test-bucket", "test-key").await.unwrap();
        assert_eq!(data.as_ref(), b"part1part2");
    }
}

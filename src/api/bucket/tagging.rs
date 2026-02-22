//! Bucket tagging operations

use bytes::Bytes;
use http_body_util::Full;
use hyper::{Response, StatusCode};

use crate::storage::StorageEngine;
use crate::types::bucket::{Tag, TagSet};
use crate::types::error::{S3Error, S3ErrorCode, S3Result};
use crate::types::response::{GetBucketTaggingResponse, TagSetXml};
use crate::utils::xml::to_xml;

/// Handle GetBucketTagging request
pub async fn get_bucket_tagging(
    storage: &StorageEngine,
    bucket: &str,
) -> S3Result<Response<Full<Bytes>>> {
    let tags = storage.get_bucket_tags(bucket).await?;

    let response_body = GetBucketTaggingResponse {
        tag_set: TagSetXml::from(&tags),
    };

    let xml = to_xml(&response_body)?;

    let response = Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/xml")
        .body(Full::new(Bytes::from(xml)))
        .unwrap();

    Ok(response)
}

/// Handle PutBucketTagging request
pub async fn put_bucket_tagging(
    storage: &StorageEngine,
    bucket: &str,
    body: Bytes,
) -> S3Result<Response<Full<Bytes>>> {
    // Check bucket exists
    if !storage.bucket_exists(bucket).await {
        return Err(S3Error::no_such_bucket(bucket));
    }

    // Parse the request body XML
    let tags = parse_tagging_request(&body)?;

    storage.set_bucket_tags(bucket, tags).await?;

    let response = Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Full::new(Bytes::new()))
        .unwrap();

    Ok(response)
}

/// Handle DeleteBucketTagging request
pub async fn delete_bucket_tagging(
    storage: &StorageEngine,
    bucket: &str,
) -> S3Result<Response<Full<Bytes>>> {
    // Check bucket exists
    if !storage.bucket_exists(bucket).await {
        return Err(S3Error::no_such_bucket(bucket));
    }

    storage.delete_bucket_tags(bucket).await?;

    let response = Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Full::new(Bytes::new()))
        .unwrap();

    Ok(response)
}

/// Parse the Tagging request XML body
/// Format:
/// <Tagging>
///   <TagSet>
///     <Tag><Key>key1</Key><Value>value1</Value></Tag>
///     <Tag><Key>key2</Key><Value>value2</Value></Tag>
///   </TagSet>
/// </Tagging>
fn parse_tagging_request(body: &[u8]) -> S3Result<TagSet> {
    let body_str = std::str::from_utf8(body)
        .map_err(|_| S3Error::new(S3ErrorCode::MalformedXML, "Invalid UTF-8 in request body"))?;

    let mut tags = Vec::new();

    // Simple XML parsing - find all <Tag> elements with <Key> and <Value> children
    let mut remaining = body_str;

    while let Some(tag_start) = remaining.find("<Tag>") {
        let after_tag = &remaining[tag_start + 5..];

        if let Some(tag_end) = after_tag.find("</Tag>") {
            let tag_content = &after_tag[..tag_end];

            // Extract Key and Value
            let key = extract_xml_value(tag_content, "Key");
            let value = extract_xml_value(tag_content, "Value");

            if let (Some(key), Some(value)) = (key, value) {
                // Decode XML entities
                let key = decode_xml_entities(&key);
                let value = decode_xml_entities(&value);
                tags.push(Tag::new(key, value));
            }

            remaining = &after_tag[tag_end + 6..];
        } else {
            break;
        }
    }

    Ok(TagSet { tags })
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

/// Decode XML entities
fn decode_xml_entities(s: &str) -> String {
    s.replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
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
    fn test_parse_tagging_request() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
        <Tagging>
            <TagSet>
                <Tag><Key>env</Key><Value>dev</Value></Tag>
                <Tag><Key>team</Key><Value>platform</Value></Tag>
            </TagSet>
        </Tagging>"#;

        let tags = parse_tagging_request(xml.as_bytes()).unwrap();

        assert_eq!(tags.tags.len(), 2);
        assert_eq!(tags.tags[0].key, "env");
        assert_eq!(tags.tags[0].value, "dev");
        assert_eq!(tags.tags[1].key, "team");
        assert_eq!(tags.tags[1].value, "platform");
    }

    #[test]
    fn test_parse_tagging_request_empty() {
        let xml = r#"<Tagging><TagSet></TagSet></Tagging>"#;

        let tags = parse_tagging_request(xml.as_bytes()).unwrap();

        assert_eq!(tags.tags.len(), 0);
    }

    #[test]
    fn test_parse_tagging_request_with_entities() {
        let xml = r#"<Tagging><TagSet>
            <Tag><Key>key&amp;name</Key><Value>value&lt;test&gt;</Value></Tag>
        </TagSet></Tagging>"#;

        let tags = parse_tagging_request(xml.as_bytes()).unwrap();

        assert_eq!(tags.tags.len(), 1);
        assert_eq!(tags.tags[0].key, "key&name");
        assert_eq!(tags.tags[0].value, "value<test>");
    }

    #[tokio::test]
    async fn test_put_get_delete_bucket_tagging() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        // Put tags
        let xml = r#"<Tagging><TagSet>
            <Tag><Key>env</Key><Value>dev</Value></Tag>
        </TagSet></Tagging>"#;

        let response = put_bucket_tagging(&storage, "test-bucket", Bytes::from(xml))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        // Get tags
        let response = get_bucket_tagging(&storage, "test-bucket").await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Delete tags
        let response = delete_bucket_tagging(&storage, "test-bucket")
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        // Get tags should fail now
        let result = get_bucket_tagging(&storage, "test-bucket").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_bucket_tagging_no_tags() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        // Should fail with NoSuchTagSet
        let result = get_bucket_tagging(&storage, "test-bucket").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_put_bucket_tagging_bucket_not_found() {
        let (storage, _temp) = create_test_storage().await;

        let xml = r#"<Tagging><TagSet><Tag><Key>k</Key><Value>v</Value></Tag></TagSet></Tagging>"#;
        let result = put_bucket_tagging(&storage, "nonexistent", Bytes::from(xml)).await;

        assert!(result.is_err());
    }
}

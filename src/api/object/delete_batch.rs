//! DeleteObjects (batch delete) operation

use bytes::Bytes;
use http_body_util::Full;
use hyper::{Response, StatusCode};

use crate::storage::StorageEngine;
use crate::types::error::{S3Error, S3ErrorCode, S3Result};

/// Represents an object to be deleted
#[derive(Debug)]
struct ObjectToDelete {
    key: String,
    version_id: Option<String>,
}

/// Represents a successfully deleted object
#[derive(Debug)]
struct DeletedObject {
    key: String,
    version_id: Option<String>,
    delete_marker: bool,
    delete_marker_version_id: Option<String>,
}

/// Represents an error deleting an object
#[derive(Debug)]
struct DeleteError {
    key: String,
    version_id: Option<String>,
    code: String,
    message: String,
}

/// Handle DeleteObjects request (batch delete)
pub async fn delete_objects(
    storage: &StorageEngine,
    bucket: &str,
    body: Bytes,
) -> S3Result<Response<Full<Bytes>>> {
    // Check bucket exists
    if !storage.bucket_exists(bucket).await {
        return Err(S3Error::no_such_bucket(bucket));
    }

    // Parse the request body XML
    let objects = parse_delete_request(&body)?;

    let mut deleted: Vec<DeletedObject> = Vec::new();
    let errors: Vec<DeleteError> = Vec::new();

    // Delete each object using versioned delete (handles both versioned and non-versioned buckets)
    for obj in objects {
        match storage
            .delete_object_versioned(bucket, &obj.key, obj.version_id.as_deref())
            .await
        {
            Ok(result) => {
                deleted.push(DeletedObject {
                    key: obj.key,
                    version_id: result.version_id,
                    delete_marker: result.delete_marker,
                    delete_marker_version_id: result.delete_marker_version_id,
                });
            }
            Err(e) => {
                // Only bucket not found is a real error here
                if e.code == S3ErrorCode::NoSuchBucket {
                    return Err(e);
                }
                // For NoSuchKey, S3 still considers it a successful delete
                deleted.push(DeletedObject {
                    key: obj.key,
                    version_id: obj.version_id,
                    delete_marker: false,
                    delete_marker_version_id: None,
                });
            }
        }
    }

    // Build XML response
    let xml = build_delete_response(&deleted, &errors);

    let response = Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/xml")
        .body(Full::new(Bytes::from(xml)))
        .unwrap();

    Ok(response)
}

/// Parse the Delete request XML body
fn parse_delete_request(body: &[u8]) -> S3Result<Vec<ObjectToDelete>> {
    let body_str = std::str::from_utf8(body)
        .map_err(|_| S3Error::new(S3ErrorCode::MalformedXML, "Invalid UTF-8 in request body"))?;

    let mut objects = Vec::new();

    // Simple XML parsing - find all <Object> elements with <Key> children
    // Format: <Delete><Object><Key>key1</Key></Object>...</Delete>
    let mut remaining = body_str;

    while let Some(obj_start) = remaining.find("<Object>") {
        let after_obj = &remaining[obj_start + 8..];

        if let Some(obj_end) = after_obj.find("</Object>") {
            let obj_content = &after_obj[..obj_end];

            // Extract Key
            let key = extract_xml_value(obj_content, "Key");
            let version_id = extract_xml_value(obj_content, "VersionId");

            if let Some(key) = key {
                objects.push(ObjectToDelete { key, version_id });
            }

            remaining = &after_obj[obj_end + 9..];
        } else {
            break;
        }
    }

    if objects.is_empty() {
        return Err(S3Error::new(
            S3ErrorCode::MalformedXML,
            "No objects specified in delete request",
        ));
    }

    Ok(objects)
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

/// Build the DeleteResult XML response
fn build_delete_response(deleted: &[DeletedObject], errors: &[DeleteError]) -> String {
    let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    xml.push_str("<DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n");

    for d in deleted {
        xml.push_str("  <Deleted>\n");
        xml.push_str(&format!("    <Key>{}</Key>\n", escape_xml(&d.key)));
        if let Some(ref vid) = d.version_id {
            xml.push_str(&format!("    <VersionId>{}</VersionId>\n", vid));
        }
        if d.delete_marker {
            xml.push_str("    <DeleteMarker>true</DeleteMarker>\n");
        }
        if let Some(ref dm_vid) = d.delete_marker_version_id {
            xml.push_str(&format!(
                "    <DeleteMarkerVersionId>{}</DeleteMarkerVersionId>\n",
                dm_vid
            ));
        }
        xml.push_str("  </Deleted>\n");
    }

    for e in errors {
        xml.push_str("  <Error>\n");
        xml.push_str(&format!("    <Key>{}</Key>\n", escape_xml(&e.key)));
        if let Some(ref vid) = e.version_id {
            xml.push_str(&format!("    <VersionId>{}</VersionId>\n", vid));
        }
        xml.push_str(&format!("    <Code>{}</Code>\n", e.code));
        xml.push_str(&format!("    <Message>{}</Message>\n", escape_xml(&e.message)));
        xml.push_str("  </Error>\n");
    }

    xml.push_str("</DeleteResult>");
    xml
}

/// Escape special XML characters
fn escape_xml(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
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
    fn test_parse_delete_request() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
        <Delete>
            <Object><Key>key1</Key></Object>
            <Object><Key>key2</Key></Object>
            <Object><Key>key3</Key><VersionId>v1</VersionId></Object>
        </Delete>"#;

        let objects = parse_delete_request(xml.as_bytes()).unwrap();

        assert_eq!(objects.len(), 3);
        assert_eq!(objects[0].key, "key1");
        assert_eq!(objects[0].version_id, None);
        assert_eq!(objects[1].key, "key2");
        assert_eq!(objects[2].key, "key3");
        assert_eq!(objects[2].version_id, Some("v1".to_string()));
    }

    #[test]
    fn test_parse_delete_request_empty() {
        let xml = r#"<Delete></Delete>"#;

        let result = parse_delete_request(xml.as_bytes());

        assert!(result.is_err());
    }

    #[test]
    fn test_build_delete_response() {
        let deleted = vec![
            DeletedObject {
                key: "key1".to_string(),
                version_id: None,
                delete_marker: false,
                delete_marker_version_id: None,
            },
            DeletedObject {
                key: "key2".to_string(),
                version_id: Some("v1".to_string()),
                delete_marker: true,
                delete_marker_version_id: Some("dm-v1".to_string()),
            },
        ];
        let errors: Vec<DeleteError> = vec![];

        let xml = build_delete_response(&deleted, &errors);

        assert!(xml.contains("<DeleteResult"));
        assert!(xml.contains("<Deleted>"));
        assert!(xml.contains("<Key>key1</Key>"));
        assert!(xml.contains("<Key>key2</Key>"));
        assert!(xml.contains("<VersionId>v1</VersionId>"));
        assert!(xml.contains("<DeleteMarker>true</DeleteMarker>"));
        assert!(xml.contains("<DeleteMarkerVersionId>dm-v1</DeleteMarkerVersionId>"));
    }

    #[tokio::test]
    async fn test_delete_objects_success() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();
        storage
            .put_object("test-bucket", "key1", Bytes::from("data1"), None, None)
            .await
            .unwrap();
        storage
            .put_object("test-bucket", "key2", Bytes::from("data2"), None, None)
            .await
            .unwrap();

        let xml = r#"<Delete><Object><Key>key1</Key></Object><Object><Key>key2</Key></Object></Delete>"#;
        let response = delete_objects(&storage, "test-bucket", Bytes::from(xml))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        // Verify objects are deleted
        assert!(!storage.object_exists("test-bucket", "key1").await);
        assert!(!storage.object_exists("test-bucket", "key2").await);
    }

    #[tokio::test]
    async fn test_delete_objects_nonexistent() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        // S3 returns success even for non-existent keys
        let xml = r#"<Delete><Object><Key>nonexistent</Key></Object></Delete>"#;
        let response = delete_objects(&storage, "test-bucket", Bytes::from(xml))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_delete_objects_bucket_not_found() {
        let (storage, _temp) = create_test_storage().await;

        let xml = r#"<Delete><Object><Key>key</Key></Object></Delete>"#;
        let result = delete_objects(&storage, "nonexistent", Bytes::from(xml)).await;

        assert!(result.is_err());
    }
}

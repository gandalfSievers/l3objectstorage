//! GetObjectAttributes operation

use bytes::Bytes;
use http_body_util::Full;
use hyper::{Response, StatusCode};

use crate::storage::StorageEngine;
use crate::types::error::S3Result;
use crate::types::response::GetObjectAttributesResponse;
use crate::utils::xml::to_xml;

/// Handle GetObjectAttributes request
pub async fn get_object_attributes(
    storage: &StorageEngine,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
    attributes: Vec<String>,
) -> S3Result<Response<Full<Bytes>>> {
    // Get the object metadata
    let object = storage.head_object_versioned(bucket, key, version_id).await?;

    // Build response based on requested attributes
    let mut response_body = GetObjectAttributesResponse {
        etag: None,
        checksum: None,
        object_parts: None,
        storage_class: None,
        object_size: None,
    };

    for attr in &attributes {
        match attr.as_str() {
            "ETag" => {
                response_body.etag = Some(object.etag.clone());
            }
            "ObjectSize" => {
                response_body.object_size = Some(object.size);
            }
            "StorageClass" => {
                response_body.storage_class = Some(object.storage_class.as_str().to_string());
            }
            // Checksum and ObjectParts would require additional storage support
            // For now, we'll leave them as None
            "Checksum" => {
                // Not implemented yet
            }
            "ObjectParts" => {
                // Not implemented yet (would require tracking multipart parts)
            }
            _ => {
                // Unknown attribute, ignore
            }
        }
    }

    let xml = to_xml(&response_body)?;

    let mut response_builder = Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/xml");

    // Add version ID header if present
    if let Some(ref vid) = object.version_id {
        response_builder = response_builder.header("x-amz-version-id", vid);
    }

    // Add last modified header
    response_builder = response_builder.header(
        "Last-Modified",
        crate::utils::time::format_http_date(&object.last_modified),
    );

    let response = response_builder
        .body(Full::new(Bytes::from(xml)))
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
    async fn test_get_object_attributes() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();
        storage
            .put_object(
                "test-bucket",
                "test-key",
                Bytes::from("hello world"),
                None,
                None,
            )
            .await
            .unwrap();

        let response = get_object_attributes(
            &storage,
            "test-bucket",
            "test-key",
            None,
            vec!["ETag".to_string(), "ObjectSize".to_string(), "StorageClass".to_string()],
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

        assert!(body_str.contains("<ETag>"), "Should contain ETag");
        assert!(body_str.contains("<ObjectSize>11</ObjectSize>"), "Should contain ObjectSize");
        assert!(body_str.contains("<StorageClass>STANDARD</StorageClass>"), "Should contain StorageClass");
    }

    #[tokio::test]
    async fn test_get_object_attributes_not_found() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        let result = get_object_attributes(
            &storage,
            "test-bucket",
            "nonexistent",
            None,
            vec!["ETag".to_string()],
        )
        .await;

        assert!(result.is_err());
    }
}

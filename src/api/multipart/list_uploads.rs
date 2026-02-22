//! ListMultipartUploads operation

use bytes::Bytes;
use http_body_util::Full;
use hyper::{Response, StatusCode};
use std::collections::HashSet;

use crate::storage::StorageEngine;
use crate::types::error::S3Result;
use crate::types::response::{CommonPrefix, ListMultipartUploadsResponse, Owner, UploadInfo};
use crate::utils::xml::to_xml;

/// Handle ListMultipartUploads request with pagination and delimiter support
pub async fn list_multipart_uploads(
    storage: &StorageEngine,
    bucket: &str,
    prefix: Option<&str>,
    delimiter: Option<&str>,
    max_uploads: Option<i32>,
    key_marker: Option<&str>,
) -> S3Result<Response<Full<Bytes>>> {
    let mut all_uploads = storage.list_multipart_uploads(bucket, prefix).await?;

    let max_uploads = max_uploads.unwrap_or(1000).min(1000).max(1);
    let prefix_str = prefix.unwrap_or("");

    // S3 API requires uploads to be sorted by key (then upload_id)
    all_uploads.sort_by(|a, b| {
        match a.key.cmp(&b.key) {
            std::cmp::Ordering::Equal => a.upload_id.cmp(&b.upload_id),
            other => other,
        }
    });

    // Filter by key_marker (return uploads after the marker)
    let filtered_uploads: Vec<_> = all_uploads
        .into_iter()
        .filter(|u| {
            if let Some(marker) = key_marker {
                u.key.as_str() > marker
            } else {
                true
            }
        })
        .collect();

    // Handle delimiter for common prefixes
    let mut common_prefix_set: HashSet<String> = HashSet::new();
    let mut uploads_to_list = Vec::new();

    if let Some(delim) = delimiter {
        for upload in filtered_uploads {
            let key_after_prefix = if upload.key.starts_with(prefix_str) {
                &upload.key[prefix_str.len()..]
            } else {
                // This upload doesn't have the prefix, skip it
                // (This shouldn't happen if storage layer filters correctly, but be safe)
                continue;
            };

            if let Some(pos) = key_after_prefix.find(delim) {
                // Found delimiter - add to common prefixes
                let common_prefix = format!("{}{}", prefix_str, &key_after_prefix[..pos + delim.len()]);
                common_prefix_set.insert(common_prefix);
            } else {
                // No delimiter - include in uploads
                uploads_to_list.push(upload);
            }
        }
    } else {
        uploads_to_list = filtered_uploads;
    }

    // Sort uploads by key for consistent pagination
    uploads_to_list.sort_by(|a, b| a.key.cmp(&b.key));

    // Sort common prefixes
    let mut common_prefixes: Vec<CommonPrefix> = common_prefix_set
        .into_iter()
        .map(|p| CommonPrefix { prefix: p })
        .collect();
    common_prefixes.sort_by(|a, b| a.prefix.cmp(&b.prefix));

    // Determine if truncated and apply limit
    let is_truncated = uploads_to_list.len() > max_uploads as usize;
    let uploads_limited: Vec<_> = uploads_to_list
        .into_iter()
        .take(max_uploads as usize)
        .collect();

    let next_key_marker = if is_truncated {
        uploads_limited.last().map(|u| u.key.clone()).unwrap_or_default()
    } else {
        String::new()
    };

    let upload_infos: Vec<UploadInfo> = uploads_limited
        .iter()
        .map(|u| UploadInfo {
            key: u.key.clone(),
            upload_id: u.upload_id.clone(),
            initiator: Owner::default(),
            owner: Owner::default(),
            storage_class: "STANDARD".to_string(),
            initiated: u.initiated.to_rfc3339(),
        })
        .collect();

    let response_body = ListMultipartUploadsResponse {
        bucket: bucket.to_string(),
        key_marker: key_marker.unwrap_or("").to_string(),
        upload_id_marker: String::new(),
        next_key_marker,
        next_upload_id_marker: String::new(),
        max_uploads,
        is_truncated,
        uploads: upload_infos,
        prefix: prefix.map(String::from),
        delimiter: delimiter.map(String::from),
        common_prefixes,
    };

    let xml = to_xml(&response_body)?;

    let response = Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/xml")
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
    async fn test_list_multipart_uploads() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        // Create some uploads
        let _upload1 = storage
            .create_multipart_upload("test-bucket", "key1")
            .await
            .unwrap();
        let _upload2 = storage
            .create_multipart_upload("test-bucket", "key2")
            .await
            .unwrap();

        let response = list_multipart_uploads(&storage, "test-bucket", None, None, None, None)
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body();
        let body_bytes = http_body_util::BodyExt::collect(body)
            .await
            .unwrap()
            .to_bytes();
        let body_str = String::from_utf8_lossy(&body_bytes);

        assert!(body_str.contains("<Key>key1</Key>"));
        assert!(body_str.contains("<Key>key2</Key>"));
    }

    #[tokio::test]
    async fn test_list_multipart_uploads_with_prefix() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        // Create uploads with different prefixes
        let _upload1 = storage
            .create_multipart_upload("test-bucket", "alpha/key1")
            .await
            .unwrap();
        let _upload2 = storage
            .create_multipart_upload("test-bucket", "beta/key2")
            .await
            .unwrap();

        let response = list_multipart_uploads(&storage, "test-bucket", Some("alpha/"), None, None, None)
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body();
        let body_bytes = http_body_util::BodyExt::collect(body)
            .await
            .unwrap()
            .to_bytes();
        let body_str = String::from_utf8_lossy(&body_bytes);

        assert!(body_str.contains("<Key>alpha/key1</Key>"));
        assert!(!body_str.contains("<Key>beta/key2</Key>"));
    }

    #[tokio::test]
    async fn test_list_multipart_uploads_empty() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        let response = list_multipart_uploads(&storage, "test-bucket", None, None, None, None)
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }
}

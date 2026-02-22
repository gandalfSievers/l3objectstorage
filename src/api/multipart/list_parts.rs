//! ListParts operation

use bytes::Bytes;
use http_body_util::Full;
use hyper::{Response, StatusCode};

use crate::storage::StorageEngine;
use crate::types::error::S3Result;
use crate::types::response::{ListPartsResponse, Owner, PartInfo};
use crate::utils::xml::to_xml;

/// Handle ListParts request with pagination support
pub async fn list_parts(
    storage: &StorageEngine,
    bucket: &str,
    key: &str,
    upload_id: &str,
    max_parts: Option<i32>,
    part_number_marker: Option<i32>,
) -> S3Result<Response<Full<Bytes>>> {
    // Verify upload exists and get its info
    let _upload = storage.get_multipart_upload(bucket, upload_id).await?;

    let all_parts = storage.list_parts(bucket, upload_id).await?;

    let max_parts = max_parts.unwrap_or(1000).min(1000).max(1);
    let part_number_marker = part_number_marker.unwrap_or(0);

    // Filter parts based on part_number_marker (return parts after the marker)
    let filtered_parts: Vec<_> = all_parts
        .into_iter()
        .filter(|p| p.part_number > part_number_marker)
        .collect();

    // Determine if truncated and apply limit
    let is_truncated = filtered_parts.len() > max_parts as usize;
    let parts_to_return: Vec<_> = filtered_parts
        .into_iter()
        .take(max_parts as usize)
        .collect();

    let part_infos: Vec<PartInfo> = parts_to_return
        .iter()
        .map(|p| PartInfo {
            part_number: p.part_number,
            last_modified: p.last_modified.to_rfc3339(),
            etag: p.etag.clone(),
            size: p.size,
        })
        .collect();

    let next_part_number_marker = if is_truncated {
        parts_to_return.last().map(|p| p.part_number).unwrap_or(0)
    } else {
        0
    };

    let response_body = ListPartsResponse {
        bucket: bucket.to_string(),
        key: key.to_string(),
        upload_id: upload_id.to_string(),
        part_number_marker,
        next_part_number_marker,
        max_parts,
        is_truncated,
        parts: part_infos,
        initiator: Owner::default(),
        owner: Owner::default(),
        storage_class: "STANDARD".to_string(),
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
    async fn test_list_parts() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();
        let upload_id = storage
            .create_multipart_upload("test-bucket", "test-key")
            .await
            .unwrap();

        // Upload some parts
        storage
            .upload_part("test-bucket", "test-key", &upload_id, 1, Bytes::from("part1"))
            .await
            .unwrap();
        storage
            .upload_part("test-bucket", "test-key", &upload_id, 2, Bytes::from("part2"))
            .await
            .unwrap();

        let response = list_parts(&storage, "test-bucket", "test-key", &upload_id, None, None)
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body();
        let body_bytes = http_body_util::BodyExt::collect(body)
            .await
            .unwrap()
            .to_bytes();
        let body_str = String::from_utf8_lossy(&body_bytes);

        assert!(body_str.contains("<PartNumber>1</PartNumber>"));
        assert!(body_str.contains("<PartNumber>2</PartNumber>"));
    }

    #[tokio::test]
    async fn test_list_parts_invalid_upload() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        let result = list_parts(&storage, "test-bucket", "test-key", "nonexistent", None, None).await;

        assert!(result.is_err());
    }
}

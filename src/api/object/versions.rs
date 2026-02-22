//! ListObjectVersions operation

use bytes::Bytes;
use http_body_util::Full;
use hyper::{Response, StatusCode};
use std::collections::HashSet;

use crate::storage::StorageEngine;
use crate::types::error::S3Result;
use crate::types::response::{
    CommonPrefix, DeleteMarkerXml, ListObjectVersionsResponse, ObjectVersionXml, Owner,
};
use crate::utils::xml::to_xml;

/// Handle ListObjectVersions request with pagination and delimiter support
pub async fn list_object_versions(
    storage: &StorageEngine,
    bucket: &str,
    prefix: Option<&str>,
    delimiter: Option<&str>,
    max_keys: Option<i32>,
    key_marker: Option<&str>,
    version_id_marker: Option<&str>,
) -> S3Result<Response<Full<Bytes>>> {
    let max_keys = max_keys.unwrap_or(1000).min(1000).max(1);
    let prefix_str = prefix.unwrap_or("");

    // Fetch all versions - the storage layer doesn't filter by markers
    let (all_versions, all_delete_markers) = storage
        .list_object_versions(bucket, prefix, None, None, 10000)
        .await?;

    // Combine versions and delete markers for processing
    #[derive(Clone)]
    enum VersionItem {
        Version(crate::types::object::ObjectVersion),
        DeleteMarker(crate::types::object::DeleteMarker),
    }

    let mut all_items: Vec<VersionItem> = Vec::new();
    for v in all_versions {
        all_items.push(VersionItem::Version(v));
    }
    for dm in all_delete_markers {
        all_items.push(VersionItem::DeleteMarker(dm));
    }

    // Sort by key, then by version_id (newest first for same key)
    all_items.sort_by(|a, b| {
        let (key_a, vid_a) = match a {
            VersionItem::Version(v) => (&v.key, &v.version_id),
            VersionItem::DeleteMarker(dm) => (&dm.key, &dm.version_id),
        };
        let (key_b, vid_b) = match b {
            VersionItem::Version(v) => (&v.key, &v.version_id),
            VersionItem::DeleteMarker(dm) => (&dm.key, &dm.version_id),
        };
        match key_a.cmp(key_b) {
            std::cmp::Ordering::Equal => vid_b.cmp(vid_a), // Newest version first
            other => other,
        }
    });

    // Filter by key_marker and version_id_marker (return items after the marker)
    let filtered_items: Vec<_> = match (key_marker, version_id_marker) {
        (Some(km), Some(vm)) => {
            // Both markers - filter for items after (key_marker, version_id_marker)
            all_items
                .into_iter()
                .filter(|item| {
                    let (key, vid) = match item {
                        VersionItem::Version(v) => (&v.key, &v.version_id),
                        VersionItem::DeleteMarker(dm) => (&dm.key, &dm.version_id),
                    };
                    // Items after marker: key > km, OR (key == km AND vid < vm)
                    // Note: vid < vm because versions are sorted newest first (descending vid)
                    key.as_str() > km || (key.as_str() == km && vid.as_str() < vm)
                })
                .collect()
        }
        (Some(km), None) => {
            // Only key_marker - filter for items where key > key_marker
            all_items
                .into_iter()
                .filter(|item| {
                    let key = match item {
                        VersionItem::Version(v) => &v.key,
                        VersionItem::DeleteMarker(dm) => &dm.key,
                    };
                    key.as_str() > km
                })
                .collect()
        }
        _ => all_items,
    };
    let all_items = filtered_items;

    // Handle delimiter for common prefixes
    // Keep items as VersionItem to preserve ordering for pagination
    let mut common_prefix_set: HashSet<String> = HashSet::new();
    let mut items_to_list: Vec<VersionItem> = Vec::new();

    if let Some(delim) = delimiter {
        for item in all_items {
            let key = match &item {
                VersionItem::Version(v) => &v.key,
                VersionItem::DeleteMarker(dm) => &dm.key,
            };

            // Skip items that don't start with the prefix (shouldn't happen but be safe)
            if !prefix_str.is_empty() && !key.starts_with(prefix_str) {
                continue;
            }

            let key_after_prefix = &key[prefix_str.len()..];

            if let Some(pos) = key_after_prefix.find(delim) {
                // Found delimiter - add to common prefixes
                let common_prefix = format!("{}{}", prefix_str, &key_after_prefix[..pos + delim.len()]);
                common_prefix_set.insert(common_prefix);
            } else {
                // No delimiter - include in results
                items_to_list.push(item);
            }
        }
    } else {
        items_to_list = all_items;
    }

    // Sort common prefixes
    let mut common_prefixes: Vec<CommonPrefix> = common_prefix_set
        .into_iter()
        .map(|p| CommonPrefix { prefix: p })
        .collect();
    common_prefixes.sort_by(|a, b| a.prefix.cmp(&b.prefix));

    // Determine if truncated and apply limit
    let total_count = items_to_list.len();
    let is_truncated = total_count > max_keys as usize;

    // Take only max_keys items (preserving the sorted order)
    let items_limited: Vec<_> = items_to_list.into_iter().take(max_keys as usize).collect();

    // Determine next markers if truncated (from the last item in the limited list)
    let (next_key_marker, next_version_id_marker) = if is_truncated {
        items_limited.last().map(|item| {
            match item {
                VersionItem::Version(v) => (Some(v.key.clone()), Some(v.version_id.clone())),
                VersionItem::DeleteMarker(dm) => (Some(dm.key.clone()), Some(dm.version_id.clone())),
            }
        }).unwrap_or((None, None))
    } else {
        (None, None)
    };

    // Separate versions and delete markers for the response
    let mut versions_limited: Vec<crate::types::object::ObjectVersion> = Vec::new();
    let mut delete_markers_limited: Vec<crate::types::object::DeleteMarker> = Vec::new();
    for item in items_limited {
        match item {
            VersionItem::Version(v) => versions_limited.push(v),
            VersionItem::DeleteMarker(dm) => delete_markers_limited.push(dm),
        }
    }

    // Convert to XML response types
    let version_xmls: Vec<ObjectVersionXml> = versions_limited
        .into_iter()
        .map(|v| ObjectVersionXml {
            key: v.key,
            version_id: v.version_id,
            is_latest: v.is_latest,
            last_modified: v.last_modified.to_rfc3339(),
            etag: v.etag,
            size: v.size,
            storage_class: v.storage_class.as_str().to_string(),
            owner: Owner {
                id: v.owner_id,
                display_name: v.owner_display_name,
            },
        })
        .collect();

    let delete_marker_xmls: Vec<DeleteMarkerXml> = delete_markers_limited
        .into_iter()
        .map(|dm| DeleteMarkerXml {
            key: dm.key,
            version_id: dm.version_id,
            is_latest: dm.is_latest,
            last_modified: dm.last_modified.to_rfc3339(),
            owner: Owner {
                id: dm.owner_id,
                display_name: dm.owner_display_name,
            },
        })
        .collect();

    let response_body = ListObjectVersionsResponse {
        xmlns: "http://s3.amazonaws.com/doc/2006-03-01/".to_string(),
        name: bucket.to_string(),
        prefix: prefix_str.to_string(),
        key_marker: key_marker.unwrap_or("").to_string(),
        version_id_marker: version_id_marker.unwrap_or("").to_string(),
        next_key_marker,
        next_version_id_marker,
        max_keys,
        is_truncated,
        versions: version_xmls,
        delete_markers: delete_marker_xmls,
        common_prefixes,
        delimiter: delimiter.map(String::from),
        encoding_type: None,
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
    use crate::types::bucket::VersioningStatus;
    use tempfile::TempDir;

    async fn create_test_storage() -> (StorageEngine, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::new().with_data_dir(temp_dir.path());
        let storage = StorageEngine::new(config).await.unwrap();
        (storage, temp_dir)
    }

    #[tokio::test]
    async fn test_list_object_versions_empty() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        let response = list_object_versions(&storage, "test-bucket", None, None, None, None, None)
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_list_object_versions_with_versioning() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();
        storage
            .set_bucket_versioning("test-bucket", VersioningStatus::Enabled)
            .await
            .unwrap();

        // Put two versions
        storage
            .put_object_versioned(
                "test-bucket",
                "key1",
                Bytes::from("v1"),
                None,
                None,
            )
            .await
            .unwrap();
        storage
            .put_object_versioned(
                "test-bucket",
                "key1",
                Bytes::from("v2"),
                None,
                None,
            )
            .await
            .unwrap();

        let response = list_object_versions(&storage, "test-bucket", None, None, None, None, None)
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        // Check the response body contains versions
        let body = response.into_body();
        let bytes = http_body_util::BodyExt::collect(body)
            .await
            .unwrap()
            .to_bytes();
        let body_str = String::from_utf8_lossy(&bytes);

        assert!(body_str.contains("<Version>"), "Should contain Version elements");
        assert!(body_str.contains("<Key>key1</Key>"), "Should contain the key");
    }
}

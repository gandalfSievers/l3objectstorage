//! Multipart upload storage management

use bytes::Bytes;
use chrono::Utc;
use std::path::PathBuf;
use tokio::fs;
use uuid::Uuid;

use crate::types::error::{S3Error, S3Result};
use crate::types::object::{MultipartUpload, UploadedPart};
use crate::utils::etag::calculate_etag;

/// Manages multipart upload storage on disk
///
/// Storage layout:
/// ```text
/// /data/buckets/{bucket}/.multipart/{upload_id}/
///     upload.json          # Metadata (key, initiated, etc.)
///     parts/
///         1.part           # Part data
///         1.meta.json      # Part metadata (ETag, size)
///         2.part
///         2.meta.json
/// ```
pub struct MultipartStore {
    data_dir: PathBuf,
}

impl MultipartStore {
    /// Create a new multipart store
    pub fn new(data_dir: &PathBuf) -> Self {
        Self {
            data_dir: data_dir.clone(),
        }
    }

    /// Get the multipart directory for a bucket
    fn multipart_dir(&self, bucket: &str) -> PathBuf {
        self.data_dir.join("buckets").join(bucket).join(".multipart")
    }

    /// Get the directory for a specific upload
    fn upload_dir(&self, bucket: &str, upload_id: &str) -> PathBuf {
        self.multipart_dir(bucket).join(upload_id)
    }

    /// Get the parts directory for an upload
    fn parts_dir(&self, bucket: &str, upload_id: &str) -> PathBuf {
        self.upload_dir(bucket, upload_id).join("parts")
    }

    /// Create a new multipart upload, returns upload_id
    pub async fn create(&self, bucket: &str, key: &str, sse_algorithm: Option<String>) -> S3Result<String> {
        let upload_id = Uuid::new_v4().to_string();
        let upload = MultipartUpload::new(bucket, key, &upload_id).with_sse(sse_algorithm);

        let upload_dir = self.upload_dir(bucket, &upload_id);
        fs::create_dir_all(&upload_dir).await?;
        fs::create_dir_all(self.parts_dir(bucket, &upload_id)).await?;

        // Write upload metadata
        let metadata_path = upload_dir.join("upload.json");
        let metadata_json = serde_json::to_string_pretty(&upload)?;
        fs::write(&metadata_path, metadata_json).await?;

        Ok(upload_id)
    }

    /// Get multipart upload by ID
    pub async fn get(&self, bucket: &str, upload_id: &str) -> S3Result<MultipartUpload> {
        let metadata_path = self.upload_dir(bucket, upload_id).join("upload.json");

        if !metadata_path.exists() {
            return Err(S3Error::no_such_upload(upload_id));
        }

        let metadata_json = fs::read_to_string(&metadata_path).await?;
        let upload: MultipartUpload = serde_json::from_str(&metadata_json)?;

        Ok(upload)
    }

    /// Store a part
    pub async fn put_part(
        &self,
        bucket: &str,
        _key: &str,
        upload_id: &str,
        part_number: i32,
        data: Bytes,
    ) -> S3Result<UploadedPart> {
        // Verify upload exists
        let _ = self.get(bucket, upload_id).await?;

        let parts_dir = self.parts_dir(bucket, upload_id);
        let part_path = parts_dir.join(format!("{}.part", part_number));
        let meta_path = parts_dir.join(format!("{}.meta.json", part_number));

        // Calculate ETag
        let etag = calculate_etag(&data);
        let size = data.len() as u64;

        // Write part data
        fs::write(&part_path, &data).await?;

        // Write part metadata
        let part = UploadedPart {
            part_number,
            etag: etag.clone(),
            size,
            last_modified: Utc::now(),
        };
        let part_json = serde_json::to_string_pretty(&part)?;
        fs::write(&meta_path, part_json).await?;

        Ok(part)
    }

    /// List parts for an upload
    pub async fn list_parts(&self, bucket: &str, upload_id: &str) -> S3Result<Vec<UploadedPart>> {
        // Verify upload exists
        let _ = self.get(bucket, upload_id).await?;

        let parts_dir = self.parts_dir(bucket, upload_id);
        let mut parts = Vec::new();

        if !parts_dir.exists() {
            return Ok(parts);
        }

        let mut entries = fs::read_dir(&parts_dir).await?;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension().map(|e| e == "json").unwrap_or(false)
                && path.file_stem().map(|s| s.to_string_lossy().ends_with(".meta")).unwrap_or(false)
            {
                let meta_json = fs::read_to_string(&path).await?;
                let part: UploadedPart = serde_json::from_str(&meta_json)?;
                parts.push(part);
            }
        }

        // Sort by part number
        parts.sort_by_key(|p| p.part_number);

        Ok(parts)
    }

    /// Complete upload - assemble parts into final object data
    /// Returns the assembled data and the final ETag
    pub async fn complete(
        &self,
        bucket: &str,
        upload_id: &str,
        part_etags: Vec<(i32, String)>,
    ) -> S3Result<(Bytes, String)> {
        // Verify upload exists
        let _ = self.get(bucket, upload_id).await?;

        let parts_dir = self.parts_dir(bucket, upload_id);
        let mut assembled_data = Vec::new();
        let mut etags_for_hash: Vec<String> = Vec::new();

        // Process parts in order
        for (part_number, expected_etag) in &part_etags {
            let part_path = parts_dir.join(format!("{}.part", part_number));
            let meta_path = parts_dir.join(format!("{}.meta.json", part_number));

            if !part_path.exists() {
                return Err(S3Error::invalid_part(*part_number));
            }

            // Verify ETag
            let meta_json = fs::read_to_string(&meta_path).await?;
            let part_meta: UploadedPart = serde_json::from_str(&meta_json)?;

            // Normalize ETags for comparison (decode XML entities and remove quotes)
            let stored_etag = part_meta.etag.trim_matches('"');
            // Decode XML entities like &quot; -> "
            let decoded_etag = expected_etag
                .replace("&quot;", "\"")
                .replace("&amp;", "&")
                .replace("&lt;", "<")
                .replace("&gt;", ">")
                .replace("&apos;", "'");
            let provided_etag = decoded_etag.trim_matches('"');

            if stored_etag != provided_etag {
                return Err(S3Error::invalid_part(*part_number));
            }

            // Read and append part data
            let part_data = fs::read(&part_path).await?;
            assembled_data.extend_from_slice(&part_data);

            // Collect ETags for multipart ETag calculation
            etags_for_hash.push(part_meta.etag.clone());
        }

        // Calculate multipart ETag: hash(concat(hash_1, hash_2, ...))-N
        let final_etag = crate::utils::etag::calculate_multipart_etag(&etags_for_hash);

        // Cleanup multipart upload directory
        let upload_dir = self.upload_dir(bucket, upload_id);
        fs::remove_dir_all(&upload_dir).await?;

        Ok((Bytes::from(assembled_data), final_etag))
    }

    /// Abort upload - delete all parts
    pub async fn abort(&self, bucket: &str, upload_id: &str) -> S3Result<()> {
        // Verify upload exists
        let _ = self.get(bucket, upload_id).await?;

        let upload_dir = self.upload_dir(bucket, upload_id);
        fs::remove_dir_all(&upload_dir).await?;

        Ok(())
    }

    /// List all in-progress uploads for a bucket
    pub async fn list_uploads(
        &self,
        bucket: &str,
        prefix: Option<&str>,
    ) -> S3Result<Vec<MultipartUpload>> {
        let multipart_dir = self.multipart_dir(bucket);
        let mut uploads = Vec::new();

        if !multipart_dir.exists() {
            return Ok(uploads);
        }

        let mut entries = fs::read_dir(&multipart_dir).await?;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.is_dir() {
                let metadata_path = path.join("upload.json");
                if metadata_path.exists() {
                    let metadata_json = fs::read_to_string(&metadata_path).await?;
                    let upload: MultipartUpload = serde_json::from_str(&metadata_json)?;

                    // Filter by prefix if provided
                    if let Some(p) = prefix {
                        if !upload.key.starts_with(p) {
                            continue;
                        }
                    }

                    uploads.push(upload);
                }
            }
        }

        // Sort by initiated time
        uploads.sort_by(|a, b| a.initiated.cmp(&b.initiated));

        Ok(uploads)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    async fn create_test_store() -> (MultipartStore, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let store = MultipartStore::new(&temp_dir.path().to_path_buf());

        // Create bucket directory
        fs::create_dir_all(temp_dir.path().join("buckets/test-bucket"))
            .await
            .unwrap();

        (store, temp_dir)
    }

    #[tokio::test]
    async fn test_create_upload() {
        let (store, _temp) = create_test_store().await;

        let upload_id = store.create("test-bucket", "test-key", None).await.unwrap();

        assert!(!upload_id.is_empty());

        let upload = store.get("test-bucket", &upload_id).await.unwrap();
        assert_eq!(upload.bucket, "test-bucket");
        assert_eq!(upload.key, "test-key");
        assert_eq!(upload.upload_id, upload_id);
    }

    #[tokio::test]
    async fn test_get_nonexistent_upload() {
        let (store, _temp) = create_test_store().await;

        let result = store.get("test-bucket", "nonexistent").await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_put_and_list_parts() {
        let (store, _temp) = create_test_store().await;

        let upload_id = store.create("test-bucket", "test-key", None).await.unwrap();

        // Upload parts
        let part1 = store
            .put_part("test-bucket", "test-key", &upload_id, 1, Bytes::from("part1 data"))
            .await
            .unwrap();
        let part2 = store
            .put_part("test-bucket", "test-key", &upload_id, 2, Bytes::from("part2 data"))
            .await
            .unwrap();

        assert_eq!(part1.part_number, 1);
        assert_eq!(part2.part_number, 2);

        // List parts
        let parts = store.list_parts("test-bucket", &upload_id).await.unwrap();

        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].part_number, 1);
        assert_eq!(parts[1].part_number, 2);
    }

    #[tokio::test]
    async fn test_complete_upload() {
        let (store, _temp) = create_test_store().await;

        let upload_id = store.create("test-bucket", "test-key", None).await.unwrap();

        // Upload parts
        let part1 = store
            .put_part("test-bucket", "test-key", &upload_id, 1, Bytes::from("part1"))
            .await
            .unwrap();
        let part2 = store
            .put_part("test-bucket", "test-key", &upload_id, 2, Bytes::from("part2"))
            .await
            .unwrap();

        // Complete
        let part_etags = vec![
            (1, part1.etag.clone()),
            (2, part2.etag.clone()),
        ];

        let (data, etag) = store.complete("test-bucket", &upload_id, part_etags).await.unwrap();

        assert_eq!(data.as_ref(), b"part1part2");
        assert!(etag.contains("-2\"")); // Multipart ETag format: "hash-N"
    }

    #[tokio::test]
    async fn test_abort_upload() {
        let (store, _temp) = create_test_store().await;

        let upload_id = store.create("test-bucket", "test-key", None).await.unwrap();

        // Upload a part
        store
            .put_part("test-bucket", "test-key", &upload_id, 1, Bytes::from("data"))
            .await
            .unwrap();

        // Abort
        store.abort("test-bucket", &upload_id).await.unwrap();

        // Verify upload no longer exists
        let result = store.get("test-bucket", &upload_id).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_list_uploads() {
        let (store, _temp) = create_test_store().await;

        // Create multiple uploads
        let _upload1 = store.create("test-bucket", "key1", None).await.unwrap();
        let _upload2 = store.create("test-bucket", "key2", None).await.unwrap();
        let _upload3 = store.create("test-bucket", "prefix/key3", None).await.unwrap();

        // List all
        let uploads = store.list_uploads("test-bucket", None).await.unwrap();
        assert_eq!(uploads.len(), 3);

        // List with prefix
        let uploads = store.list_uploads("test-bucket", Some("prefix/")).await.unwrap();
        assert_eq!(uploads.len(), 1);
        assert_eq!(uploads[0].key, "prefix/key3");
    }
}

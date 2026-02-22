//! ListBuckets and ListObjects operations

use bytes::Bytes;
use http_body_util::Full;
use hyper::{Response, StatusCode};

use crate::storage::StorageEngine;
use crate::types::error::S3Result;
use crate::types::response::{
    BucketInfo, BucketList, ListBucketsResponse, ListObjectsV2Response, ObjectInfo, Owner,
};
use crate::utils::xml::to_xml;

/// Handle ListBuckets request (GET /)
pub async fn list_buckets(storage: &StorageEngine) -> S3Result<Response<Full<Bytes>>> {
    let buckets = storage.list_buckets().await;

    let response_body = ListBucketsResponse {
        owner: Owner::default(),
        buckets: BucketList {
            buckets: buckets.iter().map(BucketInfo::from).collect(),
        },
    };

    let xml = to_xml(&response_body).map_err(|e| {
        crate::types::error::S3Error::internal_error(&format!("XML serialization error: {}", e))
    })?;

    let response = Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/xml")
        .body(Full::new(Bytes::from(xml)))
        .unwrap();

    Ok(response)
}

/// Handle GET bucket request (list objects or bucket info)
pub async fn get_bucket(
    storage: &StorageEngine,
    bucket: &str,
    query: &str,
) -> S3Result<Response<Full<Bytes>>> {
    // Parse query parameters
    let params: std::collections::HashMap<_, _> = url::form_urlencoded::parse(query.as_bytes())
        .into_owned()
        .collect();

    // Check for location query
    if params.contains_key("location") {
        return get_bucket_location(storage, bucket).await;
    }

    // Default to ListObjectsV2
    let prefix = params.get("prefix").map(|s| s.as_str());
    let delimiter = params.get("delimiter").map(|s| s.as_str());
    let max_keys = params
        .get("max-keys")
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000);
    let continuation_token = params.get("continuation-token").map(|s| s.as_str());

    let result = storage
        .list_objects(bucket, prefix, delimiter, max_keys, continuation_token)
        .await?;

    // Build common prefixes for XML response
    let common_prefixes: Vec<crate::types::response::CommonPrefix> = result
        .common_prefixes
        .iter()
        .map(|p| crate::types::response::CommonPrefix {
            prefix: p.clone(),
        })
        .collect();

    let response_body = ListObjectsV2Response {
        name: bucket.to_string(),
        prefix: prefix.unwrap_or("").to_string(),
        key_count: result.objects.len() as i32,
        max_keys,
        is_truncated: result.is_truncated,
        contents: result.objects.iter().map(ObjectInfo::from).collect(),
        common_prefixes,
        continuation_token: continuation_token.map(String::from),
        next_continuation_token: result.next_continuation_token,
        start_after: None,
        delimiter: delimiter.map(String::from),
        encoding_type: None,
    };

    let xml = to_xml(&response_body).map_err(|e| {
        crate::types::error::S3Error::internal_error(&format!("XML serialization error: {}", e))
    })?;

    let response = Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/xml")
        .body(Full::new(Bytes::from(xml)))
        .unwrap();

    Ok(response)
}

/// Handle GetBucketLocation request
async fn get_bucket_location(
    storage: &StorageEngine,
    bucket: &str,
) -> S3Result<Response<Full<Bytes>>> {
    let bucket_info = storage.get_bucket(bucket).await?;

    // For us-east-1, AWS returns an empty element
    let location_xml = if bucket_info.region == "us-east-1" {
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<LocationConstraint xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"/>".to_string()
    } else {
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<LocationConstraint xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">{}</LocationConstraint>",
            bucket_info.region
        )
    };

    let response = Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/xml")
        .body(Full::new(Bytes::from(location_xml)))
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
    async fn test_list_buckets_empty() {
        let (storage, _temp) = create_test_storage().await;

        let response = list_buckets(&storage).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("content-type").unwrap(),
            "application/xml"
        );
    }

    #[tokio::test]
    async fn test_list_buckets_with_buckets() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("bucket1").await.unwrap();
        storage.create_bucket("bucket2").await.unwrap();

        let response = list_buckets(&storage).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        // Check response body contains bucket names
        let body = response.into_body();
        let bytes = http_body_util::BodyExt::collect(body)
            .await
            .unwrap()
            .to_bytes();
        let body_str = String::from_utf8_lossy(&bytes);

        assert!(body_str.contains("<Name>bucket1</Name>"));
        assert!(body_str.contains("<Name>bucket2</Name>"));
    }

    #[tokio::test]
    async fn test_list_objects() {
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

        let response = get_bucket(&storage, "test-bucket", "").await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body();
        let bytes = http_body_util::BodyExt::collect(body)
            .await
            .unwrap()
            .to_bytes();
        let body_str = String::from_utf8_lossy(&bytes);

        assert!(body_str.contains("<Key>key1</Key>"));
        assert!(body_str.contains("<Key>key2</Key>"));
    }

    #[tokio::test]
    async fn test_list_objects_with_prefix() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();
        storage
            .put_object("test-bucket", "prefix/key1", Bytes::from("data1"), None, None)
            .await
            .unwrap();
        storage
            .put_object("test-bucket", "other/key2", Bytes::from("data2"), None, None)
            .await
            .unwrap();

        let response = get_bucket(&storage, "test-bucket", "prefix=prefix/")
            .await
            .unwrap();

        let body = response.into_body();
        let bytes = http_body_util::BodyExt::collect(body)
            .await
            .unwrap()
            .to_bytes();
        let body_str = String::from_utf8_lossy(&bytes);

        assert!(body_str.contains("<Key>prefix/key1</Key>"));
        assert!(!body_str.contains("<Key>other/key2</Key>"));
    }

    #[tokio::test]
    async fn test_get_bucket_location() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        let response = get_bucket(&storage, "test-bucket", "location")
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body();
        let bytes = http_body_util::BodyExt::collect(body)
            .await
            .unwrap()
            .to_bytes();
        let body_str = String::from_utf8_lossy(&bytes);

        assert!(body_str.contains("LocationConstraint"));
    }
}

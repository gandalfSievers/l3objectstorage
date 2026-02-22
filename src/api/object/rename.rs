//! RenameObject operation

use bytes::Bytes;
use http_body_util::Full;
use hyper::{Response, StatusCode};

use crate::storage::StorageEngine;
use crate::types::error::S3Result;

/// Rename an object within a bucket
///
/// This is an atomic rename operation - the source object is removed and the
/// destination object is created with the same content and metadata.
pub async fn rename_object(
    storage: &StorageEngine,
    bucket: &str,
    source_key: &str,
    dest_key: &str,
) -> S3Result<Response<Full<Bytes>>> {
    // Perform the rename operation
    storage.rename_object(bucket, source_key, dest_key).await?;

    // Return empty 200 OK response (per S3 API spec)
    let response = Response::builder()
        .status(StatusCode::OK)
        .body(Full::new(Bytes::new()))
        .unwrap();

    Ok(response)
}

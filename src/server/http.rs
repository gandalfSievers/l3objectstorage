//! HTTP server setup

use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

use crate::api;
use crate::auth::{has_presigned_params, Credentials, PresignedUrlParams, SigV4Verifier};
use crate::config::Config;
use crate::storage::StorageEngine;
use crate::types::error::{S3Error, S3ErrorCode};

/// The main S3-compatible server
pub struct Server {
    config: Config,
    storage: Arc<StorageEngine>,
}

impl Server {
    /// Create a new server with the given configuration
    pub async fn new(config: Config) -> Result<Self, S3Error> {
        let storage = StorageEngine::new(config.clone()).await?;

        Ok(Self {
            config,
            storage: Arc::new(storage),
        })
    }

    /// Run the server
    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let addr = self.config.socket_addr();
        let listener = TcpListener::bind(addr).await?;

        tracing::info!("Server listening on {}", addr);
        if self.config.require_auth {
            tracing::info!("Authentication is ENABLED");
        } else {
            tracing::info!("Authentication is DISABLED (anonymous access allowed)");
        }
        if let Some(ref domain) = self.config.domain {
            tracing::info!("Virtual hosted-style enabled for domain: {}", domain);
        }

        loop {
            let (stream, remote_addr) = listener.accept().await?;
            let io = TokioIo::new(stream);
            let storage = Arc::clone(&self.storage);
            let config = self.config.clone();

            tokio::spawn(async move {
                let service = service_fn(move |req| {
                    let storage = Arc::clone(&storage);
                    let config = config.clone();
                    async move { handle_request(req, storage, &config, remote_addr).await }
                });

                if let Err(err) = http1::Builder::new()
                    .serve_connection(io, service)
                    .await
                {
                    tracing::error!("Error serving connection: {:?}", err);
                }
            });
        }
    }

    /// Run the server with graceful shutdown support
    ///
    /// This method listens for a shutdown signal and gracefully shuts down:
    /// 1. Stops accepting new connections immediately
    /// 2. Waits for in-flight requests to complete (up to shutdown_timeout)
    /// 3. Forces shutdown if timeout is exceeded
    pub async fn run_with_shutdown(
        &self,
        shutdown_rx: oneshot::Receiver<()>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let addr = self.config.socket_addr();
        let listener = TcpListener::bind(addr).await?;
        let shutdown_timeout = self.config.shutdown_timeout;

        tracing::info!("Server listening on {}", addr);
        if self.config.require_auth {
            tracing::info!("Authentication is ENABLED");
        } else {
            tracing::info!("Authentication is DISABLED (anonymous access allowed)");
        }
        if let Some(ref domain) = self.config.domain {
            tracing::info!("Virtual hosted-style enabled for domain: {}", domain);
        }

        // Track active connections
        let active_connections = Arc::new(AtomicUsize::new(0));

        // Convert oneshot to a future we can use in select!
        let mut shutdown_rx = shutdown_rx;

        loop {
            tokio::select! {
                // Handle shutdown signal
                _ = &mut shutdown_rx => {
                    tracing::info!("Shutdown signal received, stopping new connections");
                    break;
                }

                // Accept new connections
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, remote_addr)) => {
                            let io = TokioIo::new(stream);
                            let storage = Arc::clone(&self.storage);
                            let config = self.config.clone();
                            let active_connections = Arc::clone(&active_connections);

                            // Increment active connection count
                            active_connections.fetch_add(1, Ordering::SeqCst);

                            tokio::spawn(async move {
                                let service = service_fn(move |req| {
                                    let storage = Arc::clone(&storage);
                                    let config = config.clone();
                                    async move { handle_request(req, storage, &config, remote_addr).await }
                                });

                                if let Err(err) = http1::Builder::new()
                                    .serve_connection(io, service)
                                    .await
                                {
                                    tracing::error!("Error serving connection: {:?}", err);
                                }

                                // Decrement active connection count
                                active_connections.fetch_sub(1, Ordering::SeqCst);
                            });
                        }
                        Err(e) => {
                            tracing::error!("Failed to accept connection: {}", e);
                        }
                    }
                }
            }
        }

        // Wait for in-flight requests to complete (with timeout)
        let start = std::time::Instant::now();
        loop {
            let count = active_connections.load(Ordering::SeqCst);
            if count == 0 {
                tracing::info!("All connections closed, shutdown complete");
                break;
            }

            if start.elapsed() > shutdown_timeout {
                tracing::warn!(
                    "Shutdown timeout exceeded with {} active connections, forcing shutdown",
                    count
                );
                break;
            }

            tracing::info!(
                "Waiting for {} active connection(s) to complete...",
                count
            );
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        Ok(())
    }
}

async fn handle_request(
    req: Request<Incoming>,
    storage: Arc<StorageEngine>,
    config: &Config,
    _remote_addr: SocketAddr,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let method = req.method().clone();
    let uri = req.uri().clone();
    let path = uri.path().to_string();

    // Extract headers we need before consuming the request
    let content_type = req
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(String::from);

    let copy_source = req
        .headers()
        .get("x-amz-copy-source")
        .and_then(|v| v.to_str().ok())
        .map(String::from);

    let canned_acl = req
        .headers()
        .get("x-amz-acl")
        .and_then(|v| v.to_str().ok())
        .map(String::from);

    // Extract x-amz-rename-source header for RenameObject
    let rename_source = req
        .headers()
        .get("x-amz-rename-source")
        .and_then(|v| v.to_str().ok())
        .map(String::from);

    // Extract Range header for partial content requests
    let range_header = req
        .headers()
        .get("range")
        .and_then(|v| v.to_str().ok())
        .map(String::from);

    // Extract conditional request headers
    let if_match_header = req
        .headers()
        .get("if-match")
        .and_then(|v| v.to_str().ok())
        .map(String::from);

    let if_none_match_header = req
        .headers()
        .get("if-none-match")
        .and_then(|v| v.to_str().ok())
        .map(String::from);

    // Extract server-side encryption header
    let sse_header = req
        .headers()
        .get("x-amz-server-side-encryption")
        .and_then(|v| v.to_str().ok())
        .map(String::from);

    // Collect all headers for auth verification
    let header_pairs: Vec<(String, String)> = req
        .headers()
        .iter()
        .map(|(k, v)| {
            (
                k.as_str().to_lowercase(),
                v.to_str().unwrap_or("").to_string(),
            )
        })
        .collect();

    // Get authorization header for auth verification
    let auth_header = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(String::from);

    // Extract Host header for virtual hosted-style bucket detection
    let host_header = req
        .headers()
        .get("host")
        .and_then(|v| v.to_str().ok())
        .map(String::from);

    // Parse query string for special operations
    let query = uri.query().unwrap_or("");
    let query_params = parse_query(query);

    tracing::debug!("{} {}", method, path);

    // Try virtual hosted-style: extract bucket from Host header if domain is configured
    let vhost_bucket = host_header
        .as_deref()
        .and_then(|host| {
            config
                .domain
                .as_deref()
                .and_then(|domain| extract_bucket_from_host(host, domain))
        })
        .map(String::from);

    // Parse bucket and key from path or use virtual hosted-style bucket
    let (bucket, key) = if let Some(ref vhost_bucket) = vhost_bucket {
        // Virtual hosted-style: bucket from Host, entire path is the key
        tracing::debug!(
            "Virtual hosted-style request: bucket={}, host={:?}",
            vhost_bucket,
            host_header
        );
        let key_path = path.trim_start_matches('/');
        let key: Option<&str> = if key_path.is_empty() { None } else { Some(key_path) };
        (Some(vhost_bucket.as_str()), key)
    } else {
        parse_path(&path)
    };

    // Collect the request body
    let body = match req.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(e) => {
            tracing::error!("Failed to read request body: {}", e);
            return Ok(error_response(
                StatusCode::BAD_REQUEST,
                "Failed to read request body",
            ));
        }
    };

    // Verify authentication
    // Pre-signed URLs are ALWAYS verified (they carry their own auth)
    // Header-based auth is only required if config.require_auth is true
    let is_presigned = has_presigned_params(&query_params);

    if is_presigned {
        // Pre-signed URLs must always be verified - they ARE the authentication
        let auth_result = verify_presigned_url(&method, &path, query, &query_params, &header_pairs, config);
        if let Err(e) = auth_result {
            tracing::warn!("Pre-signed URL authentication failed: {}", e);
            return Ok(s3_error_response(&e));
        }
    } else if config.require_auth {
        // Header-based auth only required when configured
        let auth_result = verify_signature(
            &method,
            &path,
            query,
            &header_pairs,
            &body,
            auth_header.as_deref(),
            config,
        );
        if let Err(e) = auth_result {
            tracing::warn!("Authentication failed: {}", e);
            return Ok(s3_error_response(&e));
        }
    }

    let region = &config.region;

    // Check for specific query params
    let has_uploads = query_params.contains_key("uploads");
    let has_upload_id = query_params.contains_key("uploadId");
    let has_part_number = query_params.contains_key("partNumber");
    let has_tagging = query_params.contains_key("tagging");
    let has_cors = query_params.contains_key("cors");
    let has_versioning = query_params.contains_key("versioning");
    let has_versions = query_params.contains_key("versions");
    let has_policy = query_params.contains_key("policy");
    let has_acl = query_params.contains_key("acl");
    let has_attributes = query_params.contains_key("attributes");
    let has_lifecycle = query_params.contains_key("lifecycle");
    let has_encryption = query_params.contains_key("encryption");
    let has_public_access_block = query_params.contains_key("publicAccessBlock");
    let has_website = query_params.contains_key("website");
    let has_ownership_controls = query_params.contains_key("ownershipControls");
    let has_object_lock = query_params.contains_key("object-lock");
    let has_legal_hold = query_params.contains_key("legal-hold");
    let has_retention = query_params.contains_key("retention");
    let has_rename_object = query_params.contains_key("renameObject");
    let has_logging = query_params.contains_key("logging");
    let has_notification = query_params.contains_key("notification");
    let has_replication = query_params.contains_key("replication");
    let has_request_payment = query_params.contains_key("requestPayment");
    let has_select = query_params.contains_key("select");
    let upload_id = query_params.get("uploadId").map(|s| s.as_str());
    let version_id = query_params.get("versionId").map(|s| s.as_str());

    // Parse x-amz-object-attributes header for GetObjectAttributes
    // Note: header_pairs already has lowercase keys
    let object_attributes: Vec<String> = header_pairs
        .iter()
        .filter(|(k, _)| k == "x-amz-object-attributes")
        .flat_map(|(_, v)| v.split(',').map(|s| s.trim().to_string()))
        .collect();

    // If no attributes specified but has_attributes query param, use defaults
    let object_attributes = if object_attributes.is_empty() && has_attributes {
        vec![
            "ETag".to_string(),
            "ObjectSize".to_string(),
            "StorageClass".to_string(),
        ]
    } else {
        object_attributes
    };
    let part_number: Option<i32> = query_params
        .get("partNumber")
        .and_then(|s| s.parse().ok());
    let prefix = query_params.get("prefix").map(|s| s.as_str());

    // Route the request
    let result = match (&method, bucket, key) {
        // Service operations (no bucket)
        (&Method::GET, None, None) => {
            api::bucket::list_buckets(&storage).await
        }

        // Bucket operations
        // PUT /{bucket}?tagging - PutBucketTagging
        (&Method::PUT, Some(bucket), None) if has_tagging => {
            api::bucket::put_bucket_tagging(&storage, bucket, body).await
        }
        // PUT /{bucket}?cors - PutBucketCors
        (&Method::PUT, Some(bucket), None) if has_cors => {
            api::bucket::put_bucket_cors(&storage, bucket, body).await
        }
        // PUT /{bucket}?versioning - PutBucketVersioning
        (&Method::PUT, Some(bucket), None) if has_versioning => {
            api::bucket::put_bucket_versioning(&storage, bucket, body).await
        }
        // PUT /{bucket}?policy - PutBucketPolicy
        (&Method::PUT, Some(bucket), None) if has_policy => {
            api::bucket::put_bucket_policy(&storage, bucket, body).await
        }
        // PUT /{bucket}?acl - PutBucketAcl
        (&Method::PUT, Some(bucket), None) if has_acl => {
            api::bucket::put_bucket_acl(&storage, bucket, canned_acl.as_deref(), body).await
        }
        // PUT /{bucket}?lifecycle - PutBucketLifecycleConfiguration
        (&Method::PUT, Some(bucket), None) if has_lifecycle => {
            api::bucket::put_bucket_lifecycle_configuration(&storage, bucket, body).await
        }
        // PUT /{bucket}?encryption - PutBucketEncryption
        (&Method::PUT, Some(bucket), None) if has_encryption => {
            api::bucket::put_bucket_encryption(&storage, bucket, body).await
        }
        // PUT /{bucket}?object-lock - PutObjectLockConfiguration
        (&Method::PUT, Some(bucket), None) if has_object_lock => {
            api::bucket::put_object_lock_configuration(&storage, bucket, body).await
        }
        // PUT /{bucket}?publicAccessBlock - PutPublicAccessBlock
        (&Method::PUT, Some(bucket), None) if has_public_access_block => {
            api::bucket::put_public_access_block(&storage, bucket, body).await
        }
        // PUT /{bucket}?website - PutBucketWebsite
        (&Method::PUT, Some(bucket), None) if has_website => {
            api::bucket::put_bucket_website(&storage, bucket, body).await
        }
        // PUT /{bucket}?ownershipControls - PutBucketOwnershipControls
        (&Method::PUT, Some(bucket), None) if has_ownership_controls => {
            api::bucket::put_bucket_ownership_controls(&storage, bucket, body).await
        }
        // PUT /{bucket}?logging - PutBucketLogging
        (&Method::PUT, Some(bucket), None) if has_logging => {
            api::bucket::put_bucket_logging(&storage, bucket, body).await
        }
        // PUT /{bucket}?notification - PutBucketNotificationConfiguration
        (&Method::PUT, Some(bucket), None) if has_notification => {
            api::bucket::put_bucket_notification_configuration(&storage, bucket, body).await
        }
        // PUT /{bucket}?replication - PutBucketReplication
        (&Method::PUT, Some(bucket), None) if has_replication => {
            api::bucket::put_bucket_replication(&storage, bucket, body).await
        }
        // PUT /{bucket}?requestPayment - PutBucketRequestPayment
        (&Method::PUT, Some(bucket), None) if has_request_payment => {
            api::bucket::put_bucket_request_payment(&storage, bucket, body).await
        }
        // PUT /{bucket} - CreateBucket (with optional x-amz-acl header and object lock)
        (&Method::PUT, Some(bucket), None) => {
            // Check for x-amz-bucket-object-lock-enabled header
            let object_lock_enabled = header_pairs
                .iter()
                .any(|(k, v)| k == "x-amz-bucket-object-lock-enabled" && v.to_lowercase() == "true");

            if object_lock_enabled {
                api::bucket::create_bucket_with_object_lock(&storage, bucket, region, canned_acl.as_deref()).await
            } else {
                api::bucket::create_bucket(&storage, bucket, region, canned_acl.as_deref()).await
            }
        }
        // DELETE /{bucket}?tagging - DeleteBucketTagging
        (&Method::DELETE, Some(bucket), None) if has_tagging => {
            api::bucket::delete_bucket_tagging(&storage, bucket).await
        }
        // DELETE /{bucket}?cors - DeleteBucketCors
        (&Method::DELETE, Some(bucket), None) if has_cors => {
            api::bucket::delete_bucket_cors(&storage, bucket).await
        }
        // DELETE /{bucket}?policy - DeleteBucketPolicy
        (&Method::DELETE, Some(bucket), None) if has_policy => {
            api::bucket::delete_bucket_policy(&storage, bucket).await
        }
        // DELETE /{bucket}?lifecycle - DeleteBucketLifecycle
        (&Method::DELETE, Some(bucket), None) if has_lifecycle => {
            api::bucket::delete_bucket_lifecycle(&storage, bucket).await
        }
        // DELETE /{bucket}?encryption - DeleteBucketEncryption
        (&Method::DELETE, Some(bucket), None) if has_encryption => {
            api::bucket::delete_bucket_encryption(&storage, bucket).await
        }
        // DELETE /{bucket}?publicAccessBlock - DeletePublicAccessBlock
        (&Method::DELETE, Some(bucket), None) if has_public_access_block => {
            api::bucket::delete_public_access_block(&storage, bucket).await
        }
        // DELETE /{bucket}?website - DeleteBucketWebsite
        (&Method::DELETE, Some(bucket), None) if has_website => {
            api::bucket::delete_bucket_website(&storage, bucket).await
        }
        // DELETE /{bucket}?ownershipControls - DeleteBucketOwnershipControls
        (&Method::DELETE, Some(bucket), None) if has_ownership_controls => {
            api::bucket::delete_bucket_ownership_controls(&storage, bucket).await
        }
        // DELETE /{bucket}?replication - DeleteBucketReplication
        (&Method::DELETE, Some(bucket), None) if has_replication => {
            api::bucket::delete_bucket_replication(&storage, bucket).await
        }
        (&Method::DELETE, Some(bucket), None) => {
            api::bucket::delete_bucket(&storage, bucket).await
        }
        (&Method::HEAD, Some(bucket), None) => {
            api::bucket::head_bucket(&storage, bucket).await
        }
        // GET /{bucket}?uploads - ListMultipartUploads
        (&Method::GET, Some(bucket), None) if has_uploads => {
            let delimiter = query_params.get("delimiter").map(|s| s.as_str());
            let max_uploads = query_params.get("max-uploads").and_then(|v| v.parse().ok());
            let key_marker = query_params.get("key-marker").map(|s| s.as_str());
            api::multipart::list_multipart_uploads(&storage, bucket, prefix, delimiter, max_uploads, key_marker).await
        }
        // GET /{bucket}?tagging - GetBucketTagging
        (&Method::GET, Some(bucket), None) if has_tagging => {
            api::bucket::get_bucket_tagging(&storage, bucket).await
        }
        // GET /{bucket}?cors - GetBucketCors
        (&Method::GET, Some(bucket), None) if has_cors => {
            api::bucket::get_bucket_cors(&storage, bucket).await
        }
        // GET /{bucket}?versioning - GetBucketVersioning
        (&Method::GET, Some(bucket), None) if has_versioning => {
            api::bucket::get_bucket_versioning(&storage, bucket).await
        }
        // GET /{bucket}?policyStatus - GetBucketPolicyStatus
        (&Method::GET, Some(bucket), None) if query_params.contains_key("policyStatus") => {
            api::bucket::get_bucket_policy_status(&storage, bucket).await
        }
        // GET /{bucket}?policy - GetBucketPolicy
        (&Method::GET, Some(bucket), None) if has_policy => {
            api::bucket::get_bucket_policy(&storage, bucket).await
        }
        // GET /{bucket}?acl - GetBucketAcl
        (&Method::GET, Some(bucket), None) if has_acl => {
            api::bucket::get_bucket_acl(&storage, bucket).await
        }
        // GET /{bucket}?lifecycle - GetBucketLifecycleConfiguration
        (&Method::GET, Some(bucket), None) if has_lifecycle => {
            api::bucket::get_bucket_lifecycle_configuration(&storage, bucket).await
        }
        // GET /{bucket}?encryption - GetBucketEncryption
        (&Method::GET, Some(bucket), None) if has_encryption => {
            api::bucket::get_bucket_encryption(&storage, bucket).await
        }
        // GET /{bucket}?object-lock - GetObjectLockConfiguration
        (&Method::GET, Some(bucket), None) if has_object_lock => {
            api::bucket::get_object_lock_configuration(&storage, bucket).await
        }
        // GET /{bucket}?publicAccessBlock - GetPublicAccessBlock
        (&Method::GET, Some(bucket), None) if has_public_access_block => {
            api::bucket::get_public_access_block(&storage, bucket).await
        }
        // GET /{bucket}?website - GetBucketWebsite
        (&Method::GET, Some(bucket), None) if has_website => {
            api::bucket::get_bucket_website(&storage, bucket).await
        }
        // GET /{bucket}?ownershipControls - GetBucketOwnershipControls
        (&Method::GET, Some(bucket), None) if has_ownership_controls => {
            api::bucket::get_bucket_ownership_controls(&storage, bucket).await
        }
        // GET /{bucket}?logging - GetBucketLogging
        (&Method::GET, Some(bucket), None) if has_logging => {
            api::bucket::get_bucket_logging(&storage, bucket).await
        }
        // GET /{bucket}?notification - GetBucketNotificationConfiguration
        (&Method::GET, Some(bucket), None) if has_notification => {
            api::bucket::get_bucket_notification_configuration(&storage, bucket).await
        }
        // GET /{bucket}?replication - GetBucketReplication
        (&Method::GET, Some(bucket), None) if has_replication => {
            api::bucket::get_bucket_replication(&storage, bucket).await
        }
        // GET /{bucket}?requestPayment - GetBucketRequestPayment
        (&Method::GET, Some(bucket), None) if has_request_payment => {
            api::bucket::get_bucket_request_payment(&storage, bucket).await
        }
        // GET /{bucket}?versions - ListObjectVersions
        (&Method::GET, Some(bucket), None) if has_versions => {
            let delimiter = query_params.get("delimiter").map(|s| s.as_str());
            let max_keys = query_params.get("max-keys").and_then(|v| v.parse().ok());
            let key_marker = query_params.get("key-marker").map(|s| s.as_str());
            let version_id_marker = query_params.get("version-id-marker").map(|s| s.as_str());
            api::object::list_object_versions(&storage, bucket, prefix, delimiter, max_keys, key_marker, version_id_marker).await
        }
        (&Method::GET, Some(bucket), None) => {
            api::bucket::get_bucket(&storage, bucket, query).await
        }
        // POST /{bucket}?delete - DeleteObjects (batch delete)
        (&Method::POST, Some(bucket), None) if query == "delete" || query.starts_with("delete&") => {
            api::object::delete_objects(&storage, bucket, body).await
        }

        // POST /{bucket}/{key}?select - SelectObjectContent
        (&Method::POST, Some(bucket), Some(key)) if has_select => {
            api::object::select_object_content(&storage, bucket, key, body).await
        }

        // Multipart upload operations (must be checked before regular object operations)
        // POST /{bucket}/{key}?uploads - CreateMultipartUpload
        (&Method::POST, Some(bucket), Some(key)) if has_uploads => {
            api::multipart::create_multipart_upload(&storage, bucket, key, sse_header.as_deref()).await
        }
        // PUT /{bucket}/{key}?partNumber=N&uploadId=ID - UploadPart or UploadPartCopy
        (&Method::PUT, Some(bucket), Some(key)) if has_part_number && has_upload_id => {
            match (upload_id, part_number, copy_source.as_ref()) {
                (Some(uid), Some(pn), Some(source)) => {
                    // UploadPartCopy - copy from another object
                    let copy_source_range = header_pairs
                        .iter()
                        .find(|(k, _)| k.to_lowercase() == "x-amz-copy-source-range")
                        .map(|(_, v)| v.as_str());
                    api::multipart::upload_part_copy(&storage, bucket, key, uid, pn, source, copy_source_range).await
                }
                (Some(uid), Some(pn), None) => {
                    // Regular UploadPart
                    api::multipart::upload_part(&storage, bucket, key, uid, pn, body).await
                }
                _ => Ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Full::new(Bytes::from("Missing uploadId or partNumber")))
                    .unwrap()),
            }
        }
        // POST /{bucket}/{key}?uploadId=ID - CompleteMultipartUpload
        (&Method::POST, Some(bucket), Some(key)) if has_upload_id => {
            match upload_id {
                Some(uid) => {
                    api::multipart::complete_multipart_upload(&storage, bucket, key, uid, body, sse_header.as_deref()).await
                }
                None => Ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Full::new(Bytes::from("Missing uploadId")))
                    .unwrap()),
            }
        }
        // GET /{bucket}/{key}?uploadId=ID - ListParts
        (&Method::GET, Some(bucket), Some(key)) if has_upload_id => {
            match upload_id {
                Some(uid) => {
                    let max_parts = query_params.get("max-parts").and_then(|v| v.parse().ok());
                    let part_number_marker = query_params.get("part-number-marker").and_then(|v| v.parse().ok());
                    api::multipart::list_parts(&storage, bucket, key, uid, max_parts, part_number_marker).await
                }
                None => Ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Full::new(Bytes::from("Missing uploadId")))
                    .unwrap()),
            }
        }
        // DELETE /{bucket}/{key}?uploadId=ID - AbortMultipartUpload
        (&Method::DELETE, Some(bucket), Some(_key)) if has_upload_id => {
            match upload_id {
                Some(uid) => {
                    api::multipart::abort_multipart_upload(&storage, bucket, uid).await
                }
                None => Ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Full::new(Bytes::from("Missing uploadId")))
                    .unwrap()),
            }
        }

        // Object ACL operations
        // GET /{bucket}/{key}?acl - GetObjectAcl
        (&Method::GET, Some(bucket), Some(key)) if has_acl => {
            api::object::get_object_acl(&storage, bucket, key).await
        }
        // PUT /{bucket}/{key}?acl - PutObjectAcl
        (&Method::PUT, Some(bucket), Some(key)) if has_acl => {
            api::object::put_object_acl(&storage, bucket, key, canned_acl.as_deref(), body).await
        }

        // Object Lock operations
        // GET /{bucket}/{key}?legal-hold - GetObjectLegalHold
        (&Method::GET, Some(bucket), Some(key)) if has_legal_hold => {
            api::object::get_object_legal_hold(&storage, bucket, key, version_id).await
        }
        // PUT /{bucket}/{key}?legal-hold - PutObjectLegalHold
        (&Method::PUT, Some(bucket), Some(key)) if has_legal_hold => {
            api::object::put_object_legal_hold(&storage, bucket, key, version_id, body).await
        }
        // GET /{bucket}/{key}?retention - GetObjectRetention
        (&Method::GET, Some(bucket), Some(key)) if has_retention => {
            api::object::get_object_retention(&storage, bucket, key, version_id).await
        }
        // PUT /{bucket}/{key}?retention - PutObjectRetention
        (&Method::PUT, Some(bucket), Some(key)) if has_retention => {
            // Check for x-amz-bypass-governance-retention header
            let bypass = header_pairs
                .iter()
                .any(|(k, v)| k == "x-amz-bypass-governance-retention" && v.to_lowercase() == "true");
            api::object::put_object_retention(&storage, bucket, key, version_id, bypass, body).await
        }

        // Object tagging operations
        // GET /{bucket}/{key}?tagging - GetObjectTagging
        (&Method::GET, Some(bucket), Some(key)) if has_tagging => {
            api::object::get_object_tagging(&storage, bucket, key, version_id).await
        }
        // PUT /{bucket}/{key}?tagging - PutObjectTagging
        (&Method::PUT, Some(bucket), Some(key)) if has_tagging => {
            api::object::put_object_tagging(&storage, bucket, key, version_id, body).await
        }
        // DELETE /{bucket}/{key}?tagging - DeleteObjectTagging
        (&Method::DELETE, Some(bucket), Some(key)) if has_tagging => {
            api::object::delete_object_tagging(&storage, bucket, key, version_id).await
        }

        // GET /{bucket}/{key}?attributes - GetObjectAttributes
        (&Method::GET, Some(bucket), Some(key)) if has_attributes => {
            api::object::get_object_attributes(&storage, bucket, key, version_id, object_attributes.clone()).await
        }

        // PUT /{bucket}/{key}?renameObject - RenameObject
        (&Method::PUT, Some(bucket), Some(key)) if has_rename_object => {
            match rename_source {
                Some(source) => {
                    // Use source key as-is (keys are stored URL-encoded)
                    api::object::rename_object(&storage, bucket, &source, key).await
                }
                None => {
                    // Missing x-amz-rename-source header
                    Ok(Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .header("Content-Type", "application/xml")
                        .body(Full::new(Bytes::from(
                            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                            <Error>\n  \
                            <Code>MissingHeaderException</Code>\n  \
                            <Message>Missing required header: x-amz-rename-source</Message>\n\
                            </Error>"
                        )))
                        .unwrap())
                }
            }
        }

        // Regular object operations
        // PUT /{bucket}/{key} with x-amz-copy-source - CopyObject
        (&Method::PUT, Some(bucket), Some(key)) if copy_source.is_some() => {
            let source = copy_source.unwrap();
            // Parse source bucket and key from the copy source header
            // Format: /<bucket>/<key> or <bucket>/<key>
            let source = source.trim_start_matches('/');
            if let Some((src_bucket, src_key)) = source.split_once('/') {
                // URL decode the source key
                let src_key = percent_encoding::percent_decode_str(src_key)
                    .decode_utf8_lossy()
                    .to_string();

                // Extract metadata directive
                let metadata_directive = header_pairs
                    .iter()
                    .find(|(k, _)| k == "x-amz-metadata-directive")
                    .map(|(_, v)| v.as_str())
                    .unwrap_or("COPY");

                // Extract custom metadata if directive is REPLACE
                let (custom_metadata, custom_content_type) = if metadata_directive == "REPLACE" {
                    let mut metadata = std::collections::HashMap::new();
                    let mut ct = None;
                    for (k, v) in &header_pairs {
                        if k.starts_with("x-amz-meta-") {
                            metadata.insert(k.clone(), v.clone());
                        }
                        if k == "content-type" {
                            ct = Some(v.clone());
                        }
                    }
                    (Some(metadata), ct)
                } else {
                    (None, None)
                };

                copy_object(
                    &storage, src_bucket, &src_key, bucket, key,
                    sse_header.as_deref(),
                    custom_metadata,
                    custom_content_type.as_deref(),
                ).await
            } else {
                Ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Full::new(Bytes::from("Invalid x-amz-copy-source")))
                    .unwrap())
            }
        }
        (&Method::PUT, Some(bucket), Some(key)) => {
            // Extract custom metadata headers (x-amz-meta-*)
            let custom_metadata: std::collections::HashMap<String, String> = header_pairs
                .iter()
                .filter(|(k, _)| k.starts_with("x-amz-meta-"))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            let custom_metadata = if custom_metadata.is_empty() { None } else { Some(custom_metadata) };

            api::object::put_object_conditional_with_metadata(
                &storage,
                bucket,
                key,
                body,
                content_type,
                canned_acl.as_deref(),
                if_none_match_header.as_deref(),
                sse_header.as_deref(),
                custom_metadata,
            ).await
        }
        (&Method::GET, Some(bucket), Some(key)) => {
            // Parse response header overrides from query parameters
            let response_overrides = api::object::ResponseHeaderOverrides {
                content_type: query_params.get("response-content-type").cloned(),
                content_disposition: query_params.get("response-content-disposition").cloned(),
                content_encoding: query_params.get("response-content-encoding").cloned(),
                content_language: query_params.get("response-content-language").cloned(),
                cache_control: query_params.get("response-cache-control").cloned(),
                expires: query_params.get("response-expires").cloned(),
            };

            // Check if any override is set
            let has_overrides = response_overrides.content_type.is_some()
                || response_overrides.content_disposition.is_some()
                || response_overrides.content_encoding.is_some()
                || response_overrides.content_language.is_some()
                || response_overrides.cache_control.is_some()
                || response_overrides.expires.is_some();

            if has_overrides {
                api::object::get_object_full(
                    &storage,
                    bucket,
                    key,
                    version_id,
                    range_header.as_deref(),
                    if_match_header.as_deref(),
                    if_none_match_header.as_deref(),
                    Some(&response_overrides),
                ).await
            } else {
                api::object::get_object_with_conditionals(
                    &storage,
                    bucket,
                    key,
                    version_id,
                    range_header.as_deref(),
                    if_match_header.as_deref(),
                    if_none_match_header.as_deref(),
                ).await
            }
        }
        (&Method::HEAD, Some(bucket), Some(key)) => {
            api::object::head_object_conditional(
                &storage, bucket, key, version_id,
                if_match_header.as_deref(), if_none_match_header.as_deref(),
            ).await
        }
        (&Method::DELETE, Some(bucket), Some(key)) => {
            // Check for x-amz-bypass-governance-retention header
            let bypass_governance = header_pairs
                .iter()
                .any(|(k, v)| k == "x-amz-bypass-governance-retention" && v.to_lowercase() == "true");
            api::object::delete_object_versioned_with_bypass(&storage, bucket, key, version_id, bypass_governance).await
        }

        // Method not allowed
        _ => {
            Ok(Response::builder()
                .status(StatusCode::METHOD_NOT_ALLOWED)
                .body(Full::new(Bytes::from("Method Not Allowed")))
                .unwrap())
        }
    };

    match result {
        Ok(response) => Ok(response),
        Err(e) => {
            tracing::error!("Request error: {}", e);
            Ok(s3_error_response(&e))
        }
    }
}

/// Extract bucket from the Host header for virtual hosted-style requests.
///
/// Returns `Some(bucket_name)` if the Host matches `<bucket>.<domain>`,
/// otherwise `None` (fall back to path-style).
fn extract_bucket_from_host<'a>(host: &'a str, domain: &str) -> Option<&'a str> {
    // Strip port from host if present (e.g., "mybucket.s3.local:9000" -> "mybucket.s3.local")
    let host_without_port = host.split(':').next().unwrap_or(host);

    let suffix = format!(".{}", domain);
    if let Some(bucket) = host_without_port.strip_suffix(&suffix) {
        if !bucket.is_empty() {
            return Some(bucket);
        }
    }
    None
}

/// Parse bucket and key from the request path
fn parse_path(path: &str) -> (Option<&str>, Option<&str>) {
    let path = path.trim_start_matches('/');

    if path.is_empty() {
        return (None, None);
    }

    match path.find('/') {
        Some(idx) => {
            let bucket = &path[..idx];
            let key = &path[idx + 1..];
            if key.is_empty() {
                (Some(bucket), None)
            } else {
                (Some(bucket), Some(key))
            }
        }
        None => (Some(path), None),
    }
}

fn error_response(status: StatusCode, message: &str) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header("Content-Type", "application/xml")
        .body(Full::new(Bytes::from(format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Message>{}</Message></Error>",
            message
        ))))
        .unwrap()
}

fn s3_error_response(error: &S3Error) -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::from_u16(error.http_status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR))
        .header("Content-Type", "application/xml")
        .body(Full::new(Bytes::from(error.to_xml())))
        .unwrap()
}

/// Handle CopyObject request
async fn copy_object(
    storage: &StorageEngine,
    src_bucket: &str,
    src_key: &str,
    dest_bucket: &str,
    dest_key: &str,
    sse_header: Option<&str>,
    custom_metadata: Option<std::collections::HashMap<String, String>>,
    custom_content_type: Option<&str>,
) -> crate::types::error::S3Result<Response<Full<Bytes>>> {
    // Determine effective SSE algorithm (explicit header or bucket default)
    let sse_algorithm = storage
        .get_sse_algorithm_for_object(dest_bucket, sse_header)
        .await?;

    let obj = storage
        .copy_object_with_metadata(
            src_bucket, src_key, dest_bucket, dest_key,
            sse_algorithm.as_ref(),
            custom_metadata,
            custom_content_type,
        )
        .await?;

    // Send notification for successful copy
    storage.notify_event(
        dest_bucket, "s3:ObjectCreated:Copy", dest_key,
        obj.size, &obj.etag, obj.version_id.as_deref(),
    ).await;

    // Build CopyObjectResult XML response
    let xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
        <CopyObjectResult>\n  \
        <LastModified>{}</LastModified>\n  \
        <ETag>{}</ETag>\n\
        </CopyObjectResult>",
        obj.last_modified.to_rfc3339(),
        obj.etag
    );

    let mut response_builder = Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/xml");

    // Add SSE header if encryption was applied
    if let Some(ref sse) = obj.sse_algorithm {
        response_builder = response_builder.header("x-amz-server-side-encryption", sse);
    }

    let response = response_builder
        .body(Full::new(Bytes::from(xml)))
        .unwrap();

    Ok(response)
}

/// Parse query string into a HashMap
fn parse_query(query: &str) -> HashMap<String, String> {
    use percent_encoding::percent_decode_str;

    let mut params = HashMap::new();

    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        if let Some((key, value)) = pair.split_once('=') {
            // URL-decode both key and value
            let key = percent_decode_str(key).decode_utf8_lossy().to_string();
            let value = percent_decode_str(value).decode_utf8_lossy().to_string();
            params.insert(key, value);
        } else {
            // Query param without value (e.g., "uploads")
            let key = percent_decode_str(pair).decode_utf8_lossy().to_string();
            params.insert(key, String::new());
        }
    }

    params
}

/// Verify the AWS Signature Version 4 signature on a request
fn verify_signature(
    method: &Method,
    path: &str,
    query: &str,
    headers: &[(String, String)],
    body: &Bytes,
    auth_header: Option<&str>,
    config: &Config,
) -> crate::types::error::S3Result<()> {
    // Get authorization header
    let auth_header = auth_header.ok_or_else(|| {
        S3Error::new(
            S3ErrorCode::MissingSecurityHeader,
            "Missing Authorization header",
        )
    })?;

    // Verify it's AWS4-HMAC-SHA256
    if !auth_header.starts_with("AWS4-HMAC-SHA256 ") {
        return Err(S3Error::new(
            S3ErrorCode::InvalidSecurity,
            "Invalid authorization header format",
        ));
    }

    // Extract access key ID from the authorization header and validate
    let access_key_id = extract_access_key_id(auth_header)?;
    if access_key_id != config.access_key {
        return Err(S3Error::new(
            S3ErrorCode::InvalidAccessKeyId,
            "The AWS Access Key Id you provided does not exist in our records",
        ));
    }

    // Get payload hash from x-amz-content-sha256 header or calculate from body
    // AWS SDK sends this header, and we must use it (not recalculate) for signature verification
    // Special values include:
    // - UNSIGNED-PAYLOAD: payload is not signed
    // - STREAMING-UNSIGNED-PAYLOAD-TRAILER: streaming body with trailing checksum
    // - STREAMING-AWS4-HMAC-SHA256-PAYLOAD: signed streaming (chunked encoding)
    // - STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER: signed streaming with trailers
    let payload_hash = headers
        .iter()
        .find(|(k, _)| k == "x-amz-content-sha256")
        .map(|(_, v)| v.clone())
        .unwrap_or_else(|| calculate_sha256_hex(body));

    // Debug logging for signature verification troubleshooting
    tracing::debug!("=== SigV4 Verification Debug ===");
    tracing::debug!("Method: {}", method);
    tracing::debug!("Path: {}", path);
    tracing::debug!("Query: {}", query);
    tracing::debug!("Payload hash: {}", payload_hash);
    tracing::debug!("Body length: {} bytes", body.len());
    tracing::debug!("Authorization: {}", auth_header);
    tracing::debug!("Request headers ({} total):", headers.len());
    for (k, v) in headers {
        // Truncate long values for readability
        let display_value = if v.len() > 100 {
            format!("{}... ({} chars)", &v[..100], v.len())
        } else {
            v.clone()
        };
        tracing::debug!("  {}: {}", k, display_value);
    }

    // Create verifier with configured credentials
    let credentials = Credentials::new(&config.access_key, &config.secret_key);
    let verifier = SigV4Verifier::new(credentials, &config.region);

    // Build URI with query string for signature verification
    let uri = if query.is_empty() {
        path.to_string()
    } else {
        format!("{}?{}", path, query)
    };

    // Verify the signature
    verifier.verify(method.as_str(), &uri, headers, &payload_hash, auth_header)
}

/// Verify pre-signed URL authentication
fn verify_presigned_url(
    method: &Method,
    path: &str,
    query: &str,
    query_params: &HashMap<String, String>,
    headers: &[(String, String)],
    config: &Config,
) -> crate::types::error::S3Result<()> {
    tracing::debug!("Verifying pre-signed URL authentication");

    // Parse pre-signed URL parameters
    let params = PresignedUrlParams::from_query_params(query_params)?;

    // Validate algorithm
    if params.algorithm != "AWS4-HMAC-SHA256" {
        return Err(S3Error::new(
            S3ErrorCode::InvalidArgument,
            "Unsupported signing algorithm",
        ));
    }

    // Validate access key
    let access_key = params.access_key_id()?;
    if access_key != config.access_key {
        return Err(S3Error::new(
            S3ErrorCode::InvalidAccessKeyId,
            "The AWS Access Key Id you provided does not exist in our records",
        ));
    }

    // Check expiration
    if params.is_expired() {
        tracing::warn!("Pre-signed URL has expired");
        return Err(S3Error::new(
            S3ErrorCode::AccessDenied,
            "Request has expired",
        ));
    }

    // Determine payload hash
    // For GET/HEAD/DELETE requests, pre-signed URLs use UNSIGNED-PAYLOAD
    // For PUT, check x-amz-content-sha256 header or use UNSIGNED-PAYLOAD
    let payload_hash = if method == Method::GET || method == Method::HEAD || method == Method::DELETE
    {
        "UNSIGNED-PAYLOAD".to_string()
    } else {
        headers
            .iter()
            .find(|(k, _)| k == "x-amz-content-sha256")
            .map(|(_, v)| v.clone())
            .unwrap_or_else(|| "UNSIGNED-PAYLOAD".to_string())
    };

    // Create verifier with configured credentials
    let credentials = Credentials::new(&config.access_key, &config.secret_key);
    let verifier = SigV4Verifier::new(credentials, &config.region);

    // Verify the signature
    verifier.verify_presigned(method.as_str(), path, query, headers, &payload_hash, &params)
}

/// Extract the access key ID from the Authorization header
fn extract_access_key_id(auth_header: &str) -> crate::types::error::S3Result<String> {
    // Format: AWS4-HMAC-SHA256 Credential=AKID/date/region/s3/aws4_request, ...
    let header = auth_header.strip_prefix("AWS4-HMAC-SHA256 ").ok_or_else(|| {
        S3Error::new(S3ErrorCode::InvalidSecurity, "Invalid authorization header")
    })?;

    for part in header.split(", ") {
        if let Some(credential) = part.strip_prefix("Credential=") {
            // Credential format: AKID/date/region/service/aws4_request
            if let Some(akid) = credential.split('/').next() {
                return Ok(akid.to_string());
            }
        }
    }

    Err(S3Error::new(
        S3ErrorCode::InvalidSecurity,
        "Missing Credential in Authorization header",
    ))
}

/// Calculate SHA256 hash of data and return as hex string
fn calculate_sha256_hex(data: &[u8]) -> String {
    use ring::digest::{digest, SHA256};
    let hash = digest(&SHA256, data);
    hex::encode(hash.as_ref())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_path_empty() {
        assert_eq!(parse_path("/"), (None, None));
        assert_eq!(parse_path(""), (None, None));
    }

    #[test]
    fn test_parse_path_bucket_only() {
        assert_eq!(parse_path("/my-bucket"), (Some("my-bucket"), None));
        assert_eq!(parse_path("/my-bucket/"), (Some("my-bucket"), None));
    }

    #[test]
    fn test_parse_path_bucket_and_key() {
        assert_eq!(
            parse_path("/my-bucket/my-key"),
            (Some("my-bucket"), Some("my-key"))
        );
        assert_eq!(
            parse_path("/my-bucket/path/to/key"),
            (Some("my-bucket"), Some("path/to/key"))
        );
    }

    #[test]
    fn test_parse_path_special_characters() {
        assert_eq!(
            parse_path("/bucket/key%20with%20spaces"),
            (Some("bucket"), Some("key%20with%20spaces"))
        );
    }

    #[test]
    fn test_extract_bucket_from_host_virtual_hosted() {
        assert_eq!(
            extract_bucket_from_host("mybucket.s3.local", "s3.local"),
            Some("mybucket")
        );
        assert_eq!(
            extract_bucket_from_host("mybucket.s3.local:9000", "s3.local"),
            Some("mybucket")
        );
        assert_eq!(
            extract_bucket_from_host("my-bucket.s3.local:9000", "s3.local"),
            Some("my-bucket")
        );
    }

    #[test]
    fn test_extract_bucket_from_host_no_match() {
        // Host is the domain itself (no bucket prefix)
        assert_eq!(extract_bucket_from_host("s3.local", "s3.local"), None);
        assert_eq!(extract_bucket_from_host("s3.local:9000", "s3.local"), None);
        // Host doesn't match domain at all
        assert_eq!(
            extract_bucket_from_host("localhost:9000", "s3.local"),
            None
        );
        // No domain configured
        assert_eq!(extract_bucket_from_host("mybucket.s3.local", "other.domain"), None);
    }
}

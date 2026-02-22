//! Request routing

use std::collections::HashMap;

/// S3 request context parsed from the incoming request
#[derive(Debug)]
#[allow(dead_code)]
pub struct RequestContext {
    pub bucket: Option<String>,
    pub key: Option<String>,
    pub query_params: HashMap<String, String>,
    pub operation: S3Operation,
}

/// S3 API operations
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum S3Operation {
    // Service operations
    ListBuckets,

    // Bucket operations
    CreateBucket,
    DeleteBucket,
    HeadBucket,
    GetBucketLocation,
    ListObjectsV2,
    ListObjects,
    GetBucketAcl,
    PutBucketAcl,
    GetBucketCors,
    PutBucketCors,
    DeleteBucketCors,
    GetBucketVersioning,
    PutBucketVersioning,
    GetBucketTagging,
    PutBucketTagging,
    DeleteBucketTagging,
    GetBucketPolicy,
    PutBucketPolicy,
    DeleteBucketPolicy,
    GetBucketPolicyStatus,
    GetBucketLifecycle,
    PutBucketLifecycle,
    DeleteBucketLifecycle,
    GetPublicAccessBlock,
    PutPublicAccessBlock,
    DeletePublicAccessBlock,
    GetBucketWebsite,
    PutBucketWebsite,
    DeleteBucketWebsite,
    GetBucketOwnershipControls,
    PutBucketOwnershipControls,
    DeleteBucketOwnershipControls,
    ListMultipartUploads,

    // Object operations
    PutObject,
    GetObject,
    HeadObject,
    DeleteObject,
    DeleteObjects,
    CopyObject,
    RenameObject,
    GetObjectAcl,
    PutObjectAcl,
    GetObjectTagging,
    PutObjectTagging,
    DeleteObjectTagging,

    // Multipart upload operations
    CreateMultipartUpload,
    UploadPart,
    CompleteMultipartUpload,
    AbortMultipartUpload,
    ListParts,

    // Unknown/unsupported
    Unknown,
}

/// Router for determining the S3 operation from a request
pub struct Router;

impl Router {
    /// Determine the S3 operation from request method, path, and query parameters
    pub fn route(
        method: &str,
        bucket: Option<&str>,
        key: Option<&str>,
        query_params: &HashMap<String, String>,
    ) -> S3Operation {
        match (method, bucket, key) {
            // Service level
            ("GET", None, None) => S3Operation::ListBuckets,

            // Bucket level with query parameters
            ("GET", Some(_), None) => {
                if query_params.contains_key("location") {
                    S3Operation::GetBucketLocation
                } else if query_params.contains_key("acl") {
                    S3Operation::GetBucketAcl
                } else if query_params.contains_key("cors") {
                    S3Operation::GetBucketCors
                } else if query_params.contains_key("versioning") {
                    S3Operation::GetBucketVersioning
                } else if query_params.contains_key("tagging") {
                    S3Operation::GetBucketTagging
                } else if query_params.contains_key("policyStatus") {
                    S3Operation::GetBucketPolicyStatus
                } else if query_params.contains_key("policy") {
                    S3Operation::GetBucketPolicy
                } else if query_params.contains_key("lifecycle") {
                    S3Operation::GetBucketLifecycle
                } else if query_params.contains_key("publicAccessBlock") {
                    S3Operation::GetPublicAccessBlock
                } else if query_params.contains_key("website") {
                    S3Operation::GetBucketWebsite
                } else if query_params.contains_key("ownershipControls") {
                    S3Operation::GetBucketOwnershipControls
                } else if query_params.contains_key("uploads") {
                    S3Operation::ListMultipartUploads
                } else if query_params.contains_key("list-type") {
                    S3Operation::ListObjectsV2
                } else {
                    S3Operation::ListObjects
                }
            }
            ("PUT", Some(_), None) => {
                if query_params.contains_key("acl") {
                    S3Operation::PutBucketAcl
                } else if query_params.contains_key("cors") {
                    S3Operation::PutBucketCors
                } else if query_params.contains_key("versioning") {
                    S3Operation::PutBucketVersioning
                } else if query_params.contains_key("tagging") {
                    S3Operation::PutBucketTagging
                } else if query_params.contains_key("policy") {
                    S3Operation::PutBucketPolicy
                } else if query_params.contains_key("lifecycle") {
                    S3Operation::PutBucketLifecycle
                } else if query_params.contains_key("publicAccessBlock") {
                    S3Operation::PutPublicAccessBlock
                } else if query_params.contains_key("website") {
                    S3Operation::PutBucketWebsite
                } else if query_params.contains_key("ownershipControls") {
                    S3Operation::PutBucketOwnershipControls
                } else {
                    S3Operation::CreateBucket
                }
            }
            ("DELETE", Some(_), None) => {
                if query_params.contains_key("cors") {
                    S3Operation::DeleteBucketCors
                } else if query_params.contains_key("tagging") {
                    S3Operation::DeleteBucketTagging
                } else if query_params.contains_key("policy") {
                    S3Operation::DeleteBucketPolicy
                } else if query_params.contains_key("lifecycle") {
                    S3Operation::DeleteBucketLifecycle
                } else if query_params.contains_key("publicAccessBlock") {
                    S3Operation::DeletePublicAccessBlock
                } else if query_params.contains_key("website") {
                    S3Operation::DeleteBucketWebsite
                } else if query_params.contains_key("ownershipControls") {
                    S3Operation::DeleteBucketOwnershipControls
                } else {
                    S3Operation::DeleteBucket
                }
            }
            ("HEAD", Some(_), None) => S3Operation::HeadBucket,

            // Object level with query parameters
            ("GET", Some(_), Some(_)) => {
                if query_params.contains_key("acl") {
                    S3Operation::GetObjectAcl
                } else if query_params.contains_key("tagging") {
                    S3Operation::GetObjectTagging
                } else {
                    S3Operation::GetObject
                }
            }
            ("PUT", Some(_), Some(_)) => {
                if query_params.contains_key("acl") {
                    S3Operation::PutObjectAcl
                } else if query_params.contains_key("tagging") {
                    S3Operation::PutObjectTagging
                } else if query_params.contains_key("partNumber") && query_params.contains_key("uploadId") {
                    S3Operation::UploadPart
                } else if query_params.contains_key("renameObject") {
                    S3Operation::RenameObject
                } else {
                    S3Operation::PutObject
                }
            }
            ("DELETE", Some(_), Some(_)) => {
                if query_params.contains_key("tagging") {
                    S3Operation::DeleteObjectTagging
                } else if query_params.contains_key("uploadId") {
                    S3Operation::AbortMultipartUpload
                } else {
                    S3Operation::DeleteObject
                }
            }
            ("HEAD", Some(_), Some(_)) => S3Operation::HeadObject,
            ("POST", Some(_), Some(_)) => {
                if query_params.contains_key("uploads") {
                    S3Operation::CreateMultipartUpload
                } else if query_params.contains_key("uploadId") {
                    S3Operation::CompleteMultipartUpload
                } else {
                    S3Operation::Unknown
                }
            }
            ("POST", Some(_), None) => {
                if query_params.contains_key("delete") {
                    S3Operation::DeleteObjects
                } else {
                    S3Operation::Unknown
                }
            }

            _ => S3Operation::Unknown,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_params() -> HashMap<String, String> {
        HashMap::new()
    }

    fn params(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    #[test]
    fn test_route_list_buckets() {
        assert_eq!(
            Router::route("GET", None, None, &empty_params()),
            S3Operation::ListBuckets
        );
    }

    #[test]
    fn test_route_bucket_operations() {
        assert_eq!(
            Router::route("PUT", Some("bucket"), None, &empty_params()),
            S3Operation::CreateBucket
        );
        assert_eq!(
            Router::route("DELETE", Some("bucket"), None, &empty_params()),
            S3Operation::DeleteBucket
        );
        assert_eq!(
            Router::route("HEAD", Some("bucket"), None, &empty_params()),
            S3Operation::HeadBucket
        );
    }

    #[test]
    fn test_route_list_objects() {
        assert_eq!(
            Router::route("GET", Some("bucket"), None, &empty_params()),
            S3Operation::ListObjects
        );
        assert_eq!(
            Router::route("GET", Some("bucket"), None, &params(&[("list-type", "2")])),
            S3Operation::ListObjectsV2
        );
    }

    #[test]
    fn test_route_bucket_location() {
        assert_eq!(
            Router::route("GET", Some("bucket"), None, &params(&[("location", "")])),
            S3Operation::GetBucketLocation
        );
    }

    #[test]
    fn test_route_bucket_acl() {
        assert_eq!(
            Router::route("GET", Some("bucket"), None, &params(&[("acl", "")])),
            S3Operation::GetBucketAcl
        );
        assert_eq!(
            Router::route("PUT", Some("bucket"), None, &params(&[("acl", "")])),
            S3Operation::PutBucketAcl
        );
    }

    #[test]
    fn test_route_object_operations() {
        assert_eq!(
            Router::route("PUT", Some("bucket"), Some("key"), &empty_params()),
            S3Operation::PutObject
        );
        assert_eq!(
            Router::route("GET", Some("bucket"), Some("key"), &empty_params()),
            S3Operation::GetObject
        );
        assert_eq!(
            Router::route("HEAD", Some("bucket"), Some("key"), &empty_params()),
            S3Operation::HeadObject
        );
        assert_eq!(
            Router::route("DELETE", Some("bucket"), Some("key"), &empty_params()),
            S3Operation::DeleteObject
        );
    }

    #[test]
    fn test_route_multipart_operations() {
        assert_eq!(
            Router::route("POST", Some("bucket"), Some("key"), &params(&[("uploads", "")])),
            S3Operation::CreateMultipartUpload
        );
        assert_eq!(
            Router::route(
                "PUT",
                Some("bucket"),
                Some("key"),
                &params(&[("partNumber", "1"), ("uploadId", "abc")])
            ),
            S3Operation::UploadPart
        );
        assert_eq!(
            Router::route(
                "POST",
                Some("bucket"),
                Some("key"),
                &params(&[("uploadId", "abc")])
            ),
            S3Operation::CompleteMultipartUpload
        );
        assert_eq!(
            Router::route(
                "DELETE",
                Some("bucket"),
                Some("key"),
                &params(&[("uploadId", "abc")])
            ),
            S3Operation::AbortMultipartUpload
        );
    }

    #[test]
    fn test_route_delete_objects() {
        assert_eq!(
            Router::route("POST", Some("bucket"), None, &params(&[("delete", "")])),
            S3Operation::DeleteObjects
        );
    }

    #[test]
    fn test_route_unknown() {
        assert_eq!(
            Router::route("PATCH", Some("bucket"), Some("key"), &empty_params()),
            S3Operation::Unknown
        );
    }

    #[test]
    fn test_route_bucket_website() {
        assert_eq!(
            Router::route("GET", Some("bucket"), None, &params(&[("website", "")])),
            S3Operation::GetBucketWebsite
        );
        assert_eq!(
            Router::route("PUT", Some("bucket"), None, &params(&[("website", "")])),
            S3Operation::PutBucketWebsite
        );
        assert_eq!(
            Router::route("DELETE", Some("bucket"), None, &params(&[("website", "")])),
            S3Operation::DeleteBucketWebsite
        );
    }

    #[test]
    fn test_route_rename_object() {
        assert_eq!(
            Router::route("PUT", Some("bucket"), Some("new-key"), &params(&[("renameObject", "")])),
            S3Operation::RenameObject
        );
    }
}

//! S3 XML response types

use serde::Serialize;

use super::bucket::Bucket;
use super::object::Object;

/// Response for ListBuckets operation
#[derive(Debug, Serialize)]
#[serde(rename = "ListAllMyBucketsResult")]
pub struct ListBucketsResponse {
    #[serde(rename = "Owner")]
    pub owner: Owner,
    #[serde(rename = "Buckets")]
    pub buckets: BucketList,
}

#[derive(Debug, Serialize)]
pub struct BucketList {
    #[serde(rename = "Bucket", default)]
    pub buckets: Vec<BucketInfo>,
}

#[derive(Debug, Serialize)]
pub struct BucketInfo {
    #[serde(rename = "Name")]
    pub name: String,
    #[serde(rename = "CreationDate")]
    pub creation_date: String,
}

impl From<&Bucket> for BucketInfo {
    fn from(bucket: &Bucket) -> Self {
        Self {
            name: bucket.name.clone(),
            creation_date: bucket.creation_date.to_rfc3339(),
        }
    }
}

/// Owner information
#[derive(Debug, Serialize, Clone)]
pub struct Owner {
    #[serde(rename = "ID")]
    pub id: String,
    #[serde(rename = "DisplayName")]
    pub display_name: String,
}

impl Default for Owner {
    fn default() -> Self {
        Self {
            id: "local-owner-id".to_string(),
            display_name: "Local Owner".to_string(),
        }
    }
}

/// Response for ListObjectsV2 operation
#[derive(Debug, Serialize)]
#[serde(rename = "ListBucketResult")]
pub struct ListObjectsV2Response {
    #[serde(rename = "Name")]
    pub name: String,
    #[serde(rename = "Prefix")]
    pub prefix: String,
    #[serde(rename = "KeyCount")]
    pub key_count: i32,
    #[serde(rename = "MaxKeys")]
    pub max_keys: i32,
    #[serde(rename = "IsTruncated")]
    pub is_truncated: bool,
    #[serde(rename = "Contents", skip_serializing_if = "Vec::is_empty")]
    pub contents: Vec<ObjectInfo>,
    #[serde(rename = "CommonPrefixes", skip_serializing_if = "Vec::is_empty")]
    pub common_prefixes: Vec<CommonPrefix>,
    #[serde(rename = "ContinuationToken", skip_serializing_if = "Option::is_none")]
    pub continuation_token: Option<String>,
    #[serde(rename = "NextContinuationToken", skip_serializing_if = "Option::is_none")]
    pub next_continuation_token: Option<String>,
    #[serde(rename = "StartAfter", skip_serializing_if = "Option::is_none")]
    pub start_after: Option<String>,
    #[serde(rename = "Delimiter", skip_serializing_if = "Option::is_none")]
    pub delimiter: Option<String>,
    #[serde(rename = "EncodingType", skip_serializing_if = "Option::is_none")]
    pub encoding_type: Option<String>,
}

/// Object info in list responses
#[derive(Debug, Serialize)]
pub struct ObjectInfo {
    #[serde(rename = "Key")]
    pub key: String,
    #[serde(rename = "LastModified")]
    pub last_modified: String,
    #[serde(rename = "ETag")]
    pub etag: String,
    #[serde(rename = "Size")]
    pub size: u64,
    #[serde(rename = "StorageClass")]
    pub storage_class: String,
    #[serde(rename = "Owner", skip_serializing_if = "Option::is_none")]
    pub owner: Option<Owner>,
}

impl From<&Object> for ObjectInfo {
    fn from(obj: &Object) -> Self {
        Self {
            key: obj.key.clone(),
            last_modified: obj.last_modified.to_rfc3339(),
            etag: obj.etag.clone(),
            size: obj.size,
            storage_class: obj.storage_class.to_string(),
            owner: None,
        }
    }
}

/// Common prefix in list responses (for delimiter-based listing)
#[derive(Debug, Serialize)]
pub struct CommonPrefix {
    #[serde(rename = "Prefix")]
    pub prefix: String,
}

/// Response for InitiateMultipartUpload
#[derive(Debug, Serialize)]
#[serde(rename = "InitiateMultipartUploadResult")]
pub struct InitiateMultipartUploadResponse {
    #[serde(rename = "Bucket")]
    pub bucket: String,
    #[serde(rename = "Key")]
    pub key: String,
    #[serde(rename = "UploadId")]
    pub upload_id: String,
}

/// Response for CompleteMultipartUpload
#[derive(Debug, Serialize)]
#[serde(rename = "CompleteMultipartUploadResult")]
pub struct CompleteMultipartUploadResponse {
    #[serde(rename = "Location")]
    pub location: String,
    #[serde(rename = "Bucket")]
    pub bucket: String,
    #[serde(rename = "Key")]
    pub key: String,
    #[serde(rename = "ETag")]
    pub etag: String,
}

/// Response for ListParts
#[derive(Debug, Serialize)]
#[serde(rename = "ListPartsResult")]
pub struct ListPartsResponse {
    #[serde(rename = "Bucket")]
    pub bucket: String,
    #[serde(rename = "Key")]
    pub key: String,
    #[serde(rename = "UploadId")]
    pub upload_id: String,
    #[serde(rename = "PartNumberMarker")]
    pub part_number_marker: i32,
    #[serde(rename = "NextPartNumberMarker")]
    pub next_part_number_marker: i32,
    #[serde(rename = "MaxParts")]
    pub max_parts: i32,
    #[serde(rename = "IsTruncated")]
    pub is_truncated: bool,
    #[serde(rename = "Part")]
    pub parts: Vec<PartInfo>,
    #[serde(rename = "Initiator")]
    pub initiator: Owner,
    #[serde(rename = "Owner")]
    pub owner: Owner,
    #[serde(rename = "StorageClass")]
    pub storage_class: String,
}

/// Part info in ListParts response
#[derive(Debug, Serialize)]
pub struct PartInfo {
    #[serde(rename = "PartNumber")]
    pub part_number: i32,
    #[serde(rename = "LastModified")]
    pub last_modified: String,
    #[serde(rename = "ETag")]
    pub etag: String,
    #[serde(rename = "Size")]
    pub size: u64,
}

/// Response for ListMultipartUploads
#[derive(Debug, Serialize)]
#[serde(rename = "ListMultipartUploadsResult")]
pub struct ListMultipartUploadsResponse {
    #[serde(rename = "Bucket")]
    pub bucket: String,
    #[serde(rename = "KeyMarker")]
    pub key_marker: String,
    #[serde(rename = "UploadIdMarker")]
    pub upload_id_marker: String,
    #[serde(rename = "NextKeyMarker")]
    pub next_key_marker: String,
    #[serde(rename = "NextUploadIdMarker")]
    pub next_upload_id_marker: String,
    #[serde(rename = "MaxUploads")]
    pub max_uploads: i32,
    #[serde(rename = "IsTruncated")]
    pub is_truncated: bool,
    #[serde(rename = "Upload", skip_serializing_if = "Vec::is_empty")]
    pub uploads: Vec<UploadInfo>,
    #[serde(rename = "Prefix", skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    #[serde(rename = "Delimiter", skip_serializing_if = "Option::is_none")]
    pub delimiter: Option<String>,
    #[serde(rename = "CommonPrefixes", skip_serializing_if = "Vec::is_empty")]
    pub common_prefixes: Vec<CommonPrefix>,
}

/// Upload info in ListMultipartUploads response
#[derive(Debug, Serialize)]
pub struct UploadInfo {
    #[serde(rename = "Key")]
    pub key: String,
    #[serde(rename = "UploadId")]
    pub upload_id: String,
    #[serde(rename = "Initiator")]
    pub initiator: Owner,
    #[serde(rename = "Owner")]
    pub owner: Owner,
    #[serde(rename = "StorageClass")]
    pub storage_class: String,
    #[serde(rename = "Initiated")]
    pub initiated: String,
}

/// Response for DeleteObjects (batch delete)
#[derive(Debug, Serialize)]
#[serde(rename = "DeleteResult")]
pub struct DeleteObjectsResponse {
    #[serde(rename = "Deleted", skip_serializing_if = "Vec::is_empty")]
    pub deleted: Vec<DeletedObject>,
    #[serde(rename = "Error", skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<DeleteError>,
}

#[derive(Debug, Serialize)]
pub struct DeletedObject {
    #[serde(rename = "Key")]
    pub key: String,
    #[serde(rename = "VersionId", skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
    #[serde(rename = "DeleteMarker", skip_serializing_if = "Option::is_none")]
    pub delete_marker: Option<bool>,
    #[serde(rename = "DeleteMarkerVersionId", skip_serializing_if = "Option::is_none")]
    pub delete_marker_version_id: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct DeleteError {
    #[serde(rename = "Key")]
    pub key: String,
    #[serde(rename = "VersionId", skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
    #[serde(rename = "Code")]
    pub code: String,
    #[serde(rename = "Message")]
    pub message: String,
}

/// Response for CopyObject
#[derive(Debug, Serialize)]
#[serde(rename = "CopyObjectResult")]
pub struct CopyObjectResponse {
    #[serde(rename = "LastModified")]
    pub last_modified: String,
    #[serde(rename = "ETag")]
    pub etag: String,
}

/// Response for GetBucketLocation
#[derive(Debug, Serialize)]
#[serde(rename = "LocationConstraint")]
pub struct GetBucketLocationResponse {
    #[serde(rename = "$value", skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
}

/// Response for GetBucketVersioning
#[derive(Debug, Serialize)]
#[serde(rename = "VersioningConfiguration")]
pub struct GetBucketVersioningResponse {
    #[serde(rename = "Status", skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(rename = "MFADelete", skip_serializing_if = "Option::is_none")]
    pub mfa_delete: Option<String>,
}

/// Response for GetBucketTagging
#[derive(Debug, Serialize)]
#[serde(rename = "Tagging")]
pub struct GetBucketTaggingResponse {
    #[serde(rename = "TagSet")]
    pub tag_set: TagSetXml,
}

/// Response for GetObjectTagging (same format as bucket tagging)
pub type GetObjectTaggingResponse = GetBucketTaggingResponse;

/// XML representation of a tag set
#[derive(Debug, Serialize)]
pub struct TagSetXml {
    #[serde(rename = "Tag")]
    pub tags: Vec<TagXml>,
}

/// XML representation of a single tag
#[derive(Debug, Serialize)]
pub struct TagXml {
    #[serde(rename = "Key")]
    pub key: String,
    #[serde(rename = "Value")]
    pub value: String,
}

impl From<&super::bucket::Tag> for TagXml {
    fn from(tag: &super::bucket::Tag) -> Self {
        Self {
            key: tag.key.clone(),
            value: tag.value.clone(),
        }
    }
}

impl From<&super::bucket::TagSet> for TagSetXml {
    fn from(tag_set: &super::bucket::TagSet) -> Self {
        Self {
            tags: tag_set.tags.iter().map(TagXml::from).collect(),
        }
    }
}

/// Response for GetBucketCors
#[derive(Debug, Serialize)]
#[serde(rename = "CORSConfiguration")]
pub struct GetBucketCorsResponse {
    #[serde(rename = "CORSRule")]
    pub cors_rules: Vec<CorsRuleXml>,
}

/// XML representation of a CORS rule
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct CorsRuleXml {
    #[serde(rename = "ID", skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(rename = "AllowedOrigin")]
    pub allowed_origins: Vec<String>,
    #[serde(rename = "AllowedMethod")]
    pub allowed_methods: Vec<String>,
    #[serde(rename = "AllowedHeader", skip_serializing_if = "Vec::is_empty", default)]
    pub allowed_headers: Vec<String>,
    #[serde(rename = "ExposeHeader", skip_serializing_if = "Vec::is_empty", default)]
    pub expose_headers: Vec<String>,
    #[serde(rename = "MaxAgeSeconds", skip_serializing_if = "Option::is_none")]
    pub max_age_seconds: Option<i32>,
}

impl CorsRuleXml {
    pub fn new() -> Self {
        Self {
            id: None,
            allowed_origins: Vec::new(),
            allowed_methods: Vec::new(),
            allowed_headers: Vec::new(),
            expose_headers: Vec::new(),
            max_age_seconds: None,
        }
    }
}

impl Default for CorsRuleXml {
    fn default() -> Self {
        Self::new()
    }
}

/// CORS configuration for storage
#[derive(Debug, Clone, Serialize, serde::Deserialize, Default)]
pub struct CorsConfiguration {
    pub rules: Vec<CorsRuleXml>,
}

impl CorsConfiguration {
    pub fn new() -> Self {
        Self { rules: Vec::new() }
    }

    pub fn with_rule(mut self, rule: CorsRuleXml) -> Self {
        self.rules.push(rule);
        self
    }
}

/// Response for GetBucketAcl / GetObjectAcl
#[derive(Debug, Serialize)]
#[serde(rename = "AccessControlPolicy")]
pub struct GetAclResponse {
    #[serde(rename = "Owner")]
    pub owner: OwnerXml,
    #[serde(rename = "AccessControlList")]
    pub access_control_list: AccessControlListXml,
}

/// XML representation of Owner
#[derive(Debug, Serialize)]
pub struct OwnerXml {
    #[serde(rename = "ID")]
    pub id: String,
    #[serde(rename = "DisplayName")]
    pub display_name: String,
}

impl From<&super::bucket::Owner> for OwnerXml {
    fn from(owner: &super::bucket::Owner) -> Self {
        Self {
            id: owner.id.clone(),
            display_name: owner.display_name.clone(),
        }
    }
}

/// XML representation of AccessControlList
#[derive(Debug, Serialize)]
pub struct AccessControlListXml {
    #[serde(rename = "Grant")]
    pub grants: Vec<GrantXml>,
}

/// XML representation of a Grant
#[derive(Debug, Serialize)]
pub struct GrantXml {
    #[serde(rename = "Grantee")]
    pub grantee: GranteeXml,
    #[serde(rename = "Permission")]
    pub permission: String,
}

/// XML representation of a Grantee
#[derive(Debug, Serialize)]
pub struct GranteeXml {
    #[serde(rename = "@xmlns:xsi")]
    pub xmlns_xsi: String,
    #[serde(rename = "@xsi:type")]
    pub xsi_type: String,
    #[serde(rename = "ID", skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(rename = "DisplayName", skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(rename = "URI", skip_serializing_if = "Option::is_none")]
    pub uri: Option<String>,
}

impl From<&super::bucket::AccessControlList> for GetAclResponse {
    fn from(acl: &super::bucket::AccessControlList) -> Self {
        Self {
            owner: OwnerXml::from(&acl.owner),
            access_control_list: AccessControlListXml {
                grants: acl.grants.iter().map(GrantXml::from).collect(),
            },
        }
    }
}

impl From<&super::bucket::Grant> for GrantXml {
    fn from(grant: &super::bucket::Grant) -> Self {
        let (xsi_type, id, display_name, uri) = match &grant.grantee {
            super::bucket::Grantee::CanonicalUser { id, display_name } => (
                "CanonicalUser".to_string(),
                Some(id.clone()),
                display_name.clone(),
                None,
            ),
            super::bucket::Grantee::Group { uri } => {
                ("Group".to_string(), None, None, Some(uri.clone()))
            }
        };

        Self {
            grantee: GranteeXml {
                xmlns_xsi: "http://www.w3.org/2001/XMLSchema-instance".to_string(),
                xsi_type,
                id,
                display_name,
                uri,
            },
            permission: grant.permission.as_str().to_string(),
        }
    }
}

// =============================================================================
// Object Versioning Responses
// =============================================================================

/// Response for ListObjectVersions operation
#[derive(Debug, Serialize)]
#[serde(rename = "ListVersionsResult")]
pub struct ListObjectVersionsResponse {
    #[serde(rename = "@xmlns")]
    pub xmlns: String,
    #[serde(rename = "Name")]
    pub name: String,
    #[serde(rename = "Prefix")]
    pub prefix: String,
    #[serde(rename = "KeyMarker")]
    pub key_marker: String,
    #[serde(rename = "VersionIdMarker")]
    pub version_id_marker: String,
    #[serde(rename = "NextKeyMarker", skip_serializing_if = "Option::is_none")]
    pub next_key_marker: Option<String>,
    #[serde(rename = "NextVersionIdMarker", skip_serializing_if = "Option::is_none")]
    pub next_version_id_marker: Option<String>,
    #[serde(rename = "MaxKeys")]
    pub max_keys: i32,
    #[serde(rename = "IsTruncated")]
    pub is_truncated: bool,
    #[serde(rename = "Version", skip_serializing_if = "Vec::is_empty")]
    pub versions: Vec<ObjectVersionXml>,
    #[serde(rename = "DeleteMarker", skip_serializing_if = "Vec::is_empty")]
    pub delete_markers: Vec<DeleteMarkerXml>,
    #[serde(rename = "CommonPrefixes", skip_serializing_if = "Vec::is_empty")]
    pub common_prefixes: Vec<CommonPrefix>,
    #[serde(rename = "Delimiter", skip_serializing_if = "Option::is_none")]
    pub delimiter: Option<String>,
    #[serde(rename = "EncodingType", skip_serializing_if = "Option::is_none")]
    pub encoding_type: Option<String>,
}

/// Object version info in ListObjectVersions response
#[derive(Debug, Serialize)]
pub struct ObjectVersionXml {
    #[serde(rename = "Key")]
    pub key: String,
    #[serde(rename = "VersionId")]
    pub version_id: String,
    #[serde(rename = "IsLatest")]
    pub is_latest: bool,
    #[serde(rename = "LastModified")]
    pub last_modified: String,
    #[serde(rename = "ETag")]
    pub etag: String,
    #[serde(rename = "Size")]
    pub size: u64,
    #[serde(rename = "StorageClass")]
    pub storage_class: String,
    #[serde(rename = "Owner")]
    pub owner: Owner,
}

/// Delete marker info in ListObjectVersions response
#[derive(Debug, Serialize)]
pub struct DeleteMarkerXml {
    #[serde(rename = "Key")]
    pub key: String,
    #[serde(rename = "VersionId")]
    pub version_id: String,
    #[serde(rename = "IsLatest")]
    pub is_latest: bool,
    #[serde(rename = "LastModified")]
    pub last_modified: String,
    #[serde(rename = "Owner")]
    pub owner: Owner,
}

// =============================================================================
// GetObjectAttributes Responses
// =============================================================================

/// Response for GetObjectAttributes operation
#[derive(Debug, Serialize)]
#[serde(rename = "GetObjectAttributesResponse")]
pub struct GetObjectAttributesResponse {
    #[serde(rename = "ETag", skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,
    #[serde(rename = "Checksum", skip_serializing_if = "Option::is_none")]
    pub checksum: Option<ChecksumXml>,
    #[serde(rename = "ObjectParts", skip_serializing_if = "Option::is_none")]
    pub object_parts: Option<ObjectPartsXml>,
    #[serde(rename = "StorageClass", skip_serializing_if = "Option::is_none")]
    pub storage_class: Option<String>,
    #[serde(rename = "ObjectSize", skip_serializing_if = "Option::is_none")]
    pub object_size: Option<u64>,
}

/// Checksum info in GetObjectAttributes response
#[derive(Debug, Serialize)]
pub struct ChecksumXml {
    #[serde(rename = "ChecksumCRC32", skip_serializing_if = "Option::is_none")]
    pub checksum_crc32: Option<String>,
    #[serde(rename = "ChecksumCRC32C", skip_serializing_if = "Option::is_none")]
    pub checksum_crc32c: Option<String>,
    #[serde(rename = "ChecksumSHA1", skip_serializing_if = "Option::is_none")]
    pub checksum_sha1: Option<String>,
    #[serde(rename = "ChecksumSHA256", skip_serializing_if = "Option::is_none")]
    pub checksum_sha256: Option<String>,
}

/// Object parts info in GetObjectAttributes response (for multipart objects)
#[derive(Debug, Serialize)]
pub struct ObjectPartsXml {
    #[serde(rename = "TotalPartsCount", skip_serializing_if = "Option::is_none")]
    pub total_parts_count: Option<i32>,
    #[serde(rename = "PartNumberMarker", skip_serializing_if = "Option::is_none")]
    pub part_number_marker: Option<i32>,
    #[serde(rename = "NextPartNumberMarker", skip_serializing_if = "Option::is_none")]
    pub next_part_number_marker: Option<i32>,
    #[serde(rename = "MaxParts", skip_serializing_if = "Option::is_none")]
    pub max_parts: Option<i32>,
    #[serde(rename = "IsTruncated", skip_serializing_if = "Option::is_none")]
    pub is_truncated: Option<bool>,
    #[serde(rename = "Part", skip_serializing_if = "Vec::is_empty", default)]
    pub parts: Vec<ObjectPartXml>,
}

/// Individual part info in ObjectParts
#[derive(Debug, Serialize)]
pub struct ObjectPartXml {
    #[serde(rename = "PartNumber")]
    pub part_number: i32,
    #[serde(rename = "Size")]
    pub size: u64,
    #[serde(rename = "ChecksumCRC32", skip_serializing_if = "Option::is_none")]
    pub checksum_crc32: Option<String>,
    #[serde(rename = "ChecksumCRC32C", skip_serializing_if = "Option::is_none")]
    pub checksum_crc32c: Option<String>,
    #[serde(rename = "ChecksumSHA1", skip_serializing_if = "Option::is_none")]
    pub checksum_sha1: Option<String>,
    #[serde(rename = "ChecksumSHA256", skip_serializing_if = "Option::is_none")]
    pub checksum_sha256: Option<String>,
}

// =============================================================================
// UploadPartCopy Response
// =============================================================================

/// Response for UploadPartCopy operation
#[derive(Debug, Serialize)]
#[serde(rename = "CopyPartResult")]
pub struct CopyPartResult {
    #[serde(rename = "ETag")]
    pub etag: String,
    #[serde(rename = "LastModified")]
    pub last_modified: String,
    #[serde(rename = "ChecksumCRC32", skip_serializing_if = "Option::is_none")]
    pub checksum_crc32: Option<String>,
    #[serde(rename = "ChecksumCRC32C", skip_serializing_if = "Option::is_none")]
    pub checksum_crc32c: Option<String>,
    #[serde(rename = "ChecksumSHA1", skip_serializing_if = "Option::is_none")]
    pub checksum_sha1: Option<String>,
    #[serde(rename = "ChecksumSHA256", skip_serializing_if = "Option::is_none")]
    pub checksum_sha256: Option<String>,
}

impl CopyPartResult {
    /// Create a simple CopyPartResult with just ETag and LastModified
    pub fn new(etag: impl Into<String>, last_modified: impl Into<String>) -> Self {
        Self {
            etag: etag.into(),
            last_modified: last_modified.into(),
            checksum_crc32: None,
            checksum_crc32c: None,
            checksum_sha1: None,
            checksum_sha256: None,
        }
    }
}

// =============================================================================
// Lifecycle Configuration Response Types
// =============================================================================

/// Response for GetBucketLifecycleConfiguration
#[derive(Debug, Serialize)]
#[serde(rename = "LifecycleConfiguration")]
pub struct GetBucketLifecycleConfigurationResponse {
    #[serde(rename = "@xmlns")]
    pub xmlns: String,
    #[serde(rename = "Rule")]
    pub rules: Vec<LifecycleRuleXml>,
}

impl Default for GetBucketLifecycleConfigurationResponse {
    fn default() -> Self {
        Self {
            xmlns: "http://s3.amazonaws.com/doc/2006-03-01/".to_string(),
            rules: Vec::new(),
        }
    }
}

/// XML representation of a lifecycle rule
#[derive(Debug, Serialize)]
pub struct LifecycleRuleXml {
    #[serde(rename = "ID", skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(rename = "Status")]
    pub status: String,
    #[serde(rename = "Filter", skip_serializing_if = "Option::is_none")]
    pub filter: Option<LifecycleRuleFilterXml>,
    #[serde(rename = "Expiration", skip_serializing_if = "Option::is_none")]
    pub expiration: Option<LifecycleExpirationXml>,
    #[serde(rename = "NoncurrentVersionExpiration", skip_serializing_if = "Option::is_none")]
    pub noncurrent_version_expiration: Option<NoncurrentVersionExpirationXml>,
    #[serde(rename = "Transition", skip_serializing_if = "Option::is_none")]
    pub transitions: Option<Vec<LifecycleTransitionXml>>,
    #[serde(rename = "NoncurrentVersionTransition", skip_serializing_if = "Option::is_none")]
    pub noncurrent_version_transitions: Option<Vec<NoncurrentVersionTransitionXml>>,
    #[serde(rename = "AbortIncompleteMultipartUpload", skip_serializing_if = "Option::is_none")]
    pub abort_incomplete_multipart_upload: Option<AbortIncompleteMultipartUploadXml>,
}

/// XML representation of lifecycle rule filter
#[derive(Debug, Serialize)]
pub struct LifecycleRuleFilterXml {
    #[serde(rename = "Prefix", skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    #[serde(rename = "Tag", skip_serializing_if = "Option::is_none")]
    pub tag: Option<TagXml>,
    #[serde(rename = "ObjectSizeGreaterThan", skip_serializing_if = "Option::is_none")]
    pub object_size_greater_than: Option<i64>,
    #[serde(rename = "ObjectSizeLessThan", skip_serializing_if = "Option::is_none")]
    pub object_size_less_than: Option<i64>,
    #[serde(rename = "And", skip_serializing_if = "Option::is_none")]
    pub and: Option<LifecycleRuleAndOperatorXml>,
}

/// XML representation of lifecycle And operator
#[derive(Debug, Serialize)]
pub struct LifecycleRuleAndOperatorXml {
    #[serde(rename = "Prefix", skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    #[serde(rename = "Tag", skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<TagXml>>,
    #[serde(rename = "ObjectSizeGreaterThan", skip_serializing_if = "Option::is_none")]
    pub object_size_greater_than: Option<i64>,
    #[serde(rename = "ObjectSizeLessThan", skip_serializing_if = "Option::is_none")]
    pub object_size_less_than: Option<i64>,
}

/// XML representation of expiration
#[derive(Debug, Serialize)]
pub struct LifecycleExpirationXml {
    #[serde(rename = "Days", skip_serializing_if = "Option::is_none")]
    pub days: Option<i32>,
    #[serde(rename = "Date", skip_serializing_if = "Option::is_none")]
    pub date: Option<String>,
    #[serde(rename = "ExpiredObjectDeleteMarker", skip_serializing_if = "Option::is_none")]
    pub expired_object_delete_marker: Option<bool>,
}

/// XML representation of noncurrent version expiration
#[derive(Debug, Serialize)]
pub struct NoncurrentVersionExpirationXml {
    #[serde(rename = "NoncurrentDays", skip_serializing_if = "Option::is_none")]
    pub noncurrent_days: Option<i32>,
    #[serde(rename = "NewerNoncurrentVersions", skip_serializing_if = "Option::is_none")]
    pub newer_noncurrent_versions: Option<i32>,
}

/// XML representation of transition
#[derive(Debug, Serialize)]
pub struct LifecycleTransitionXml {
    #[serde(rename = "Days", skip_serializing_if = "Option::is_none")]
    pub days: Option<i32>,
    #[serde(rename = "Date", skip_serializing_if = "Option::is_none")]
    pub date: Option<String>,
    #[serde(rename = "StorageClass")]
    pub storage_class: String,
}

/// XML representation of noncurrent version transition
#[derive(Debug, Serialize)]
pub struct NoncurrentVersionTransitionXml {
    #[serde(rename = "NoncurrentDays", skip_serializing_if = "Option::is_none")]
    pub noncurrent_days: Option<i32>,
    #[serde(rename = "StorageClass")]
    pub storage_class: String,
    #[serde(rename = "NewerNoncurrentVersions", skip_serializing_if = "Option::is_none")]
    pub newer_noncurrent_versions: Option<i32>,
}

/// XML representation of abort incomplete multipart upload
#[derive(Debug, Serialize)]
pub struct AbortIncompleteMultipartUploadXml {
    #[serde(rename = "DaysAfterInitiation")]
    pub days_after_initiation: i32,
}

// Conversion implementations from storage types to XML types
impl From<&super::bucket::LifecycleConfiguration> for GetBucketLifecycleConfigurationResponse {
    fn from(config: &super::bucket::LifecycleConfiguration) -> Self {
        Self {
            xmlns: "http://s3.amazonaws.com/doc/2006-03-01/".to_string(),
            rules: config.rules.iter().map(LifecycleRuleXml::from).collect(),
        }
    }
}

impl From<&super::bucket::LifecycleRule> for LifecycleRuleXml {
    fn from(rule: &super::bucket::LifecycleRule) -> Self {
        Self {
            id: rule.id.clone(),
            status: rule.status.as_str().to_string(),
            filter: Some(LifecycleRuleFilterXml::from(&rule.filter)),
            expiration: rule.expiration.as_ref().map(LifecycleExpirationXml::from),
            noncurrent_version_expiration: rule
                .noncurrent_version_expiration
                .as_ref()
                .map(NoncurrentVersionExpirationXml::from),
            transitions: rule
                .transitions
                .as_ref()
                .map(|t| t.iter().map(LifecycleTransitionXml::from).collect()),
            noncurrent_version_transitions: rule
                .noncurrent_version_transitions
                .as_ref()
                .map(|t| t.iter().map(NoncurrentVersionTransitionXml::from).collect()),
            abort_incomplete_multipart_upload: rule
                .abort_incomplete_multipart_upload
                .as_ref()
                .map(AbortIncompleteMultipartUploadXml::from),
        }
    }
}

impl From<&super::bucket::LifecycleRuleFilter> for LifecycleRuleFilterXml {
    fn from(filter: &super::bucket::LifecycleRuleFilter) -> Self {
        Self {
            prefix: filter.prefix.clone(),
            tag: filter.tag.as_ref().map(TagXml::from),
            object_size_greater_than: filter.object_size_greater_than,
            object_size_less_than: filter.object_size_less_than,
            and: filter.and.as_ref().map(LifecycleRuleAndOperatorXml::from),
        }
    }
}

impl From<&super::bucket::LifecycleRuleAndOperator> for LifecycleRuleAndOperatorXml {
    fn from(op: &super::bucket::LifecycleRuleAndOperator) -> Self {
        Self {
            prefix: op.prefix.clone(),
            tags: op
                .tags
                .as_ref()
                .map(|tags| tags.iter().map(TagXml::from).collect()),
            object_size_greater_than: op.object_size_greater_than,
            object_size_less_than: op.object_size_less_than,
        }
    }
}

impl From<&super::bucket::LifecycleExpiration> for LifecycleExpirationXml {
    fn from(exp: &super::bucket::LifecycleExpiration) -> Self {
        Self {
            days: exp.days,
            date: exp.date.clone(),
            expired_object_delete_marker: exp.expired_object_delete_marker,
        }
    }
}

impl From<&super::bucket::NoncurrentVersionExpiration> for NoncurrentVersionExpirationXml {
    fn from(exp: &super::bucket::NoncurrentVersionExpiration) -> Self {
        Self {
            noncurrent_days: exp.noncurrent_days,
            newer_noncurrent_versions: exp.newer_noncurrent_versions,
        }
    }
}

impl From<&super::bucket::LifecycleTransition> for LifecycleTransitionXml {
    fn from(trans: &super::bucket::LifecycleTransition) -> Self {
        Self {
            days: trans.days,
            date: trans.date.clone(),
            storage_class: trans.storage_class.clone(),
        }
    }
}

impl From<&super::bucket::NoncurrentVersionTransition> for NoncurrentVersionTransitionXml {
    fn from(trans: &super::bucket::NoncurrentVersionTransition) -> Self {
        Self {
            noncurrent_days: trans.noncurrent_days,
            storage_class: trans.storage_class.clone(),
            newer_noncurrent_versions: trans.newer_noncurrent_versions,
        }
    }
}

impl From<&super::bucket::AbortIncompleteMultipartUpload> for AbortIncompleteMultipartUploadXml {
    fn from(abort: &super::bucket::AbortIncompleteMultipartUpload) -> Self {
        Self {
            days_after_initiation: abort.days_after_initiation,
        }
    }
}

// =============================================================================
// Server-Side Encryption Response Types
// =============================================================================

/// Response for GetBucketEncryption operation
#[derive(Debug, Serialize)]
#[serde(rename = "ServerSideEncryptionConfiguration")]
pub struct GetBucketEncryptionResponse {
    #[serde(rename = "@xmlns")]
    pub xmlns: String,
    #[serde(rename = "Rule")]
    pub rules: Vec<ServerSideEncryptionRuleXml>,
}

impl Default for GetBucketEncryptionResponse {
    fn default() -> Self {
        Self {
            xmlns: "http://s3.amazonaws.com/doc/2006-03-01/".to_string(),
            rules: Vec::new(),
        }
    }
}

/// XML representation of a server-side encryption rule
#[derive(Debug, Serialize)]
pub struct ServerSideEncryptionRuleXml {
    #[serde(
        rename = "ApplyServerSideEncryptionByDefault",
        skip_serializing_if = "Option::is_none"
    )]
    pub apply_server_side_encryption_by_default: Option<ServerSideEncryptionByDefaultXml>,
    #[serde(rename = "BucketKeyEnabled", skip_serializing_if = "Option::is_none")]
    pub bucket_key_enabled: Option<bool>,
}

/// XML representation of default server-side encryption settings
#[derive(Debug, Serialize)]
pub struct ServerSideEncryptionByDefaultXml {
    #[serde(rename = "SSEAlgorithm")]
    pub sse_algorithm: String,
    #[serde(rename = "KMSMasterKeyID", skip_serializing_if = "Option::is_none")]
    pub kms_master_key_id: Option<String>,
}

impl From<&super::bucket::ServerSideEncryptionConfiguration> for GetBucketEncryptionResponse {
    fn from(config: &super::bucket::ServerSideEncryptionConfiguration) -> Self {
        Self {
            xmlns: "http://s3.amazonaws.com/doc/2006-03-01/".to_string(),
            rules: config.rules.iter().map(ServerSideEncryptionRuleXml::from).collect(),
        }
    }
}

impl From<&super::bucket::ServerSideEncryptionRule> for ServerSideEncryptionRuleXml {
    fn from(rule: &super::bucket::ServerSideEncryptionRule) -> Self {
        Self {
            apply_server_side_encryption_by_default: rule
                .apply_server_side_encryption_by_default
                .as_ref()
                .map(ServerSideEncryptionByDefaultXml::from),
            bucket_key_enabled: if rule.bucket_key_enabled {
                Some(true)
            } else {
                None
            },
        }
    }
}

impl From<&super::bucket::ServerSideEncryptionByDefault> for ServerSideEncryptionByDefaultXml {
    fn from(default: &super::bucket::ServerSideEncryptionByDefault) -> Self {
        Self {
            sse_algorithm: default.sse_algorithm.as_str().to_string(),
            kms_master_key_id: default.kms_master_key_id.clone(),
        }
    }
}

// =============================================================================
// Website Configuration Response Types
// =============================================================================

/// Response for GetBucketWebsite operation
#[derive(Debug, Serialize)]
#[serde(rename = "WebsiteConfiguration")]
pub struct GetBucketWebsiteResponse {
    #[serde(rename = "@xmlns")]
    pub xmlns: String,
    #[serde(rename = "IndexDocument", skip_serializing_if = "Option::is_none")]
    pub index_document: Option<IndexDocumentXml>,
    #[serde(rename = "ErrorDocument", skip_serializing_if = "Option::is_none")]
    pub error_document: Option<ErrorDocumentXml>,
    #[serde(rename = "RedirectAllRequestsTo", skip_serializing_if = "Option::is_none")]
    pub redirect_all_requests_to: Option<RedirectAllRequestsToXml>,
    #[serde(rename = "RoutingRules", skip_serializing_if = "Option::is_none")]
    pub routing_rules: Option<RoutingRulesXml>,
}

impl Default for GetBucketWebsiteResponse {
    fn default() -> Self {
        Self {
            xmlns: "http://s3.amazonaws.com/doc/2006-03-01/".to_string(),
            index_document: None,
            error_document: None,
            redirect_all_requests_to: None,
            routing_rules: None,
        }
    }
}

/// XML representation of index document
#[derive(Debug, Serialize)]
pub struct IndexDocumentXml {
    #[serde(rename = "Suffix")]
    pub suffix: String,
}

/// XML representation of error document
#[derive(Debug, Serialize)]
pub struct ErrorDocumentXml {
    #[serde(rename = "Key")]
    pub key: String,
}

/// XML representation of redirect all requests
#[derive(Debug, Serialize)]
pub struct RedirectAllRequestsToXml {
    #[serde(rename = "HostName")]
    pub host_name: String,
    #[serde(rename = "Protocol", skip_serializing_if = "Option::is_none")]
    pub protocol: Option<String>,
}

/// XML representation of routing rules container
#[derive(Debug, Serialize)]
pub struct RoutingRulesXml {
    #[serde(rename = "RoutingRule")]
    pub rules: Vec<RoutingRuleXml>,
}

/// XML representation of a routing rule
#[derive(Debug, Serialize)]
pub struct RoutingRuleXml {
    #[serde(rename = "Condition", skip_serializing_if = "Option::is_none")]
    pub condition: Option<RoutingRuleConditionXml>,
    #[serde(rename = "Redirect")]
    pub redirect: RoutingRuleRedirectXml,
}

/// XML representation of routing rule condition
#[derive(Debug, Serialize)]
pub struct RoutingRuleConditionXml {
    #[serde(rename = "HttpErrorCodeReturnedEquals", skip_serializing_if = "Option::is_none")]
    pub http_error_code_returned_equals: Option<String>,
    #[serde(rename = "KeyPrefixEquals", skip_serializing_if = "Option::is_none")]
    pub key_prefix_equals: Option<String>,
}

/// XML representation of routing rule redirect
#[derive(Debug, Serialize)]
pub struct RoutingRuleRedirectXml {
    #[serde(rename = "HostName", skip_serializing_if = "Option::is_none")]
    pub host_name: Option<String>,
    #[serde(rename = "HttpRedirectCode", skip_serializing_if = "Option::is_none")]
    pub http_redirect_code: Option<String>,
    #[serde(rename = "Protocol", skip_serializing_if = "Option::is_none")]
    pub protocol: Option<String>,
    #[serde(rename = "ReplaceKeyWith", skip_serializing_if = "Option::is_none")]
    pub replace_key_with: Option<String>,
    #[serde(rename = "ReplaceKeyPrefixWith", skip_serializing_if = "Option::is_none")]
    pub replace_key_prefix_with: Option<String>,
}

impl From<&super::bucket::WebsiteConfiguration> for GetBucketWebsiteResponse {
    fn from(config: &super::bucket::WebsiteConfiguration) -> Self {
        Self {
            xmlns: "http://s3.amazonaws.com/doc/2006-03-01/".to_string(),
            index_document: config.index_document.as_ref().map(|d| IndexDocumentXml {
                suffix: d.suffix.clone(),
            }),
            error_document: config.error_document.as_ref().map(|d| ErrorDocumentXml {
                key: d.key.clone(),
            }),
            redirect_all_requests_to: config.redirect_all_requests_to.as_ref().map(|r| {
                RedirectAllRequestsToXml {
                    host_name: r.host_name.clone(),
                    protocol: r.protocol.clone(),
                }
            }),
            routing_rules: if config.routing_rules.is_empty() {
                None
            } else {
                Some(RoutingRulesXml {
                    rules: config.routing_rules.iter().map(RoutingRuleXml::from).collect(),
                })
            },
        }
    }
}

impl From<&super::bucket::RoutingRule> for RoutingRuleXml {
    fn from(rule: &super::bucket::RoutingRule) -> Self {
        Self {
            condition: rule.condition.as_ref().map(|c| RoutingRuleConditionXml {
                http_error_code_returned_equals: c.http_error_code_returned_equals.clone(),
                key_prefix_equals: c.key_prefix_equals.clone(),
            }),
            redirect: RoutingRuleRedirectXml {
                host_name: rule.redirect.host_name.clone(),
                http_redirect_code: rule.redirect.http_redirect_code.clone(),
                protocol: rule.redirect.protocol.clone(),
                replace_key_with: rule.redirect.replace_key_with.clone(),
                replace_key_prefix_with: rule.redirect.replace_key_prefix_with.clone(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_owner_default() {
        let owner = Owner::default();
        assert_eq!(owner.id, "local-owner-id");
        assert_eq!(owner.display_name, "Local Owner");
    }

    #[test]
    fn test_bucket_info_from_bucket() {
        let bucket = Bucket::new("test-bucket", "us-east-1");
        let info = BucketInfo::from(&bucket);

        assert_eq!(info.name, "test-bucket");
        assert!(!info.creation_date.is_empty());
    }

    #[test]
    fn test_object_info_from_object() {
        let obj = Object::new("test-key", 1024, "\"abc123\"");
        let info = ObjectInfo::from(&obj);

        assert_eq!(info.key, "test-key");
        assert_eq!(info.size, 1024);
        assert_eq!(info.etag, "\"abc123\"");
        assert_eq!(info.storage_class, "STANDARD");
    }

    #[test]
    fn test_list_buckets_response() {
        let response = ListBucketsResponse {
            owner: Owner::default(),
            buckets: BucketList {
                buckets: vec![
                    BucketInfo {
                        name: "bucket1".to_string(),
                        creation_date: "2024-01-01T00:00:00Z".to_string(),
                    },
                ],
            },
        };

        assert_eq!(response.buckets.buckets.len(), 1);
        assert_eq!(response.buckets.buckets[0].name, "bucket1");
    }

    #[test]
    fn test_list_objects_v2_response() {
        let response = ListObjectsV2Response {
            name: "test-bucket".to_string(),
            prefix: "".to_string(),
            key_count: 1,
            max_keys: 1000,
            is_truncated: false,
            contents: vec![],
            common_prefixes: vec![],
            continuation_token: None,
            next_continuation_token: None,
            start_after: None,
            delimiter: None,
            encoding_type: None,
        };

        assert_eq!(response.name, "test-bucket");
        assert!(!response.is_truncated);
    }

    #[test]
    fn test_delete_objects_response() {
        let response = DeleteObjectsResponse {
            deleted: vec![DeletedObject {
                key: "deleted-key".to_string(),
                version_id: None,
                delete_marker: None,
                delete_marker_version_id: None,
            }],
            errors: vec![DeleteError {
                key: "error-key".to_string(),
                version_id: None,
                code: "NoSuchKey".to_string(),
                message: "The specified key does not exist".to_string(),
            }],
        };

        assert_eq!(response.deleted.len(), 1);
        assert_eq!(response.errors.len(), 1);
    }
}

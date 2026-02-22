//! Bucket type definitions

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::types::response::CorsConfiguration;

/// Represents an S3 bucket
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bucket {
    /// Bucket name
    pub name: String,
    /// Creation timestamp
    pub creation_date: DateTime<Utc>,
    /// Region the bucket is in
    pub region: String,
    /// Versioning status
    pub versioning: VersioningStatus,
    /// Bucket tags
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tags: Option<TagSet>,
    /// CORS configuration
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cors: Option<CorsConfiguration>,
    /// Bucket policy (JSON string)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<String>,
    /// Access Control List
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acl: Option<AccessControlList>,
    /// Lifecycle configuration
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lifecycle: Option<LifecycleConfiguration>,
    /// Whether Object Lock is enabled for this bucket
    #[serde(default)]
    pub object_lock_enabled: bool,
    /// Object Lock configuration
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_lock_configuration: Option<ObjectLockConfiguration>,
    /// Server-Side Encryption configuration
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encryption: Option<ServerSideEncryptionConfiguration>,
    /// Public Access Block configuration
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub public_access_block: Option<PublicAccessBlockConfiguration>,
    /// Website configuration
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub website: Option<WebsiteConfiguration>,
    /// Ownership controls configuration
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ownership_controls: Option<OwnershipControls>,
    /// Logging configuration
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub logging: Option<LoggingConfiguration>,
    /// Notification configuration
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notification: Option<NotificationConfiguration>,
    /// Replication configuration
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replication: Option<ReplicationConfiguration>,
    /// Request payment configuration
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_payment: Option<RequestPaymentConfiguration>,
}

impl Bucket {
    /// Create a new bucket with defaults
    pub fn new(name: impl Into<String>, region: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            creation_date: Utc::now(),
            region: region.into(),
            versioning: VersioningStatus::Disabled,
            tags: None,
            cors: None,
            policy: None,
            acl: Some(AccessControlList::default()),
            lifecycle: None,
            object_lock_enabled: false,
            object_lock_configuration: None,
            encryption: None,
            public_access_block: None,
            website: None,
            ownership_controls: None,
            logging: None,
            notification: None,
            replication: None,
            request_payment: None,
        }
    }

    /// Create a new bucket with Object Lock enabled
    /// Note: Object Lock automatically enables versioning
    pub fn new_with_object_lock(name: impl Into<String>, region: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            creation_date: Utc::now(),
            region: region.into(),
            versioning: VersioningStatus::Enabled, // Object Lock requires versioning
            tags: None,
            cors: None,
            policy: None,
            acl: Some(AccessControlList::default()),
            lifecycle: None,
            object_lock_enabled: true,
            object_lock_configuration: Some(ObjectLockConfiguration::default()),
            encryption: None,
            public_access_block: None,
            website: None,
            ownership_controls: None,
            logging: None,
            notification: None,
            replication: None,
            request_payment: None,
        }
    }

    /// Create a new bucket with a specific canned ACL
    pub fn new_with_acl(
        name: impl Into<String>,
        region: impl Into<String>,
        canned_acl: CannedAcl,
    ) -> Self {
        Self {
            name: name.into(),
            creation_date: Utc::now(),
            region: region.into(),
            versioning: VersioningStatus::Disabled,
            tags: None,
            cors: None,
            policy: None,
            acl: Some(AccessControlList::from_canned(canned_acl)),
            lifecycle: None,
            object_lock_enabled: false,
            object_lock_configuration: None,
            encryption: None,
            public_access_block: None,
            website: None,
            ownership_controls: None,
            logging: None,
            notification: None,
            replication: None,
            request_payment: None,
        }
    }

    /// Validate bucket name according to S3 naming rules
    pub fn validate_name(name: &str) -> Result<(), BucketNameError> {
        // Length check: 3-63 characters
        if name.len() < 3 {
            return Err(BucketNameError::TooShort);
        }
        if name.len() > 63 {
            return Err(BucketNameError::TooLong);
        }

        // Must start with lowercase letter or number
        let first_char = name.chars().next().unwrap();
        if !first_char.is_ascii_lowercase() && !first_char.is_ascii_digit() {
            return Err(BucketNameError::InvalidStartCharacter);
        }

        // Must end with lowercase letter or number
        let last_char = name.chars().last().unwrap();
        if !last_char.is_ascii_lowercase() && !last_char.is_ascii_digit() {
            return Err(BucketNameError::InvalidEndCharacter);
        }

        // Check all characters are valid
        for (i, c) in name.chars().enumerate() {
            if !c.is_ascii_lowercase() && !c.is_ascii_digit() && c != '-' && c != '.' {
                return Err(BucketNameError::InvalidCharacter(c));
            }

            // No consecutive periods
            if c == '.' && i > 0 && name.chars().nth(i - 1) == Some('.') {
                return Err(BucketNameError::ConsecutivePeriods);
            }

            // No period after hyphen or hyphen after period
            if c == '.' && i > 0 && name.chars().nth(i - 1) == Some('-') {
                return Err(BucketNameError::InvalidPeriodHyphenSequence);
            }
            if c == '-' && i > 0 && name.chars().nth(i - 1) == Some('.') {
                return Err(BucketNameError::InvalidPeriodHyphenSequence);
            }
        }

        // Must not be formatted as IP address
        if name.split('.').count() == 4
            && name.split('.').all(|part| part.parse::<u8>().is_ok())
        {
            return Err(BucketNameError::IpAddressFormat);
        }

        Ok(())
    }
}

/// A single tag (key-value pair)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Tag {
    pub key: String,
    pub value: String,
}

impl Tag {
    pub fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

/// A set of tags
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct TagSet {
    pub tags: Vec<Tag>,
}

impl TagSet {
    pub fn new() -> Self {
        Self { tags: Vec::new() }
    }

    pub fn with_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tags.push(Tag::new(key, value));
        self
    }
}

/// Bucket versioning status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum VersioningStatus {
    #[default]
    Disabled,
    Enabled,
    Suspended,
}

impl VersioningStatus {
    pub fn as_str(&self) -> Option<&'static str> {
        match self {
            VersioningStatus::Disabled => None,
            VersioningStatus::Enabled => Some("Enabled"),
            VersioningStatus::Suspended => Some("Suspended"),
        }
    }
}

/// Errors that can occur when validating bucket names
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BucketNameError {
    TooShort,
    TooLong,
    InvalidStartCharacter,
    InvalidEndCharacter,
    InvalidCharacter(char),
    ConsecutivePeriods,
    InvalidPeriodHyphenSequence,
    IpAddressFormat,
}

impl std::fmt::Display for BucketNameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BucketNameError::TooShort => write!(f, "Bucket name must be at least 3 characters"),
            BucketNameError::TooLong => write!(f, "Bucket name must be at most 63 characters"),
            BucketNameError::InvalidStartCharacter => {
                write!(f, "Bucket name must start with a lowercase letter or number")
            }
            BucketNameError::InvalidEndCharacter => {
                write!(f, "Bucket name must end with a lowercase letter or number")
            }
            BucketNameError::InvalidCharacter(c) => {
                write!(f, "Bucket name contains invalid character: {}", c)
            }
            BucketNameError::ConsecutivePeriods => {
                write!(f, "Bucket name cannot contain consecutive periods")
            }
            BucketNameError::InvalidPeriodHyphenSequence => {
                write!(f, "Bucket name cannot contain period adjacent to hyphen")
            }
            BucketNameError::IpAddressFormat => {
                write!(f, "Bucket name cannot be formatted as an IP address")
            }
        }
    }
}

impl std::error::Error for BucketNameError {}

/// Access Control List for a bucket or object
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AccessControlList {
    /// The owner of the resource
    pub owner: Owner,
    /// List of grants
    pub grants: Vec<Grant>,
}

impl AccessControlList {
    /// Create a new ACL with the default owner and FULL_CONTROL grant
    pub fn default_private() -> Self {
        let owner = Owner::default();
        Self {
            grants: vec![Grant {
                grantee: Grantee::CanonicalUser {
                    id: owner.id.clone(),
                    display_name: Some(owner.display_name.clone()),
                },
                permission: Permission::FullControl,
            }],
            owner,
        }
    }

    /// Create an ACL from a canned ACL type
    pub fn from_canned(canned: CannedAcl) -> Self {
        let owner = Owner::default();
        let mut grants = vec![Grant {
            grantee: Grantee::CanonicalUser {
                id: owner.id.clone(),
                display_name: Some(owner.display_name.clone()),
            },
            permission: Permission::FullControl,
        }];

        match canned {
            CannedAcl::Private => {
                // Only owner grant, already added
            }
            CannedAcl::PublicRead => {
                grants.push(Grant {
                    grantee: Grantee::Group {
                        uri: "http://acs.amazonaws.com/groups/global/AllUsers".to_string(),
                    },
                    permission: Permission::Read,
                });
            }
            CannedAcl::PublicReadWrite => {
                grants.push(Grant {
                    grantee: Grantee::Group {
                        uri: "http://acs.amazonaws.com/groups/global/AllUsers".to_string(),
                    },
                    permission: Permission::Read,
                });
                grants.push(Grant {
                    grantee: Grantee::Group {
                        uri: "http://acs.amazonaws.com/groups/global/AllUsers".to_string(),
                    },
                    permission: Permission::Write,
                });
            }
            CannedAcl::AuthenticatedRead => {
                grants.push(Grant {
                    grantee: Grantee::Group {
                        uri: "http://acs.amazonaws.com/groups/global/AuthenticatedUsers"
                            .to_string(),
                    },
                    permission: Permission::Read,
                });
            }
        }

        Self { owner, grants }
    }
}

impl Default for AccessControlList {
    fn default() -> Self {
        Self::default_private()
    }
}

/// Owner of a resource (bucket or object)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Owner {
    pub id: String,
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

/// A grant in an ACL
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Grant {
    pub grantee: Grantee,
    pub permission: Permission,
}

/// The recipient of a grant
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type")]
pub enum Grantee {
    /// A canonical user (by ID)
    CanonicalUser {
        id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        display_name: Option<String>,
    },
    /// A group (by URI)
    Group { uri: String },
}

/// Permission types for ACL grants
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum Permission {
    FullControl,
    Write,
    WriteAcp,
    Read,
    ReadAcp,
}

impl Permission {
    pub fn as_str(&self) -> &'static str {
        match self {
            Permission::FullControl => "FULL_CONTROL",
            Permission::Write => "WRITE",
            Permission::WriteAcp => "WRITE_ACP",
            Permission::Read => "READ",
            Permission::ReadAcp => "READ_ACP",
        }
    }
}

/// Canned ACL types supported by S3
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CannedAcl {
    Private,
    PublicRead,
    PublicReadWrite,
    AuthenticatedRead,
}

impl CannedAcl {
    /// Parse from the x-amz-acl header value
    pub fn from_header(value: &str) -> Option<Self> {
        match value {
            "private" => Some(CannedAcl::Private),
            "public-read" => Some(CannedAcl::PublicRead),
            "public-read-write" => Some(CannedAcl::PublicReadWrite),
            "authenticated-read" => Some(CannedAcl::AuthenticatedRead),
            _ => None,
        }
    }
}

// =============================================================================
// Lifecycle Configuration Types
// =============================================================================

/// Lifecycle configuration for a bucket
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LifecycleConfiguration {
    pub rules: Vec<LifecycleRule>,
}

impl LifecycleConfiguration {
    pub fn new() -> Self {
        Self { rules: Vec::new() }
    }

    pub fn with_rule(mut self, rule: LifecycleRule) -> Self {
        self.rules.push(rule);
        self
    }
}

/// A lifecycle rule
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleRule {
    /// Unique identifier for the rule (max 255 chars)
    pub id: Option<String>,
    /// Whether the rule is enabled or disabled
    pub status: LifecycleRuleStatus,
    /// Filter to identify objects the rule applies to
    #[serde(default)]
    pub filter: LifecycleRuleFilter,
    /// Expiration configuration for current objects
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expiration: Option<LifecycleExpiration>,
    /// Expiration configuration for noncurrent object versions
    #[serde(skip_serializing_if = "Option::is_none")]
    pub noncurrent_version_expiration: Option<NoncurrentVersionExpiration>,
    /// Transition configuration for current objects
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transitions: Option<Vec<LifecycleTransition>>,
    /// Transition configuration for noncurrent versions
    #[serde(skip_serializing_if = "Option::is_none")]
    pub noncurrent_version_transitions: Option<Vec<NoncurrentVersionTransition>>,
    /// Configuration for aborting incomplete multipart uploads
    #[serde(skip_serializing_if = "Option::is_none")]
    pub abort_incomplete_multipart_upload: Option<AbortIncompleteMultipartUpload>,
}

impl LifecycleRule {
    pub fn new(status: LifecycleRuleStatus) -> Self {
        Self {
            id: None,
            status,
            filter: LifecycleRuleFilter::default(),
            expiration: None,
            noncurrent_version_expiration: None,
            transitions: None,
            noncurrent_version_transitions: None,
            abort_incomplete_multipart_upload: None,
        }
    }
}

/// Status of a lifecycle rule
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LifecycleRuleStatus {
    Enabled,
    Disabled,
}

impl LifecycleRuleStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            LifecycleRuleStatus::Enabled => "Enabled",
            LifecycleRuleStatus::Disabled => "Disabled",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "Enabled" => Some(LifecycleRuleStatus::Enabled),
            "Disabled" => Some(LifecycleRuleStatus::Disabled),
            _ => None,
        }
    }
}

/// Filter for identifying objects a lifecycle rule applies to
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LifecycleRuleFilter {
    /// Prefix filter - rule applies to objects with this key prefix
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    /// Tag filter - rule applies to objects with this tag
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tag: Option<Tag>,
    /// Size greater than filter
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_size_greater_than: Option<i64>,
    /// Size less than filter
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_size_less_than: Option<i64>,
    /// And operator for combining filters
    #[serde(skip_serializing_if = "Option::is_none")]
    pub and: Option<LifecycleRuleAndOperator>,
}

/// And operator for combining lifecycle rule filters
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LifecycleRuleAndOperator {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<Tag>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_size_greater_than: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_size_less_than: Option<i64>,
}

/// Expiration configuration for current objects
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleExpiration {
    /// Number of days after creation to expire objects
    #[serde(skip_serializing_if = "Option::is_none")]
    pub days: Option<i32>,
    /// Specific date when objects expire
    #[serde(skip_serializing_if = "Option::is_none")]
    pub date: Option<String>,
    /// Whether to delete expired delete markers
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expired_object_delete_marker: Option<bool>,
}

/// Expiration configuration for noncurrent object versions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoncurrentVersionExpiration {
    /// Number of days after becoming noncurrent to expire
    #[serde(skip_serializing_if = "Option::is_none")]
    pub noncurrent_days: Option<i32>,
    /// Number of newer versions to retain
    #[serde(skip_serializing_if = "Option::is_none")]
    pub newer_noncurrent_versions: Option<i32>,
}

/// Transition configuration for moving objects to different storage classes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleTransition {
    /// Number of days after creation to transition
    #[serde(skip_serializing_if = "Option::is_none")]
    pub days: Option<i32>,
    /// Specific date for transition
    #[serde(skip_serializing_if = "Option::is_none")]
    pub date: Option<String>,
    /// Target storage class
    pub storage_class: String,
}

/// Transition configuration for noncurrent versions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoncurrentVersionTransition {
    /// Number of days after becoming noncurrent to transition
    #[serde(skip_serializing_if = "Option::is_none")]
    pub noncurrent_days: Option<i32>,
    /// Target storage class
    pub storage_class: String,
    /// Number of newer versions to retain
    #[serde(skip_serializing_if = "Option::is_none")]
    pub newer_noncurrent_versions: Option<i32>,
}

/// Configuration for aborting incomplete multipart uploads
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AbortIncompleteMultipartUpload {
    /// Number of days after initiation to abort incomplete uploads
    pub days_after_initiation: i32,
}

// =============================================================================
// Object Lock Configuration Types
// =============================================================================

/// Object Lock configuration for a bucket
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ObjectLockConfiguration {
    /// Whether Object Lock is enabled
    pub object_lock_enabled: ObjectLockEnabled,
    /// Default retention rule
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rule: Option<ObjectLockRule>,
}

impl ObjectLockConfiguration {
    pub fn new() -> Self {
        Self {
            object_lock_enabled: ObjectLockEnabled::Enabled,
            rule: None,
        }
    }

    pub fn with_rule(mut self, rule: ObjectLockRule) -> Self {
        self.rule = Some(rule);
        self
    }
}

/// Whether Object Lock is enabled
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ObjectLockEnabled {
    #[default]
    Enabled,
}

impl ObjectLockEnabled {
    pub fn as_str(&self) -> &'static str {
        match self {
            ObjectLockEnabled::Enabled => "Enabled",
        }
    }
}

/// Object Lock rule containing default retention
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectLockRule {
    /// Default retention settings
    pub default_retention: DefaultRetention,
}

impl ObjectLockRule {
    pub fn new(default_retention: DefaultRetention) -> Self {
        Self { default_retention }
    }
}

/// Default retention settings for Object Lock
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DefaultRetention {
    /// Retention mode (GOVERNANCE or COMPLIANCE)
    pub mode: ObjectLockRetentionMode,
    /// Number of days for retention
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub days: Option<i32>,
    /// Number of years for retention
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub years: Option<i32>,
}

impl DefaultRetention {
    pub fn governance_days(days: i32) -> Self {
        Self {
            mode: ObjectLockRetentionMode::Governance,
            days: Some(days),
            years: None,
        }
    }

    pub fn compliance_days(days: i32) -> Self {
        Self {
            mode: ObjectLockRetentionMode::Compliance,
            days: Some(days),
            years: None,
        }
    }

    pub fn governance_years(years: i32) -> Self {
        Self {
            mode: ObjectLockRetentionMode::Governance,
            days: None,
            years: Some(years),
        }
    }

    pub fn compliance_years(years: i32) -> Self {
        Self {
            mode: ObjectLockRetentionMode::Compliance,
            days: None,
            years: Some(years),
        }
    }
}

/// Object Lock retention mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ObjectLockRetentionMode {
    /// Objects can be deleted by users with s3:BypassGovernanceRetention permission
    Governance,
    /// Objects cannot be deleted by anyone until retention period expires
    Compliance,
}

impl ObjectLockRetentionMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            ObjectLockRetentionMode::Governance => "GOVERNANCE",
            ObjectLockRetentionMode::Compliance => "COMPLIANCE",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "GOVERNANCE" => Some(ObjectLockRetentionMode::Governance),
            "COMPLIANCE" => Some(ObjectLockRetentionMode::Compliance),
            _ => None,
        }
    }
}

/// Object retention settings (applied to individual objects)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectRetention {
    /// Retention mode
    pub mode: ObjectLockRetentionMode,
    /// Date until which the object is retained
    pub retain_until_date: DateTime<Utc>,
}

impl ObjectRetention {
    pub fn new(mode: ObjectLockRetentionMode, retain_until_date: DateTime<Utc>) -> Self {
        Self {
            mode,
            retain_until_date,
        }
    }

    /// Check if the retention period has expired
    pub fn is_expired(&self) -> bool {
        Utc::now() > self.retain_until_date
    }
}

/// Legal hold status for an object
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ObjectLockLegalHoldStatus {
    On,
    #[default]
    Off,
}

impl ObjectLockLegalHoldStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            ObjectLockLegalHoldStatus::On => "ON",
            ObjectLockLegalHoldStatus::Off => "OFF",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "ON" => Some(ObjectLockLegalHoldStatus::On),
            "OFF" => Some(ObjectLockLegalHoldStatus::Off),
            _ => None,
        }
    }

    pub fn is_on(&self) -> bool {
        matches!(self, ObjectLockLegalHoldStatus::On)
    }
}

/// Legal hold for an object
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ObjectLegalHold {
    pub status: ObjectLockLegalHoldStatus,
}

impl ObjectLegalHold {
    pub fn on() -> Self {
        Self {
            status: ObjectLockLegalHoldStatus::On,
        }
    }

    pub fn off() -> Self {
        Self {
            status: ObjectLockLegalHoldStatus::Off,
        }
    }
}

// =============================================================================
// Server-Side Encryption Configuration Types
// =============================================================================

/// Server-Side Encryption configuration for a bucket
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ServerSideEncryptionConfiguration {
    pub rules: Vec<ServerSideEncryptionRule>,
}

impl ServerSideEncryptionConfiguration {
    pub fn new() -> Self {
        Self { rules: Vec::new() }
    }

    pub fn with_rule(mut self, rule: ServerSideEncryptionRule) -> Self {
        self.rules.push(rule);
        self
    }
}

/// A single SSE rule
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerSideEncryptionRule {
    /// Default encryption settings to apply
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub apply_server_side_encryption_by_default: Option<ServerSideEncryptionByDefault>,
    /// Whether to use bucket key for SSE-KMS (not implemented, for future)
    #[serde(default)]
    pub bucket_key_enabled: bool,
}

impl ServerSideEncryptionRule {
    pub fn new(sse_default: ServerSideEncryptionByDefault) -> Self {
        Self {
            apply_server_side_encryption_by_default: Some(sse_default),
            bucket_key_enabled: false,
        }
    }
}

impl Default for ServerSideEncryptionRule {
    fn default() -> Self {
        Self {
            apply_server_side_encryption_by_default: None,
            bucket_key_enabled: false,
        }
    }
}

/// Default encryption settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerSideEncryptionByDefault {
    /// Encryption algorithm: "AES256" for SSE-S3
    pub sse_algorithm: SseAlgorithm,
    /// KMS key ID (only for SSE-KMS, not implemented)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kms_master_key_id: Option<String>,
}

impl ServerSideEncryptionByDefault {
    pub fn aes256() -> Self {
        Self {
            sse_algorithm: SseAlgorithm::Aes256,
            kms_master_key_id: None,
        }
    }
}

/// SSE algorithm types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SseAlgorithm {
    /// SSE-S3 (AES-256)
    #[serde(rename = "AES256")]
    Aes256,
    /// SSE-KMS (not implemented, for future compatibility)
    #[serde(rename = "aws:kms")]
    AwsKms,
}

impl SseAlgorithm {
    pub fn as_str(&self) -> &'static str {
        match self {
            SseAlgorithm::Aes256 => "AES256",
            SseAlgorithm::AwsKms => "aws:kms",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "AES256" => Some(SseAlgorithm::Aes256),
            "aws:kms" => Some(SseAlgorithm::AwsKms),
            _ => None,
        }
    }
}

impl Default for SseAlgorithm {
    fn default() -> Self {
        SseAlgorithm::Aes256
    }
}

// =============================================================================
// Public Access Block Configuration Types
// =============================================================================

/// Public Access Block configuration for a bucket
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PublicAccessBlockConfiguration {
    /// Whether Amazon S3 should block public access control lists (ACLs) for this bucket
    #[serde(default)]
    pub block_public_acls: bool,
    /// Whether Amazon S3 should ignore public ACLs for this bucket
    #[serde(default)]
    pub ignore_public_acls: bool,
    /// Whether Amazon S3 should block public bucket policies for this bucket
    #[serde(default)]
    pub block_public_policy: bool,
    /// Whether Amazon S3 should restrict public bucket policies for this bucket
    #[serde(default)]
    pub restrict_public_buckets: bool,
}

impl PublicAccessBlockConfiguration {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a configuration that blocks all public access
    pub fn block_all() -> Self {
        Self {
            block_public_acls: true,
            ignore_public_acls: true,
            block_public_policy: true,
            restrict_public_buckets: true,
        }
    }

    pub fn with_block_public_acls(mut self, value: bool) -> Self {
        self.block_public_acls = value;
        self
    }

    pub fn with_ignore_public_acls(mut self, value: bool) -> Self {
        self.ignore_public_acls = value;
        self
    }

    pub fn with_block_public_policy(mut self, value: bool) -> Self {
        self.block_public_policy = value;
        self
    }

    pub fn with_restrict_public_buckets(mut self, value: bool) -> Self {
        self.restrict_public_buckets = value;
        self
    }
}

// =============================================================================
// Website Configuration Types
// =============================================================================

/// Website configuration for a bucket
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WebsiteConfiguration {
    /// Index document configuration
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_document: Option<IndexDocument>,
    /// Error document configuration
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_document: Option<ErrorDocument>,
    /// Redirect all requests to another host
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub redirect_all_requests_to: Option<RedirectAllRequestsTo>,
    /// Routing rules for redirects
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub routing_rules: Vec<RoutingRule>,
}

impl WebsiteConfiguration {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_index_document(mut self, suffix: impl Into<String>) -> Self {
        self.index_document = Some(IndexDocument {
            suffix: suffix.into(),
        });
        self
    }

    pub fn with_error_document(mut self, key: impl Into<String>) -> Self {
        self.error_document = Some(ErrorDocument {
            key: key.into(),
        });
        self
    }

    pub fn with_redirect(mut self, host_name: impl Into<String>, protocol: Option<String>) -> Self {
        self.redirect_all_requests_to = Some(RedirectAllRequestsTo {
            host_name: host_name.into(),
            protocol,
        });
        self
    }

    pub fn with_routing_rule(mut self, rule: RoutingRule) -> Self {
        self.routing_rules.push(rule);
        self
    }
}

/// Index document configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDocument {
    /// Suffix that is appended to requests for a directory
    pub suffix: String,
}

/// Error document configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorDocument {
    /// The object key name to use for error pages
    pub key: String,
}

/// Redirect all requests to another host
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedirectAllRequestsTo {
    /// Name of the host where requests are redirected
    pub host_name: String,
    /// Protocol to use when redirecting (http or https)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol: Option<String>,
}

/// A single routing rule
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingRule {
    /// Condition for when to apply this rule
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub condition: Option<RoutingRuleCondition>,
    /// Redirect configuration
    pub redirect: RoutingRuleRedirect,
}

/// Condition for a routing rule
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RoutingRuleCondition {
    /// The HTTP error code condition
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http_error_code_returned_equals: Option<String>,
    /// Key prefix to match
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_prefix_equals: Option<String>,
}

/// Redirect configuration for a routing rule
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RoutingRuleRedirect {
    /// The host name to redirect to
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host_name: Option<String>,
    /// The HTTP redirect code (301, 302, 303, 307, 308)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http_redirect_code: Option<String>,
    /// Protocol to use (http, https)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol: Option<String>,
    /// Object key to replace the entire key
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replace_key_with: Option<String>,
    /// String to replace the key prefix
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replace_key_prefix_with: Option<String>,
}

// =============================================================================
// Bucket Ownership Controls Types
// =============================================================================

/// Ownership controls configuration for a bucket
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct OwnershipControls {
    pub rules: Vec<OwnershipControlsRule>,
}

impl OwnershipControls {
    pub fn new() -> Self {
        Self { rules: Vec::new() }
    }

    pub fn with_rule(mut self, rule: OwnershipControlsRule) -> Self {
        self.rules.push(rule);
        self
    }
}

/// A single ownership controls rule
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OwnershipControlsRule {
    /// The object ownership setting
    pub object_ownership: ObjectOwnership,
}

impl OwnershipControlsRule {
    pub fn new(object_ownership: ObjectOwnership) -> Self {
        Self { object_ownership }
    }
}

/// Object ownership setting
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ObjectOwnership {
    /// ACLs are disabled. Bucket owner automatically owns every object.
    #[default]
    BucketOwnerEnforced,
    /// Objects uploaded with bucket-owner-full-control ACL are owned by bucket owner.
    BucketOwnerPreferred,
    /// The uploading account owns the object.
    ObjectWriter,
}

impl ObjectOwnership {
    pub fn as_str(&self) -> &'static str {
        match self {
            ObjectOwnership::BucketOwnerEnforced => "BucketOwnerEnforced",
            ObjectOwnership::BucketOwnerPreferred => "BucketOwnerPreferred",
            ObjectOwnership::ObjectWriter => "ObjectWriter",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "BucketOwnerEnforced" => Some(ObjectOwnership::BucketOwnerEnforced),
            "BucketOwnerPreferred" => Some(ObjectOwnership::BucketOwnerPreferred),
            "ObjectWriter" => Some(ObjectOwnership::ObjectWriter),
            _ => None,
        }
    }
}

// =============================================================================
// Bucket Logging Configuration Types
// =============================================================================

/// Logging configuration for a bucket
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LoggingConfiguration {
    /// Target bucket for access logs
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_bucket: Option<String>,
    /// Prefix for log object keys
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_prefix: Option<String>,
    /// Target grants for log delivery permissions
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub target_grants: Vec<LoggingTargetGrant>,
}

impl LoggingConfiguration {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_target(mut self, bucket: impl Into<String>, prefix: impl Into<String>) -> Self {
        self.target_bucket = Some(bucket.into());
        self.target_prefix = Some(prefix.into());
        self
    }

    /// Returns true if logging is enabled (has target bucket)
    pub fn is_enabled(&self) -> bool {
        self.target_bucket.is_some()
    }
}

/// Grant for log delivery
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingTargetGrant {
    pub grantee: Grantee,
    pub permission: LoggingPermission,
}

/// Permissions for logging
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LoggingPermission {
    FullControl,
    Read,
    Write,
}

impl LoggingPermission {
    pub fn as_str(&self) -> &'static str {
        match self {
            LoggingPermission::FullControl => "FULL_CONTROL",
            LoggingPermission::Read => "READ",
            LoggingPermission::Write => "WRITE",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "FULL_CONTROL" => Some(LoggingPermission::FullControl),
            "READ" => Some(LoggingPermission::Read),
            "WRITE" => Some(LoggingPermission::Write),
            _ => None,
        }
    }
}

// =============================================================================
// Bucket Notification Configuration Types
// =============================================================================

/// Notification configuration for a bucket
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NotificationConfiguration {
    /// SNS topic configurations
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub topic_configurations: Vec<TopicConfiguration>,
    /// SQS queue configurations
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub queue_configurations: Vec<QueueConfiguration>,
    /// Lambda function configurations
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub lambda_function_configurations: Vec<LambdaFunctionConfiguration>,
    /// EventBridge configuration
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_bridge_configuration: Option<EventBridgeConfiguration>,
}

impl NotificationConfiguration {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns true if any notification is configured
    pub fn is_configured(&self) -> bool {
        !self.topic_configurations.is_empty()
            || !self.queue_configurations.is_empty()
            || !self.lambda_function_configurations.is_empty()
            || self.event_bridge_configuration.is_some()
    }
}

/// SNS Topic notification configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicConfiguration {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub topic_arn: String,
    pub events: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<NotificationFilter>,
}

/// SQS Queue notification configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueConfiguration {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub queue_arn: String,
    pub events: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<NotificationFilter>,
}

/// Lambda function notification configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LambdaFunctionConfiguration {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub lambda_function_arn: String,
    pub events: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<NotificationFilter>,
}

/// EventBridge configuration (empty - just enables EventBridge)
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct EventBridgeConfiguration {}

/// Filter for notifications based on object key
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationFilter {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<NotificationFilterKey>,
}

/// Key-based filter rules
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationFilterKey {
    pub filter_rules: Vec<FilterRule>,
}

/// Individual filter rule (prefix or suffix)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterRule {
    pub name: String, // "prefix" or "suffix"
    pub value: String,
}

// =============================================================================
// Bucket Replication Configuration Types
// =============================================================================

/// Replication configuration for a bucket
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationConfiguration {
    /// IAM role ARN for replication
    pub role: String,
    /// Replication rules
    pub rules: Vec<ReplicationRule>,
}

impl ReplicationConfiguration {
    pub fn new(role: impl Into<String>) -> Self {
        Self {
            role: role.into(),
            rules: Vec::new(),
        }
    }

    pub fn with_rule(mut self, rule: ReplicationRule) -> Self {
        self.rules.push(rule);
        self
    }
}

/// Single replication rule
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationRule {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub status: ReplicationRuleStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<ReplicationRuleFilter>,
    pub destination: ReplicationDestination,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delete_marker_replication: Option<DeleteMarkerReplication>,
}

/// Replication rule status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicationRuleStatus {
    Enabled,
    Disabled,
}

impl ReplicationRuleStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            ReplicationRuleStatus::Enabled => "Enabled",
            ReplicationRuleStatus::Disabled => "Disabled",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "Enabled" => Some(ReplicationRuleStatus::Enabled),
            "Disabled" => Some(ReplicationRuleStatus::Disabled),
            _ => None,
        }
    }
}

/// Filter for replication rule
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ReplicationRuleFilter {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag: Option<Tag>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub and: Option<ReplicationRuleAndOperator>,
}

/// And operator for replication filter
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ReplicationRuleAndOperator {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<Tag>,
}

/// Destination for replication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationDestination {
    /// Destination bucket ARN
    pub bucket: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_class: Option<String>,
}

/// Delete marker replication settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteMarkerReplication {
    pub status: ReplicationRuleStatus,
}

// =============================================================================
// Request Payment Configuration Types
// =============================================================================

/// Request payment configuration for a bucket
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RequestPaymentConfiguration {
    /// Who pays for the request and data transfer costs
    pub payer: Payer,
}

impl RequestPaymentConfiguration {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_payer(mut self, payer: Payer) -> Self {
        self.payer = payer;
        self
    }
}

/// Specifies who pays for the request and data transfer
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum Payer {
    /// The bucket owner pays (default)
    #[default]
    BucketOwner,
    /// The requester pays
    Requester,
}

impl Payer {
    pub fn as_str(&self) -> &'static str {
        match self {
            Payer::BucketOwner => "BucketOwner",
            Payer::Requester => "Requester",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "BucketOwner" => Some(Payer::BucketOwner),
            "Requester" => Some(Payer::Requester),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bucket_creation() {
        let bucket = Bucket::new("my-bucket", "us-east-1");

        assert_eq!(bucket.name, "my-bucket");
        assert_eq!(bucket.region, "us-east-1");
        assert_eq!(bucket.versioning, VersioningStatus::Disabled);
    }

    #[test]
    fn test_valid_bucket_names() {
        let valid_names = vec![
            "my-bucket".to_string(),
            "mybucket123".to_string(),
            "123bucket".to_string(),
            "my.bucket.name".to_string(),
            "a1b".to_string(),  // minimum length
            "a".repeat(63), // maximum length
        ];

        for name in valid_names {
            assert!(
                Bucket::validate_name(&name).is_ok(),
                "Expected '{}' to be valid",
                name
            );
        }
    }

    #[test]
    fn test_bucket_name_too_short() {
        assert_eq!(
            Bucket::validate_name("ab"),
            Err(BucketNameError::TooShort)
        );
        assert_eq!(
            Bucket::validate_name("a"),
            Err(BucketNameError::TooShort)
        );
        assert_eq!(
            Bucket::validate_name(""),
            Err(BucketNameError::TooShort)
        );
    }

    #[test]
    fn test_bucket_name_too_long() {
        let long_name = "a".repeat(64);
        assert_eq!(
            Bucket::validate_name(&long_name),
            Err(BucketNameError::TooLong)
        );
    }

    #[test]
    fn test_bucket_name_invalid_start() {
        assert_eq!(
            Bucket::validate_name("-bucket"),
            Err(BucketNameError::InvalidStartCharacter)
        );
        assert_eq!(
            Bucket::validate_name(".bucket"),
            Err(BucketNameError::InvalidStartCharacter)
        );
        assert_eq!(
            Bucket::validate_name("Bucket"),
            Err(BucketNameError::InvalidStartCharacter)
        );
    }

    #[test]
    fn test_bucket_name_invalid_end() {
        assert_eq!(
            Bucket::validate_name("bucket-"),
            Err(BucketNameError::InvalidEndCharacter)
        );
        assert_eq!(
            Bucket::validate_name("bucket."),
            Err(BucketNameError::InvalidEndCharacter)
        );
    }

    #[test]
    fn test_bucket_name_invalid_characters() {
        assert!(matches!(
            Bucket::validate_name("bucket_name"),
            Err(BucketNameError::InvalidCharacter('_'))
        ));
        assert!(matches!(
            Bucket::validate_name("bucket name"),
            Err(BucketNameError::InvalidCharacter(' '))
        ));
    }

    #[test]
    fn test_bucket_name_consecutive_periods() {
        assert_eq!(
            Bucket::validate_name("bucket..name"),
            Err(BucketNameError::ConsecutivePeriods)
        );
    }

    #[test]
    fn test_bucket_name_ip_address_format() {
        assert_eq!(
            Bucket::validate_name("192.168.1.1"),
            Err(BucketNameError::IpAddressFormat)
        );
    }

    #[test]
    fn test_versioning_status() {
        assert_eq!(VersioningStatus::Disabled.as_str(), None);
        assert_eq!(VersioningStatus::Enabled.as_str(), Some("Enabled"));
        assert_eq!(VersioningStatus::Suspended.as_str(), Some("Suspended"));
    }
}

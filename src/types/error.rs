//! S3 Error types and handling

use std::fmt;

/// Result type alias for S3 operations
pub type S3Result<T> = Result<T, S3Error>;

/// S3 Error codes as defined by AWS
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum S3ErrorCode {
    AccessDenied,
    BucketAlreadyExists,
    BucketAlreadyOwnedByYou,
    BucketNotEmpty,
    EntityTooLarge,
    EntityTooSmall,
    InvalidAccessKeyId,
    InvalidArgument,
    InvalidBucketName,
    InvalidDigest,
    InvalidPart,
    InvalidPartOrder,
    InvalidRange,
    InvalidRequest,
    InvalidRetentionPeriod,
    InvalidSecurity,
    InvalidStorageClass,
    InvalidTargetBucketForLogging,
    InvalidToken,
    KeyTooLong,
    MalformedXML,
    MaxMessageLengthExceeded,
    MaxPostPreDataLengthExceeded,
    MetadataTooLarge,
    MethodNotAllowed,
    MissingContentLength,
    MissingRequestBodyError,
    MissingSecurityElement,
    MissingSecurityHeader,
    NoLoggingStatusForKey,
    NoSuchBucket,
    NoSuchBucketPolicy,
    NoSuchCORSConfiguration,
    NoSuchKey,
    NoSuchLifecycleConfiguration,
    NoSuchObjectLockConfiguration,
    NoSuchPublicAccessBlockConfiguration,
    NoSuchWebsiteConfiguration,
    ReplicationConfigurationNotFoundError,
    NoSuchTagSet,
    NoSuchUpload,
    NoSuchVersion,
    NotImplemented,
    OwnershipControlsNotFoundError,
    ServerSideEncryptionConfigurationNotFoundError,
    ObjectLockConfigurationNotFoundError,
    OperationAborted,
    PermanentRedirect,
    PreconditionFailed,
    Redirect,
    RequestIsNotMultiPartContent,
    RequestTimeout,
    RequestTimeTooSkewed,
    SignatureDoesNotMatch,
    SlowDown,
    TemporaryRedirect,
    TooManyBuckets,
    UnexpectedContent,
    UnresolvableGrantByEmailAddress,
    UserKeyMustBeSpecified,
    InternalError,
}

impl S3ErrorCode {
    /// Get the HTTP status code for this error
    pub fn http_status(&self) -> u16 {
        match self {
            S3ErrorCode::AccessDenied => 403,
            S3ErrorCode::BucketAlreadyExists => 409,
            S3ErrorCode::BucketAlreadyOwnedByYou => 409,
            S3ErrorCode::BucketNotEmpty => 409,
            S3ErrorCode::EntityTooLarge => 400,
            S3ErrorCode::EntityTooSmall => 400,
            S3ErrorCode::InvalidAccessKeyId => 403,
            S3ErrorCode::InvalidArgument => 400,
            S3ErrorCode::InvalidBucketName => 400,
            S3ErrorCode::InvalidDigest => 400,
            S3ErrorCode::InvalidPart => 400,
            S3ErrorCode::InvalidPartOrder => 400,
            S3ErrorCode::InvalidRange => 416,
            S3ErrorCode::InvalidRequest => 400,
            S3ErrorCode::InvalidRetentionPeriod => 400,
            S3ErrorCode::InvalidSecurity => 403,
            S3ErrorCode::InvalidStorageClass => 400,
            S3ErrorCode::InvalidTargetBucketForLogging => 400,
            S3ErrorCode::InvalidToken => 400,
            S3ErrorCode::KeyTooLong => 400,
            S3ErrorCode::MalformedXML => 400,
            S3ErrorCode::MaxMessageLengthExceeded => 400,
            S3ErrorCode::MaxPostPreDataLengthExceeded => 400,
            S3ErrorCode::MetadataTooLarge => 400,
            S3ErrorCode::MethodNotAllowed => 405,
            S3ErrorCode::MissingContentLength => 411,
            S3ErrorCode::MissingRequestBodyError => 400,
            S3ErrorCode::MissingSecurityElement => 400,
            S3ErrorCode::MissingSecurityHeader => 400,
            S3ErrorCode::NoLoggingStatusForKey => 400,
            S3ErrorCode::NoSuchBucket => 404,
            S3ErrorCode::NoSuchBucketPolicy => 404,
            S3ErrorCode::NoSuchCORSConfiguration => 404,
            S3ErrorCode::NoSuchKey => 404,
            S3ErrorCode::NoSuchLifecycleConfiguration => 404,
            S3ErrorCode::NoSuchObjectLockConfiguration => 404,
            S3ErrorCode::NoSuchPublicAccessBlockConfiguration => 404,
            S3ErrorCode::NoSuchWebsiteConfiguration => 404,
            S3ErrorCode::ReplicationConfigurationNotFoundError => 404,
            S3ErrorCode::NoSuchTagSet => 404,
            S3ErrorCode::NoSuchUpload => 404,
            S3ErrorCode::NoSuchVersion => 404,
            S3ErrorCode::NotImplemented => 501,
            S3ErrorCode::OwnershipControlsNotFoundError => 404,
            S3ErrorCode::ServerSideEncryptionConfigurationNotFoundError => 404,
            S3ErrorCode::ObjectLockConfigurationNotFoundError => 404,
            S3ErrorCode::OperationAborted => 409,
            S3ErrorCode::PermanentRedirect => 301,
            S3ErrorCode::PreconditionFailed => 412,
            S3ErrorCode::Redirect => 307,
            S3ErrorCode::RequestIsNotMultiPartContent => 400,
            S3ErrorCode::RequestTimeout => 400,
            S3ErrorCode::RequestTimeTooSkewed => 403,
            S3ErrorCode::SignatureDoesNotMatch => 403,
            S3ErrorCode::SlowDown => 503,
            S3ErrorCode::TemporaryRedirect => 307,
            S3ErrorCode::TooManyBuckets => 400,
            S3ErrorCode::UnexpectedContent => 400,
            S3ErrorCode::UnresolvableGrantByEmailAddress => 400,
            S3ErrorCode::UserKeyMustBeSpecified => 400,
            S3ErrorCode::InternalError => 500,
        }
    }

    /// Get the error code string as returned by S3
    pub fn as_str(&self) -> &'static str {
        match self {
            S3ErrorCode::AccessDenied => "AccessDenied",
            S3ErrorCode::BucketAlreadyExists => "BucketAlreadyExists",
            S3ErrorCode::BucketAlreadyOwnedByYou => "BucketAlreadyOwnedByYou",
            S3ErrorCode::BucketNotEmpty => "BucketNotEmpty",
            S3ErrorCode::EntityTooLarge => "EntityTooLarge",
            S3ErrorCode::EntityTooSmall => "EntityTooSmall",
            S3ErrorCode::InvalidAccessKeyId => "InvalidAccessKeyId",
            S3ErrorCode::InvalidArgument => "InvalidArgument",
            S3ErrorCode::InvalidBucketName => "InvalidBucketName",
            S3ErrorCode::InvalidDigest => "InvalidDigest",
            S3ErrorCode::InvalidPart => "InvalidPart",
            S3ErrorCode::InvalidPartOrder => "InvalidPartOrder",
            S3ErrorCode::InvalidRange => "InvalidRange",
            S3ErrorCode::InvalidRequest => "InvalidRequest",
            S3ErrorCode::InvalidRetentionPeriod => "InvalidRetentionPeriod",
            S3ErrorCode::InvalidSecurity => "InvalidSecurity",
            S3ErrorCode::InvalidStorageClass => "InvalidStorageClass",
            S3ErrorCode::InvalidTargetBucketForLogging => "InvalidTargetBucketForLogging",
            S3ErrorCode::InvalidToken => "InvalidToken",
            S3ErrorCode::KeyTooLong => "KeyTooLong",
            S3ErrorCode::MalformedXML => "MalformedXML",
            S3ErrorCode::MaxMessageLengthExceeded => "MaxMessageLengthExceeded",
            S3ErrorCode::MaxPostPreDataLengthExceeded => "MaxPostPreDataLengthExceeded",
            S3ErrorCode::MetadataTooLarge => "MetadataTooLarge",
            S3ErrorCode::MethodNotAllowed => "MethodNotAllowed",
            S3ErrorCode::MissingContentLength => "MissingContentLength",
            S3ErrorCode::MissingRequestBodyError => "MissingRequestBodyError",
            S3ErrorCode::MissingSecurityElement => "MissingSecurityElement",
            S3ErrorCode::MissingSecurityHeader => "MissingSecurityHeader",
            S3ErrorCode::NoLoggingStatusForKey => "NoLoggingStatusForKey",
            S3ErrorCode::NoSuchBucket => "NoSuchBucket",
            S3ErrorCode::NoSuchBucketPolicy => "NoSuchBucketPolicy",
            S3ErrorCode::NoSuchCORSConfiguration => "NoSuchCORSConfiguration",
            S3ErrorCode::NoSuchKey => "NoSuchKey",
            S3ErrorCode::NoSuchLifecycleConfiguration => "NoSuchLifecycleConfiguration",
            S3ErrorCode::NoSuchObjectLockConfiguration => "NoSuchObjectLockConfiguration",
            S3ErrorCode::NoSuchPublicAccessBlockConfiguration => "NoSuchPublicAccessBlockConfiguration",
            S3ErrorCode::NoSuchWebsiteConfiguration => "NoSuchWebsiteConfiguration",
            S3ErrorCode::ReplicationConfigurationNotFoundError => "ReplicationConfigurationNotFoundError",
            S3ErrorCode::NoSuchTagSet => "NoSuchTagSet",
            S3ErrorCode::NoSuchUpload => "NoSuchUpload",
            S3ErrorCode::NoSuchVersion => "NoSuchVersion",
            S3ErrorCode::NotImplemented => "NotImplemented",
            S3ErrorCode::OwnershipControlsNotFoundError => "OwnershipControlsNotFoundError",
            S3ErrorCode::ServerSideEncryptionConfigurationNotFoundError => "ServerSideEncryptionConfigurationNotFoundError",
            S3ErrorCode::ObjectLockConfigurationNotFoundError => "ObjectLockConfigurationNotFoundError",
            S3ErrorCode::OperationAborted => "OperationAborted",
            S3ErrorCode::PermanentRedirect => "PermanentRedirect",
            S3ErrorCode::PreconditionFailed => "PreconditionFailed",
            S3ErrorCode::Redirect => "Redirect",
            S3ErrorCode::RequestIsNotMultiPartContent => "RequestIsNotMultiPartContent",
            S3ErrorCode::RequestTimeout => "RequestTimeout",
            S3ErrorCode::RequestTimeTooSkewed => "RequestTimeTooSkewed",
            S3ErrorCode::SignatureDoesNotMatch => "SignatureDoesNotMatch",
            S3ErrorCode::SlowDown => "SlowDown",
            S3ErrorCode::TemporaryRedirect => "TemporaryRedirect",
            S3ErrorCode::TooManyBuckets => "TooManyBuckets",
            S3ErrorCode::UnexpectedContent => "UnexpectedContent",
            S3ErrorCode::UnresolvableGrantByEmailAddress => "UnresolvableGrantByEmailAddress",
            S3ErrorCode::UserKeyMustBeSpecified => "UserKeyMustBeSpecified",
            S3ErrorCode::InternalError => "InternalError",
        }
    }
}

impl fmt::Display for S3ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// S3 Error with code, message, and optional resource/request info
#[derive(Debug, Clone)]
pub struct S3Error {
    pub code: S3ErrorCode,
    pub message: String,
    pub resource: Option<String>,
    pub request_id: Option<String>,
}

impl S3Error {
    /// Create a new S3 error
    pub fn new(code: S3ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            resource: None,
            request_id: None,
        }
    }

    /// Add resource information to the error
    pub fn with_resource(mut self, resource: impl Into<String>) -> Self {
        self.resource = Some(resource.into());
        self
    }

    /// Add request ID to the error
    pub fn with_request_id(mut self, request_id: impl Into<String>) -> Self {
        self.request_id = Some(request_id.into());
        self
    }

    /// Get HTTP status code for this error
    pub fn http_status(&self) -> u16 {
        self.code.http_status()
    }

    /// Convert error to XML response
    pub fn to_xml(&self) -> String {
        let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        xml.push_str("<Error>\n");
        xml.push_str(&format!("  <Code>{}</Code>\n", self.code));
        xml.push_str(&format!("  <Message>{}</Message>\n", self.message));
        if let Some(ref resource) = self.resource {
            xml.push_str(&format!("  <Resource>{}</Resource>\n", resource));
        }
        if let Some(ref request_id) = self.request_id {
            xml.push_str(&format!("  <RequestId>{}</RequestId>\n", request_id));
        }
        xml.push_str("</Error>");
        xml
    }

    // Convenience constructors for common errors

    pub fn no_such_bucket(bucket: &str) -> Self {
        Self::new(
            S3ErrorCode::NoSuchBucket,
            "The specified bucket does not exist",
        )
        .with_resource(bucket.to_string())
    }

    pub fn no_such_key(key: &str) -> Self {
        Self::new(S3ErrorCode::NoSuchKey, "The specified key does not exist")
            .with_resource(key.to_string())
    }

    pub fn bucket_already_exists(bucket: &str) -> Self {
        Self::new(
            S3ErrorCode::BucketAlreadyOwnedByYou,
            "Your previous request to create the named bucket succeeded and you already own it",
        )
        .with_resource(bucket.to_string())
    }

    pub fn bucket_not_empty(bucket: &str) -> Self {
        Self::new(
            S3ErrorCode::BucketNotEmpty,
            "The bucket you tried to delete is not empty",
        )
        .with_resource(bucket.to_string())
    }

    pub fn invalid_bucket_name(bucket: &str) -> Self {
        Self::new(
            S3ErrorCode::InvalidBucketName,
            "The specified bucket is not valid",
        )
        .with_resource(bucket.to_string())
    }

    pub fn access_denied(message: &str) -> Self {
        Self::new(S3ErrorCode::AccessDenied, message)
    }

    pub fn internal_error(message: &str) -> Self {
        Self::new(S3ErrorCode::InternalError, message)
    }

    pub fn not_implemented(operation: &str) -> Self {
        Self::new(
            S3ErrorCode::NotImplemented,
            format!("The {} operation is not implemented", operation),
        )
    }

    pub fn no_such_upload(upload_id: &str) -> Self {
        Self::new(
            S3ErrorCode::NoSuchUpload,
            "The specified upload does not exist",
        )
        .with_resource(upload_id.to_string())
    }

    pub fn invalid_part(part_number: i32) -> Self {
        Self::new(
            S3ErrorCode::InvalidPart,
            format!("One or more of the specified parts could not be found. Part number: {}", part_number),
        )
    }
}

impl fmt::Display for S3Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for S3Error {}

impl From<std::io::Error> for S3Error {
    fn from(err: std::io::Error) -> Self {
        S3Error::internal_error(&format!("IO error: {}", err))
    }
}

impl From<serde_json::Error> for S3Error {
    fn from(err: serde_json::Error) -> Self {
        S3Error::internal_error(&format!("JSON error: {}", err))
    }
}

impl From<quick_xml::DeError> for S3Error {
    fn from(err: quick_xml::DeError) -> Self {
        S3Error::internal_error(&format!("XML error: {}", err))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_code_http_status() {
        assert_eq!(S3ErrorCode::NoSuchBucket.http_status(), 404);
        assert_eq!(S3ErrorCode::NoSuchKey.http_status(), 404);
        assert_eq!(S3ErrorCode::AccessDenied.http_status(), 403);
        assert_eq!(S3ErrorCode::BucketNotEmpty.http_status(), 409);
        assert_eq!(S3ErrorCode::InternalError.http_status(), 500);
    }

    #[test]
    fn test_error_code_as_str() {
        assert_eq!(S3ErrorCode::NoSuchBucket.as_str(), "NoSuchBucket");
        assert_eq!(S3ErrorCode::AccessDenied.as_str(), "AccessDenied");
    }

    #[test]
    fn test_error_to_xml() {
        let error = S3Error::no_such_bucket("my-bucket")
            .with_request_id("test-request-id");

        let xml = error.to_xml();

        assert!(xml.contains("<Code>NoSuchBucket</Code>"));
        assert!(xml.contains("<Resource>my-bucket</Resource>"));
        assert!(xml.contains("<RequestId>test-request-id</RequestId>"));
    }

    #[test]
    fn test_error_display() {
        let error = S3Error::no_such_key("my-key");
        let display = format!("{}", error);

        assert!(display.contains("NoSuchKey"));
        assert!(display.contains("does not exist"));
    }

    #[test]
    fn test_convenience_constructors() {
        let err = S3Error::no_such_bucket("test-bucket");
        assert_eq!(err.code, S3ErrorCode::NoSuchBucket);
        assert_eq!(err.resource, Some("test-bucket".to_string()));

        let err = S3Error::bucket_not_empty("test-bucket");
        assert_eq!(err.code, S3ErrorCode::BucketNotEmpty);

        let err = S3Error::not_implemented("SelectObjectContent");
        assert_eq!(err.code, S3ErrorCode::NotImplemented);
    }
}

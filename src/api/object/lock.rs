//! Object Lock operations (Legal Hold and Retention)

use bytes::Bytes;
use chrono::{DateTime, Utc};
use http_body_util::Full;
use hyper::{Response, StatusCode};

use crate::storage::StorageEngine;
use crate::types::bucket::{
    ObjectLegalHold, ObjectLockLegalHoldStatus, ObjectLockRetentionMode, ObjectRetention,
};
use crate::types::error::{S3Error, S3ErrorCode, S3Result};

// =============================================================================
// Legal Hold Operations
// =============================================================================

/// Handle GetObjectLegalHold request
pub async fn get_object_legal_hold(
    storage: &StorageEngine,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
) -> S3Result<Response<Full<Bytes>>> {
    // Check bucket has object lock enabled
    if !storage.is_object_lock_enabled(bucket).await? {
        return Err(S3Error::new(
            S3ErrorCode::InvalidRequest,
            "Bucket is not Object Lock enabled",
        ));
    }

    let legal_hold = storage.get_object_legal_hold(bucket, key, version_id).await?;

    let xml = build_legal_hold_xml(&legal_hold);

    let response = Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/xml")
        .body(Full::new(Bytes::from(xml)))
        .unwrap();

    Ok(response)
}

/// Handle PutObjectLegalHold request
pub async fn put_object_legal_hold(
    storage: &StorageEngine,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
    body: Bytes,
) -> S3Result<Response<Full<Bytes>>> {
    // Check bucket has object lock enabled
    if !storage.is_object_lock_enabled(bucket).await? {
        return Err(S3Error::new(
            S3ErrorCode::InvalidRequest,
            "Bucket is not Object Lock enabled",
        ));
    }

    // Parse the request body XML
    let legal_hold = parse_legal_hold_xml(&body)?;

    storage
        .set_object_legal_hold(bucket, key, version_id, legal_hold)
        .await?;

    let response = Response::builder()
        .status(StatusCode::OK)
        .body(Full::new(Bytes::new()))
        .unwrap();

    Ok(response)
}

/// Build XML response for Legal Hold
fn build_legal_hold_xml(legal_hold: &ObjectLegalHold) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
        <LegalHold xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  \
        <Status>{}</Status>\n\
        </LegalHold>",
        legal_hold.status.as_str()
    )
}

/// Parse Legal Hold XML
fn parse_legal_hold_xml(body: &[u8]) -> S3Result<ObjectLegalHold> {
    let body_str = std::str::from_utf8(body)
        .map_err(|_| S3Error::new(S3ErrorCode::MalformedXML, "Invalid UTF-8 in request body"))?;

    let status = if body_str.contains("<Status>ON</Status>") {
        ObjectLockLegalHoldStatus::On
    } else if body_str.contains("<Status>OFF</Status>") {
        ObjectLockLegalHoldStatus::Off
    } else {
        return Err(S3Error::new(
            S3ErrorCode::MalformedXML,
            "Invalid or missing Status in LegalHold",
        ));
    };

    Ok(ObjectLegalHold { status })
}

// =============================================================================
// Retention Operations
// =============================================================================

/// Handle GetObjectRetention request
pub async fn get_object_retention(
    storage: &StorageEngine,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
) -> S3Result<Response<Full<Bytes>>> {
    // Check bucket has object lock enabled
    if !storage.is_object_lock_enabled(bucket).await? {
        return Err(S3Error::new(
            S3ErrorCode::InvalidRequest,
            "Bucket is not Object Lock enabled",
        ));
    }

    let retention = storage.get_object_retention(bucket, key, version_id).await?;

    let xml = build_retention_xml(&retention);

    let response = Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/xml")
        .body(Full::new(Bytes::from(xml)))
        .unwrap();

    Ok(response)
}

/// Handle PutObjectRetention request
pub async fn put_object_retention(
    storage: &StorageEngine,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
    bypass_governance: bool,
    body: Bytes,
) -> S3Result<Response<Full<Bytes>>> {
    // Check bucket has object lock enabled
    if !storage.is_object_lock_enabled(bucket).await? {
        return Err(S3Error::new(
            S3ErrorCode::InvalidRequest,
            "Bucket is not Object Lock enabled",
        ));
    }

    // Parse the request body XML
    let retention = parse_retention_xml(&body)?;

    // Check if we're trying to modify COMPLIANCE mode without bypass
    if let Some(existing) = storage
        .get_object_retention(bucket, key, version_id)
        .await
        .ok()
    {
        if existing.mode == ObjectLockRetentionMode::Compliance
            && !existing.is_expired()
            && !bypass_governance
        {
            return Err(S3Error::new(
                S3ErrorCode::AccessDenied,
                "Cannot modify COMPLIANCE mode retention without bypass",
            ));
        }
    }

    storage
        .set_object_retention(bucket, key, version_id, retention)
        .await?;

    let response = Response::builder()
        .status(StatusCode::OK)
        .body(Full::new(Bytes::new()))
        .unwrap();

    Ok(response)
}

/// Build XML response for Retention
fn build_retention_xml(retention: &ObjectRetention) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
        <Retention xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  \
        <Mode>{}</Mode>\n  \
        <RetainUntilDate>{}</RetainUntilDate>\n\
        </Retention>",
        retention.mode.as_str(),
        retention.retain_until_date.to_rfc3339()
    )
}

/// Parse Retention XML
fn parse_retention_xml(body: &[u8]) -> S3Result<ObjectRetention> {
    let body_str = std::str::from_utf8(body)
        .map_err(|_| S3Error::new(S3ErrorCode::MalformedXML, "Invalid UTF-8 in request body"))?;

    // Parse Mode
    let mode = if body_str.contains("<Mode>GOVERNANCE</Mode>") {
        ObjectLockRetentionMode::Governance
    } else if body_str.contains("<Mode>COMPLIANCE</Mode>") {
        ObjectLockRetentionMode::Compliance
    } else {
        return Err(S3Error::new(
            S3ErrorCode::MalformedXML,
            "Invalid or missing Mode in Retention",
        ));
    };

    // Parse RetainUntilDate
    let retain_until_date = extract_xml_value(body_str, "RetainUntilDate")
        .ok_or_else(|| {
            S3Error::new(
                S3ErrorCode::MalformedXML,
                "Missing RetainUntilDate in Retention",
            )
        })
        .and_then(|s| {
            // Try to parse as RFC3339 or ISO8601
            DateTime::parse_from_rfc3339(&s)
                .map(|dt| dt.with_timezone(&Utc))
                .or_else(|_| {
                    // Try parsing with milliseconds
                    chrono::DateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S%.3fZ")
                        .map(|dt| dt.with_timezone(&Utc))
                })
                .or_else(|_| {
                    // Try parsing timestamp in milliseconds
                    s.parse::<i64>()
                        .ok()
                        .and_then(|ms| DateTime::from_timestamp_millis(ms))
                        .ok_or_else(|| ())
                })
                .map_err(|_| {
                    S3Error::new(
                        S3ErrorCode::InvalidRetentionPeriod,
                        "Invalid RetainUntilDate format",
                    )
                })
        })?;

    Ok(ObjectRetention {
        mode,
        retain_until_date,
    })
}

/// Extract a single value from an XML element
fn extract_xml_value(content: &str, tag: &str) -> Option<String> {
    let open_tag = format!("<{}>", tag);
    let close_tag = format!("</{}>", tag);

    if let Some(start) = content.find(&open_tag) {
        let after_open = &content[start + open_tag.len()..];
        if let Some(end) = after_open.find(&close_tag) {
            let value = &after_open[..end];
            return Some(value.to_string());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Datelike;

    #[test]
    fn test_parse_legal_hold_on() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
        <LegalHold>
            <Status>ON</Status>
        </LegalHold>"#;

        let legal_hold = parse_legal_hold_xml(xml.as_bytes()).unwrap();
        assert_eq!(legal_hold.status, ObjectLockLegalHoldStatus::On);
    }

    #[test]
    fn test_parse_legal_hold_off() {
        let xml = r#"<LegalHold><Status>OFF</Status></LegalHold>"#;

        let legal_hold = parse_legal_hold_xml(xml.as_bytes()).unwrap();
        assert_eq!(legal_hold.status, ObjectLockLegalHoldStatus::Off);
    }

    #[test]
    fn test_build_legal_hold_xml() {
        let legal_hold = ObjectLegalHold {
            status: ObjectLockLegalHoldStatus::On,
        };

        let xml = build_legal_hold_xml(&legal_hold);
        assert!(xml.contains("<Status>ON</Status>"));
    }

    #[test]
    fn test_parse_retention_governance() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
        <Retention>
            <Mode>GOVERNANCE</Mode>
            <RetainUntilDate>2025-12-31T23:59:59Z</RetainUntilDate>
        </Retention>"#;

        let retention = parse_retention_xml(xml.as_bytes()).unwrap();
        assert_eq!(retention.mode, ObjectLockRetentionMode::Governance);
        assert_eq!(retention.retain_until_date.year(), 2025);
    }

    #[test]
    fn test_parse_retention_compliance() {
        let xml = r#"<Retention>
            <Mode>COMPLIANCE</Mode>
            <RetainUntilDate>2030-01-15T00:00:00.000Z</RetainUntilDate>
        </Retention>"#;

        let retention = parse_retention_xml(xml.as_bytes()).unwrap();
        assert_eq!(retention.mode, ObjectLockRetentionMode::Compliance);
    }

    #[test]
    fn test_build_retention_xml() {
        let retention = ObjectRetention {
            mode: ObjectLockRetentionMode::Governance,
            retain_until_date: Utc::now(),
        };

        let xml = build_retention_xml(&retention);
        assert!(xml.contains("<Mode>GOVERNANCE</Mode>"));
        assert!(xml.contains("<RetainUntilDate>"));
    }
}

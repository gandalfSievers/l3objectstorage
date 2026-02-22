//! Bucket Request Payment operations

use bytes::Bytes;
use http_body_util::Full;
use hyper::{Response, StatusCode};

use crate::storage::StorageEngine;
use crate::types::bucket::{Payer, RequestPaymentConfiguration};
use crate::types::error::{S3Error, S3ErrorCode, S3Result};

/// Handle GetBucketRequestPayment request
pub async fn get_bucket_request_payment(
    storage: &StorageEngine,
    bucket: &str,
) -> S3Result<Response<Full<Bytes>>> {
    // Check bucket exists
    if !storage.bucket_exists(bucket).await {
        return Err(S3Error::no_such_bucket(bucket));
    }

    let config = storage.get_bucket_request_payment(bucket).await?;

    // Generate XML response
    let xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<RequestPaymentConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Payer>{}</Payer>
</RequestPaymentConfiguration>"#,
        config.payer.as_str()
    );

    let response = Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/xml")
        .body(Full::new(Bytes::from(xml)))
        .unwrap();

    Ok(response)
}

/// Handle PutBucketRequestPayment request
pub async fn put_bucket_request_payment(
    storage: &StorageEngine,
    bucket: &str,
    body: Bytes,
) -> S3Result<Response<Full<Bytes>>> {
    // Check bucket exists
    if !storage.bucket_exists(bucket).await {
        return Err(S3Error::no_such_bucket(bucket));
    }

    // Parse the request body XML
    let config = parse_request_payment_configuration(&body)?;

    storage.set_bucket_request_payment(bucket, config).await?;

    let response = Response::builder()
        .status(StatusCode::OK)
        .body(Full::new(Bytes::new()))
        .unwrap();

    Ok(response)
}

/// Parse the RequestPaymentConfiguration request XML body
fn parse_request_payment_configuration(body: &[u8]) -> S3Result<RequestPaymentConfiguration> {
    let body_str = std::str::from_utf8(body)
        .map_err(|_| S3Error::new(S3ErrorCode::MalformedXML, "Invalid UTF-8 in request body"))?;

    // Parse Payer element
    let payer = if let Some(payer_value) = extract_xml_value(body_str, "Payer") {
        Payer::from_str(&payer_value).ok_or_else(|| {
            S3Error::new(
                S3ErrorCode::MalformedXML,
                format!("Invalid Payer value: {}. Must be 'BucketOwner' or 'Requester'", payer_value),
            )
        })?
    } else {
        return Err(S3Error::new(
            S3ErrorCode::MalformedXML,
            "Missing required element: Payer",
        ));
    };

    Ok(RequestPaymentConfiguration { payer })
}

/// Extract a single value from an XML element
fn extract_xml_value(content: &str, tag: &str) -> Option<String> {
    let open_tag = format!("<{}>", tag);
    let close_tag = format!("</{}>", tag);

    if let Some(start) = content.find(&open_tag) {
        let after_open = &content[start + open_tag.len()..];
        if let Some(end) = after_open.find(&close_tag) {
            let value = &after_open[..end];
            return Some(decode_xml_entities(value.trim()));
        }
    }
    None
}

/// Decode XML entities
fn decode_xml_entities(s: &str) -> String {
    s.replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
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

    #[test]
    fn test_parse_request_payment_requester() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
        <RequestPaymentConfiguration>
            <Payer>Requester</Payer>
        </RequestPaymentConfiguration>"#;

        let config = parse_request_payment_configuration(xml.as_bytes()).unwrap();

        assert_eq!(config.payer, Payer::Requester);
    }

    #[test]
    fn test_parse_request_payment_bucket_owner() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
        <RequestPaymentConfiguration>
            <Payer>BucketOwner</Payer>
        </RequestPaymentConfiguration>"#;

        let config = parse_request_payment_configuration(xml.as_bytes()).unwrap();

        assert_eq!(config.payer, Payer::BucketOwner);
    }

    #[test]
    fn test_parse_request_payment_invalid_payer() {
        let xml = r#"<RequestPaymentConfiguration>
            <Payer>InvalidPayer</Payer>
        </RequestPaymentConfiguration>"#;

        let result = parse_request_payment_configuration(xml.as_bytes());
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_request_payment_missing_payer() {
        let xml = r#"<RequestPaymentConfiguration></RequestPaymentConfiguration>"#;

        let result = parse_request_payment_configuration(xml.as_bytes());
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_put_get_bucket_request_payment() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        // Put request payment configuration
        let xml = r#"<RequestPaymentConfiguration>
            <Payer>Requester</Payer>
        </RequestPaymentConfiguration>"#;

        let response = put_bucket_request_payment(&storage, "test-bucket", Bytes::from(xml))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Get request payment configuration
        let response = get_bucket_request_payment(&storage, "test-bucket")
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_bucket_request_payment_default() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        // Should return default (BucketOwner) when not configured
        let response = get_bucket_request_payment(&storage, "test-bucket")
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_put_bucket_request_payment_bucket_not_found() {
        let (storage, _temp) = create_test_storage().await;

        let xml = r#"<RequestPaymentConfiguration>
            <Payer>Requester</Payer>
        </RequestPaymentConfiguration>"#;

        let result = put_bucket_request_payment(&storage, "nonexistent", Bytes::from(xml)).await;
        assert!(result.is_err());
    }
}

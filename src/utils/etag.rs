//! ETag calculation utilities

use ring::digest::{Context, SHA256};

/// Calculate the ETag (MD5 hash) for the given data
/// Returns the ETag in the format "\"<hash>\""
pub fn calculate_etag(data: &[u8]) -> String {
    // Note: S3 traditionally uses MD5 for ETags, but we'll use SHA256
    // for new objects. For multipart uploads, the ETag is different.
    let mut context = Context::new(&SHA256);
    context.update(data);
    let digest = context.finish();
    let hash = hex::encode(digest.as_ref());

    // ETag must be quoted
    format!("\"{}\"", &hash[..32]) // Use first 32 chars (16 bytes) like MD5
}

/// Calculate the ETag for a multipart upload
/// This follows S3's convention of hash-partcount
pub fn calculate_multipart_etag(part_etags: &[String]) -> String {
    let mut context = Context::new(&SHA256);

    for etag in part_etags {
        // Remove quotes and decode the hex
        let etag_clean = etag.trim_matches('"');
        if let Ok(bytes) = hex::decode(etag_clean) {
            context.update(&bytes);
        }
    }

    let digest = context.finish();
    let hash = hex::encode(digest.as_ref());

    format!("\"{}-{}\"", &hash[..32], part_etags.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_etag() {
        let data = b"hello world";
        let etag = calculate_etag(data);

        // ETag should be quoted
        assert!(etag.starts_with('"'));
        assert!(etag.ends_with('"'));

        // ETag should be 32 chars (16 bytes hex) plus quotes
        assert_eq!(etag.len(), 34);
    }

    #[test]
    fn test_etag_consistency() {
        let data = b"test data";
        let etag1 = calculate_etag(data);
        let etag2 = calculate_etag(data);

        assert_eq!(etag1, etag2);
    }

    #[test]
    fn test_etag_different_data() {
        let etag1 = calculate_etag(b"data1");
        let etag2 = calculate_etag(b"data2");

        assert_ne!(etag1, etag2);
    }

    #[test]
    fn test_calculate_multipart_etag() {
        let part_etags = vec![
            "\"abc123abc123abc123abc123abc123ab\"".to_string(),
            "\"def456def456def456def456def456de\"".to_string(),
        ];

        let etag = calculate_multipart_etag(&part_etags);

        // Multipart ETag format: "hash-partcount"
        assert!(etag.starts_with('"'));
        assert!(etag.ends_with('"'));
        assert!(etag.contains("-2\""));
    }

    #[test]
    fn test_empty_data_etag() {
        let etag = calculate_etag(b"");

        // Should still produce a valid ETag
        assert!(etag.starts_with('"'));
        assert!(etag.ends_with('"'));
        assert_eq!(etag.len(), 34);
    }
}

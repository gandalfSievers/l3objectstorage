//! Time formatting utilities

use chrono::{DateTime, Utc};

/// Format a DateTime as an S3 compatible date string (ISO 8601)
pub fn format_s3_date(dt: &DateTime<Utc>) -> String {
    dt.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

/// Format a DateTime as an S3 HTTP header date (RFC 7231)
pub fn format_http_date(dt: &DateTime<Utc>) -> String {
    dt.format("%a, %d %b %Y %H:%M:%S GMT").to_string()
}

/// Format a DateTime for AWS SigV4 (YYYYMMDD'T'HHMMSS'Z')
pub fn format_sigv4_date(dt: &DateTime<Utc>) -> String {
    dt.format("%Y%m%dT%H%M%SZ").to_string()
}

/// Format just the date portion for AWS SigV4 (YYYYMMDD)
pub fn format_sigv4_date_only(dt: &DateTime<Utc>) -> String {
    dt.format("%Y%m%d").to_string()
}

/// Parse an AWS SigV4 date string
pub fn parse_sigv4_date(s: &str) -> Option<DateTime<Utc>> {
    chrono::NaiveDateTime::parse_from_str(s, "%Y%m%dT%H%M%SZ")
        .ok()
        .map(|dt| dt.and_utc())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Datelike, TimeZone, Timelike};

    #[test]
    fn test_format_s3_date() {
        let dt = Utc.with_ymd_and_hms(2024, 1, 15, 12, 30, 45).unwrap();
        let formatted = format_s3_date(&dt);

        assert!(formatted.starts_with("2024-01-15T12:30:45"));
        assert!(formatted.ends_with('Z'));
    }

    #[test]
    fn test_format_http_date() {
        let dt = Utc.with_ymd_and_hms(2024, 1, 15, 12, 30, 45).unwrap();
        let formatted = format_http_date(&dt);

        assert_eq!(formatted, "Mon, 15 Jan 2024 12:30:45 GMT");
    }

    #[test]
    fn test_format_sigv4_date() {
        let dt = Utc.with_ymd_and_hms(2024, 1, 15, 12, 30, 45).unwrap();
        let formatted = format_sigv4_date(&dt);

        assert_eq!(formatted, "20240115T123045Z");
    }

    #[test]
    fn test_format_sigv4_date_only() {
        let dt = Utc.with_ymd_and_hms(2024, 1, 15, 12, 30, 45).unwrap();
        let formatted = format_sigv4_date_only(&dt);

        assert_eq!(formatted, "20240115");
    }

    #[test]
    fn test_parse_sigv4_date() {
        let parsed = parse_sigv4_date("20240115T123045Z");

        assert!(parsed.is_some());
        let dt = parsed.unwrap();
        assert_eq!(dt.year(), 2024);
        assert_eq!(dt.month(), 1);
        assert_eq!(dt.day(), 15);
        assert_eq!(dt.hour(), 12);
        assert_eq!(dt.minute(), 30);
        assert_eq!(dt.second(), 45);
    }

    #[test]
    fn test_parse_invalid_sigv4_date() {
        let parsed = parse_sigv4_date("invalid");
        assert!(parsed.is_none());
    }
}

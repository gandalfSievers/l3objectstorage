use serde::Serialize;
use std::time::{SystemTime, UNIX_EPOCH};

/// Top-level S3 event notification, matching the AWS S3 event notification JSON format.
#[derive(Debug, Clone, Serialize)]
pub struct S3EventNotification {
    #[serde(rename = "Records")]
    pub records: Vec<S3EventRecord>,
}

#[derive(Debug, Clone, Serialize)]
pub struct S3EventRecord {
    #[serde(rename = "eventVersion")]
    pub event_version: String,
    #[serde(rename = "eventSource")]
    pub event_source: String,
    #[serde(rename = "awsRegion")]
    pub aws_region: String,
    #[serde(rename = "eventTime")]
    pub event_time: String,
    #[serde(rename = "eventName")]
    pub event_name: String,
    pub s3: S3Entity,
}

#[derive(Debug, Clone, Serialize)]
pub struct S3Entity {
    pub bucket: S3BucketEntity,
    pub object: S3ObjectEntity,
}

#[derive(Debug, Clone, Serialize)]
pub struct S3BucketEntity {
    pub name: String,
    pub arn: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct S3ObjectEntity {
    pub key: String,
    pub size: u64,
    #[serde(rename = "eTag")]
    pub etag: String,
    #[serde(rename = "versionId", skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
    pub sequencer: String,
}

/// Build an S3 event notification record.
///
/// `event_name` should be the full form like `"s3:ObjectCreated:Put"`.
/// The `s3:` prefix is stripped in the record's `eventName` field.
pub fn build_event(
    region: &str,
    event_name: &str,
    bucket: &str,
    key: &str,
    size: u64,
    etag: &str,
    version_id: Option<&str>,
) -> S3EventNotification {
    let record_event_name = event_name.strip_prefix("s3:").unwrap_or(event_name);

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let event_time = {
        let secs = now.as_secs();
        // Format as ISO 8601 UTC
        let (y, mo, d, h, mi, s) = seconds_to_datetime(secs);
        format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
            y,
            mo,
            d,
            h,
            mi,
            s,
            now.subsec_millis()
        )
    };

    let sequencer = format!("{:016X}", now.as_nanos());

    S3EventNotification {
        records: vec![S3EventRecord {
            event_version: "2.1".to_string(),
            event_source: "aws:s3".to_string(),
            aws_region: region.to_string(),
            event_time,
            event_name: record_event_name.to_string(),
            s3: S3Entity {
                bucket: S3BucketEntity {
                    name: bucket.to_string(),
                    arn: format!("arn:aws:s3:::{}", bucket),
                },
                object: S3ObjectEntity {
                    key: key.to_string(),
                    size,
                    etag: etag.to_string(),
                    version_id: version_id.map(|v| v.to_string()),
                    sequencer,
                },
            },
        }],
    }
}

/// Convert epoch seconds to (year, month, day, hour, minute, second).
fn seconds_to_datetime(epoch: u64) -> (u64, u64, u64, u64, u64, u64) {
    let s = epoch % 60;
    let total_min = epoch / 60;
    let mi = total_min % 60;
    let total_hr = total_min / 60;
    let h = total_hr % 24;
    let mut days = total_hr / 24;

    // Calculate year
    let mut y = 1970u64;
    loop {
        let days_in_year = if is_leap(y) { 366 } else { 365 };
        if days < days_in_year {
            break;
        }
        days -= days_in_year;
        y += 1;
    }

    // Calculate month
    let month_days = if is_leap(y) {
        [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    } else {
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    };
    let mut mo = 1u64;
    for &md in &month_days {
        if days < md {
            break;
        }
        days -= md;
        mo += 1;
    }
    let d = days + 1;

    (y, mo, d, h, mi, s)
}

fn is_leap(y: u64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_event_produces_valid_json_with_expected_fields() {
        let event = build_event(
            "us-east-1",
            "s3:ObjectCreated:Put",
            "my-bucket",
            "photos/pic.jpg",
            12345,
            "abc123",
            Some("v1"),
        );

        let json = serde_json::to_value(&event).unwrap();
        let record = &json["Records"][0];

        assert_eq!(record["eventVersion"], "2.1");
        assert_eq!(record["eventSource"], "aws:s3");
        assert_eq!(record["awsRegion"], "us-east-1");
        assert_eq!(record["eventName"], "ObjectCreated:Put");
        assert_eq!(record["s3"]["bucket"]["name"], "my-bucket");
        assert_eq!(record["s3"]["bucket"]["arn"], "arn:aws:s3:::my-bucket");
        assert_eq!(record["s3"]["object"]["key"], "photos/pic.jpg");
        assert_eq!(record["s3"]["object"]["size"], 12345);
        assert_eq!(record["s3"]["object"]["eTag"], "abc123");
        assert_eq!(record["s3"]["object"]["versionId"], "v1");
        assert!(record["s3"]["object"]["sequencer"].as_str().unwrap().len() > 0);
        assert!(record["eventTime"].as_str().unwrap().ends_with('Z'));
    }

    #[test]
    fn event_name_has_no_s3_prefix() {
        let event = build_event(
            "us-east-1",
            "s3:ObjectRemoved:Delete",
            "bucket",
            "key",
            0,
            "",
            None,
        );

        let json = serde_json::to_value(&event).unwrap();
        let name = json["Records"][0]["eventName"].as_str().unwrap();
        assert_eq!(name, "ObjectRemoved:Delete");
        assert!(!name.starts_with("s3:"));
    }

    #[test]
    fn version_id_none_is_omitted() {
        let event = build_event("us-east-1", "s3:ObjectCreated:Put", "b", "k", 0, "", None);
        let json = serde_json::to_value(&event).unwrap();
        assert!(json["Records"][0]["s3"]["object"]["versionId"].is_null());
    }
}

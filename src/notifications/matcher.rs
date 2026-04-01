use crate::types::bucket::NotificationFilter;

/// Check if an actual event name matches a configured event pattern.
///
/// Supports wildcard suffix matching:
/// - `"s3:ObjectCreated:*"` matches `"s3:ObjectCreated:Put"`
/// - `"s3:*"` matches everything starting with `"s3:"`
/// - Otherwise exact match
pub fn event_matches(configured: &str, actual: &str) -> bool {
    if configured.ends_with(":*") {
        let prefix = &configured[..configured.len() - 1]; // keep the ':'
        actual.starts_with(prefix)
    } else {
        configured == actual
    }
}

/// Check if an object key matches the notification filter rules.
///
/// - No filter or no key filter → always matches
/// - For each `FilterRule`: `"prefix"` checks `key.starts_with(value)`,
///   `"suffix"` checks `key.ends_with(value)`
/// - All rules must match (AND logic)
pub fn filter_matches(filter: &Option<NotificationFilter>, key: &str) -> bool {
    let filter = match filter {
        Some(f) => f,
        None => return true,
    };

    let key_filter = match &filter.key {
        Some(k) => k,
        None => return true,
    };

    for rule in &key_filter.filter_rules {
        match rule.name.to_lowercase().as_str() {
            "prefix" => {
                if !key.starts_with(&rule.value) {
                    return false;
                }
            }
            "suffix" => {
                if !key.ends_with(&rule.value) {
                    return false;
                }
            }
            _ => {}
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::bucket::{FilterRule, NotificationFilterKey};

    #[test]
    fn test_wildcard_match() {
        assert!(event_matches("s3:ObjectCreated:*", "s3:ObjectCreated:Put"));
        assert!(event_matches(
            "s3:ObjectCreated:*",
            "s3:ObjectCreated:CompleteMultipartUpload"
        ));
    }

    #[test]
    fn test_wildcard_mismatch() {
        assert!(!event_matches(
            "s3:ObjectCreated:*",
            "s3:ObjectRemoved:Delete"
        ));
    }

    #[test]
    fn test_exact_match() {
        assert!(event_matches(
            "s3:ObjectCreated:Put",
            "s3:ObjectCreated:Put"
        ));
    }

    #[test]
    fn test_exact_mismatch() {
        assert!(!event_matches(
            "s3:ObjectCreated:Put",
            "s3:ObjectCreated:Copy"
        ));
    }

    #[test]
    fn test_star_matches_everything() {
        assert!(event_matches("s3:*", "s3:ObjectCreated:Put"));
        assert!(event_matches("s3:*", "s3:ObjectRemoved:Delete"));
    }

    #[test]
    fn test_no_filter_matches() {
        assert!(filter_matches(&None, "anything/goes.txt"));
    }

    #[test]
    fn test_empty_key_filter_matches() {
        let filter = Some(NotificationFilter { key: None });
        assert!(filter_matches(&filter, "anything/goes.txt"));
    }

    #[test]
    fn test_prefix_filter() {
        let filter = Some(NotificationFilter {
            key: Some(NotificationFilterKey {
                filter_rules: vec![FilterRule {
                    name: "prefix".to_string(),
                    value: "images/".to_string(),
                }],
            }),
        });
        assert!(filter_matches(&filter, "images/photo.jpg"));
        assert!(!filter_matches(&filter, "docs/readme.txt"));
    }

    #[test]
    fn test_suffix_filter() {
        let filter = Some(NotificationFilter {
            key: Some(NotificationFilterKey {
                filter_rules: vec![FilterRule {
                    name: "suffix".to_string(),
                    value: ".jpg".to_string(),
                }],
            }),
        });
        assert!(filter_matches(&filter, "images/photo.jpg"));
        assert!(!filter_matches(&filter, "images/photo.png"));
    }

    #[test]
    fn test_prefix_and_suffix_filter() {
        let filter = Some(NotificationFilter {
            key: Some(NotificationFilterKey {
                filter_rules: vec![
                    FilterRule {
                        name: "prefix".to_string(),
                        value: "images/".to_string(),
                    },
                    FilterRule {
                        name: "suffix".to_string(),
                        value: ".jpg".to_string(),
                    },
                ],
            }),
        });
        assert!(filter_matches(&filter, "images/photo.jpg"));
        assert!(!filter_matches(&filter, "images/photo.png"));
        assert!(!filter_matches(&filter, "docs/photo.jpg"));
    }
}

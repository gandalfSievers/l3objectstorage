//! Credential management

use serde::{Deserialize, Serialize};

/// AWS-style credentials
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Credentials {
    pub access_key_id: String,
    pub secret_access_key: String,
}

impl Credentials {
    pub fn new(access_key_id: impl Into<String>, secret_access_key: impl Into<String>) -> Self {
        Self {
            access_key_id: access_key_id.into(),
            secret_access_key: secret_access_key.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_credentials_creation() {
        let creds = Credentials::new("access_key", "secret_key");

        assert_eq!(creds.access_key_id, "access_key");
        assert_eq!(creds.secret_access_key, "secret_key");
    }
}

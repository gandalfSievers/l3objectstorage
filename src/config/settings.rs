//! Configuration settings

use base64::Engine;
use std::path::PathBuf;
use std::time::Duration;

/// Server configuration
#[derive(Debug, Clone)]
pub struct Config {
    /// Server host address
    pub host: String,
    /// Server port
    pub port: u16,
    /// Data directory for storage
    pub data_dir: PathBuf,
    /// Default region
    pub region: String,
    /// Access key ID
    pub access_key: String,
    /// Secret access key
    pub secret_key: String,
    /// Whether to require authentication
    pub require_auth: bool,
    /// Master encryption key for SSE-S3 (32 bytes)
    /// Set via LOCAL_S3_ENCRYPTION_KEY env var (hex or base64 encoded)
    pub encryption_key: Option<Vec<u8>>,
    /// Graceful shutdown timeout - how long to wait for in-flight requests
    /// Set via LOCAL_S3_SHUTDOWN_TIMEOUT env var (seconds)
    pub shutdown_timeout: Duration,
    /// SNS endpoint for sending notification events
    /// Set via LOCAL_S3_SNS_ENDPOINT env var
    pub sns_endpoint: Option<String>,
    /// SQS endpoint for sending notification events
    /// Set via LOCAL_S3_SQS_ENDPOINT env var
    pub sqs_endpoint: Option<String>,
    /// Base domain for virtual hosted-style bucket addressing
    /// When set, requests to `<bucket>.<domain>` will extract the bucket from the Host header
    /// Set via LOCAL_S3_DOMAIN env var (e.g., "s3.local")
    pub domain: Option<String>,
}

impl Config {
    /// Create configuration from environment variables
    pub fn from_env() -> Result<Self, ConfigError> {
        // Load .env file if present
        let _ = dotenvy::dotenv();

        let host = std::env::var("LOCAL_S3_HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
        let port = std::env::var("LOCAL_S3_PORT")
            .unwrap_or_else(|_| "9000".to_string())
            .parse()
            .map_err(|_| ConfigError::InvalidPort)?;

        let data_dir = std::env::var("LOCAL_S3_DATA_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("/data"));

        let region = std::env::var("LOCAL_S3_REGION").unwrap_or_else(|_| "us-east-1".to_string());

        let access_key =
            std::env::var("LOCAL_S3_ACCESS_KEY").unwrap_or_else(|_| "localadmin".to_string());
        let secret_key =
            std::env::var("LOCAL_S3_SECRET_KEY").unwrap_or_else(|_| "localadmin".to_string());

        let require_auth = std::env::var("LOCAL_S3_REQUIRE_AUTH")
            .map(|v| v.to_lowercase() == "true" || v == "1")
            .unwrap_or(false);

        // Parse encryption key from env var (hex or base64 encoded, must be 32 bytes)
        let encryption_key = std::env::var("LOCAL_S3_ENCRYPTION_KEY")
            .ok()
            .and_then(|key| {
                // Try hex decode first, then base64
                hex::decode(&key)
                    .or_else(|_| {
                        base64::engine::general_purpose::STANDARD
                            .decode(&key)
                            .map_err(|_| hex::FromHexError::InvalidHexCharacter { c: '?', index: 0 })
                    })
                    .ok()
            })
            .filter(|k| k.len() == 32); // Must be 256 bits (32 bytes)

        let sns_endpoint = std::env::var("LOCAL_S3_SNS_ENDPOINT").ok();
        let sqs_endpoint = std::env::var("LOCAL_S3_SQS_ENDPOINT").ok();
        let domain = std::env::var("LOCAL_S3_DOMAIN").ok();

        // Parse shutdown timeout (default 30 seconds)
        let shutdown_timeout = std::env::var("LOCAL_S3_SHUTDOWN_TIMEOUT")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or_else(|| Duration::from_secs(30));

        Ok(Self {
            host,
            port,
            data_dir,
            region,
            access_key,
            secret_key,
            require_auth,
            encryption_key,
            shutdown_timeout,
            sns_endpoint,
            sqs_endpoint,
            domain,
        })
    }

    /// Create a new configuration with defaults
    pub fn new() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 9000,
            data_dir: PathBuf::from("/data"),
            region: "us-east-1".to_string(),
            access_key: "localadmin".to_string(),
            secret_key: "localadmin".to_string(),
            require_auth: false,
            encryption_key: None,
            shutdown_timeout: Duration::from_secs(30),
            sns_endpoint: None,
            sqs_endpoint: None,
            domain: None,
        }
    }

    /// Set the encryption key for SSE-S3
    pub fn with_encryption_key(mut self, key: Vec<u8>) -> Self {
        if key.len() == 32 {
            self.encryption_key = Some(key);
        }
        self
    }

    /// Set the data directory
    pub fn with_data_dir(mut self, path: impl Into<PathBuf>) -> Self {
        self.data_dir = path.into();
        self
    }

    /// Set the port
    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Set the region
    pub fn with_region(mut self, region: impl Into<String>) -> Self {
        self.region = region.into();
        self
    }

    /// Set authentication credentials
    pub fn with_credentials(
        mut self,
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
    ) -> Self {
        self.access_key = access_key.into();
        self.secret_key = secret_key.into();
        self
    }

    /// Enable or disable authentication
    pub fn with_require_auth(mut self, require_auth: bool) -> Self {
        self.require_auth = require_auth;
        self
    }

    /// Set the graceful shutdown timeout
    pub fn with_shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }

    /// Set the SNS endpoint URL
    pub fn with_sns_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.sns_endpoint = Some(endpoint.into());
        self
    }

    /// Set the SQS endpoint URL
    pub fn with_sqs_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.sqs_endpoint = Some(endpoint.into());
        self
    }

    /// Set the base domain for virtual hosted-style bucket addressing
    pub fn with_domain(mut self, domain: impl Into<String>) -> Self {
        self.domain = Some(domain.into());
        self
    }

    /// Get the socket address for binding
    pub fn socket_addr(&self) -> std::net::SocketAddr {
        format!("{}:{}", self.host, self.port)
            .parse()
            .expect("Invalid socket address")
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::new()
    }
}

/// Configuration errors
#[derive(Debug, Clone)]
pub enum ConfigError {
    InvalidPort,
    InvalidPath,
    MissingRequired(String),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::InvalidPort => write!(f, "Invalid port number"),
            ConfigError::InvalidPath => write!(f, "Invalid path"),
            ConfigError::MissingRequired(key) => write!(f, "Missing required config: {}", key),
        }
    }
}

impl std::error::Error for ConfigError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_defaults() {
        let config = Config::new();

        assert_eq!(config.host, "0.0.0.0");
        assert_eq!(config.port, 9000);
        assert_eq!(config.region, "us-east-1");
        assert_eq!(config.access_key, "localadmin");
        assert_eq!(config.secret_key, "localadmin");
        assert!(!config.require_auth);
        assert_eq!(config.shutdown_timeout, Duration::from_secs(30));
    }

    #[test]
    fn test_config_builder() {
        let config = Config::new()
            .with_port(8080)
            .with_data_dir("/tmp/s3")
            .with_region("eu-west-1")
            .with_credentials("mykey", "mysecret")
            .with_require_auth(true)
            .with_shutdown_timeout(Duration::from_secs(60));

        assert_eq!(config.port, 8080);
        assert_eq!(config.data_dir, PathBuf::from("/tmp/s3"));
        assert_eq!(config.region, "eu-west-1");
        assert_eq!(config.access_key, "mykey");
        assert_eq!(config.secret_key, "mysecret");
        assert!(config.require_auth);
        assert_eq!(config.shutdown_timeout, Duration::from_secs(60));
    }

    #[test]
    fn test_socket_addr() {
        let config = Config::new().with_port(9000);
        let addr = config.socket_addr();

        assert_eq!(addr.port(), 9000);
    }
}

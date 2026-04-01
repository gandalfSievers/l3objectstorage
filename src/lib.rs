//! L3ObjectStorage - A local S3-compatible object storage server
//!
//! This crate provides a fully compatible Amazon S3 API implementation
//! for local development and testing purposes.

pub mod api;
pub mod auth;
pub mod config;
pub mod crypto;
pub mod notifications;
pub mod server;
pub mod storage;
pub mod types;
pub mod utils;

pub use config::Config;
pub use server::Server;
pub use storage::StorageEngine;
pub use types::error::S3Error;

/// Re-export commonly used types
pub mod prelude {
    pub use crate::config::Config;
    pub use crate::server::Server;
    pub use crate::storage::StorageEngine;
    pub use crate::types::error::{S3Error, S3Result};
}

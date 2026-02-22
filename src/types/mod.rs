//! S3 type definitions

pub mod bucket;
pub mod error;
pub mod object;
pub mod response;

pub use bucket::Bucket;
pub use error::{S3Error, S3Result};
pub use object::Object;

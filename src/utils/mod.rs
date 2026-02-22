//! Utility functions

pub mod etag;
pub mod time;
pub mod xml;

pub use etag::calculate_etag;
pub use time::format_s3_date;

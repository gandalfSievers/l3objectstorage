//! Authentication module

mod credentials;
pub mod presigned;
mod sigv4;

pub use credentials::Credentials;
pub use presigned::{has_presigned_params, PresignedUrlParams};
pub use sigv4::SigV4Verifier;

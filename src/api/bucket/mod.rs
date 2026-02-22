//! Bucket API handlers

mod acl;
mod cors;
mod create;
mod delete;
mod encryption;
mod head;
mod lifecycle;
mod list;
mod logging;
mod notification;
mod object_lock;
mod replication;
mod request_payment;
mod ownership_controls;
mod policy;
mod public_access_block;
mod tagging;
mod versioning;
mod website;

pub use acl::{get_bucket_acl, put_bucket_acl};
pub use cors::{delete_bucket_cors, get_bucket_cors, put_bucket_cors};
pub use create::{create_bucket, create_bucket_with_object_lock};
pub use delete::delete_bucket;
pub use encryption::{delete_bucket_encryption, get_bucket_encryption, put_bucket_encryption};
pub use head::head_bucket;
pub use lifecycle::{
    delete_bucket_lifecycle, get_bucket_lifecycle_configuration,
    put_bucket_lifecycle_configuration,
};
pub use list::{get_bucket, list_buckets};
pub use logging::{get_bucket_logging, put_bucket_logging};
pub use notification::{get_bucket_notification_configuration, put_bucket_notification_configuration};
pub use object_lock::{get_object_lock_configuration, put_object_lock_configuration};
pub use replication::{delete_bucket_replication, get_bucket_replication, put_bucket_replication};
pub use request_payment::{get_bucket_request_payment, put_bucket_request_payment};
pub use ownership_controls::{
    delete_bucket_ownership_controls, get_bucket_ownership_controls,
    put_bucket_ownership_controls,
};
pub use policy::{delete_bucket_policy, get_bucket_policy, get_bucket_policy_status, put_bucket_policy};
pub use public_access_block::{
    delete_public_access_block, get_public_access_block, put_public_access_block,
};
pub use tagging::{delete_bucket_tagging, get_bucket_tagging, put_bucket_tagging};
pub use versioning::{get_bucket_versioning, put_bucket_versioning};
pub use website::{delete_bucket_website, get_bucket_website, put_bucket_website};

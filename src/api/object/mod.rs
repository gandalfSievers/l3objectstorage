//! Object API handlers

mod acl;
mod attributes;
mod delete;
mod delete_batch;
mod get;
mod head;
mod lock;
mod put;
mod rename;
mod select;
mod tagging;
mod versions;

pub use acl::{get_object_acl, put_object_acl};
pub use attributes::get_object_attributes;
pub use delete::{delete_object, delete_object_versioned, delete_object_versioned_with_bypass};
pub use delete_batch::delete_objects;
pub use get::{get_object, get_object_full, get_object_versioned, get_object_with_conditionals, ResponseHeaderOverrides};
pub use head::{head_object, head_object_conditional, head_object_versioned};
pub use lock::{
    get_object_legal_hold, get_object_retention, put_object_legal_hold, put_object_retention,
};
pub use put::{put_object, put_object_conditional, put_object_conditional_with_metadata, put_object_versioned};
pub use rename::rename_object;
pub use select::select_object_content;
pub use tagging::{delete_object_tagging, get_object_tagging, put_object_tagging};
pub use versions::list_object_versions;

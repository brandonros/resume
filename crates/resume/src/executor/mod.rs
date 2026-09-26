//! Claimed attempts, durable step transactions, and generic resource locks.

mod job;
mod lock;

pub use job::Job;
pub use lock::lock_resource;

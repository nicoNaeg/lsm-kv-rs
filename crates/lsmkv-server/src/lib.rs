//! Redis wire protocol server over the LSM-tree engine.
//!
//! The protocol lives here as a library so it can be exercised on raw bytes,
//! without a socket and without a store behind it.

pub mod resp;

//! Redis wire protocol server over the LSM-tree engine.
//!
//! The protocol and the command set live here as a library, so both can be
//! exercised on raw bytes, without a socket and without a store behind them.

pub mod command;
pub mod resp;
pub mod server;

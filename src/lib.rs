//! Rust SDK for S2.

#![warn(missing_docs)]

#[rustfmt::skip]
mod api;

mod append_session;
mod service;

pub mod batching;
pub mod client;
pub mod types;

pub use client::{BasinClient, Client, ClientConfig, StreamClient};
pub use service::Streaming;

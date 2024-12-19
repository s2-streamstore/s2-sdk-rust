//! Rust SDK for S2.
//!
//! The Rust SDK provides ergonomic wrappers and utilities to interact with the
//! [S2 API](https://s2.dev/docs/interface/grpc-api).
//!
//! # Getting started
//!
//! 1. Ensure you have `tokio` added as a dependency. The SDK relies on [Tokio](https://crates.io/crates/tokio)
//!    for executing async code.
//!
//!    ```bash
//!    cargo add tokio --features full
//!    ```
//!
//! 1. Add the `streamstore` dependency to your project:
//!
//!    ```bash
//!    cargo add streamstore
//!    ```
//!
//! 1. Generate an authentication token by logging onto the web console at [s2.dev](https://s2.dev/dashboard).
//!
//! 1. Make a request using SDK client.
//!
//!    ```no_run
//!    # let _ = async move {
//!    let config = s2::ClientConfig::new("<YOUR AUTH TOKEN>");
//!    let client = s2::Client::new(config);
//!
//!    let basins = client.list_basins(Default::default()).await?;
//!    println!("My basins: {:?}", basins);
//!    # return Ok::<(), s2::client::ClientError>(()); };
//!    ```
//!
//! See documentation for the [`client`] module for more information on how to
//! use the client, and what requests can be made.
//!
//! # Examples
//!
//! We have curated a bunch of examples in the
//! [SDK repository](https://github.com/s2-streamstore/s2-sdk-rust/tree/main/examples)
//! demonstrating how to use the SDK effectively:
//!
//! * [List all basins](https://github.com/s2-streamstore/s2-sdk-rust/blob/main/examples/list_all_basins.rs)
//! * [Explicit stream trimming](https://github.com/s2-streamstore/s2-sdk-rust/blob/main/examples/explicit_trim.rs)
//! * [Producer (with concurrency control)](https://github.com/s2-streamstore/s2-sdk-rust/blob/main/examples/producer.rs)
//! * [Consumer](https://github.com/s2-streamstore/s2-sdk-rust/blob/main/examples/consumer.rs)
//! * and many more...
//!
//! This documentation is generated using
//! [`rustdoc-scrape-examples`](https://doc.rust-lang.org/rustdoc/scraped-examples.html),
//! so you will be able to see snippets from examples right here in the
//! documentation.
//!
//! # Feedback
//!
//! We use [Github Issues](https://github.com/s2-streamstore/s2-sdk-rust/issues)
//! to track feature requests and issues with the SDK. If you wish to provide
//! feedback, report a bug or request a feature, feel free to open a Github
//! issue.
//!
//! # Quick Links
//!
//! * [S2 Website](https://s2.dev)
//! * [S2 Documentation](https://s2.dev/docs)
//! * [CHANGELOG](https://github.com/s2-streamstore/s2-sdk-rust/blob/main/CHANGELOG.md)

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

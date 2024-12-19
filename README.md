<div align="center">
  <p>
    <!-- Light mode logo -->
    <a href="https://s2.dev#gh-light-mode-only">
      <img src="./assets/s2-black.png" height="60">
    </a>
    <!-- Dark mode logo -->
    <a href="https://s2.dev#gh-dark-mode-only">
      <img src="./assets/s2-white.png" height="60">
    </a>
  </p>

  <h1>Rust SDK for S2</h1>

  <p>
    <!-- Crates.io -->
    <a href="https://crates.io/crates/streamstore"><img src="https://img.shields.io/crates/v/streamstore.svg" /></a>
    <!-- Docs.rs -->
    <a href="https://docs.rs/streamstore/latest/streamstore/"><img src="https://img.shields.io/docsrs/streamstore" /></a>
    <!-- Github Actions (CI) -->
    <a href="https://github.com/s2-streamstore/s2-sdk-rust/actions?query=branch%3Amain++"><img src="https://github.com/s2-streamstore/s2-sdk-rust/actions/workflows/ci.yml/badge.svg" /></a>
    <!-- Discord (chat) -->
    <a href="https://discord.gg/vTCs7kMkAf"><img src="https://img.shields.io/discord/1209937852528599092?logo=discord" /></a>
    <!-- LICENSE -->
    <a href="./LICENSE"><img src="https://img.shields.io/github/license/s2-streamstore/s2-sdk-rust" /></a>
  </p>
</div>

The Rust SDK provides ergonomic wrappers and utilities to interact with the
[S2 API](https://buf.build/streamstore/s2/docs/main:s2.v1alpha).

## Getting started

1. Ensure you have `tokio` added as a dependency. The SDK relies on
   [Tokio](https://crates.io/crates/tokio) for executing async code.
   ```bash
   cargo add tokio --features full
   ```

1. Add the `streamstore` dependency to your project:
   ```bash
   cargo add streamstore
   ```

1. Generate an authentication token by logging onto the web console at
   [s2.dev](https://s2.dev/dashboard).

1. Make a request using SDK client.
   ```rust
   use streamstore::client::{Client, ClientConfig};
   use streamstore::types::ListBasinsRequest;

   #[tokio::main]
   async fn main() -> Result<(), Box<dyn std::error::Error>> {
       let config = ClientConfig::new("<YOUR AUTH TOKEN>");
       let client = Client::new(config);

       let basins = client.list_basins(ListBasinsRequest::new()).await?;
       println!("My basins: {:?}", basins);

       Ok(())
   }
   ```

## Examples

The [`examples`](./examples) directory in this repository contains a variety of
example use cases demonstrating how to use the SDK effectively.

Run any example using the following command:

```bash
export S2_AUTH_TOKEN="<YOUR AUTH TOKEN>"
cargo run --example <example_name>
```

> [!NOTE]
> You might want to update the basin name in the example before running since
> basin names are globally unique and each example uses the same basin name
> (`"my-favorite-basin"`).

## SDK Docs and Reference

Head over to [docs.rs](https://docs.rs/streamstore/latest/streamstore/) for
detailed documentation and crate reference.

## Feedback

We use [Github Issues](https://github.com/s2-streamstore/s2-sdk-rust/issues) to
track feature requests and issues with the SDK. If you wish to provide feedback,
report a bug or request a feature, feel free to open a Github issue.

### Contributing

Developers are welcome to submit Pull Requests on the repository. If there is
no tracking issue for the bug or feature request corresponding to the PR, we
encourage you to open one for discussion before submitting the PR.

## Reach out to us

Join our [Discord](https://discord.gg/vTCs7kMkAf) server. We would love to hear
from you.

You can also email us at [hi@s2.dev](mailto:hi@s2.dev).

## License

This project is licensed under the [Apache-2.0 License](./LICENSE).

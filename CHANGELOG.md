# Changelog

All notable changes to this project will be documented in this file.

## [0.11.0] - 2025-04-15

### Features

- Access token methods [S2-758] ([#163](https://github.com/s2-streamstore/s2-sdk-rust/issues/163))

## [0.10.0] - 2025-03-19

### Features

- [**breaking**] Timestamped records  ([#157](https://github.com/s2-streamstore/s2-sdk-rust/issues/157))

## [0.9.0] - 2025-03-12

### Features

- Configurable option for compression ([#151](https://github.com/s2-streamstore/s2-sdk-rust/issues/151))

### Miscellaneous Tasks

- Upgrade proto ([#153](https://github.com/s2-streamstore/s2-sdk-rust/issues/153))
- Proto update ([#154](https://github.com/s2-streamstore/s2-sdk-rust/issues/154))

## [0.8.2] - 2025-02-07

### Bug Fixes

- Retry CANCELLED gRPC status code ([#149](https://github.com/s2-streamstore/s2-sdk-rust/issues/149))

## [0.8.1] - 2025-02-05

### Features

- Enable compression ([#147](https://github.com/s2-streamstore/s2-sdk-rust/issues/147))

## [0.8.0] - 2025-01-21

### Bug Fixes

- Respect limits with read session resumption ([#139](https://github.com/s2-streamstore/s2-sdk-rust/issues/139))

### Miscellaneous Tasks

- Make `with_limit()` take option ([#145](https://github.com/s2-streamstore/s2-sdk-rust/issues/145))

## [0.7.0] - 2025-01-16

### Miscellaneous Tasks

- Update proto ([#135](https://github.com/s2-streamstore/s2-sdk-rust/issues/135))

## [0.6.0] - 2025-01-13

### Documentation

- Update README link for docs.rs ([#128](https://github.com/s2-streamstore/s2-sdk-rust/issues/128))

### Miscellaneous Tasks

- Update proto ([#129](https://github.com/s2-streamstore/s2-sdk-rust/issues/129))
- Default impl for AppendInput ([#130](https://github.com/s2-streamstore/s2-sdk-rust/issues/130))
- Update protos ([#133](https://github.com/s2-streamstore/s2-sdk-rust/issues/133))

## [0.5.1] - 2024-12-20

### Documentation

- Update S2 doc links ([#126](https://github.com/s2-streamstore/s2-sdk-rust/issues/126))

## [0.5.0] - 2024-12-19

### Documentation

- `batching` module Rust docs ([#119](https://github.com/s2-streamstore/s2-sdk-rust/issues/119))
- Update basin and stream names ([#122](https://github.com/s2-streamstore/s2-sdk-rust/issues/122))
- Update README API link ([#123](https://github.com/s2-streamstore/s2-sdk-rust/issues/123))
- `s2::client` ([#121](https://github.com/s2-streamstore/s2-sdk-rust/issues/121))
- Crate level documentation ([#124](https://github.com/s2-streamstore/s2-sdk-rust/issues/124))

### Miscellaneous Tasks

- Rename `[lib]` to `s2` ([#120](https://github.com/s2-streamstore/s2-sdk-rust/issues/120))
- *(release)* 0.5.0

## [0.4.1] - 2024-12-17

### Bug Fixes

- Deadlock potential due to `await`s in append_session's `select!` loop ([#115](https://github.com/s2-streamstore/s2-sdk-rust/issues/115))

## [0.4.0] - 2024-12-16

### Features

- Add `FencingToken::generate` method ([#110](https://github.com/s2-streamstore/s2-sdk-rust/issues/110))
- Return `StreamInfo` from `BasinClient::create_stream` ([#114](https://github.com/s2-streamstore/s2-sdk-rust/issues/114))

### Miscellaneous Tasks

- Remove `GH_TOKEN` use to clone submodule in CI ([#109](https://github.com/s2-streamstore/s2-sdk-rust/issues/109))
- Proto up-to-date check ([#112](https://github.com/s2-streamstore/s2-sdk-rust/issues/112))
- Upgrade proto ([#111](https://github.com/s2-streamstore/s2-sdk-rust/issues/111))
- Add examples for API ([#113](https://github.com/s2-streamstore/s2-sdk-rust/issues/113))
- Add `README.md` ([#116](https://github.com/s2-streamstore/s2-sdk-rust/issues/116))

## [0.3.1] - 2024-12-12

### Miscellaneous Tasks

- Switch on `missing_docs` ([#106](https://github.com/s2-streamstore/s2-sdk-rust/issues/106))

## [0.3.0] - 2024-12-11

### Features

- Return reconfigured stream ([#95](https://github.com/s2-streamstore/s2-sdk-rust/issues/95))
- Implement `SequencedRecord::as_command_record` ([#96](https://github.com/s2-streamstore/s2-sdk-rust/issues/96))
- Make protoc requirement optional ([#103](https://github.com/s2-streamstore/s2-sdk-rust/issues/103))

### Bug Fixes

- Tonic-side-effect version ([#102](https://github.com/s2-streamstore/s2-sdk-rust/issues/102))

### Miscellaneous Tasks

- Update proto and associated types for non-optional `start_seq_num` ([#97](https://github.com/s2-streamstore/s2-sdk-rust/issues/97))
- `CommandRecord::Fence` requires `FencingToken` even if empty ([#98](https://github.com/s2-streamstore/s2-sdk-rust/issues/98))
- Rm serde ([#99](https://github.com/s2-streamstore/s2-sdk-rust/issues/99))
- Lower `max_append_inflight_bytes` default
- Move sync_docs to separate repository ([#101](https://github.com/s2-streamstore/s2-sdk-rust/issues/101))

## [0.2.0] - 2024-12-04

### Features

- Redo endpoint logic ([#39](https://github.com/s2-streamstore/s2-sdk-rust/issues/39)) ([#40](https://github.com/s2-streamstore/s2-sdk-rust/issues/40))
- Metered_size impl ([#51](https://github.com/s2-streamstore/s2-sdk-rust/issues/51))
- Allow custom tonic connectors & expose more errors ([#54](https://github.com/s2-streamstore/s2-sdk-rust/issues/54))
- Implement lingering for append record stream ([#55](https://github.com/s2-streamstore/s2-sdk-rust/issues/55))
- Read session resumption ([#64](https://github.com/s2-streamstore/s2-sdk-rust/issues/64))
- Pre-validate append record batch ([#72](https://github.com/s2-streamstore/s2-sdk-rust/issues/72))
- Retryable `append_session` + side-effect logic
- Validate fencing token length ([#87](https://github.com/s2-streamstore/s2-sdk-rust/issues/87))
- S2_request_token header (exercise idempotence) ([#86](https://github.com/s2-streamstore/s2-sdk-rust/issues/86))

### Bug Fixes

- Only connect lazily (remove option to connect eagerly) ([#49](https://github.com/s2-streamstore/s2-sdk-rust/issues/49))
- Update `HostEndpoints::from_env` with new spec ([#58](https://github.com/s2-streamstore/s2-sdk-rust/issues/58))
- Add Send bound to Streaming wrapper ([#63](https://github.com/s2-streamstore/s2-sdk-rust/issues/63))
- Validate append input when converting from sdk type to api ([#65](https://github.com/s2-streamstore/s2-sdk-rust/issues/65))
- Limit retries when read resumes but stream keeps erroring ([#66](https://github.com/s2-streamstore/s2-sdk-rust/issues/66))
- Retry on deadline exceeded ([#67](https://github.com/s2-streamstore/s2-sdk-rust/issues/67))
- Remove `ConnectionError` in favour of pre-processing ([#68](https://github.com/s2-streamstore/s2-sdk-rust/issues/68))
- Rename 'max_retries' to 'max_attempts'
- Validate `types::AppendRecord` for metered size ([#79](https://github.com/s2-streamstore/s2-sdk-rust/issues/79))
- Adapt to recent gRPC interface updates ([#84](https://github.com/s2-streamstore/s2-sdk-rust/issues/84))
- Use `if_exists` for delete basin/stream ([#85](https://github.com/s2-streamstore/s2-sdk-rust/issues/85))
- `append_session` inner loop while condition ([#91](https://github.com/s2-streamstore/s2-sdk-rust/issues/91))

### Documentation

- ConnectError

### Testing

- `fencing_token` and `match_seq_num` for `AppendRecordsBatchStream` ([#77](https://github.com/s2-streamstore/s2-sdk-rust/issues/77))

### Miscellaneous Tasks

- Rename `ClientError` to `ConnectError` ([#47](https://github.com/s2-streamstore/s2-sdk-rust/issues/47))
- Make `ReadLimit` fields pub
- Add clippy to CI ([#50](https://github.com/s2-streamstore/s2-sdk-rust/issues/50))
- Expose Aborted as an error variant ([#52](https://github.com/s2-streamstore/s2-sdk-rust/issues/52))
- Expose tonic Internal error message ([#53](https://github.com/s2-streamstore/s2-sdk-rust/issues/53))
- Refactor errors to return tonic::Status ([#57](https://github.com/s2-streamstore/s2-sdk-rust/issues/57))
- Conversion from `HostCloud` for `HostEndpoints` ([#59](https://github.com/s2-streamstore/s2-sdk-rust/issues/59))
- Rm unneeded async ([#62](https://github.com/s2-streamstore/s2-sdk-rust/issues/62))
- Create LICENSE
- Update Cargo.toml with license
- Update license for sync_docs
- Add expect messages instead of unwraps ([#69](https://github.com/s2-streamstore/s2-sdk-rust/issues/69))
- Make `ClientConfig` fields private + revise docs ([#73](https://github.com/s2-streamstore/s2-sdk-rust/issues/73))
- Whoops, max_attempts -> with_max_attempts
- Endpoints re-rejig ([#70](https://github.com/s2-streamstore/s2-sdk-rust/issues/70))
- Add back `S2Endpoints::from_env()` ([#74](https://github.com/s2-streamstore/s2-sdk-rust/issues/74))
- Example from_env
- Assertions instead of errors for batch capacity & size ([#75](https://github.com/s2-streamstore/s2-sdk-rust/issues/75))
- Simplify s2_request_token creation
- Remove `bytesize` dependency ([#89](https://github.com/s2-streamstore/s2-sdk-rust/issues/89))
- Update proto ([#93](https://github.com/s2-streamstore/s2-sdk-rust/issues/93))

## [0.1.0] - 2024-11-06

### Features

- Implement `BasinService/{ListStreams, GetBasinConfig}` ([#3](https://github.com/s2-streamstore/s2-sdk-rust/issues/3))
- Implement `BasinService/{CreateStream, GetStreamConfig}` ([#8](https://github.com/s2-streamstore/s2-sdk-rust/issues/8))
- Implement `AccountService/{ListBasins, DeleteBasin}` ([#10](https://github.com/s2-streamstore/s2-sdk-rust/issues/10))
- Implement `BasinService` ([#12](https://github.com/s2-streamstore/s2-sdk-rust/issues/12))
- Add request timeout ([#14](https://github.com/s2-streamstore/s2-sdk-rust/issues/14))
- Display impl for types::BasinState ([#18](https://github.com/s2-streamstore/s2-sdk-rust/issues/18))
- Implement `StreamService` (complete) ([#16](https://github.com/s2-streamstore/s2-sdk-rust/issues/16))
- Implement `AppendRecordStream` with batching support ([#24](https://github.com/s2-streamstore/s2-sdk-rust/issues/24))
- Enable tls config and make connection uri depend on env ([#25](https://github.com/s2-streamstore/s2-sdk-rust/issues/25))
- Doc reuse ([#32](https://github.com/s2-streamstore/s2-sdk-rust/issues/32))
- Support for overriding user-agent ([#33](https://github.com/s2-streamstore/s2-sdk-rust/issues/33))
- Sync rpc - sdk wrapper docs ([#34](https://github.com/s2-streamstore/s2-sdk-rust/issues/34))

### Bug Fixes

- Use usize for ListBasins ([#15](https://github.com/s2-streamstore/s2-sdk-rust/issues/15))
- Make all errors public ([#20](https://github.com/s2-streamstore/s2-sdk-rust/issues/20))

### Miscellaneous Tasks

- Update proto submodule ([#7](https://github.com/s2-streamstore/s2-sdk-rust/issues/7))
- Replace url with http::uri::Uri ([#6](https://github.com/s2-streamstore/s2-sdk-rust/issues/6))
- Update `HAS_NO_SIDE_EFFECTS` to `IDEMPOTENCY_LEVEL` ([#11](https://github.com/s2-streamstore/s2-sdk-rust/issues/11))
- FromStr impl to convert str to StorageClass enum ([#13](https://github.com/s2-streamstore/s2-sdk-rust/issues/13))
- Move `get_basin_config`, `reconfigure_basin` to `AccountService` ([#17](https://github.com/s2-streamstore/s2-sdk-rust/issues/17))
- Remove usage of deprecated `tonic_build` method ([#21](https://github.com/s2-streamstore/s2-sdk-rust/issues/21))
- Add+feature-gate serde Serialize/Deserialize derives ([#22](https://github.com/s2-streamstore/s2-sdk-rust/issues/22))
- Deps
- Updated repo for proto submodule ([#38](https://github.com/s2-streamstore/s2-sdk-rust/issues/38))
- Http2 adaptive window [S2-412] ([#41](https://github.com/s2-streamstore/s2-sdk-rust/issues/41))
- Add CI action ([#44](https://github.com/s2-streamstore/s2-sdk-rust/issues/44))
- Add release action ([#45](https://github.com/s2-streamstore/s2-sdk-rust/issues/45))

<!-- generated by git-cliff -->

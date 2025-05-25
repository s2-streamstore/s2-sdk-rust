//! Types for interacting with S2 services.

use std::{collections::HashSet, ops::Deref, str::FromStr, sync::OnceLock, time::Duration};

use bytes::Bytes;
use rand::Rng;
use regex::Regex;
use sync_docs::sync_docs;

use crate::api;

pub(crate) const MIB_BYTES: u64 = 1024 * 1024;
pub(crate) const RETRY_AFTER_MS_METADATA_KEY: &str = "retry-after-ms";

/// Error related to conversion from one type to another.
#[derive(Debug, Clone, thiserror::Error)]
#[error("{0}")]
pub struct ConvertError(String);

impl<T: Into<String>> From<T> for ConvertError {
    fn from(value: T) -> Self {
        Self(value.into())
    }
}

/// Metered size of the object in bytes.
///
/// Bytes are calculated using the "metered bytes" formula:
///
/// ```python
/// metered_bytes = lambda record: 8 + 2 * len(record.headers) + sum((len(h.key) + len(h.value)) for h in record.headers) + len(record.body)
/// ```
pub trait MeteredBytes {
    /// Return the metered bytes of the object.
    fn metered_bytes(&self) -> u64;
}

impl<T: MeteredBytes> MeteredBytes for Vec<T> {
    fn metered_bytes(&self) -> u64 {
        self.iter().fold(0, |acc, item| acc + item.metered_bytes())
    }
}

macro_rules! metered_impl {
    ($ty:ty) => {
        impl MeteredBytes for $ty {
            fn metered_bytes(&self) -> u64 {
                let bytes = 8
                    + (2 * self.headers.len())
                    + self
                        .headers
                        .iter()
                        .map(|h| h.name.len() + h.value.len())
                        .sum::<usize>()
                    + self.body.len();
                bytes as u64
            }
        }
    };
}

#[sync_docs]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BasinScope {
    AwsUsEast1,
}

impl From<BasinScope> for api::BasinScope {
    fn from(value: BasinScope) -> Self {
        match value {
            BasinScope::AwsUsEast1 => Self::AwsUsEast1,
        }
    }
}

impl From<api::BasinScope> for Option<BasinScope> {
    fn from(value: api::BasinScope) -> Self {
        match value {
            api::BasinScope::Unspecified => None,
            api::BasinScope::AwsUsEast1 => Some(BasinScope::AwsUsEast1),
        }
    }
}

impl FromStr for BasinScope {
    type Err = ConvertError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "aws:us-east-1" => Ok(Self::AwsUsEast1),
            _ => Err("invalid basin scope value".into()),
        }
    }
}

#[sync_docs]
#[derive(Debug, Clone)]
pub struct CreateBasinRequest {
    pub basin: BasinName,
    pub config: Option<BasinConfig>,
    pub scope: Option<BasinScope>,
}

impl CreateBasinRequest {
    /// Create a new request with basin name.
    pub fn new(basin: BasinName) -> Self {
        Self {
            basin,
            config: None,
            scope: None,
        }
    }

    /// Overwrite basin configuration.
    pub fn with_config(self, config: BasinConfig) -> Self {
        Self {
            config: Some(config),
            ..self
        }
    }

    /// Overwrite basin scope.
    pub fn with_scope(self, scope: BasinScope) -> Self {
        Self {
            scope: Some(scope),
            ..self
        }
    }
}

impl From<CreateBasinRequest> for api::CreateBasinRequest {
    fn from(value: CreateBasinRequest) -> Self {
        let CreateBasinRequest {
            basin,
            config,
            scope,
        } = value;
        Self {
            basin: basin.0,
            config: config.map(Into::into),
            scope: scope.map(api::BasinScope::from).unwrap_or_default().into(),
        }
    }
}

#[sync_docs]
#[derive(Debug, Clone, Default)]
pub struct BasinConfig {
    pub default_stream_config: Option<StreamConfig>,
    pub create_stream_on_append: bool,
    pub create_stream_on_read: bool,
}

impl BasinConfig {
    /// Create a new basin config.
    pub fn new() -> Self {
        Self::default()
    }

    /// Overwrite the default stream config.
    pub fn with_default_stream_config(self, default_stream_config: StreamConfig) -> Self {
        Self {
            default_stream_config: Some(default_stream_config),
            ..self
        }
    }

    /// Overwrite `create_stream_on_append`.
    pub fn with_create_stream_on_append(self, create_stream_on_append: bool) -> Self {
        Self {
            create_stream_on_append,
            ..self
        }
    }

    /// Overwrite `create_stream_on_read`.
    pub fn with_create_stream_on_read(self, create_stream_on_read: bool) -> Self {
        Self {
            create_stream_on_read,
            ..self
        }
    }
}

impl From<BasinConfig> for api::BasinConfig {
    fn from(value: BasinConfig) -> Self {
        let BasinConfig {
            default_stream_config,
            create_stream_on_append,
            create_stream_on_read,
        } = value;
        Self {
            default_stream_config: default_stream_config.map(Into::into),
            create_stream_on_append,
            create_stream_on_read,
        }
    }
}

impl From<api::BasinConfig> for BasinConfig {
    fn from(value: api::BasinConfig) -> Self {
        let api::BasinConfig {
            default_stream_config,
            create_stream_on_append,
            create_stream_on_read,
        } = value;
        Self {
            default_stream_config: default_stream_config.map(Into::into),
            create_stream_on_append,
            create_stream_on_read,
        }
    }
}

#[sync_docs]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimestampingMode {
    ClientPrefer,
    ClientRequire,
    Arrival,
}

impl From<TimestampingMode> for api::TimestampingMode {
    fn from(value: TimestampingMode) -> Self {
        match value {
            TimestampingMode::ClientPrefer => Self::ClientPrefer,
            TimestampingMode::ClientRequire => Self::ClientRequire,
            TimestampingMode::Arrival => Self::Arrival,
        }
    }
}

impl From<api::TimestampingMode> for Option<TimestampingMode> {
    fn from(value: api::TimestampingMode) -> Self {
        match value {
            api::TimestampingMode::Unspecified => None,
            api::TimestampingMode::ClientPrefer => Some(TimestampingMode::ClientPrefer),
            api::TimestampingMode::ClientRequire => Some(TimestampingMode::ClientRequire),
            api::TimestampingMode::Arrival => Some(TimestampingMode::Arrival),
        }
    }
}

#[sync_docs(TimestampingConfig = "Timestamping")]
#[derive(Debug, Clone, Default)]
/// Timestamping behavior.
pub struct TimestampingConfig {
    pub mode: Option<TimestampingMode>,
    pub uncapped: Option<bool>,
}

impl TimestampingConfig {
    /// Create a new timestamping config.
    pub fn new() -> Self {
        Self::default()
    }

    /// Overwrite timestamping mode.
    pub fn with_mode(self, mode: TimestampingMode) -> Self {
        Self {
            mode: Some(mode),
            ..self
        }
    }

    /// Overwrite the uncapped knob.
    pub fn with_uncapped(self, uncapped: bool) -> Self {
        Self {
            uncapped: Some(uncapped),
            ..self
        }
    }
}

impl From<TimestampingConfig> for api::stream_config::Timestamping {
    fn from(value: TimestampingConfig) -> Self {
        Self {
            mode: value
                .mode
                .map(api::TimestampingMode::from)
                .unwrap_or_default()
                .into(),
            uncapped: value.uncapped,
        }
    }
}

impl From<api::stream_config::Timestamping> for TimestampingConfig {
    fn from(value: api::stream_config::Timestamping) -> Self {
        let mode = value.mode().into();
        let uncapped = value.uncapped;
        Self { mode, uncapped }
    }
}

#[sync_docs]
#[derive(Debug, Clone, Default)]
pub struct StreamConfig {
    pub storage_class: Option<StorageClass>,
    pub retention_policy: Option<RetentionPolicy>,
    pub timestamping: Option<TimestampingConfig>,
}

impl StreamConfig {
    /// Create a new stream config.
    pub fn new() -> Self {
        Self::default()
    }

    /// Overwrite storage class.
    pub fn with_storage_class(self, storage_class: StorageClass) -> Self {
        Self {
            storage_class: Some(storage_class),
            ..self
        }
    }

    /// Overwrite retention policy.
    pub fn with_retention_policy(self, retention_policy: RetentionPolicy) -> Self {
        Self {
            retention_policy: Some(retention_policy),
            ..self
        }
    }

    /// Overwrite timestamping config.
    pub fn with_timestamping(self, timestamping: TimestampingConfig) -> Self {
        Self {
            timestamping: Some(timestamping),
            ..self
        }
    }
}

impl From<StreamConfig> for api::StreamConfig {
    fn from(value: StreamConfig) -> Self {
        let StreamConfig {
            storage_class,
            retention_policy,
            timestamping,
        } = value;
        Self {
            storage_class: storage_class
                .map(api::StorageClass::from)
                .unwrap_or_default()
                .into(),
            retention_policy: retention_policy.map(Into::into),
            timestamping: timestamping.map(Into::into),
        }
    }
}

impl From<api::StreamConfig> for StreamConfig {
    fn from(value: api::StreamConfig) -> Self {
        Self {
            storage_class: value.storage_class().into(),
            retention_policy: value.retention_policy.map(Into::into),
            timestamping: value.timestamping.map(Into::into),
        }
    }
}

#[sync_docs]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageClass {
    Standard,
    Express,
}

impl From<StorageClass> for api::StorageClass {
    fn from(value: StorageClass) -> Self {
        match value {
            StorageClass::Standard => Self::Standard,
            StorageClass::Express => Self::Express,
        }
    }
}

impl From<api::StorageClass> for Option<StorageClass> {
    fn from(value: api::StorageClass) -> Self {
        match value {
            api::StorageClass::Unspecified => None,
            api::StorageClass::Standard => Some(StorageClass::Standard),
            api::StorageClass::Express => Some(StorageClass::Express),
        }
    }
}

impl FromStr for StorageClass {
    type Err = ConvertError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "standard" => Ok(Self::Standard),
            "express" => Ok(Self::Express),
            v => Err(format!("unknown storage class: {v}").into()),
        }
    }
}

#[sync_docs(Age = "Age")]
#[derive(Debug, Clone)]
pub enum RetentionPolicy {
    Age(Duration),
}

impl From<RetentionPolicy> for api::stream_config::RetentionPolicy {
    fn from(value: RetentionPolicy) -> Self {
        match value {
            RetentionPolicy::Age(duration) => Self::Age(duration.as_secs()),
        }
    }
}

impl From<api::stream_config::RetentionPolicy> for RetentionPolicy {
    fn from(value: api::stream_config::RetentionPolicy) -> Self {
        match value {
            api::stream_config::RetentionPolicy::Age(secs) => Self::Age(Duration::from_secs(secs)),
        }
    }
}

#[sync_docs]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BasinState {
    Active,
    Creating,
    Deleting,
}

impl From<BasinState> for api::BasinState {
    fn from(value: BasinState) -> Self {
        match value {
            BasinState::Active => Self::Active,
            BasinState::Creating => Self::Creating,
            BasinState::Deleting => Self::Deleting,
        }
    }
}

impl From<api::BasinState> for Option<BasinState> {
    fn from(value: api::BasinState) -> Self {
        match value {
            api::BasinState::Unspecified => None,
            api::BasinState::Active => Some(BasinState::Active),
            api::BasinState::Creating => Some(BasinState::Creating),
            api::BasinState::Deleting => Some(BasinState::Deleting),
        }
    }
}

impl std::fmt::Display for BasinState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BasinState::Active => write!(f, "active"),
            BasinState::Creating => write!(f, "creating"),
            BasinState::Deleting => write!(f, "deleting"),
        }
    }
}

#[sync_docs]
#[derive(Debug, Clone)]
pub struct BasinInfo {
    pub name: String,
    pub scope: Option<BasinScope>,
    pub state: Option<BasinState>,
}

impl From<BasinInfo> for api::BasinInfo {
    fn from(value: BasinInfo) -> Self {
        let BasinInfo { name, scope, state } = value;
        Self {
            name,
            scope: scope.map(api::BasinScope::from).unwrap_or_default().into(),
            state: state.map(api::BasinState::from).unwrap_or_default().into(),
        }
    }
}

impl From<api::BasinInfo> for BasinInfo {
    fn from(value: api::BasinInfo) -> Self {
        let scope = value.scope().into();
        let state = value.state().into();
        let name = value.name;
        Self { name, scope, state }
    }
}

impl TryFrom<api::CreateBasinResponse> for BasinInfo {
    type Error = ConvertError;
    fn try_from(value: api::CreateBasinResponse) -> Result<Self, Self::Error> {
        let api::CreateBasinResponse { info } = value;
        let info = info.ok_or("missing basin info")?;
        Ok(info.into())
    }
}

#[sync_docs]
#[derive(Debug, Clone, Default)]
pub struct ListStreamsRequest {
    pub prefix: String,
    pub start_after: String,
    pub limit: Option<usize>,
}

impl ListStreamsRequest {
    /// Create a new request.
    pub fn new() -> Self {
        Self::default()
    }

    /// Overwrite prefix.
    pub fn with_prefix(self, prefix: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
            ..self
        }
    }

    /// Overwrite start after.
    pub fn with_start_after(self, start_after: impl Into<String>) -> Self {
        Self {
            start_after: start_after.into(),
            ..self
        }
    }

    /// Overwrite limit.
    pub fn with_limit(self, limit: impl Into<Option<usize>>) -> Self {
        Self {
            limit: limit.into(),
            ..self
        }
    }
}

impl TryFrom<ListStreamsRequest> for api::ListStreamsRequest {
    type Error = ConvertError;
    fn try_from(value: ListStreamsRequest) -> Result<Self, Self::Error> {
        let ListStreamsRequest {
            prefix,
            start_after,
            limit,
        } = value;
        Ok(Self {
            prefix,
            start_after,
            limit: limit.map(|n| n as u64),
        })
    }
}

#[sync_docs]
#[derive(Debug, Clone)]
pub struct StreamInfo {
    pub name: String,
    pub created_at: u32,
    pub deleted_at: Option<u32>,
}

impl From<api::StreamInfo> for StreamInfo {
    fn from(value: api::StreamInfo) -> Self {
        Self {
            name: value.name,
            created_at: value.created_at,
            deleted_at: value.deleted_at,
        }
    }
}

impl TryFrom<api::CreateStreamResponse> for StreamInfo {
    type Error = ConvertError;

    fn try_from(value: api::CreateStreamResponse) -> Result<Self, Self::Error> {
        let api::CreateStreamResponse { info } = value;
        let info = info.ok_or("missing stream info")?;
        Ok(info.into())
    }
}

#[sync_docs]
#[derive(Debug, Clone)]
pub struct ListStreamsResponse {
    pub streams: Vec<StreamInfo>,
    pub has_more: bool,
}

impl From<api::ListStreamsResponse> for ListStreamsResponse {
    fn from(value: api::ListStreamsResponse) -> Self {
        let api::ListStreamsResponse { streams, has_more } = value;
        let streams = streams.into_iter().map(Into::into).collect();
        Self { streams, has_more }
    }
}

impl TryFrom<api::GetBasinConfigResponse> for BasinConfig {
    type Error = ConvertError;

    fn try_from(value: api::GetBasinConfigResponse) -> Result<Self, Self::Error> {
        let api::GetBasinConfigResponse { config } = value;
        let config = config.ok_or("missing basin config")?;
        Ok(config.into())
    }
}

impl TryFrom<api::GetStreamConfigResponse> for StreamConfig {
    type Error = ConvertError;

    fn try_from(value: api::GetStreamConfigResponse) -> Result<Self, Self::Error> {
        let api::GetStreamConfigResponse { config } = value;
        let config = config.ok_or("missing stream config")?;
        Ok(config.into())
    }
}

#[sync_docs]
#[derive(Debug, Clone)]
pub struct CreateStreamRequest {
    pub stream: String,
    pub config: Option<StreamConfig>,
}

impl CreateStreamRequest {
    /// Create a new request with stream name.
    pub fn new(stream: impl Into<String>) -> Self {
        Self {
            stream: stream.into(),
            config: None,
        }
    }

    /// Overwrite stream config.
    pub fn with_config(self, config: StreamConfig) -> Self {
        Self {
            config: Some(config),
            ..self
        }
    }
}

impl From<CreateStreamRequest> for api::CreateStreamRequest {
    fn from(value: CreateStreamRequest) -> Self {
        let CreateStreamRequest { stream, config } = value;
        Self {
            stream,
            config: config.map(Into::into),
        }
    }
}

#[sync_docs]
#[derive(Debug, Clone, Default)]
pub struct ListBasinsRequest {
    pub prefix: String,
    pub start_after: String,
    pub limit: Option<usize>,
}

impl ListBasinsRequest {
    /// Create a new request.
    pub fn new() -> Self {
        Self::default()
    }

    /// Overwrite prefix.
    pub fn with_prefix(self, prefix: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
            ..self
        }
    }

    /// Overwrite start after.
    pub fn with_start_after(self, start_after: impl Into<String>) -> Self {
        Self {
            start_after: start_after.into(),
            ..self
        }
    }

    /// Overwrite limit.
    pub fn with_limit(self, limit: impl Into<Option<usize>>) -> Self {
        Self {
            limit: limit.into(),
            ..self
        }
    }
}

impl TryFrom<ListBasinsRequest> for api::ListBasinsRequest {
    type Error = ConvertError;
    fn try_from(value: ListBasinsRequest) -> Result<Self, Self::Error> {
        let ListBasinsRequest {
            prefix,
            start_after,
            limit,
        } = value;
        Ok(Self {
            prefix,
            start_after,
            limit: limit
                .map(TryInto::try_into)
                .transpose()
                .map_err(|_| "request limit does not fit into u64 bounds")?,
        })
    }
}

#[sync_docs]
#[derive(Debug, Clone)]
pub struct ListBasinsResponse {
    pub basins: Vec<BasinInfo>,
    pub has_more: bool,
}

impl TryFrom<api::ListBasinsResponse> for ListBasinsResponse {
    type Error = ConvertError;
    fn try_from(value: api::ListBasinsResponse) -> Result<Self, ConvertError> {
        let api::ListBasinsResponse { basins, has_more } = value;
        Ok(Self {
            basins: basins.into_iter().map(Into::into).collect(),
            has_more,
        })
    }
}

#[sync_docs]
#[derive(Debug, Clone)]
pub struct DeleteBasinRequest {
    pub basin: BasinName,
    /// Delete basin if it exists else do nothing.
    pub if_exists: bool,
}

impl DeleteBasinRequest {
    /// Create a new request.
    pub fn new(basin: BasinName) -> Self {
        Self {
            basin,
            if_exists: false,
        }
    }

    /// Overwrite the if exists parameter.
    pub fn with_if_exists(self, if_exists: bool) -> Self {
        Self { if_exists, ..self }
    }
}

impl From<DeleteBasinRequest> for api::DeleteBasinRequest {
    fn from(value: DeleteBasinRequest) -> Self {
        let DeleteBasinRequest { basin, .. } = value;
        Self { basin: basin.0 }
    }
}

#[sync_docs]
#[derive(Debug, Clone)]
pub struct DeleteStreamRequest {
    pub stream: String,
    /// Delete stream if it exists else do nothing.
    pub if_exists: bool,
}

impl DeleteStreamRequest {
    /// Create a new request.
    pub fn new(stream: impl Into<String>) -> Self {
        Self {
            stream: stream.into(),
            if_exists: false,
        }
    }

    /// Overwrite the if exists parameter.
    pub fn with_if_exists(self, if_exists: bool) -> Self {
        Self { if_exists, ..self }
    }
}

impl From<DeleteStreamRequest> for api::DeleteStreamRequest {
    fn from(value: DeleteStreamRequest) -> Self {
        let DeleteStreamRequest { stream, .. } = value;
        Self { stream }
    }
}

#[sync_docs]
#[derive(Debug, Clone)]
pub struct ReconfigureBasinRequest {
    pub basin: BasinName,
    pub config: Option<BasinConfig>,
    pub mask: Option<Vec<String>>,
}

impl ReconfigureBasinRequest {
    /// Create a new request with basin name.
    pub fn new(basin: BasinName) -> Self {
        Self {
            basin,
            config: None,
            mask: None,
        }
    }

    /// Overwrite basin config.
    pub fn with_config(self, config: BasinConfig) -> Self {
        Self {
            config: Some(config),
            ..self
        }
    }

    /// Overwrite field mask.
    pub fn with_mask(self, mask: impl Into<Vec<String>>) -> Self {
        Self {
            mask: Some(mask.into()),
            ..self
        }
    }
}

impl From<ReconfigureBasinRequest> for api::ReconfigureBasinRequest {
    fn from(value: ReconfigureBasinRequest) -> Self {
        let ReconfigureBasinRequest {
            basin,
            config,
            mask,
        } = value;
        Self {
            basin: basin.0,
            config: config.map(Into::into),
            mask: mask.map(|paths| prost_types::FieldMask { paths }),
        }
    }
}

impl TryFrom<api::ReconfigureBasinResponse> for BasinConfig {
    type Error = ConvertError;
    fn try_from(value: api::ReconfigureBasinResponse) -> Result<Self, Self::Error> {
        let api::ReconfigureBasinResponse { config } = value;
        let config = config.ok_or("missing basin config")?;
        Ok(config.into())
    }
}

#[sync_docs]
#[derive(Debug, Clone)]
pub struct ReconfigureStreamRequest {
    pub stream: String,
    pub config: Option<StreamConfig>,
    pub mask: Option<Vec<String>>,
}

impl ReconfigureStreamRequest {
    /// Create a new request with stream name.
    pub fn new(stream: impl Into<String>) -> Self {
        Self {
            stream: stream.into(),
            config: None,
            mask: None,
        }
    }

    /// Overwrite stream config.
    pub fn with_config(self, config: StreamConfig) -> Self {
        Self {
            config: Some(config),
            ..self
        }
    }

    /// Overwrite field mask.
    pub fn with_mask(self, mask: impl Into<Vec<String>>) -> Self {
        Self {
            mask: Some(mask.into()),
            ..self
        }
    }
}

impl From<ReconfigureStreamRequest> for api::ReconfigureStreamRequest {
    fn from(value: ReconfigureStreamRequest) -> Self {
        let ReconfigureStreamRequest {
            stream,
            config,
            mask,
        } = value;
        Self {
            stream,
            config: config.map(Into::into),
            mask: mask.map(|paths| prost_types::FieldMask { paths }),
        }
    }
}

impl TryFrom<api::ReconfigureStreamResponse> for StreamConfig {
    type Error = ConvertError;
    fn try_from(value: api::ReconfigureStreamResponse) -> Result<Self, Self::Error> {
        let api::ReconfigureStreamResponse { config } = value;
        let config = config.ok_or("missing stream config")?;
        Ok(config.into())
    }
}

impl From<api::CheckTailResponse> for StreamPosition {
    fn from(value: api::CheckTailResponse) -> Self {
        let api::CheckTailResponse {
            next_seq_num,
            last_timestamp,
        } = value;
        StreamPosition {
            seq_num: next_seq_num,
            timestamp: last_timestamp,
        }
    }
}

/// Position of a record in a stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamPosition {
    /// Sequence number assigned by the service.
    pub seq_num: u64,
    /// Timestamp, which may be user-specified or assigned by the service.
    /// If it is assigned by the service, it will represent milliseconds since Unix epoch.
    pub timestamp: u64,
}

#[sync_docs]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    pub name: Bytes,
    pub value: Bytes,
}

impl Header {
    /// Create a new header from name and value.
    pub fn new(name: impl Into<Bytes>, value: impl Into<Bytes>) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
        }
    }

    /// Create a new header from value.
    pub fn from_value(value: impl Into<Bytes>) -> Self {
        Self {
            name: Bytes::new(),
            value: value.into(),
        }
    }
}

impl From<Header> for api::Header {
    fn from(value: Header) -> Self {
        let Header { name, value } = value;
        Self { name, value }
    }
}

impl From<api::Header> for Header {
    fn from(value: api::Header) -> Self {
        let api::Header { name, value } = value;
        Self { name, value }
    }
}

/// A fencing token can be enforced on append requests.
///
/// Must not be more than 36 bytes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FencingToken(String);

impl FencingToken {
    const MAX_BYTES: usize = 36;

    /// Generate a random alphanumeric fencing token of `n` bytes.
    pub fn generate(n: usize) -> Result<Self, ConvertError> {
        rand::rng()
            .sample_iter(&rand::distr::Alphanumeric)
            .take(n)
            .map(char::from)
            .collect::<String>()
            .parse()
    }
}

impl Deref for FencingToken {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Display for FencingToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for FencingToken {
    type Err = ConvertError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value.to_string().try_into()
    }
}

impl TryFrom<String> for FencingToken {
    type Error = ConvertError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.len() > Self::MAX_BYTES {
            Err(format!("Fencing token cannot exceed {} bytes", Self::MAX_BYTES).into())
        } else {
            Ok(Self(value))
        }
    }
}

impl From<FencingToken> for String {
    fn from(value: FencingToken) -> Self {
        value.0
    }
}

/// Command to send through a `CommandRecord`.
#[derive(Debug, Clone)]
pub enum Command {
    /// Enforce a fencing token.
    ///
    /// Fencing is strongly consistent, and subsequent appends that specify a
    /// fencing token will be rejected if it does not match.
    Fence {
        /// Fencing token to enforce.
        ///
        /// Set empty to clear the token.
        fencing_token: FencingToken,
    },
    /// Request a trim till the sequence number.
    ///
    /// Trimming is eventually consistent, and trimmed records may be visible
    /// for a brief period
    Trim {
        /// Trim point.
        ///
        /// This sequence number is only allowed to advance, and any regression
        /// will be ignored.
        seq_num: u64,
    },
}

/// A command record is a special kind of [`AppendRecord`] that can be used to
/// send command messages.
///
/// Such a record is signalled by a sole header with empty name. The header
/// value represents the operation and record body acts as the payload.
#[derive(Debug, Clone)]
pub struct CommandRecord {
    /// Command kind.
    pub command: Command,
    /// Timestamp for the record.
    pub timestamp: Option<u64>,
}

impl CommandRecord {
    const FENCE: &[u8] = b"fence";
    const TRIM: &[u8] = b"trim";

    /// Create a new fence command record.
    pub fn fence(fencing_token: FencingToken) -> Self {
        Self {
            command: Command::Fence { fencing_token },
            timestamp: None,
        }
    }

    /// Create a new trim command record.
    pub fn trim(seq_num: impl Into<u64>) -> Self {
        Self {
            command: Command::Trim {
                seq_num: seq_num.into(),
            },
            timestamp: None,
        }
    }

    /// Overwrite timestamp.
    pub fn with_timestamp(self, timestamp: u64) -> Self {
        Self {
            timestamp: Some(timestamp),
            ..self
        }
    }
}

#[sync_docs]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendRecord {
    timestamp: Option<u64>,
    headers: Vec<Header>,
    body: Bytes,
    #[cfg(test)]
    max_bytes: u64,
}

metered_impl!(AppendRecord);

impl AppendRecord {
    const MAX_BYTES: u64 = MIB_BYTES;

    fn validated(self) -> Result<Self, ConvertError> {
        #[cfg(test)]
        let max_bytes = self.max_bytes;
        #[cfg(not(test))]
        let max_bytes = Self::MAX_BYTES;

        if self.metered_bytes() > max_bytes {
            Err("AppendRecord should have metered size less than 1 MiB".into())
        } else {
            Ok(self)
        }
    }

    /// Try creating a new append record with body.
    pub fn new(body: impl Into<Bytes>) -> Result<Self, ConvertError> {
        Self {
            timestamp: None,
            headers: Vec::new(),
            body: body.into(),
            #[cfg(test)]
            max_bytes: Self::MAX_BYTES,
        }
        .validated()
    }

    #[cfg(test)]
    pub(crate) fn with_max_bytes(
        max_bytes: u64,
        body: impl Into<Bytes>,
    ) -> Result<Self, ConvertError> {
        Self {
            timestamp: None,
            headers: Vec::new(),
            body: body.into(),
            max_bytes,
        }
        .validated()
    }

    /// Overwrite headers.
    pub fn with_headers(self, headers: impl Into<Vec<Header>>) -> Result<Self, ConvertError> {
        Self {
            headers: headers.into(),
            ..self
        }
        .validated()
    }

    /// Overwrite timestamp.
    pub fn with_timestamp(self, timestamp: u64) -> Self {
        Self {
            timestamp: Some(timestamp),
            ..self
        }
    }

    /// Body of the record.
    pub fn body(&self) -> &[u8] {
        &self.body
    }

    /// Headers of the record.
    pub fn headers(&self) -> &[Header] {
        &self.headers
    }

    /// Timestamp for the record.
    pub fn timestamp(&self) -> Option<u64> {
        self.timestamp
    }

    /// Consume the record and return parts.
    pub fn into_parts(self) -> AppendRecordParts {
        AppendRecordParts {
            timestamp: self.timestamp,
            headers: self.headers,
            body: self.body,
        }
    }

    /// Try creating the record from parts.
    pub fn try_from_parts(parts: AppendRecordParts) -> Result<Self, ConvertError> {
        let record = Self::new(parts.body)?.with_headers(parts.headers)?;
        if let Some(timestamp) = parts.timestamp {
            Ok(record.with_timestamp(timestamp))
        } else {
            Ok(record)
        }
    }
}

impl From<AppendRecord> for api::AppendRecord {
    fn from(value: AppendRecord) -> Self {
        Self {
            timestamp: value.timestamp,
            headers: value.headers.into_iter().map(Into::into).collect(),
            body: value.body,
        }
    }
}

impl From<CommandRecord> for AppendRecord {
    fn from(value: CommandRecord) -> Self {
        let (header_value, body) = match value.command {
            Command::Fence { fencing_token } => (
                CommandRecord::FENCE,
                Bytes::copy_from_slice(fencing_token.as_bytes()),
            ),
            Command::Trim { seq_num } => (
                CommandRecord::TRIM,
                Bytes::copy_from_slice(&seq_num.to_be_bytes()),
            ),
        };
        AppendRecordParts {
            timestamp: value.timestamp,
            headers: vec![Header::from_value(header_value)],
            body,
        }
        .try_into()
        .expect("command record is a valid append record")
    }
}

#[sync_docs(AppendRecordParts = "AppendRecord")]
#[derive(Debug, Clone)]
pub struct AppendRecordParts {
    pub timestamp: Option<u64>,
    pub headers: Vec<Header>,
    pub body: Bytes,
}

impl From<AppendRecord> for AppendRecordParts {
    fn from(value: AppendRecord) -> Self {
        value.into_parts()
    }
}

impl TryFrom<AppendRecordParts> for AppendRecord {
    type Error = ConvertError;

    fn try_from(value: AppendRecordParts) -> Result<Self, Self::Error> {
        Self::try_from_parts(value)
    }
}

/// A collection of append records that can be sent together in a batch.
#[derive(Debug, Clone)]
pub struct AppendRecordBatch {
    records: Vec<AppendRecord>,
    metered_bytes: u64,
    max_capacity: usize,
    #[cfg(test)]
    max_bytes: u64,
}

impl PartialEq for AppendRecordBatch {
    fn eq(&self, other: &Self) -> bool {
        if self.records.eq(&other.records) {
            assert_eq!(self.metered_bytes, other.metered_bytes);
            true
        } else {
            false
        }
    }
}

impl Eq for AppendRecordBatch {}

impl Default for AppendRecordBatch {
    fn default() -> Self {
        Self::new()
    }
}

impl AppendRecordBatch {
    /// Maximum number of records that a batch can hold.
    ///
    /// A record batch cannot be created with a bigger capacity.
    pub const MAX_CAPACITY: usize = 1000;

    /// Maximum metered bytes of the batch.
    pub const MAX_BYTES: u64 = MIB_BYTES;

    /// Create an empty record batch.
    pub fn new() -> Self {
        Self::with_max_capacity(Self::MAX_CAPACITY)
    }

    /// Create an empty record batch with custom max capacity.
    ///
    /// The capacity should not be more than [`Self::MAX_CAPACITY`].
    pub fn with_max_capacity(max_capacity: usize) -> Self {
        assert!(
            max_capacity > 0 && max_capacity <= Self::MAX_CAPACITY,
            "Batch capacity must be between 1 and 1000"
        );

        Self {
            records: Vec::with_capacity(max_capacity),
            metered_bytes: 0,
            max_capacity,
            #[cfg(test)]
            max_bytes: Self::MAX_BYTES,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_max_capacity_and_bytes(max_capacity: usize, max_bytes: u64) -> Self {
        #[cfg(test)]
        assert!(
            max_bytes > 0 || max_bytes <= Self::MAX_BYTES,
            "Batch size must be between 1 byte and 1 MiB"
        );

        Self {
            max_bytes,
            ..Self::with_max_capacity(max_capacity)
        }
    }

    /// Try creating a record batch from an iterator.
    ///
    /// If all the items of the iterator cannot be drained into the batch, the
    /// error returned contains a batch containing all records it could fit
    /// along-with the left over items from the iterator.
    pub fn try_from_iter<R, T>(iter: T) -> Result<Self, (Self, Vec<AppendRecord>)>
    where
        R: Into<AppendRecord>,
        T: IntoIterator<Item = R>,
    {
        let mut records = Self::new();
        let mut pending = Vec::new();

        let mut iter = iter.into_iter();

        for record in iter.by_ref() {
            if let Err(record) = records.push(record) {
                pending.push(record);
                break;
            }
        }

        if pending.is_empty() {
            Ok(records)
        } else {
            pending.extend(iter.map(Into::into));
            Err((records, pending))
        }
    }

    /// Returns true if the batch contains no records.
    pub fn is_empty(&self) -> bool {
        if self.records.is_empty() {
            assert_eq!(self.metered_bytes, 0);
            true
        } else {
            false
        }
    }

    /// Returns the number of records contained in the batch.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    #[cfg(test)]
    fn max_bytes(&self) -> u64 {
        self.max_bytes
    }

    #[cfg(not(test))]
    fn max_bytes(&self) -> u64 {
        Self::MAX_BYTES
    }

    /// Returns true if the batch cannot fit any more records.
    pub fn is_full(&self) -> bool {
        self.records.len() >= self.max_capacity || self.metered_bytes >= self.max_bytes()
    }

    /// Try to append a new record into the batch.
    pub fn push(&mut self, record: impl Into<AppendRecord>) -> Result<(), AppendRecord> {
        assert!(self.records.len() <= self.max_capacity);
        assert!(self.metered_bytes <= self.max_bytes());

        let record = record.into();
        let record_size = record.metered_bytes();
        if self.records.len() >= self.max_capacity
            || self.metered_bytes + record_size > self.max_bytes()
        {
            Err(record)
        } else {
            self.records.push(record);
            self.metered_bytes += record_size;
            Ok(())
        }
    }
}

impl MeteredBytes for AppendRecordBatch {
    fn metered_bytes(&self) -> u64 {
        self.metered_bytes
    }
}

impl IntoIterator for AppendRecordBatch {
    type Item = AppendRecord;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.records.into_iter()
    }
}

impl<'a> IntoIterator for &'a AppendRecordBatch {
    type Item = &'a AppendRecord;
    type IntoIter = std::slice::Iter<'a, AppendRecord>;

    fn into_iter(self) -> Self::IntoIter {
        self.records.iter()
    }
}

impl AsRef<[AppendRecord]> for AppendRecordBatch {
    fn as_ref(&self) -> &[AppendRecord] {
        &self.records
    }
}

#[sync_docs]
#[derive(Debug, Default, Clone)]
pub struct AppendInput {
    pub records: AppendRecordBatch,
    pub match_seq_num: Option<u64>,
    pub fencing_token: Option<FencingToken>,
}

impl MeteredBytes for AppendInput {
    fn metered_bytes(&self) -> u64 {
        self.records.metered_bytes()
    }
}

impl AppendInput {
    /// Create a new append input from record batch.
    pub fn new(records: impl Into<AppendRecordBatch>) -> Self {
        Self {
            records: records.into(),
            match_seq_num: None,
            fencing_token: None,
        }
    }

    /// Overwrite match sequence number.
    pub fn with_match_seq_num(self, match_seq_num: impl Into<u64>) -> Self {
        Self {
            match_seq_num: Some(match_seq_num.into()),
            ..self
        }
    }

    /// Overwrite fencing token.
    pub fn with_fencing_token(self, fencing_token: FencingToken) -> Self {
        Self {
            fencing_token: Some(fencing_token),
            ..self
        }
    }

    pub(crate) fn into_api_type(self, stream: impl Into<String>) -> api::AppendInput {
        let Self {
            records,
            match_seq_num,
            fencing_token,
        } = self;

        api::AppendInput {
            stream: stream.into(),
            records: records.into_iter().map(Into::into).collect(),
            match_seq_num,
            fencing_token: fencing_token.map(|f| f.0),
        }
    }
}

/// Acknowledgment to an append request.
#[derive(Debug, Clone)]
pub struct AppendAck {
    /// Sequence number and timestamp of the first record that was appended.
    pub start: StreamPosition,
    /// Sequence number of the last record that was appended + 1,
    /// and timestamp of the last record that was appended.
    /// The difference between `end.seq_num` and `start.seq_num`
    /// will be the number of records appended.
    pub end: StreamPosition,
    /// Sequence number that will be assigned to the next record on the stream,
    /// and timestamp of the last record on the stream.
    /// This can be greater than the `end` position in case of concurrent appends.
    pub tail: StreamPosition,
}

impl From<api::AppendOutput> for AppendAck {
    fn from(value: api::AppendOutput) -> Self {
        let api::AppendOutput {
            start_seq_num,
            start_timestamp,
            end_seq_num,
            end_timestamp,
            next_seq_num,
            last_timestamp,
        } = value;
        let start = StreamPosition {
            seq_num: start_seq_num,
            timestamp: start_timestamp,
        };
        let end = StreamPosition {
            seq_num: end_seq_num,
            timestamp: end_timestamp,
        };
        let tail = StreamPosition {
            seq_num: next_seq_num,
            timestamp: last_timestamp,
        };
        Self { start, end, tail }
    }
}

impl TryFrom<api::AppendResponse> for AppendAck {
    type Error = ConvertError;
    fn try_from(value: api::AppendResponse) -> Result<Self, Self::Error> {
        let api::AppendResponse { output } = value;
        let output = output.ok_or("missing append output")?;
        Ok(output.into())
    }
}

impl TryFrom<api::AppendSessionResponse> for AppendAck {
    type Error = ConvertError;
    fn try_from(value: api::AppendSessionResponse) -> Result<Self, Self::Error> {
        let api::AppendSessionResponse { output } = value;
        let output = output.ok_or("missing append output")?;
        Ok(output.into())
    }
}

#[sync_docs]
#[derive(Debug, Clone, Default)]
pub struct ReadLimit {
    pub count: Option<u64>,
    pub bytes: Option<u64>,
}

impl ReadLimit {
    /// Create a new read limit.
    pub fn new() -> Self {
        Self::default()
    }

    /// Overwrite count limit.
    pub fn with_count(self, count: u64) -> Self {
        Self {
            count: Some(count),
            ..self
        }
    }

    /// Overwrite bytes limit.
    pub fn with_bytes(self, bytes: u64) -> Self {
        Self {
            bytes: Some(bytes),
            ..self
        }
    }
}

/// Starting point for read requests.
#[derive(Debug, Clone)]
pub enum ReadStart {
    /// Sequence number.
    SeqNum(u64),
    /// Timestamp.
    Timestamp(u64),
    /// Number of records before the tail, i.e. the next sequence number.
    TailOffset(u64),
}

impl Default for ReadStart {
    fn default() -> Self {
        Self::SeqNum(0)
    }
}

impl From<ReadStart> for api::read_request::Start {
    fn from(start: ReadStart) -> Self {
        match start {
            ReadStart::SeqNum(seq_num) => api::read_request::Start::SeqNum(seq_num),
            ReadStart::Timestamp(timestamp) => api::read_request::Start::Timestamp(timestamp),
            ReadStart::TailOffset(offset) => api::read_request::Start::TailOffset(offset),
        }
    }
}

impl From<ReadStart> for api::read_session_request::Start {
    fn from(start: ReadStart) -> Self {
        match start {
            ReadStart::SeqNum(seq_num) => api::read_session_request::Start::SeqNum(seq_num),
            ReadStart::Timestamp(timestamp) => {
                api::read_session_request::Start::Timestamp(timestamp)
            }
            ReadStart::TailOffset(offset) => api::read_session_request::Start::TailOffset(offset),
        }
    }
}

#[sync_docs]
#[derive(Debug, Clone, Default)]
pub struct ReadRequest {
    pub start: ReadStart,
    pub limit: ReadLimit,
}

impl ReadRequest {
    /// Create a new request with the specified starting point.
    pub fn new(start: ReadStart) -> Self {
        Self {
            start,
            ..Default::default()
        }
    }

    /// Overwrite limit.
    pub fn with_limit(self, limit: ReadLimit) -> Self {
        Self { limit, ..self }
    }
}

impl ReadRequest {
    pub(crate) fn try_into_api_type(
        self,
        stream: impl Into<String>,
    ) -> Result<api::ReadRequest, ConvertError> {
        let Self { start, limit } = self;

        let limit = if limit.count > Some(1000) {
            Err("read limit: count must not exceed 1000 for unary request")
        } else if limit.bytes > Some(MIB_BYTES) {
            Err("read limit: bytes must not exceed 1MiB for unary request")
        } else {
            Ok(api::ReadLimit {
                count: limit.count,
                bytes: limit.bytes,
            })
        }?;

        Ok(api::ReadRequest {
            stream: stream.into(),
            start: Some(start.into()),
            limit: Some(limit),
        })
    }
}

#[sync_docs]
#[derive(Debug, Clone)]
pub struct SequencedRecord {
    pub seq_num: u64,
    pub timestamp: u64,
    pub headers: Vec<Header>,
    pub body: Bytes,
}

metered_impl!(SequencedRecord);

impl From<api::SequencedRecord> for SequencedRecord {
    fn from(value: api::SequencedRecord) -> Self {
        let api::SequencedRecord {
            seq_num,
            timestamp,
            headers,
            body,
        } = value;
        Self {
            seq_num,
            timestamp,
            headers: headers.into_iter().map(Into::into).collect(),
            body,
        }
    }
}

impl SequencedRecord {
    /// Try representing the sequenced record as a command record.
    pub fn as_command_record(&self) -> Option<CommandRecord> {
        if self.headers.len() != 1 {
            return None;
        }

        let header = self.headers.first().expect("pre-validated length");

        if !header.name.is_empty() {
            return None;
        }

        match header.value.as_ref() {
            CommandRecord::FENCE => {
                let fencing_token = std::str::from_utf8(&self.body).ok()?.parse().ok()?;
                Some(CommandRecord {
                    command: Command::Fence { fencing_token },
                    timestamp: Some(self.timestamp),
                })
            }
            CommandRecord::TRIM => {
                let body: &[u8] = &self.body;
                let seq_num = u64::from_be_bytes(body.try_into().ok()?);
                Some(CommandRecord {
                    command: Command::Trim { seq_num },
                    timestamp: Some(self.timestamp),
                })
            }
            _ => None,
        }
    }
}

#[sync_docs]
#[derive(Debug, Clone)]
pub struct SequencedRecordBatch {
    pub records: Vec<SequencedRecord>,
}

impl MeteredBytes for SequencedRecordBatch {
    fn metered_bytes(&self) -> u64 {
        self.records.metered_bytes()
    }
}

impl From<api::SequencedRecordBatch> for SequencedRecordBatch {
    fn from(value: api::SequencedRecordBatch) -> Self {
        let api::SequencedRecordBatch { records } = value;
        Self {
            records: records.into_iter().map(Into::into).collect(),
        }
    }
}

#[sync_docs(ReadOutput = "Output")]
#[derive(Debug, Clone)]
pub enum ReadOutput {
    Batch(SequencedRecordBatch),
    NextSeqNum(u64),
}

impl From<api::read_output::Output> for ReadOutput {
    fn from(value: api::read_output::Output) -> Self {
        match value {
            api::read_output::Output::Batch(batch) => Self::Batch(batch.into()),
            api::read_output::Output::NextSeqNum(next_seq_num) => Self::NextSeqNum(next_seq_num),
        }
    }
}

impl TryFrom<api::ReadOutput> for ReadOutput {
    type Error = ConvertError;
    fn try_from(value: api::ReadOutput) -> Result<Self, Self::Error> {
        let api::ReadOutput { output } = value;
        let output = output.ok_or("missing read output")?;
        Ok(output.into())
    }
}

impl TryFrom<api::ReadResponse> for ReadOutput {
    type Error = ConvertError;
    fn try_from(value: api::ReadResponse) -> Result<Self, Self::Error> {
        let api::ReadResponse { output } = value;
        let output = output.ok_or("missing output in read response")?;
        output.try_into()
    }
}

#[sync_docs]
#[derive(Debug, Clone, Default)]
pub struct ReadSessionRequest {
    pub start: ReadStart,
    pub limit: ReadLimit,
}

impl ReadSessionRequest {
    /// Create a new request with the specified starting point.
    pub fn new(start: ReadStart) -> Self {
        Self {
            start,
            ..Default::default()
        }
    }

    /// Overwrite limit.
    pub fn with_limit(self, limit: ReadLimit) -> Self {
        Self { limit, ..self }
    }

    pub(crate) fn into_api_type(self, stream: impl Into<String>) -> api::ReadSessionRequest {
        let Self { start, limit } = self;
        api::ReadSessionRequest {
            stream: stream.into(),
            start: Some(start.into()),
            limit: Some(api::ReadLimit {
                count: limit.count,
                bytes: limit.bytes,
            }),
            heartbeats: false,
        }
    }
}

impl TryFrom<api::ReadSessionResponse> for ReadOutput {
    type Error = ConvertError;
    fn try_from(value: api::ReadSessionResponse) -> Result<Self, Self::Error> {
        let api::ReadSessionResponse { output } = value;
        let output = output.ok_or("missing output in read session response")?;
        output.try_into()
    }
}

/// Name of a basin.
///
/// Must be between 8 and 48 characters in length. Must comprise lowercase
/// letters, numbers, and hyphens. Cannot begin or end with a hyphen.
#[derive(Debug, Clone)]
pub struct BasinName(String);

impl Deref for BasinName {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TryFrom<String> for BasinName {
    type Error = ConvertError;

    fn try_from(name: String) -> Result<Self, Self::Error> {
        if name.len() < 8 || name.len() > 48 {
            return Err("Basin name must be between 8 and 48 characters in length".into());
        }

        static BASIN_NAME_REGEX: OnceLock<Regex> = OnceLock::new();
        let regex = BASIN_NAME_REGEX.get_or_init(|| {
            Regex::new(r"^[a-z0-9]([a-z0-9-]*[a-z0-9])?$")
                .expect("Failed to compile basin name regex")
        });

        if !regex.is_match(&name) {
            return Err(
                "Basin name must comprise lowercase letters, numbers, and hyphens. \
                It cannot begin or end with a hyphen."
                    .into(),
            );
        }

        Ok(Self(name))
    }
}

impl FromStr for BasinName {
    type Err = ConvertError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.to_string().try_into()
    }
}

impl std::fmt::Display for BasinName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<BasinName> for String {
    fn from(value: BasinName) -> Self {
        value.0
    }
}

/// Access token ID.
/// Must be between 1 and 96 characters.
#[derive(Debug, Clone)]
pub struct AccessTokenId(String);

impl Deref for AccessTokenId {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TryFrom<String> for AccessTokenId {
    type Error = ConvertError;

    fn try_from(name: String) -> Result<Self, Self::Error> {
        if name.is_empty() {
            return Err("Access token ID must not be empty".into());
        }

        if name.len() > 96 {
            return Err("Access token ID must not exceed 96 characters".into());
        }

        Ok(Self(name))
    }
}

impl From<AccessTokenId> for String {
    fn from(value: AccessTokenId) -> Self {
        value.0
    }
}

impl FromStr for AccessTokenId {
    type Err = ConvertError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.to_string().try_into()
    }
}

impl std::fmt::Display for AccessTokenId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<AccessTokenInfo> for api::IssueAccessTokenRequest {
    fn from(value: AccessTokenInfo) -> Self {
        Self {
            info: Some(value.into()),
        }
    }
}

#[sync_docs]
#[derive(Debug, Clone)]
pub struct AccessTokenInfo {
    pub id: AccessTokenId,
    pub expires_at: Option<u32>,
    pub auto_prefix_streams: bool,
    pub scope: Option<AccessTokenScope>,
}

impl AccessTokenInfo {
    /// Create a new access token info.
    pub fn new(id: AccessTokenId) -> Self {
        Self {
            id,
            expires_at: None,
            auto_prefix_streams: false,
            scope: None,
        }
    }

    /// Overwrite expiration time.
    pub fn with_expires_at(self, expires_at: u32) -> Self {
        Self {
            expires_at: Some(expires_at),
            ..self
        }
    }

    /// Overwrite auto prefix streams.
    pub fn with_auto_prefix_streams(self, auto_prefix_streams: bool) -> Self {
        Self {
            auto_prefix_streams,
            ..self
        }
    }

    /// Overwrite scope.
    pub fn with_scope(self, scope: AccessTokenScope) -> Self {
        Self {
            scope: Some(scope),
            ..self
        }
    }
}

impl From<AccessTokenInfo> for api::AccessTokenInfo {
    fn from(value: AccessTokenInfo) -> Self {
        let AccessTokenInfo {
            id,
            expires_at,
            auto_prefix_streams,
            scope,
        } = value;
        Self {
            id: id.into(),
            expires_at,
            auto_prefix_streams,
            scope: scope.map(Into::into),
        }
    }
}

impl TryFrom<api::AccessTokenInfo> for AccessTokenInfo {
    type Error = ConvertError;

    fn try_from(value: api::AccessTokenInfo) -> Result<Self, Self::Error> {
        let api::AccessTokenInfo {
            id,
            expires_at,
            auto_prefix_streams,
            scope,
        } = value;
        Ok(Self {
            id: id.try_into()?,
            expires_at,
            auto_prefix_streams,
            scope: scope.map(Into::into),
        })
    }
}

#[sync_docs]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Operation {
    ListBasins,
    CreateBasin,
    DeleteBasin,
    ReconfigureBasin,
    GetBasinConfig,
    IssueAccessToken,
    RevokeAccessToken,
    ListAccessTokens,
    ListStreams,
    CreateStream,
    DeleteStream,
    GetStreamConfig,
    ReconfigureStream,
    CheckTail,
    Append,
    Read,
    Trim,
    Fence,
}

impl FromStr for Operation {
    type Err = ConvertError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "list-basins" => Ok(Self::ListBasins),
            "create-basin" => Ok(Self::CreateBasin),
            "delete-basin" => Ok(Self::DeleteBasin),
            "reconfigure-basin" => Ok(Self::ReconfigureBasin),
            "get-basin-config" => Ok(Self::GetBasinConfig),
            "issue-access-token" => Ok(Self::IssueAccessToken),
            "revoke-access-token" => Ok(Self::RevokeAccessToken),
            "list-access-tokens" => Ok(Self::ListAccessTokens),
            "list-streams" => Ok(Self::ListStreams),
            "create-stream" => Ok(Self::CreateStream),
            "delete-stream" => Ok(Self::DeleteStream),
            "get-stream-config" => Ok(Self::GetStreamConfig),
            "reconfigure-stream" => Ok(Self::ReconfigureStream),
            "check-tail" => Ok(Self::CheckTail),
            "append" => Ok(Self::Append),
            "read" => Ok(Self::Read),
            "trim" => Ok(Self::Trim),
            "fence" => Ok(Self::Fence),
            _ => Err("invalid operation".into()),
        }
    }
}

impl From<Operation> for api::Operation {
    fn from(value: Operation) -> Self {
        match value {
            Operation::ListBasins => Self::ListBasins,
            Operation::CreateBasin => Self::CreateBasin,
            Operation::DeleteBasin => Self::DeleteBasin,
            Operation::ReconfigureBasin => Self::ReconfigureBasin,
            Operation::GetBasinConfig => Self::GetBasinConfig,
            Operation::IssueAccessToken => Self::IssueAccessToken,
            Operation::RevokeAccessToken => Self::RevokeAccessToken,
            Operation::ListAccessTokens => Self::ListAccessTokens,
            Operation::ListStreams => Self::ListStreams,
            Operation::CreateStream => Self::CreateStream,
            Operation::DeleteStream => Self::DeleteStream,
            Operation::GetStreamConfig => Self::GetStreamConfig,
            Operation::ReconfigureStream => Self::ReconfigureStream,
            Operation::CheckTail => Self::CheckTail,
            Operation::Append => Self::Append,
            Operation::Read => Self::Read,
            Operation::Trim => Self::Trim,
            Operation::Fence => Self::Fence,
        }
    }
}

impl From<api::Operation> for Option<Operation> {
    fn from(value: api::Operation) -> Self {
        match value {
            api::Operation::Unspecified => None,
            api::Operation::ListBasins => Some(Operation::ListBasins),
            api::Operation::CreateBasin => Some(Operation::CreateBasin),
            api::Operation::DeleteBasin => Some(Operation::DeleteBasin),
            api::Operation::ReconfigureBasin => Some(Operation::ReconfigureBasin),
            api::Operation::GetBasinConfig => Some(Operation::GetBasinConfig),
            api::Operation::IssueAccessToken => Some(Operation::IssueAccessToken),
            api::Operation::RevokeAccessToken => Some(Operation::RevokeAccessToken),
            api::Operation::ListAccessTokens => Some(Operation::ListAccessTokens),
            api::Operation::ListStreams => Some(Operation::ListStreams),
            api::Operation::CreateStream => Some(Operation::CreateStream),
            api::Operation::DeleteStream => Some(Operation::DeleteStream),
            api::Operation::GetStreamConfig => Some(Operation::GetStreamConfig),
            api::Operation::ReconfigureStream => Some(Operation::ReconfigureStream),
            api::Operation::CheckTail => Some(Operation::CheckTail),
            api::Operation::Append => Some(Operation::Append),
            api::Operation::Read => Some(Operation::Read),
            api::Operation::Trim => Some(Operation::Trim),
            api::Operation::Fence => Some(Operation::Fence),
        }
    }
}

#[sync_docs]
#[derive(Debug, Clone, Default)]
pub struct AccessTokenScope {
    pub basins: Option<ResourceSet>,
    pub streams: Option<ResourceSet>,
    pub access_tokens: Option<ResourceSet>,
    pub op_groups: Option<PermittedOperationGroups>,
    pub ops: HashSet<Operation>,
}

impl AccessTokenScope {
    /// Create a new access token scope.
    pub fn new() -> Self {
        Self::default()
    }

    /// Overwrite resource set for access tokens.
    pub fn with_basins(self, basins: ResourceSet) -> Self {
        Self {
            basins: Some(basins),
            ..self
        }
    }

    /// Overwrite resource set for streams.
    pub fn with_streams(self, streams: ResourceSet) -> Self {
        Self {
            streams: Some(streams),
            ..self
        }
    }

    /// Overwrite resource set for access tokens.
    pub fn with_tokens(self, access_tokens: ResourceSet) -> Self {
        Self {
            access_tokens: Some(access_tokens),
            ..self
        }
    }

    /// Overwrite operation groups.
    pub fn with_op_groups(self, op_groups: PermittedOperationGroups) -> Self {
        Self {
            op_groups: Some(op_groups),
            ..self
        }
    }

    /// Overwrite operations.
    pub fn with_ops(self, ops: impl IntoIterator<Item = Operation>) -> Self {
        Self {
            ops: ops.into_iter().collect(),
            ..self
        }
    }

    /// Add an operation to operations.
    pub fn with_op(self, op: Operation) -> Self {
        let mut ops = self.ops;
        ops.insert(op);
        Self { ops, ..self }
    }
}

impl From<AccessTokenScope> for api::AccessTokenScope {
    fn from(value: AccessTokenScope) -> Self {
        let AccessTokenScope {
            basins,
            streams,
            access_tokens,
            op_groups,
            ops,
        } = value;
        Self {
            basins: basins.map(Into::into),
            streams: streams.map(Into::into),
            access_tokens: access_tokens.map(Into::into),
            op_groups: op_groups.map(Into::into),
            ops: ops
                .into_iter()
                .map(api::Operation::from)
                .map(Into::into)
                .collect(),
        }
    }
}

impl From<api::AccessTokenScope> for AccessTokenScope {
    fn from(value: api::AccessTokenScope) -> Self {
        let api::AccessTokenScope {
            basins,
            streams,
            access_tokens,
            op_groups,
            ops,
        } = value;
        Self {
            basins: basins.and_then(|set| set.matching.map(Into::into)),
            streams: streams.and_then(|set| set.matching.map(Into::into)),
            access_tokens: access_tokens.and_then(|set| set.matching.map(Into::into)),
            op_groups: op_groups.map(Into::into),
            ops: ops
                .into_iter()
                .map(api::Operation::try_from)
                .flat_map(Result::ok)
                .flat_map(<Option<Operation>>::from)
                .collect(),
        }
    }
}

impl From<ResourceSet> for api::ResourceSet {
    fn from(value: ResourceSet) -> Self {
        Self {
            matching: Some(value.into()),
        }
    }
}

#[sync_docs(ResourceSet = "Matching")]
#[derive(Debug, Clone)]
pub enum ResourceSet {
    Exact(String),
    Prefix(String),
}

impl From<ResourceSet> for api::resource_set::Matching {
    fn from(value: ResourceSet) -> Self {
        match value {
            ResourceSet::Exact(name) => api::resource_set::Matching::Exact(name),
            ResourceSet::Prefix(name) => api::resource_set::Matching::Prefix(name),
        }
    }
}

impl From<api::resource_set::Matching> for ResourceSet {
    fn from(value: api::resource_set::Matching) -> Self {
        match value {
            api::resource_set::Matching::Exact(name) => ResourceSet::Exact(name),
            api::resource_set::Matching::Prefix(name) => ResourceSet::Prefix(name),
        }
    }
}

#[sync_docs]
#[derive(Debug, Clone, Default)]
pub struct PermittedOperationGroups {
    pub account: Option<ReadWritePermissions>,
    pub basin: Option<ReadWritePermissions>,
    pub stream: Option<ReadWritePermissions>,
}

impl PermittedOperationGroups {
    /// Create a new permitted operation groups.
    pub fn new() -> Self {
        Self::default()
    }

    /// Overwrite account read-write permissions.
    pub fn with_account(self, account: ReadWritePermissions) -> Self {
        Self {
            account: Some(account),
            ..self
        }
    }

    /// Overwrite basin read-write permissions.
    pub fn with_basin(self, basin: ReadWritePermissions) -> Self {
        Self {
            basin: Some(basin),
            ..self
        }
    }

    /// Overwrite stream read-write permissions.
    pub fn with_stream(self, stream: ReadWritePermissions) -> Self {
        Self {
            stream: Some(stream),
            ..self
        }
    }
}

impl From<PermittedOperationGroups> for api::PermittedOperationGroups {
    fn from(value: PermittedOperationGroups) -> Self {
        let PermittedOperationGroups {
            account,
            basin,
            stream,
        } = value;
        Self {
            account: account.map(Into::into),
            basin: basin.map(Into::into),
            stream: stream.map(Into::into),
        }
    }
}

impl From<api::PermittedOperationGroups> for PermittedOperationGroups {
    fn from(value: api::PermittedOperationGroups) -> Self {
        let api::PermittedOperationGroups {
            account,
            basin,
            stream,
        } = value;
        Self {
            account: account.map(Into::into),
            basin: basin.map(Into::into),
            stream: stream.map(Into::into),
        }
    }
}

#[sync_docs]
#[derive(Debug, Clone, Default)]
pub struct ReadWritePermissions {
    pub read: bool,
    pub write: bool,
}

impl ReadWritePermissions {
    /// Create a new read-write permission.
    pub fn new() -> Self {
        Self::default()
    }

    /// Overwrite read permission.
    pub fn with_read(self, read: bool) -> Self {
        Self { read, ..self }
    }

    /// Overwrite write permission.
    pub fn with_write(self, write: bool) -> Self {
        Self { write, ..self }
    }
}

impl From<ReadWritePermissions> for api::ReadWritePermissions {
    fn from(value: ReadWritePermissions) -> Self {
        let ReadWritePermissions { read, write } = value;
        Self { read, write }
    }
}

impl From<api::ReadWritePermissions> for ReadWritePermissions {
    fn from(value: api::ReadWritePermissions) -> Self {
        let api::ReadWritePermissions { read, write } = value;
        Self { read, write }
    }
}

impl From<api::IssueAccessTokenResponse> for String {
    fn from(value: api::IssueAccessTokenResponse) -> Self {
        value.access_token
    }
}

impl From<AccessTokenId> for api::RevokeAccessTokenRequest {
    fn from(value: AccessTokenId) -> Self {
        Self { id: value.into() }
    }
}

impl TryFrom<api::RevokeAccessTokenResponse> for AccessTokenInfo {
    type Error = ConvertError;
    fn try_from(value: api::RevokeAccessTokenResponse) -> Result<Self, Self::Error> {
        let token_info = value.info.ok_or("access token info is missing")?;
        token_info.try_into()
    }
}

#[sync_docs]
#[derive(Debug, Clone, Default)]
pub struct ListAccessTokensRequest {
    pub prefix: String,
    pub start_after: String,
    pub limit: Option<usize>,
}

impl ListAccessTokensRequest {
    /// Create a new request with prefix.
    pub fn new() -> Self {
        Self::default()
    }

    /// Overwrite prefix.
    pub fn with_prefix(self, prefix: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
            ..self
        }
    }

    /// Overwrite start after.
    pub fn with_start_after(self, start_after: impl Into<String>) -> Self {
        Self {
            start_after: start_after.into(),
            ..self
        }
    }

    /// Overwrite limit.
    pub fn with_limit(self, limit: impl Into<Option<usize>>) -> Self {
        Self {
            limit: limit.into(),
            ..self
        }
    }
}

impl TryFrom<ListAccessTokensRequest> for api::ListAccessTokensRequest {
    type Error = ConvertError;
    fn try_from(value: ListAccessTokensRequest) -> Result<Self, Self::Error> {
        let ListAccessTokensRequest {
            prefix,
            start_after,
            limit,
        } = value;
        Ok(Self {
            prefix,
            start_after,
            limit: limit
                .map(TryInto::try_into)
                .transpose()
                .map_err(|_| "request limit does not fit into u64 bounds")?,
        })
    }
}

#[sync_docs]
#[derive(Debug, Clone)]
pub struct ListAccessTokensResponse {
    pub access_tokens: Vec<AccessTokenInfo>,
    pub has_more: bool,
}

impl TryFrom<api::ListAccessTokensResponse> for ListAccessTokensResponse {
    type Error = ConvertError;
    fn try_from(value: api::ListAccessTokensResponse) -> Result<Self, Self::Error> {
        let api::ListAccessTokensResponse {
            access_tokens,
            has_more,
        } = value;
        let access_tokens = access_tokens
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            access_tokens,
            has_more,
        })
    }
}

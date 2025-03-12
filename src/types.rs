//! Types for interacting with S2 services.

use std::{ops::Deref, str::FromStr, sync::OnceLock, time::Duration};

use bytes::Bytes;
use rand::{distributions::Uniform, Rng};
use regex::Regex;
use sync_docs::sync_docs;

use crate::api;

pub(crate) const MIB_BYTES: u64 = 1024 * 1024;

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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BasinScope {
    #[default]
    Unspecified,
    AwsUsEast1,
}

impl From<BasinScope> for api::BasinScope {
    fn from(value: BasinScope) -> Self {
        match value {
            BasinScope::Unspecified => Self::Unspecified,
            BasinScope::AwsUsEast1 => Self::AwsUsEast1,
        }
    }
}

impl From<api::BasinScope> for BasinScope {
    fn from(value: api::BasinScope) -> Self {
        match value {
            api::BasinScope::Unspecified => Self::Unspecified,
            api::BasinScope::AwsUsEast1 => Self::AwsUsEast1,
        }
    }
}

impl FromStr for BasinScope {
    type Err = ConvertError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "unspecified" => Ok(Self::Unspecified),
            "aws:us-east-1" => Ok(Self::AwsUsEast1),
            _ => Err("invalid basin scope value".into()),
        }
    }
}

impl From<BasinScope> for i32 {
    fn from(value: BasinScope) -> Self {
        api::BasinScope::from(value).into()
    }
}

impl TryFrom<i32> for BasinScope {
    type Error = ConvertError;
    fn try_from(value: i32) -> Result<Self, Self::Error> {
        api::BasinScope::try_from(value)
            .map(Into::into)
            .map_err(|_| "invalid basin scope value".into())
    }
}

#[sync_docs]
#[derive(Debug, Clone)]
pub struct CreateBasinRequest {
    pub basin: BasinName,
    pub config: Option<BasinConfig>,
    pub scope: BasinScope,
}

impl CreateBasinRequest {
    /// Create a new request with basin name.
    pub fn new(basin: BasinName) -> Self {
        Self {
            basin,
            config: None,
            scope: BasinScope::Unspecified,
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
        Self { scope, ..self }
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
            scope: scope.into(),
        }
    }
}

#[sync_docs]
#[derive(Debug, Clone, Default)]
pub struct BasinConfig {
    pub default_stream_config: Option<StreamConfig>,
    pub create_stream_on_append: bool,
}

impl From<BasinConfig> for api::BasinConfig {
    fn from(value: BasinConfig) -> Self {
        let BasinConfig {
            default_stream_config,
            create_stream_on_append,
        } = value;
        Self {
            default_stream_config: default_stream_config.map(Into::into),
            create_stream_on_append,
        }
    }
}

impl TryFrom<api::BasinConfig> for BasinConfig {
    type Error = ConvertError;
    fn try_from(value: api::BasinConfig) -> Result<Self, Self::Error> {
        let api::BasinConfig {
            default_stream_config,
            create_stream_on_append,
        } = value;
        Ok(Self {
            default_stream_config: default_stream_config.map(TryInto::try_into).transpose()?,
            create_stream_on_append,
        })
    }
}

#[sync_docs]
#[derive(Debug, Clone, Default)]
pub struct StreamConfig {
    pub storage_class: StorageClass,
    pub retention_policy: Option<RetentionPolicy>,
}

impl StreamConfig {
    /// Create a new request.
    pub fn new() -> Self {
        Self::default()
    }

    /// Overwrite storage class.
    pub fn with_storage_class(self, storage_class: impl Into<StorageClass>) -> Self {
        Self {
            storage_class: storage_class.into(),
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
}

impl From<StreamConfig> for api::StreamConfig {
    fn from(value: StreamConfig) -> Self {
        let StreamConfig {
            storage_class,
            retention_policy,
        } = value;
        Self {
            storage_class: storage_class.into(),
            retention_policy: retention_policy.map(Into::into),
        }
    }
}

impl TryFrom<api::StreamConfig> for StreamConfig {
    type Error = ConvertError;
    fn try_from(value: api::StreamConfig) -> Result<Self, Self::Error> {
        let api::StreamConfig {
            storage_class,
            retention_policy,
        } = value;
        Ok(Self {
            storage_class: storage_class.try_into()?,
            retention_policy: retention_policy.map(Into::into),
        })
    }
}

#[sync_docs]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StorageClass {
    #[default]
    Unspecified,
    Standard,
    Express,
}

impl From<StorageClass> for api::StorageClass {
    fn from(value: StorageClass) -> Self {
        match value {
            StorageClass::Unspecified => Self::Unspecified,
            StorageClass::Standard => Self::Standard,
            StorageClass::Express => Self::Express,
        }
    }
}

impl From<api::StorageClass> for StorageClass {
    fn from(value: api::StorageClass) -> Self {
        match value {
            api::StorageClass::Unspecified => Self::Unspecified,
            api::StorageClass::Standard => Self::Standard,
            api::StorageClass::Express => Self::Express,
        }
    }
}

impl FromStr for StorageClass {
    type Err = ConvertError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "unspecified" => Ok(Self::Unspecified),
            "standard" => Ok(Self::Standard),
            "express" => Ok(Self::Express),
            _ => Err("invalid storage class value".into()),
        }
    }
}

impl From<StorageClass> for i32 {
    fn from(value: StorageClass) -> Self {
        api::StorageClass::from(value).into()
    }
}

impl TryFrom<i32> for StorageClass {
    type Error = ConvertError;
    fn try_from(value: i32) -> Result<Self, Self::Error> {
        api::StorageClass::try_from(value)
            .map(Into::into)
            .map_err(|_| "invalid storage class value".into())
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
    Unspecified,
    Active,
    Creating,
    Deleting,
}

impl From<BasinState> for api::BasinState {
    fn from(value: BasinState) -> Self {
        match value {
            BasinState::Unspecified => Self::Unspecified,
            BasinState::Active => Self::Active,
            BasinState::Creating => Self::Creating,
            BasinState::Deleting => Self::Deleting,
        }
    }
}

impl From<api::BasinState> for BasinState {
    fn from(value: api::BasinState) -> Self {
        match value {
            api::BasinState::Unspecified => Self::Unspecified,
            api::BasinState::Active => Self::Active,
            api::BasinState::Creating => Self::Creating,
            api::BasinState::Deleting => Self::Deleting,
        }
    }
}

impl From<BasinState> for i32 {
    fn from(value: BasinState) -> Self {
        api::BasinState::from(value).into()
    }
}

impl TryFrom<i32> for BasinState {
    type Error = ConvertError;
    fn try_from(value: i32) -> Result<Self, Self::Error> {
        api::BasinState::try_from(value)
            .map(Into::into)
            .map_err(|_| "invalid basin status value".into())
    }
}

impl std::fmt::Display for BasinState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BasinState::Unspecified => write!(f, "unspecified"),
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
    pub scope: BasinScope,
    pub state: BasinState,
}

impl From<BasinInfo> for api::BasinInfo {
    fn from(value: BasinInfo) -> Self {
        let BasinInfo { name, scope, state } = value;
        Self {
            name,
            scope: scope.into(),
            state: state.into(),
        }
    }
}

impl TryFrom<api::BasinInfo> for BasinInfo {
    type Error = ConvertError;
    fn try_from(value: api::BasinInfo) -> Result<Self, Self::Error> {
        let api::BasinInfo { name, scope, state } = value;
        Ok(Self {
            name,
            scope: scope.try_into()?,
            state: state.try_into()?,
        })
    }
}

impl TryFrom<api::CreateBasinResponse> for BasinInfo {
    type Error = ConvertError;
    fn try_from(value: api::CreateBasinResponse) -> Result<Self, Self::Error> {
        let api::CreateBasinResponse { info } = value;
        let info = info.ok_or("missing basin info")?;
        info.try_into()
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
        config.try_into()
    }
}

impl TryFrom<api::GetStreamConfigResponse> for StreamConfig {
    type Error = ConvertError;
    fn try_from(value: api::GetStreamConfigResponse) -> Result<Self, Self::Error> {
        let api::GetStreamConfigResponse { config } = value;
        let config = config.ok_or("missing stream config")?;
        config.try_into()
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
            basins: basins
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<BasinInfo>, ConvertError>>()?,
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
        config.try_into()
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
        config.try_into()
    }
}

impl From<api::CheckTailResponse> for u64 {
    fn from(value: api::CheckTailResponse) -> Self {
        let api::CheckTailResponse { next_seq_num } = value;
        next_seq_num
    }
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
/// Must not be more than 16 bytes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FencingToken(Bytes);

impl FencingToken {
    const MAX_BYTES: usize = 16;

    /// Try creating a new fencing token from bytes.
    pub fn new(bytes: impl Into<Bytes>) -> Result<Self, ConvertError> {
        let bytes = bytes.into();
        if bytes.len() > Self::MAX_BYTES {
            Err(format!(
                "Size of a fencing token cannot exceed {} bytes",
                Self::MAX_BYTES
            )
            .into())
        } else {
            Ok(Self(bytes))
        }
    }

    /// Generate a random fencing token with `n` bytes.
    pub fn generate(n: usize) -> Result<Self, ConvertError> {
        Self::new(
            rand::thread_rng()
                .sample_iter(&Uniform::new_inclusive(0, u8::MAX))
                .take(n)
                .collect::<Bytes>(),
        )
    }
}

impl AsRef<Bytes> for FencingToken {
    fn as_ref(&self) -> &Bytes {
        &self.0
    }
}

impl AsRef<[u8]> for FencingToken {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl Deref for FencingToken {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<FencingToken> for Bytes {
    fn from(value: FencingToken) -> Self {
        value.0
    }
}

impl From<FencingToken> for Vec<u8> {
    fn from(value: FencingToken) -> Self {
        value.0.into()
    }
}

impl TryFrom<Bytes> for FencingToken {
    type Error = ConvertError;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<Vec<u8>> for FencingToken {
    type Error = ConvertError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

/// A command record is a special kind of [`AppendRecord`] that can be used to
/// send command messages.
///
/// Such a record is signalled by a sole header with empty name. The header
/// value represents the operation and record body acts as the payload.
#[derive(Debug, Clone)]
pub enum CommandRecord {
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

impl CommandRecord {
    const FENCE: &[u8] = b"fence";
    const TRIM: &[u8] = b"trim";

    /// Create a new fence command record.
    pub fn fence(fencing_token: FencingToken) -> Self {
        Self::Fence { fencing_token }
    }

    /// Create a new trim command record.
    pub fn trim(seq_num: impl Into<u64>) -> Self {
        Self::Trim {
            seq_num: seq_num.into(),
        }
    }
}

#[sync_docs]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendRecord {
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

    /// Body of the record.
    pub fn body(&self) -> &[u8] {
        &self.body
    }

    /// Headers of the record.
    pub fn headers(&self) -> &[Header] {
        &self.headers
    }

    /// Consume the record and return parts.
    pub fn into_parts(self) -> AppendRecordParts {
        AppendRecordParts {
            headers: self.headers,
            body: self.body,
        }
    }

    /// Try creating the record from parts.
    pub fn try_from_parts(parts: AppendRecordParts) -> Result<Self, ConvertError> {
        Self::new(parts.body)?.with_headers(parts.headers)
    }
}

impl From<AppendRecord> for api::AppendRecord {
    fn from(value: AppendRecord) -> Self {
        Self {
            headers: value.headers.into_iter().map(Into::into).collect(),
            body: value.body,
        }
    }
}

impl From<CommandRecord> for AppendRecord {
    fn from(value: CommandRecord) -> Self {
        let (header_value, body) = match value {
            CommandRecord::Fence { fencing_token } => (CommandRecord::FENCE, fencing_token.into()),
            CommandRecord::Trim { seq_num } => {
                (CommandRecord::TRIM, seq_num.to_be_bytes().to_vec())
            }
        };
        AppendRecordParts {
            headers: vec![Header::from_value(header_value)],
            body: body.into(),
        }
        .try_into()
        .expect("command record is a valid append record")
    }
}

#[sync_docs(AppendRecordParts = "AppendRecord")]
#[derive(Debug, Clone)]
pub struct AppendRecordParts {
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

#[sync_docs]
#[derive(Debug, Clone)]
pub struct AppendOutput {
    pub start_seq_num: u64,
    pub end_seq_num: u64,
    pub next_seq_num: u64,
}

impl From<api::AppendOutput> for AppendOutput {
    fn from(value: api::AppendOutput) -> Self {
        let api::AppendOutput {
            start_seq_num,
            end_seq_num,
            next_seq_num,
        } = value;
        Self {
            start_seq_num,
            end_seq_num,
            next_seq_num,
        }
    }
}

impl TryFrom<api::AppendResponse> for AppendOutput {
    type Error = ConvertError;
    fn try_from(value: api::AppendResponse) -> Result<Self, Self::Error> {
        let api::AppendResponse { output } = value;
        let output = output.ok_or("missing append output")?;
        Ok(output.into())
    }
}

impl TryFrom<api::AppendSessionResponse> for AppendOutput {
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

#[sync_docs]
#[derive(Debug, Clone, Default)]
pub struct ReadRequest {
    pub start_seq_num: u64,
    pub limit: ReadLimit,
}

impl ReadRequest {
    /// Create a new request with start sequence number.
    pub fn new(start_seq_num: u64) -> Self {
        Self {
            start_seq_num,
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
        let Self {
            start_seq_num,
            limit,
        } = self;

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
            start_seq_num,
            limit: Some(limit),
        })
    }
}

#[sync_docs]
#[derive(Debug, Clone)]
pub struct SequencedRecord {
    pub seq_num: u64,
    pub headers: Vec<Header>,
    pub body: Bytes,
}

metered_impl!(SequencedRecord);

impl From<api::SequencedRecord> for SequencedRecord {
    fn from(value: api::SequencedRecord) -> Self {
        let api::SequencedRecord {
            seq_num,
            headers,
            body,
        } = value;
        Self {
            seq_num,
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
                let fencing_token = FencingToken::new(self.body.clone()).ok()?;
                Some(CommandRecord::Fence { fencing_token })
            }
            CommandRecord::TRIM => {
                let body: &[u8] = &self.body;
                let seq_num = u64::from_be_bytes(body.try_into().ok()?);
                Some(CommandRecord::Trim { seq_num })
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
    FirstSeqNum(u64),
    NextSeqNum(u64),
}

impl From<api::read_output::Output> for ReadOutput {
    fn from(value: api::read_output::Output) -> Self {
        match value {
            api::read_output::Output::Batch(batch) => Self::Batch(batch.into()),
            api::read_output::Output::FirstSeqNum(first_seq_num) => {
                Self::FirstSeqNum(first_seq_num)
            }
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
    pub start_seq_num: u64,
    pub limit: ReadLimit,
}

impl ReadSessionRequest {
    /// Create a new request with start sequene number.
    pub fn new(start_seq_num: u64) -> Self {
        Self {
            start_seq_num,
            ..Default::default()
        }
    }

    /// Overwrite limit.
    pub fn with_limit(self, limit: ReadLimit) -> Self {
        Self { limit, ..self }
    }

    pub(crate) fn into_api_type(self, stream: impl Into<String>) -> api::ReadSessionRequest {
        let Self {
            start_seq_num,
            limit,
        } = self;
        api::ReadSessionRequest {
            stream: stream.into(),
            start_seq_num,
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

impl AsRef<str> for BasinName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

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

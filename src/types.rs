use std::{ops::Deref, str::FromStr, sync::OnceLock, time::Duration};

use bytesize::ByteSize;
use regex::Regex;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use sync_docs::sync_docs;

use crate::api;

#[derive(Debug, Clone, thiserror::Error)]
#[error("{0}")]
pub struct ConvertError(String);

impl<T: Into<String>> From<T> for ConvertError {
    fn from(value: T) -> Self {
        Self(value.into())
    }
}

pub trait MeteredSize {
    fn metered_size(&self) -> ByteSize;
}

impl<T: MeteredSize> MeteredSize for Vec<T> {
    fn metered_size(&self) -> ByteSize {
        self.iter()
            .fold(ByteSize::b(0), |acc, item| acc + item.metered_size())
    }
}

macro_rules! metered_impl {
    ($ty:ty) => {
        impl MeteredSize for $ty {
            fn metered_size(&self) -> ByteSize {
                let bytes = 8
                    + (2 * self.headers.len())
                    + self
                        .headers
                        .iter()
                        .map(|h| h.name.len() + h.value.len())
                        .sum::<usize>()
                    + self.body.len();
                ByteSize::b(bytes as u64)
            }
        }
    };
}

#[sync_docs]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone)]
pub struct CreateBasinRequest {
    pub basin: BasinName,
    pub config: Option<BasinConfig>,
    // TODO: Add assignment (when it's supported).
}

impl CreateBasinRequest {
    pub fn new(basin: BasinName) -> Self {
        Self {
            basin,
            config: None,
        }
    }

    pub fn with_config(self, config: BasinConfig) -> Self {
        Self {
            config: Some(config),
            ..self
        }
    }
}

impl TryFrom<CreateBasinRequest> for api::CreateBasinRequest {
    type Error = ConvertError;
    fn try_from(value: CreateBasinRequest) -> Result<Self, Self::Error> {
        let CreateBasinRequest { basin, config } = value;
        Ok(Self {
            basin: basin.0,
            config: config.map(TryInto::try_into).transpose()?,
            assignment: None,
        })
    }
}

#[sync_docs]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone, Default)]
pub struct BasinConfig {
    pub default_stream_config: Option<StreamConfig>,
}

impl BasinConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_default_stream_config(default_stream_config: StreamConfig) -> Self {
        Self {
            default_stream_config: Some(default_stream_config),
        }
    }
}

impl TryFrom<BasinConfig> for api::BasinConfig {
    type Error = ConvertError;
    fn try_from(value: BasinConfig) -> Result<Self, Self::Error> {
        let BasinConfig {
            default_stream_config,
        } = value;
        Ok(Self {
            default_stream_config: default_stream_config.map(TryInto::try_into).transpose()?,
        })
    }
}

impl TryFrom<api::BasinConfig> for BasinConfig {
    type Error = ConvertError;
    fn try_from(value: api::BasinConfig) -> Result<Self, Self::Error> {
        let api::BasinConfig {
            default_stream_config,
        } = value;
        Ok(Self {
            default_stream_config: default_stream_config.map(TryInto::try_into).transpose()?,
        })
    }
}

#[sync_docs]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone, Default)]
pub struct StreamConfig {
    pub storage_class: StorageClass,
    pub retention_policy: Option<RetentionPolicy>,
}

impl StreamConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_storage_class(self, storage_class: impl Into<StorageClass>) -> Self {
        Self {
            storage_class: storage_class.into(),
            ..self
        }
    }

    pub fn with_retention_policy(self, retention_policy: RetentionPolicy) -> Self {
        Self {
            retention_policy: Some(retention_policy),
            ..self
        }
    }
}

impl TryFrom<StreamConfig> for api::StreamConfig {
    type Error = ConvertError;
    fn try_from(value: StreamConfig) -> Result<Self, Self::Error> {
        let StreamConfig {
            storage_class,
            retention_policy,
        } = value;
        Ok(Self {
            storage_class: storage_class.into(),
            retention_policy: retention_policy.map(TryInto::try_into).transpose()?,
        })
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
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
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

#[sync_docs(Age = "AgeMillis")]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone)]
pub enum RetentionPolicy {
    Age(Duration),
}

impl TryFrom<RetentionPolicy> for api::stream_config::RetentionPolicy {
    type Error = ConvertError;
    fn try_from(value: RetentionPolicy) -> Result<Self, Self::Error> {
        match value {
            RetentionPolicy::Age(duration) => Ok(Self::AgeMillis(
                duration
                    .as_millis()
                    .try_into()
                    .map_err(|_| "age duration overflow in milliseconds")?,
            )),
        }
    }
}

impl From<api::stream_config::RetentionPolicy> for RetentionPolicy {
    fn from(value: api::stream_config::RetentionPolicy) -> Self {
        match value {
            api::stream_config::RetentionPolicy::AgeMillis(millis) => {
                Self::Age(Duration::from_millis(millis))
            }
        }
    }
}

#[sync_docs]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
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
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone)]
pub struct BasinInfo {
    pub name: String,
    pub scope: String,
    pub cell: String,
    pub state: BasinState,
}

impl From<BasinInfo> for api::BasinInfo {
    fn from(value: BasinInfo) -> Self {
        let BasinInfo {
            name,
            scope,
            cell,
            state,
        } = value;
        Self {
            name,
            scope,
            cell,
            state: state.into(),
        }
    }
}

impl TryFrom<api::BasinInfo> for BasinInfo {
    type Error = ConvertError;
    fn try_from(value: api::BasinInfo) -> Result<Self, Self::Error> {
        let api::BasinInfo {
            name,
            scope,
            cell,
            state,
        } = value;
        Ok(Self {
            name,
            scope,
            cell,
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
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone, Default)]
pub struct ListStreamsRequest {
    pub prefix: String,
    pub start_after: String,
    pub limit: usize,
}

impl ListStreamsRequest {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_prefix(self, prefix: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
            ..self
        }
    }

    pub fn with_start_after(self, start_after: impl Into<String>) -> Self {
        Self {
            start_after: start_after.into(),
            ..self
        }
    }

    pub fn with_limit(self, limit: impl Into<usize>) -> Self {
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
            limit: limit
                .try_into()
                .map_err(|_| "request limit does not fit into u64 bounds")?,
        })
    }
}

#[sync_docs]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
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

#[sync_docs]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
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
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone)]
pub struct CreateStreamRequest {
    pub stream: String,
    pub config: Option<StreamConfig>,
}

impl CreateStreamRequest {
    pub fn new(stream: impl Into<String>) -> Self {
        Self {
            stream: stream.into(),
            config: None,
        }
    }

    pub fn with_config(self, config: StreamConfig) -> Self {
        Self {
            config: Some(config),
            ..self
        }
    }
}

impl TryFrom<CreateStreamRequest> for api::CreateStreamRequest {
    type Error = ConvertError;
    fn try_from(value: CreateStreamRequest) -> Result<Self, Self::Error> {
        let CreateStreamRequest { stream, config } = value;
        Ok(Self {
            stream,
            config: config.map(TryInto::try_into).transpose()?,
        })
    }
}

#[sync_docs]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone, Default)]
pub struct ListBasinsRequest {
    pub prefix: String,
    pub start_after: String,
    pub limit: usize,
}

impl ListBasinsRequest {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_prefix(self, prefix: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
            ..self
        }
    }

    pub fn with_start_after(self, start_after: impl Into<String>) -> Self {
        Self {
            start_after: start_after.into(),
            ..self
        }
    }

    pub fn with_limit(self, limit: impl Into<usize>) -> Self {
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
                .try_into()
                .map_err(|_| "request limit does not fit into u64 bounds")?,
        })
    }
}

#[sync_docs]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
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
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone)]
pub struct DeleteBasinRequest {
    pub basin: BasinName,
    /// Delete basin if it exists else do nothing.
    pub if_exists: bool,
}

impl DeleteBasinRequest {
    pub fn new(basin: BasinName) -> Self {
        Self {
            basin,
            if_exists: false,
        }
    }

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
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone)]
pub struct DeleteStreamRequest {
    pub stream: String,
    /// Delete stream if it exists else do nothing.
    pub if_exists: bool,
}

impl DeleteStreamRequest {
    pub fn new(stream: impl Into<String>) -> Self {
        Self {
            stream: stream.into(),
            if_exists: false,
        }
    }

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
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone)]
pub struct ReconfigureBasinRequest {
    pub basin: BasinName,
    pub config: Option<BasinConfig>,
    pub mask: Option<Vec<String>>,
}

impl ReconfigureBasinRequest {
    pub fn new(basin: BasinName) -> Self {
        Self {
            basin,
            config: None,
            mask: None,
        }
    }

    pub fn with_config(self, config: BasinConfig) -> Self {
        Self {
            config: Some(config),
            ..self
        }
    }

    pub fn with_mask(self, mask: impl Into<Vec<String>>) -> Self {
        Self {
            mask: Some(mask.into()),
            ..self
        }
    }
}

impl TryFrom<ReconfigureBasinRequest> for api::ReconfigureBasinRequest {
    type Error = ConvertError;
    fn try_from(value: ReconfigureBasinRequest) -> Result<Self, Self::Error> {
        let ReconfigureBasinRequest {
            basin,
            config,
            mask,
        } = value;
        Ok(Self {
            basin: basin.0,
            config: config.map(TryInto::try_into).transpose()?,
            mask: mask.map(|paths| prost_types::FieldMask { paths }),
        })
    }
}

#[sync_docs]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone)]
pub struct ReconfigureStreamRequest {
    pub stream: String,
    pub config: Option<StreamConfig>,
    pub mask: Option<Vec<String>>,
}

impl ReconfigureStreamRequest {
    pub fn new(stream: impl Into<String>) -> Self {
        Self {
            stream: stream.into(),
            config: None,
            mask: None,
        }
    }

    pub fn with_config(self, config: StreamConfig) -> Self {
        Self {
            config: Some(config),
            ..self
        }
    }

    pub fn with_mask(self, mask: impl Into<Vec<String>>) -> Self {
        Self {
            mask: Some(mask.into()),
            ..self
        }
    }
}

impl TryFrom<ReconfigureStreamRequest> for api::ReconfigureStreamRequest {
    type Error = ConvertError;
    fn try_from(value: ReconfigureStreamRequest) -> Result<Self, Self::Error> {
        let ReconfigureStreamRequest {
            stream,
            config,
            mask,
        } = value;
        Ok(Self {
            stream,
            config: config.map(TryInto::try_into).transpose()?,
            mask: mask.map(|paths| prost_types::FieldMask { paths }),
        })
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

impl From<api::CheckTailResponse> for u64 {
    fn from(value: api::CheckTailResponse) -> Self {
        let api::CheckTailResponse { next_seq_num } = value;
        next_seq_num
    }
}

#[sync_docs]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    pub name: Vec<u8>,
    pub value: Vec<u8>,
}

impl Header {
    pub fn new(name: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
        }
    }

    pub fn from_value(value: impl Into<Vec<u8>>) -> Self {
        Self {
            name: Vec::new(),
            value: value.into(),
        }
    }
}

impl From<Header> for api::Header {
    fn from(value: Header) -> Self {
        let Header { name, value } = value;
        Self {
            name: name.into(),
            value: value.into(),
        }
    }
}

impl From<api::Header> for Header {
    fn from(value: api::Header) -> Self {
        let api::Header { name, value } = value;
        Self {
            name: name.into(),
            value: value.into(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FencingToken(Vec<u8>);

impl FencingToken {
    const MAX_SIZE: usize = 16;

    pub fn new(bytes: impl Into<Vec<u8>>) -> Result<Self, ConvertError> {
        let bytes = bytes.into();
        if bytes.len() > Self::MAX_SIZE {
            Err(format!(
                "size of a fencing token cannot exceed {} bytes",
                Self::MAX_SIZE
            )
            .into())
        } else {
            Ok(Self(bytes))
        }
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

impl From<FencingToken> for Vec<u8> {
    fn from(value: FencingToken) -> Self {
        value.0
    }
}

impl TryFrom<Vec<u8>> for FencingToken {
    type Error = ConvertError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

#[sync_docs]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone)]
pub enum CommandRecord {
    Fence { fencing_token: FencingToken },
    Trim { seq_num: u64 },
}

impl CommandRecord {
    pub fn fence(fencing_token: Option<FencingToken>) -> Self {
        Self::Fence {
            fencing_token: fencing_token.unwrap_or_default(),
        }
    }

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
    body: Vec<u8>,
    #[cfg(test)]
    max_size: ByteSize,
}

metered_impl!(AppendRecord);

impl AppendRecord {
    const MAX_SIZE: ByteSize = ByteSize::mib(1);

    fn validated(self) -> Result<Self, ConvertError> {
        #[cfg(test)]
        let max_size = self.max_size;
        #[cfg(not(test))]
        let max_size = Self::MAX_SIZE;

        if self.metered_size() > max_size {
            Err("AppendRecord should have metered size less than 1 MiB".into())
        } else {
            Ok(self)
        }
    }

    pub fn new(body: impl Into<Vec<u8>>) -> Result<Self, ConvertError> {
        Self {
            headers: Vec::new(),
            body: body.into(),
            #[cfg(test)]
            max_size: Self::MAX_SIZE,
        }
        .validated()
    }

    #[cfg(test)]
    pub fn with_max_size(
        max_size: ByteSize,
        body: impl Into<Vec<u8>>,
    ) -> Result<Self, ConvertError> {
        Self {
            headers: Vec::new(),
            body: body.into(),
            max_size,
        }
        .validated()
    }

    pub fn with_headers(self, headers: impl Into<Vec<Header>>) -> Result<Self, ConvertError> {
        Self {
            headers: headers.into(),
            ..self
        }
        .validated()
    }

    pub fn body(&self) -> &[u8] {
        &self.body
    }

    pub fn headers(&self) -> &[Header] {
        &self.headers
    }

    pub fn into_parts(self) -> AppendRecordParts {
        AppendRecordParts {
            headers: self.headers,
            body: self.body,
        }
    }

    pub fn try_from_parts(parts: AppendRecordParts) -> Result<Self, ConvertError> {
        Self::new(parts.body)?.with_headers(parts.headers)
    }
}

impl From<AppendRecord> for api::AppendRecord {
    fn from(value: AppendRecord) -> Self {
        Self {
            headers: value.headers.into_iter().map(Into::into).collect(),
            body: value.body.into(),
        }
    }
}

impl TryFrom<CommandRecord> for AppendRecord {
    type Error = ConvertError;

    fn try_from(value: CommandRecord) -> Result<Self, Self::Error> {
        let (header_value, body) = match value {
            CommandRecord::Fence { fencing_token } => ("fence", fencing_token.into()),
            CommandRecord::Trim { seq_num } => ("trim", seq_num.to_be_bytes().to_vec()),
        };
        Self::new(body)?.with_headers(vec![Header::from_value(header_value)])
    }
}

#[derive(Debug, Clone)]
pub struct AppendRecordParts {
    pub headers: Vec<Header>,
    pub body: Vec<u8>,
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

#[derive(Debug, Clone)]
pub struct AppendRecordBatch {
    records: Vec<AppendRecord>,
    metered_size: ByteSize,
    max_capacity: usize,
    #[cfg(test)]
    max_size: ByteSize,
}

impl PartialEq for AppendRecordBatch {
    fn eq(&self, other: &Self) -> bool {
        if self.records.eq(&other.records) {
            assert_eq!(self.metered_size, other.metered_size);
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
    pub const MAX_CAPACITY: usize = 1000;
    pub const MAX_SIZE: ByteSize = ByteSize::mib(1);

    pub fn new() -> Self {
        Self::with_max_capacity(Self::MAX_CAPACITY)
    }

    pub fn with_max_capacity(max_capacity: usize) -> Self {
        assert!(
            max_capacity > 0 && max_capacity <= Self::MAX_CAPACITY,
            "Batch capacity must be between 1 and 1000"
        );

        Self {
            records: Vec::with_capacity(max_capacity),
            metered_size: ByteSize(0),
            max_capacity,
            #[cfg(test)]
            max_size: Self::MAX_SIZE,
        }
    }

    #[cfg(test)]
    pub fn with_max_capacity_and_size(max_capacity: usize, max_size: ByteSize) -> Self {
        #[cfg(test)]
        assert!(
            max_size > ByteSize(0) || max_size <= Self::MAX_SIZE,
            "Batch size must be between 1 byte and 1 MiB"
        );

        Self {
            max_size,
            ..Self::with_max_capacity(max_capacity)
        }
    }

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

    pub fn is_empty(&self) -> bool {
        if self.records.is_empty() {
            assert_eq!(self.metered_size, ByteSize(0));
            true
        } else {
            false
        }
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }

    #[cfg(test)]
    fn max_size(&self) -> ByteSize {
        self.max_size
    }

    #[cfg(not(test))]
    fn max_size(&self) -> ByteSize {
        Self::MAX_SIZE
    }

    pub fn is_full(&self) -> bool {
        self.records.len() >= self.max_capacity || self.metered_size >= self.max_size()
    }

    pub fn push(&mut self, record: impl Into<AppendRecord>) -> Result<(), AppendRecord> {
        assert!(self.records.len() <= self.max_capacity);
        assert!(self.metered_size <= self.max_size());

        let record = record.into();
        let record_size = record.metered_size();
        if self.records.len() >= self.max_capacity
            || self.metered_size + record_size > self.max_size()
        {
            Err(record)
        } else {
            self.records.push(record);
            self.metered_size += record_size;
            Ok(())
        }
    }
}

impl MeteredSize for AppendRecordBatch {
    fn metered_size(&self) -> ByteSize {
        self.metered_size
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
#[derive(Debug, Clone)]
pub struct AppendInput {
    pub records: AppendRecordBatch,
    pub match_seq_num: Option<u64>,
    pub fencing_token: Option<FencingToken>,
}

impl MeteredSize for AppendInput {
    fn metered_size(&self) -> ByteSize {
        self.records.metered_size()
    }
}

impl AppendInput {
    pub fn new(records: impl Into<AppendRecordBatch>) -> Self {
        Self {
            records: records.into(),
            match_seq_num: None,
            fencing_token: None,
        }
    }

    pub fn with_match_seq_num(self, match_seq_num: impl Into<u64>) -> Self {
        Self {
            match_seq_num: Some(match_seq_num.into()),
            ..self
        }
    }

    pub fn with_fencing_token(self, fencing_token: FencingToken) -> Self {
        Self {
            fencing_token: Some(fencing_token),
            ..self
        }
    }

    pub fn into_api_type(self, stream: impl Into<String>) -> api::AppendInput {
        let Self {
            records,
            match_seq_num,
            fencing_token,
        } = self;

        api::AppendInput {
            stream: stream.into(),
            records: records.into_iter().map(Into::into).collect(),
            match_seq_num,
            fencing_token: fencing_token.map(|f| f.0.into()),
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
    pub count: u64,
    pub bytes: u64,
}

#[sync_docs]
#[derive(Debug, Clone, Default)]
pub struct ReadRequest {
    pub start_seq_num: Option<u64>,
    pub limit: Option<ReadLimit>,
}

impl ReadRequest {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_start_seq_num(self, start_seq_num: impl Into<u64>) -> Self {
        Self {
            start_seq_num: Some(start_seq_num.into()),
            ..self
        }
    }

    pub fn with_limit(self, limit: ReadLimit) -> Self {
        Self {
            limit: Some(limit),
            ..self
        }
    }
}

impl ReadRequest {
    pub fn try_into_api_type(
        self,
        stream: impl Into<String>,
    ) -> Result<api::ReadRequest, ConvertError> {
        let Self {
            start_seq_num,
            limit,
        } = self;

        let limit: Option<api::ReadLimit> = match limit {
            None => None,
            Some(limit) => Some({
                if limit.count > 1000 {
                    Err("read limit: count must not exceed 1000 for unary request")
                } else if limit.bytes > (1024 * 1024) {
                    Err("read limit: bytes must not exceed 1MiB for unary request")
                } else {
                    Ok(api::ReadLimit {
                        count: limit.count,
                        bytes: limit.bytes,
                    })
                }
            }?),
        };

        Ok(api::ReadRequest {
            stream: stream.into(),
            start_seq_num,
            limit,
        })
    }
}

#[sync_docs]
#[derive(Debug, Clone)]
pub struct SequencedRecord {
    pub seq_num: u64,
    pub headers: Vec<Header>,
    pub body: Vec<u8>,
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
            body: body.into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SequencedRecordBatch {
    pub records: Vec<SequencedRecord>,
}

impl MeteredSize for SequencedRecordBatch {
    fn metered_size(&self) -> ByteSize {
        self.records.metered_size()
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
    pub start_seq_num: Option<u64>,
    pub limit: Option<ReadLimit>,
}

impl ReadSessionRequest {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_start_seq_num(self, start_seq_num: impl Into<u64>) -> Self {
        Self {
            start_seq_num: Some(start_seq_num.into()),
            ..self
        }
    }

    pub fn with_limit(self, limit: ReadLimit) -> Self {
        Self {
            limit: Some(limit),
            ..self
        }
    }

    pub fn into_api_type(self, stream: impl Into<String>) -> api::ReadSessionRequest {
        let Self {
            start_seq_num,
            limit,
        } = self;
        api::ReadSessionRequest {
            stream: stream.into(),
            start_seq_num,
            limit: limit.map(|limit| api::ReadLimit {
                count: limit.count,
                bytes: limit.bytes,
            }),
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

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
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

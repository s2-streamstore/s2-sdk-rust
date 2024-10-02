use std::{str::FromStr, time::Duration};

use typed_builder::TypedBuilder;

use crate::api;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, thiserror::Error)]
#[error("{0}")]
pub struct ConvertError(String);

impl<T: Into<String>> From<T> for ConvertError {
    fn from(value: T) -> Self {
        Self(value.into())
    }
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone, TypedBuilder)]
pub struct CreateBasinRequest {
    #[builder(setter(into))]
    pub basin: String,
    #[builder(default)]
    pub config: Option<BasinConfig>,
    // TODO: Add assignment (when it's supported).
}

impl TryFrom<CreateBasinRequest> for api::CreateBasinRequest {
    type Error = ConvertError;
    fn try_from(value: CreateBasinRequest) -> Result<Self, Self::Error> {
        let CreateBasinRequest { basin, config } = value;
        Ok(Self {
            basin,
            config: config.map(TryInto::try_into).transpose()?,
            assignment: None,
        })
    }
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone, TypedBuilder)]
pub struct BasinConfig {
    #[builder]
    pub default_stream_config: Option<StreamConfig>,
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

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone, TypedBuilder)]
pub struct StreamConfig {
    #[builder(setter(into))]
    pub storage_class: StorageClass,
    #[builder(setter(into))]
    pub retention_policy: Option<RetentionPolicy>,
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

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageClass {
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

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone, TypedBuilder)]
pub struct BasinMetadata {
    #[builder(setter(into))]
    pub name: String,
    #[builder(setter(into))]
    pub scope: String,
    #[builder(setter(into))]
    pub cell: String,
    #[builder(setter(into))]
    pub state: BasinState,
}

impl From<BasinMetadata> for api::BasinMetadata {
    fn from(value: BasinMetadata) -> Self {
        let BasinMetadata {
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

impl TryFrom<api::BasinMetadata> for BasinMetadata {
    type Error = ConvertError;
    fn try_from(value: api::BasinMetadata) -> Result<Self, Self::Error> {
        let api::BasinMetadata {
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

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone)]
pub struct CreateBasinResponse {
    pub basin: BasinMetadata,
}

impl TryFrom<api::CreateBasinResponse> for CreateBasinResponse {
    type Error = ConvertError;
    fn try_from(value: api::CreateBasinResponse) -> Result<Self, Self::Error> {
        let api::CreateBasinResponse { basin } = value;
        let basin = basin.ok_or("missing basin metadata")?;
        Ok(Self {
            basin: basin.try_into()?,
        })
    }
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone, TypedBuilder)]
pub struct ListStreamsRequest {
    #[builder(default, setter(into))]
    pub prefix: String,
    #[builder(default, setter(into))]
    pub start_after: String,
    #[builder(default, setter(into))]
    pub limit: usize,
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
            limit: limit as u64,
        })
    }
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone)]
pub struct ListStreamsResponse {
    pub streams: Vec<String>,
    pub has_more: bool,
}

impl From<api::ListStreamsResponse> for ListStreamsResponse {
    fn from(value: api::ListStreamsResponse) -> Self {
        let api::ListStreamsResponse { streams, has_more } = value;
        Self { streams, has_more }
    }
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone)]
pub struct GetBasinConfigResponse {
    pub config: BasinConfig,
}

impl TryFrom<api::GetBasinConfigResponse> for GetBasinConfigResponse {
    type Error = ConvertError;
    fn try_from(value: api::GetBasinConfigResponse) -> Result<Self, Self::Error> {
        let api::GetBasinConfigResponse { config } = value;
        let config = config.ok_or("missing basin config")?;
        Ok(Self {
            config: config.try_into()?,
        })
    }
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone, TypedBuilder)]
pub struct GetStreamConfigRequest {
    #[builder(setter(into))]
    pub stream: String,
}

impl From<GetStreamConfigRequest> for api::GetStreamConfigRequest {
    fn from(value: GetStreamConfigRequest) -> Self {
        let GetStreamConfigRequest { stream } = value;
        Self { stream }
    }
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone)]
pub struct GetStreamConfigResponse {
    pub config: StreamConfig,
}

impl TryFrom<api::GetStreamConfigResponse> for GetStreamConfigResponse {
    type Error = ConvertError;
    fn try_from(value: api::GetStreamConfigResponse) -> Result<Self, Self::Error> {
        let api::GetStreamConfigResponse { config } = value;
        let config = config.ok_or("missing stream config")?;
        Ok(Self {
            config: config.try_into()?,
        })
    }
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone, TypedBuilder)]
pub struct CreateStreamRequest {
    #[builder(setter(into))]
    pub stream: String,
    #[builder(default)]
    pub config: Option<StreamConfig>,
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

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone, TypedBuilder)]
pub struct ListBasinsRequest {
    /// List basin names that begin with this prefix.  
    #[builder(default, setter(into))]
    pub prefix: String,
    /// Only return basins names that lexicographically start after this name.
    /// This can be the last basin name seen in a previous listing, to continue from there.
    /// It must be greater than or equal to the prefix if specified.
    #[builder(default, setter(into))]
    pub start_after: String,
    /// Number of results, upto a maximum of 1000.    
    #[builder(default, setter(into))]
    pub limit: usize,
}

impl From<ListBasinsRequest> for api::ListBasinsRequest {
    fn from(value: ListBasinsRequest) -> Self {
        let ListBasinsRequest {
            prefix,
            start_after,
            limit,
        } = value;
        Self {
            prefix,
            start_after,
            limit: limit as u64,
        }
    }
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone)]
pub struct ListBasinsResponse {
    /// Matching basins.
    pub basins: Vec<BasinMetadata>,
    /// If set, indicates there are more results that can be listed with `start_after`.
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
                .collect::<Result<Vec<BasinMetadata>, ConvertError>>()?,
            has_more,
        })
    }
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone, TypedBuilder)]
pub struct DeleteBasinRequest {
    /// Name of the basin to delete.
    #[builder(setter(into))]
    pub basin: String,
    /// Only delete if basin exists.
    #[builder(default, setter(into))]
    pub if_exists: bool,
}

impl From<DeleteBasinRequest> for api::DeleteBasinRequest {
    fn from(value: DeleteBasinRequest) -> Self {
        let DeleteBasinRequest { basin, .. } = value;
        Self { basin }
    }
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone, TypedBuilder)]
pub struct GetBasinConfigRequest {
    /// Name of the basin.
    #[builder(setter(into))]
    pub basin: String,
}

impl From<GetBasinConfigRequest> for api::GetBasinConfigRequest {
    fn from(value: GetBasinConfigRequest) -> Self {
        let GetBasinConfigRequest { basin } = value;
        Self { basin }
    }
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone, TypedBuilder)]
pub struct DeleteStreamRequest {
    /// Name of the stream to delete.
    #[builder(setter(into))]
    pub stream: String,
    /// Only delete if stream exists.
    #[builder(default, setter(into))]
    pub if_exists: bool,
}

impl From<DeleteStreamRequest> for api::DeleteStreamRequest {
    fn from(value: DeleteStreamRequest) -> Self {
        let DeleteStreamRequest { stream, .. } = value;
        Self { stream }
    }
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone, TypedBuilder)]
pub struct ReconfigureBasinRequest {
    /// Name of the basin.
    #[builder(setter(into))]
    pub basin: String,
    /// Updated configuration.
    #[builder(setter(strip_option))]
    pub config: Option<BasinConfig>,
    /// Fieldmask to indicate which fields to update.
    #[builder(default, setter(into, strip_option))]
    pub mask: Option<Vec<String>>,
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
            basin,
            config: config.map(TryInto::try_into).transpose()?,
            mask: mask.map(|paths| prost_types::FieldMask { paths }),
        })
    }
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone, TypedBuilder)]
pub struct ReconfigureStreamRequest {
    /// Name of the stream to reconfigure.
    #[builder(setter(into))]
    pub stream: String,
    /// Updated configuration.
    #[builder(setter(strip_option))]
    pub config: Option<StreamConfig>,
    /// Fieldmask to indicate which fields to update.
    #[builder(default, setter(into, strip_option))]
    pub mask: Option<Vec<String>>,
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

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone)]
pub struct GetNextSeqNumResponse {
    /// Next sequence number.
    pub next_seq_num: u64,
}

impl From<api::GetNextSeqNumResponse> for GetNextSeqNumResponse {
    fn from(value: api::GetNextSeqNumResponse) -> Self {
        let api::GetNextSeqNumResponse { next_seq_num } = value;
        Self { next_seq_num }
    }
}

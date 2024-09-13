use std::time::Duration;

use typed_builder::TypedBuilder;

use crate::api;

#[derive(Debug, Clone, thiserror::Error)]
#[error("{0}")]
pub struct ConvertError(String);

impl<T: Into<String>> From<T> for ConvertError {
    fn from(value: T) -> Self {
        Self(value.into())
    }
}

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

impl TryFrom<api::CreateBasinRequest> for CreateBasinRequest {
    type Error = ConvertError;
    fn try_from(value: api::CreateBasinRequest) -> Result<Self, Self::Error> {
        let api::CreateBasinRequest {
            basin,
            config,
            assignment: _,
        } = value;
        Ok(Self {
            basin,
            config: config.map(TryInto::try_into).transpose()?,
        })
    }
}

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BasinStatus {
    Unspecified,
    Active,
    Creating,
    Deleting,
}

impl From<BasinStatus> for api::BasinStatus {
    fn from(value: BasinStatus) -> Self {
        match value {
            BasinStatus::Unspecified => Self::Unspecified,
            BasinStatus::Active => Self::Active,
            BasinStatus::Creating => Self::Creating,
            BasinStatus::Deleting => Self::Deleting,
        }
    }
}

impl From<api::BasinStatus> for BasinStatus {
    fn from(value: api::BasinStatus) -> Self {
        match value {
            api::BasinStatus::Unspecified => Self::Unspecified,
            api::BasinStatus::Active => Self::Active,
            api::BasinStatus::Creating => Self::Creating,
            api::BasinStatus::Deleting => Self::Deleting,
        }
    }
}

impl From<BasinStatus> for i32 {
    fn from(value: BasinStatus) -> Self {
        api::BasinStatus::from(value).into()
    }
}

impl TryFrom<i32> for BasinStatus {
    type Error = ConvertError;
    fn try_from(value: i32) -> Result<Self, Self::Error> {
        api::BasinStatus::try_from(value)
            .map(Into::into)
            .map_err(|_| "invalid basin status value".into())
    }
}

#[derive(Debug, Clone, TypedBuilder)]
pub struct BasinMetadata {
    #[builder(setter(into))]
    pub name: String,
    #[builder(setter(into))]
    pub scope: String,
    #[builder(setter(into))]
    pub cell: String,
    #[builder(setter(into))]
    pub status: BasinStatus,
}

impl From<BasinMetadata> for api::BasinMetadata {
    fn from(value: BasinMetadata) -> Self {
        let BasinMetadata {
            name,
            scope,
            cell,
            status,
        } = value;
        Self {
            name,
            scope,
            cell,
            status: status.into(),
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
            status,
        } = value;
        Ok(Self {
            name,
            scope,
            cell,
            status: status.try_into()?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct CreateBasinResponse {
    pub basin: BasinMetadata,
}

impl From<CreateBasinResponse> for api::CreateBasinResponse {
    fn from(value: CreateBasinResponse) -> Self {
        let CreateBasinResponse { basin } = value;
        Self {
            basin: Some(basin.into()),
        }
    }
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
            limit: limit
                .try_into()
                .map_err(|_| "request limit does not fit into u32 bounds")?,
        })
    }
}

impl TryFrom<api::ListStreamsRequest> for ListStreamsRequest {
    type Error = ConvertError;
    fn try_from(value: api::ListStreamsRequest) -> Result<Self, Self::Error> {
        let api::ListStreamsRequest {
            prefix,
            start_after,
            limit,
        } = value;
        Ok(Self {
            prefix,
            start_after,
            limit: limit
                .try_into()
                .map_err(|_| "request limit does not fit into u32 bounds")?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ListStreamsResponse {
    pub streams: Vec<String>,
    pub has_more: bool,
}

impl From<ListStreamsResponse> for api::ListStreamsResponse {
    fn from(value: ListStreamsResponse) -> Self {
        let ListStreamsResponse { streams, has_more } = value;
        Self { streams, has_more }
    }
}

impl From<api::ListStreamsResponse> for ListStreamsResponse {
    fn from(value: api::ListStreamsResponse) -> Self {
        let api::ListStreamsResponse { streams, has_more } = value;
        Self { streams, has_more }
    }
}

#[derive(Debug, Clone)]
pub struct GetBasinConfigResponse {
    pub config: BasinConfig,
}

impl TryFrom<GetBasinConfigResponse> for api::GetBasinConfigResponse {
    type Error = ConvertError;
    fn try_from(value: GetBasinConfigResponse) -> Result<Self, Self::Error> {
        let GetBasinConfigResponse { config } = value;
        Ok(Self {
            config: Some(config.try_into()?),
        })
    }
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

impl From<api::GetStreamConfigRequest> for GetStreamConfigRequest {
    fn from(value: api::GetStreamConfigRequest) -> Self {
        let api::GetStreamConfigRequest { stream } = value;
        Self { stream }
    }
}

#[derive(Debug, Clone)]
pub struct GetStreamConfigResponse {
    pub config: StreamConfig,
}

impl TryFrom<GetStreamConfigResponse> for api::GetStreamConfigResponse {
    type Error = ConvertError;
    fn try_from(value: GetStreamConfigResponse) -> Result<Self, Self::Error> {
        let GetStreamConfigResponse { config } = value;
        Ok(Self {
            config: Some(config.try_into()?),
        })
    }
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

impl TryFrom<api::CreateStreamRequest> for CreateStreamRequest {
    type Error = ConvertError;
    fn try_from(value: api::CreateStreamRequest) -> Result<Self, Self::Error> {
        let api::CreateStreamRequest { stream, config } = value;
        Ok(Self {
            stream,
            config: config.map(TryInto::try_into).transpose()?,
        })
    }
}

#[derive(Debug, Clone, TypedBuilder)]
pub struct ListBasinsRequest {
    /// List basin names that begin with this prefix.  
    #[builder(setter(into))]
    pub prefix: String,
    /// Only return basins names that lexicographically start after this name.
    /// This can be the last basin name seen in a previous listing, to continue from there.
    /// It must be greater than or equal to the prefix if specified.
    #[builder(setter(into))]
    pub start_after: String,
    /// Number of results, upto a maximum of 1000.    
    #[builder(setter(into))]
    pub limit: u32,
}

#[derive(Debug, Clone)]
pub struct ListBasinsResponse {
    /// Matching basins.
    pub basins: Vec<BasinMetadata>,
    /// If set, indicates there are more results that can be listed with `start_after`.
    pub has_more: bool,
}

impl From<api::ListBasinsRequest> for ListBasinsRequest {
    fn from(value: api::ListBasinsRequest) -> Self {
        let api::ListBasinsRequest {
            prefix,
            start_after,
            limit,
        } = value;
        Self {
            prefix,
            start_after,
            limit,
        }
    }
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
            limit,
        }
    }
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

#[derive(Debug, Clone, TypedBuilder)]
pub struct DeleteBasinRequest {
    /// Name of the basin to delete.
    #[builder(setter(into))]
    pub basin: String,
}

#[derive(Debug, Clone)]
pub struct DeleteBasinResponse;

impl From<DeleteBasinRequest> for api::DeleteBasinRequest {
    fn from(value: DeleteBasinRequest) -> Self {
        let DeleteBasinRequest { basin } = value;
        Self { basin }
    }
}

impl From<api::DeleteBasinRequest> for DeleteBasinRequest {
    fn from(value: api::DeleteBasinRequest) -> Self {
        let api::DeleteBasinRequest { basin } = value;
        Self { basin }
    }
}

impl From<api::DeleteBasinResponse> for DeleteBasinResponse {
    fn from(_: api::DeleteBasinResponse) -> Self {
        Self
    }
}

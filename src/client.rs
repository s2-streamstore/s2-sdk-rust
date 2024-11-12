use std::{fmt::Display, str::FromStr, time::Duration};

use backon::{ConstantBuilder, Retryable};
use http::uri::Authority;
use secrecy::SecretString;
use sync_docs::sync_docs;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

use crate::{
    api::{
        account_service_client::AccountServiceClient, basin_service_client::BasinServiceClient,
        stream_service_client::StreamServiceClient,
    },
    service::{
        account::{
            CreateBasinError, CreateBasinServiceRequest, DeleteBasinError,
            DeleteBasinServiceRequest, GetBasinConfigError, GetBasinConfigServiceRequest,
            ListBasinsError, ListBasinsServiceRequest, ReconfigureBasinError,
            ReconfigureBasinServiceRequest,
        },
        basin::{
            CreateStreamError, CreateStreamServiceRequest, DeleteStreamError,
            DeleteStreamServiceRequest, GetStreamConfigError, GetStreamConfigServiceRequest,
            ListStreamsError, ListStreamsServiceRequest, ReconfigureStreamError,
            ReconfigureStreamServiceRequest,
        },
        send_request,
        stream::{
            AppendError, AppendServiceRequest, AppendSessionError, AppendSessionServiceRequest,
            CheckTailError, CheckTailServiceRequest, ReadError, ReadServiceRequest,
            ReadSessionError, ReadSessionServiceRequest,
        },
        RetryableRequest, ServiceError, ServiceRequest, Streaming,
    },
    types,
};

/// Cloud deployment to be used to connect the client with.
///
/// Can be used to create the client with default hosted URIs:
///
/// ```
/// # use streamstore::client::{ClientConfig, HostCloud};
/// let client_config = ClientConfig::new("<token>")
///     .with_host_uri(HostCloud::Aws);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HostCloud {
    /// S2 hosted on AWS.
    #[default]
    Aws,
}

impl HostCloud {
    const AWS: &'static str = "aws";

    fn as_str(&self) -> &'static str {
        match self {
            Self::Aws => Self::AWS,
        }
    }

    pub fn cell_endpoint(&self) -> Authority {
        format!("{}.s2.dev", self.as_str()).parse().unwrap()
    }

    pub fn basin_zone(&self) -> Option<Authority> {
        Some(format!("b.{}.s2.dev", self.as_str()).parse().unwrap())
    }
}

impl Display for HostCloud {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for HostCloud {
    type Err = InvalidHostError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.eq_ignore_ascii_case(Self::AWS) {
            Ok(Self::Aws)
        } else {
            Err(InvalidHostError(s.to_string()))
        }
    }
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("Invalid host: {0}")]
pub struct InvalidHostError(pub String);

impl From<HostCloud> for HostEndpoints {
    fn from(value: HostCloud) -> Self {
        Self {
            cell: value.cell_endpoint(),
            basin_zone: value.basin_zone(),
        }
    }
}

/// Endpoints for the hosted S2 environment.
#[derive(Debug, Clone)]
pub struct HostEndpoints {
    pub cell: Authority,
    pub basin_zone: Option<Authority>,
}

impl Default for HostEndpoints {
    fn default() -> Self {
        HostCloud::default().into()
    }
}

impl HostEndpoints {
    pub fn from_env() -> Result<Self, InvalidHostError> {
        fn env_var<T>(
            name: &str,
            parse: impl FnOnce(&str) -> Result<T, InvalidHostError>,
        ) -> Result<Option<T>, InvalidHostError> {
            match std::env::var(name) {
                Ok(value) => Ok(Some(parse(&value)?)),
                Err(std::env::VarError::NotPresent) => Ok(None),
                Err(std::env::VarError::NotUnicode(value)) => {
                    Err(InvalidHostError(value.to_string_lossy().to_string()))
                }
            }
        }
        fn parse_authority(v: &str) -> Result<Authority, InvalidHostError> {
            v.parse().map_err(|_| InvalidHostError(v.to_owned()))
        }
        let cloud = env_var("S2_CLOUD", HostCloud::from_str)?.unwrap_or(HostCloud::default());
        let cell = env_var("S2_CELL", parse_authority)?;
        let basin_zone = env_var("S2_BASIN_ZONE", parse_authority)?;
        let endpoints = match (cell, basin_zone, cloud) {
            (None, None, cloud) => cloud.into(),
            (Some(cell), basin_zone, _) => Self { cell, basin_zone },
            (None, Some(basin_zone), cloud) => Self {
                cell: cloud.cell_endpoint(),
                basin_zone: Some(basin_zone),
            },
        };
        Ok(endpoints)
    }
}

/// Client configuration to be used to connect with the host.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Auth token for the client.
    pub token: SecretString,
    /// Host URI to connect with.
    pub host_endpoint: HostEndpoints,
    /// Should the connection be lazy, i.e., only be made when making the very
    /// first request.
    pub connect_lazily: bool,
    /// Timeout for connecting/reconnecting.
    pub connection_timeout: Duration,
    /// Timeout for a particular request.
    pub request_timeout: Duration,
    /// User agent to be used for the client.
    pub user_agent: String,
}

impl ClientConfig {
    /// Construct a new client configuration with given auth token and other
    /// defaults.
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            token: token.into().into(),
            host_endpoint: HostEndpoints::default(),
            connect_lazily: true,
            connection_timeout: Duration::from_secs(3),
            request_timeout: Duration::from_secs(5),
            user_agent: "s2-sdk-rust".to_string(),
        }
    }

    /// Construct from an existing configuration with the new host URIs.
    pub fn with_host_endpoint(self, host_endpoint: impl Into<HostEndpoints>) -> Self {
        Self {
            host_endpoint: host_endpoint.into(),
            ..self
        }
    }

    /// Construct from an existing configuration with the new `connect_lazily`
    /// configuration.
    pub fn with_connect_lazily(self, connect_lazily: bool) -> Self {
        Self {
            connect_lazily,
            ..self
        }
    }

    /// Construct from an existing configuration with the new connection
    /// timeout.
    pub fn with_connection_timeout(self, connection_timeout: impl Into<Duration>) -> Self {
        Self {
            connection_timeout: connection_timeout.into(),
            ..self
        }
    }

    /// Construct from an existing configuration with the new request timeout.
    pub fn with_request_timeout(self, request_timeout: impl Into<Duration>) -> Self {
        Self {
            request_timeout: request_timeout.into(),
            ..self
        }
    }

    /// Construct from an existing configuration with the new user agent.
    pub fn with_user_agent(self, user_agent: impl Into<String>) -> Self {
        Self {
            user_agent: user_agent.into(),
            ..self
        }
    }
}

/// The S2 client to interact with the API.
#[derive(Debug, Clone)]
pub struct Client {
    inner: ClientInner,
}

impl Client {
    async fn connect_inner(
        config: ClientConfig,
        force_lazy_connection: bool,
    ) -> Result<Self, ConnectError> {
        Ok(Self {
            inner: ClientInner::connect_cell(config, force_lazy_connection).await?,
        })
    }

    /// Connect the client with the S2 API.
    pub async fn connect(config: ClientConfig) -> Result<Self, ConnectError> {
        Self::connect_inner(config, /* force_lazy_connection = */ false).await
    }

    #[cfg(feature = "connector")]
    pub async fn connect_with_connector<U>(
        config: ClientConfig,
        connector: U,
    ) -> Result<Self, ConnectError>
    where
        U: tower_service::Service<http::Uri> + Send + 'static,
        U::Response: hyper::rt::Read + hyper::rt::Write + Send + Unpin,
        U::Future: Send,
        U::Error: std::error::Error + Send + Sync + 'static,
    {
        Ok(Self {
            inner: ClientInner::connect_cell_with_connector(config, connector).await?,
        })
    }

    /// Get the client to interact with the S2 basin service API.
    pub async fn basin_client(
        &self,
        basin: impl Into<String>,
    ) -> Result<BasinClient, ConnectError> {
        Ok(BasinClient {
            inner: self
                .inner
                .connect_basin(basin, /* force_lazy_connection = */ false)
                .await?,
        })
    }

    #[sync_docs]
    pub async fn list_basins(
        &self,
        req: types::ListBasinsRequest,
    ) -> Result<types::ListBasinsResponse, ServiceError<ListBasinsError>> {
        self.inner
            .send_retryable(ListBasinsServiceRequest::new(
                self.inner.account_service_client(),
                req,
            ))
            .await
    }

    #[sync_docs]
    pub async fn create_basin(
        &self,
        req: types::CreateBasinRequest,
    ) -> Result<types::BasinMetadata, ServiceError<CreateBasinError>> {
        self.inner
            .send(CreateBasinServiceRequest::new(
                self.inner.account_service_client(),
                req,
            ))
            .await
    }

    #[sync_docs]
    pub async fn delete_basin(
        &self,
        req: types::DeleteBasinRequest,
    ) -> Result<(), ServiceError<DeleteBasinError>> {
        self.inner
            .send_retryable(DeleteBasinServiceRequest::new(
                self.inner.account_service_client(),
                req,
            ))
            .await
    }

    #[sync_docs]
    pub async fn get_basin_config(
        &self,
        basin: impl Into<String>,
    ) -> Result<types::BasinConfig, ServiceError<GetBasinConfigError>> {
        self.inner
            .send_retryable(GetBasinConfigServiceRequest::new(
                self.inner.account_service_client(),
                basin,
            ))
            .await
    }

    #[sync_docs]
    pub async fn reconfigure_basin(
        &self,
        req: types::ReconfigureBasinRequest,
    ) -> Result<(), ServiceError<ReconfigureBasinError>> {
        self.inner
            .send_retryable(ReconfigureBasinServiceRequest::new(
                self.inner.account_service_client(),
                req,
            ))
            .await
    }
}

/// Client to interact with the S2 basin service API.
#[derive(Debug, Clone)]
pub struct BasinClient {
    inner: ClientInner,
}

impl BasinClient {
    /// Connect the client with the S2 basin service API.
    pub async fn connect(
        config: ClientConfig,
        basin: impl Into<String>,
    ) -> Result<Self, ConnectError> {
        // Since we're directly trying to connect to the basin, force lazy
        // connection with the global client so we don't end up making 2
        // connections for connecting with the basin client directly (given the
        // cell URI and global URIs are different).
        let force_lazy_connection = config.host_endpoint.basin_zone.is_some();
        let client = Client::connect_inner(config, force_lazy_connection).await?;
        client.basin_client(basin).await
    }

    #[cfg(feature = "connector")]
    pub async fn connect_with_connector<U>(
        config: ClientConfig,
        basin: impl Into<String>,
        connector: U,
    ) -> Result<Self, ConnectError>
    where
        U: tower_service::Service<http::Uri> + Send + 'static,
        U::Response: hyper::rt::Read + hyper::rt::Write + Send + Unpin,
        U::Future: Send,
        U::Error: std::error::Error + Send + Sync + 'static,
    {
        let client = Client::connect_with_connector(config, connector).await?;
        client.basin_client(basin).await
    }

    /// Get the client to interact with the S2 stream service API.
    pub fn stream_client(&self, stream: impl Into<String>) -> StreamClient {
        StreamClient {
            inner: self.inner.clone(),
            stream: stream.into(),
        }
    }

    #[sync_docs]
    pub async fn create_stream(
        &self,
        req: types::CreateStreamRequest,
    ) -> Result<(), ServiceError<CreateStreamError>> {
        self.inner
            .send(CreateStreamServiceRequest::new(
                self.inner.basin_service_client(),
                req,
            ))
            .await
    }

    #[sync_docs]
    pub async fn list_streams(
        &self,
        req: types::ListStreamsRequest,
    ) -> Result<types::ListStreamsResponse, ServiceError<ListStreamsError>> {
        self.inner
            .send_retryable(ListStreamsServiceRequest::new(
                self.inner.basin_service_client(),
                req,
            ))
            .await
    }

    #[sync_docs]
    pub async fn get_stream_config(
        &self,
        stream: impl Into<String>,
    ) -> Result<types::StreamConfig, ServiceError<GetStreamConfigError>> {
        self.inner
            .send_retryable(GetStreamConfigServiceRequest::new(
                self.inner.basin_service_client(),
                stream,
            ))
            .await
    }

    #[sync_docs]
    pub async fn reconfigure_stream(
        &self,
        req: types::ReconfigureStreamRequest,
    ) -> Result<(), ServiceError<ReconfigureStreamError>> {
        self.inner
            .send(ReconfigureStreamServiceRequest::new(
                self.inner.basin_service_client(),
                req,
            ))
            .await
    }

    #[sync_docs]
    pub async fn delete_stream(
        &self,
        req: types::DeleteStreamRequest,
    ) -> Result<(), ServiceError<DeleteStreamError>> {
        self.inner
            .send_retryable(DeleteStreamServiceRequest::new(
                self.inner.basin_service_client(),
                req,
            ))
            .await
    }
}

/// Client to interact with the S2 stream service API.
#[derive(Debug, Clone)]
pub struct StreamClient {
    inner: ClientInner,
    stream: String,
}

impl StreamClient {
    /// Connect the client with the S2 stream service API.
    pub async fn connect(
        config: ClientConfig,
        basin: impl Into<String>,
        stream: impl Into<String>,
    ) -> Result<Self, ConnectError> {
        BasinClient::connect(config, basin)
            .await
            .map(|client| client.stream_client(stream))
    }

    #[cfg(feature = "connector")]
    pub async fn connect_with_connector<U>(
        config: ClientConfig,
        basin: impl Into<String>,
        stream: impl Into<String>,
        connector: U,
    ) -> Result<Self, ConnectError>
    where
        U: tower_service::Service<http::Uri> + Send + 'static,
        U::Response: hyper::rt::Read + hyper::rt::Write + Send + Unpin,
        U::Future: Send,
        U::Error: std::error::Error + Send + Sync + 'static,
    {
        BasinClient::connect_with_connector(config, basin, connector)
            .await
            .map(|client| client.stream_client(stream))
    }

    #[sync_docs]
    pub async fn check_tail(&self) -> Result<u64, ServiceError<CheckTailError>> {
        self.inner
            .send_retryable(CheckTailServiceRequest::new(
                self.inner.stream_service_client(),
                &self.stream,
            ))
            .await
    }

    #[sync_docs]
    pub async fn read(
        &self,
        req: types::ReadRequest,
    ) -> Result<types::ReadOutput, ServiceError<ReadError>> {
        self.inner
            .send_retryable(ReadServiceRequest::new(
                self.inner.stream_service_client(),
                &self.stream,
                req,
            ))
            .await
    }

    #[sync_docs]
    pub async fn read_session(
        &self,
        req: types::ReadSessionRequest,
    ) -> Result<
        Streaming<types::ReadSessionResponse, ReadSessionError>,
        ServiceError<ReadSessionError>,
    > {
        self.inner
            .send_retryable(ReadSessionServiceRequest::new(
                self.inner.stream_service_client(),
                &self.stream,
                req,
            ))
            .await
            .map(Streaming::new)
    }

    #[sync_docs]
    pub async fn append(
        &self,
        req: types::AppendInput,
    ) -> Result<types::AppendOutput, ServiceError<AppendError>> {
        self.inner
            .send(AppendServiceRequest::new(
                self.inner.stream_service_client(),
                &self.stream,
                req,
            ))
            .await
    }

    #[sync_docs]
    pub async fn append_session<S>(
        &self,
        req: S,
    ) -> Result<Streaming<types::AppendOutput, AppendSessionError>, ServiceError<AppendSessionError>>
    where
        S: 'static + Send + futures::Stream<Item = types::AppendInput> + Unpin,
    {
        self.inner
            .send(AppendSessionServiceRequest::new(
                self.inner.stream_service_client(),
                &self.stream,
                req,
            ))
            .await
            .map(Streaming::new)
    }
}

#[derive(Debug, Clone)]
struct ClientInner {
    channel: Channel,
    basin: Option<String>,
    config: ClientConfig,
}

impl ClientInner {
    async fn connect_cell(
        config: ClientConfig,
        force_lazy_connection: bool,
    ) -> Result<Self, ConnectError> {
        let cell_endpoint = config.host_endpoint.cell.clone();
        Self::connect(config, cell_endpoint, force_lazy_connection).await
    }

    #[cfg(feature = "connector")]
    async fn connect_cell_with_connector<U>(
        config: ClientConfig,
        connector: U,
    ) -> Result<Self, ConnectError>
    where
        U: tower_service::Service<http::Uri> + Send + 'static,
        U::Response: hyper::rt::Read + hyper::rt::Write + Send + Unpin,
        U::Future: Send,
        U::Error: std::error::Error + Send + Sync + 'static,
    {
        let cell_endpoint = config.host_endpoint.cell.clone();
        Self::connect_with_connector(config, cell_endpoint, connector).await
    }

    async fn connect_basin(
        &self,
        basin: impl Into<String>,
        force_lazy_connection: bool,
    ) -> Result<Self, ConnectError> {
        let basin = basin.into();

        match self.config.host_endpoint.basin_zone.clone() {
            Some(endpoint) => {
                let basin_endpoint: Authority = format!("{basin}.{endpoint}").parse()?;
                ClientInner::connect(self.config.clone(), basin_endpoint, force_lazy_connection)
                    .await
            }
            None => Ok(Self {
                basin: Some(basin),
                ..self.clone()
            }),
        }
    }

    async fn connect(
        config: ClientConfig,
        endpoint: Authority,
        force_lazy_connection: bool,
    ) -> Result<Self, ConnectError> {
        let endpoint = format!("https://{endpoint}")
            .parse::<Endpoint>()?
            .user_agent(config.user_agent.clone())?
            .http2_adaptive_window(true)
            .tls_config(
                ClientTlsConfig::default()
                    .with_webpki_roots()
                    .assume_http2(true),
            )?
            .connect_timeout(config.connection_timeout)
            .timeout(config.request_timeout);
        let channel = if config.connect_lazily || force_lazy_connection {
            endpoint.connect_lazy()
        } else {
            endpoint.connect().await?
        };
        Ok(Self {
            channel,
            basin: None,
            config,
        })
    }

    #[cfg(feature = "connector")]
    async fn connect_with_connector<U>(
        config: ClientConfig,
        endpoint: Authority,
        connector: U,
    ) -> Result<Self, ConnectError>
    where
        U: tower_service::Service<http::Uri> + Send + 'static,
        U::Response: hyper::rt::Read + hyper::rt::Write + Send + Unpin,
        U::Future: Send,
        U::Error: std::error::Error + Send + Sync + 'static,
    {
        let endpoint = format!("http://{endpoint}")
            .parse::<Endpoint>()?
            .user_agent(config.user_agent.clone())?
            .http2_adaptive_window(true)
            .keep_alive_timeout(Duration::from_secs(5))
            .http2_keep_alive_interval(Duration::from_secs(5))
            .connect_timeout(config.connection_timeout)
            .timeout(config.request_timeout);

        let channel = endpoint.connect_with_connector(connector).await?;
        Ok(Self {
            channel,
            basin: None,
            config,
        })
    }

    async fn send<T: ServiceRequest>(
        &self,
        service_req: T,
    ) -> Result<T::Response, ServiceError<T::Error>> {
        send_request(service_req, &self.config.token, self.basin.as_deref()).await
    }

    async fn send_retryable<T: RetryableRequest>(
        &self,
        service_req: T,
    ) -> Result<T::Response, ServiceError<T::Error>> {
        let retry_fn = || async { self.send(service_req.clone()).await };

        retry_fn
            .retry(ConstantBuilder::default()) // TODO: Configure retry.
            .when(|e| service_req.should_retry(e))
            .await
    }

    fn account_service_client(&self) -> AccountServiceClient<Channel> {
        AccountServiceClient::new(self.channel.clone())
    }

    fn basin_service_client(&self) -> BasinServiceClient<Channel> {
        BasinServiceClient::new(self.channel.clone())
    }

    fn stream_service_client(&self) -> StreamServiceClient<Channel> {
        StreamServiceClient::new(self.channel.clone())
    }
}

/// Error connecting to S2 endpoint.
#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    #[error(transparent)]
    TonicTransportError(#[from] tonic::transport::Error),
    #[error(transparent)]
    UriParseError(#[from] http::uri::InvalidUri),
}

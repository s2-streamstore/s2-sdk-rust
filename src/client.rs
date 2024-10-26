use std::time::Duration;

use backon::{ConstantBuilder, Retryable};
use http::{uri::Authority, Uri};
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
    /// Localhost (to be used for testing).
    #[default]
    Local,
    /// S2 hosted on AWS.
    Aws,
}

impl From<HostCloud> for HostUri {
    fn from(value: HostCloud) -> Self {
        match value {
            HostCloud::Local => HostUri {
                global: std::env::var("S2_FRONTEND_AUTHORITY")
                    .expect("S2_FRONTEND_AUTHORITY required")
                    .try_into()
                    .unwrap(),
                cell: None,
                prefix_host_with_basin: false,
            },
            HostCloud::Aws => todo!("prod aws uris"),
        }
    }
}

/// URIs for the hosted S2 environment.
#[derive(Debug, Clone)]
pub struct HostUri {
    /// Global URI to connect to.
    pub global: Uri,
    /// Cell specific URI (for basin and stream service requests).
    ///
    /// Client uses the same URI as the global URI if cell URI is absent.
    pub cell: Option<Uri>,
    /// Whether the cell URI host should be prefixed with the basin name or not.
    ///
    /// If set to true, the cell URI `cell.aws.s2.dev` would be prefixed with
    /// the basin name and set to `<basin>.cell.aws.s2.dev`.
    pub prefix_host_with_basin: bool,
}

impl Default for HostUri {
    fn default() -> Self {
        HostCloud::default().into()
    }
}

impl HostUri {
    /// Construct a new host URI with the given global URI.
    pub fn new(global_uri: impl Into<Uri>) -> Self {
        Self {
            global: global_uri.into(),
            cell: None,
            prefix_host_with_basin: false,
        }
    }

    /// Construct from an existing host URI with the given cell URI.
    pub fn with_cell_uri(self, cell_uri: impl Into<Uri>) -> Self {
        Self {
            cell: Some(cell_uri.into()),
            ..self
        }
    }

    /// Construct from an existing host URI with the new
    /// `prefix_host_with_basin` configuration.
    pub fn with_prefix_host_with_basin(self, prefix_host_with_basin: bool) -> Self {
        Self {
            prefix_host_with_basin,
            ..self
        }
    }
}

/// Client configuration to be used to connect with the host.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Auth token for the client.
    pub token: SecretString,
    /// Host URI to connect with.
    pub host_uri: HostUri,
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
            host_uri: HostUri::default(),
            connect_lazily: true,
            connection_timeout: Duration::from_secs(3),
            request_timeout: Duration::from_secs(5),
            user_agent: "s2-sdk-rust".to_string(),
        }
    }

    /// Construct from an existing configuration with the new host URIs.
    pub fn with_host_uri(self, host_uri: impl Into<HostUri>) -> Self {
        Self {
            host_uri: host_uri.into(),
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
    ) -> Result<Self, ClientError> {
        Ok(Self {
            inner: ClientInner::connect_global(config, force_lazy_connection).await?,
        })
    }

    /// Connect the client with the S2 API.
    pub async fn connect(config: ClientConfig) -> Result<Self, ClientError> {
        Self::connect_inner(config, /* force_lazy_connection = */ false).await
    }

    /// Get the client to interact with the S2 basin service API.
    pub async fn basin_client(&self, basin: impl Into<String>) -> Result<BasinClient, ClientError> {
        Ok(BasinClient {
            inner: self
                .inner
                .connect_cell(basin, /* force_lazy_connection = */ false)
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
    ) -> Result<Self, ClientError> {
        // Since we're directly trying to connect to the basin, force lazy
        // connection with the global client so we don't end up making 2
        // connections for connecting with the basin client directly (given the
        // cell URI and global URIs are different).
        let force_lazy_connection = config.host_uri.cell.is_some();
        let client = Client::connect_inner(config, force_lazy_connection).await?;
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
    ) -> Result<Self, ClientError> {
        BasinClient::connect(config, basin)
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
    async fn connect_global(
        config: ClientConfig,
        force_lazy_connection: bool,
    ) -> Result<Self, ClientError> {
        let uri = config.host_uri.global.clone();
        Self::connect(config, uri, force_lazy_connection).await
    }

    async fn connect_cell(
        &self,
        basin: impl Into<String>,
        force_lazy_connection: bool,
    ) -> Result<Self, ClientError> {
        let basin = basin.into();

        match self.config.host_uri.cell.clone() {
            Some(uri) if self.config.host_uri.prefix_host_with_basin => {
                let host = uri.host().ok_or(ClientError::MissingHost)?;
                let port = uri.port_u16().map_or(String::new(), |p| format!(":{}", p));
                let authority: Authority = format!("{basin}.{host}{port}").parse()?;
                let mut uri_parts = uri.into_parts();
                uri_parts.authority = Some(authority);

                ClientInner::connect(
                    self.config.clone(),
                    Uri::from_parts(uri_parts).expect("invalid uri"),
                    force_lazy_connection,
                )
                .await
            }
            Some(uri) => {
                ClientInner::connect(self.config.clone(), uri, force_lazy_connection).await
            }
            None => Ok(Self {
                basin: Some(basin),
                ..self.clone()
            }),
        }
    }

    async fn connect(
        config: ClientConfig,
        uri: Uri,
        force_lazy_connection: bool,
    ) -> Result<Self, ClientError> {
        let endpoint: Endpoint = uri.clone().into();
        let endpoint = endpoint
            .user_agent(config.user_agent.clone())?
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

/// Error returned while connecting to the client.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error(transparent)]
    TonicTransportError(#[from] tonic::transport::Error),
    #[error(transparent)]
    UriParseError(#[from] http::uri::InvalidUri),
    #[error("Missing host in URI")]
    MissingHost,
}

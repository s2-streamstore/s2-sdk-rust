use std::time::Duration;

use backon::{ConstantBuilder, Retryable};
use http::{uri::Authority, Uri};
use secrecy::SecretString;
use tonic::transport::{Channel, Endpoint};

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
            GetNextSeqNumError, GetNextSeqNumServiceRequest, ReadError, ReadServiceRequest,
            ReadSessionError, ReadSessionServiceRequest,
        },
        RetryableRequest, ServiceError, ServiceRequest, Streaming,
    },
    types,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HostCloud {
    /// Localhost (to be used for testing).
    #[cfg(debug_assertions)]
    Local,
    /// S2 hosted on AWS.
    #[default]
    Aws,
}

impl From<HostCloud> for HostUri {
    fn from(value: HostCloud) -> Self {
        match value {
            #[cfg(debug_assertions)]
            HostCloud::Local => HostUri {
                global: "http://localhost:4243".try_into().unwrap(),
                cell: None,
                prefix_host_with_basin: false,
            },
            HostCloud::Aws => todo!("prod aws uris"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct HostUri {
    pub global: Uri,
    pub cell: Option<Uri>,
    pub prefix_host_with_basin: bool,
}

impl Default for HostUri {
    fn default() -> Self {
        HostCloud::default().into()
    }
}

impl HostUri {
    pub fn new(global_uri: impl Into<Uri>) -> Self {
        Self {
            global: global_uri.into(),
            cell: None,
            prefix_host_with_basin: false,
        }
    }

    pub fn with_cell_uri(self, cell_uri: impl Into<Uri>) -> Self {
        Self {
            cell: Some(cell_uri.into()),
            ..self
        }
    }

    pub fn with_prefix_host_with_basin(self, prefix_host_with_basin: bool) -> Self {
        Self {
            prefix_host_with_basin,
            ..self
        }
    }
}

#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub token: SecretString,
    pub host_uri: HostUri,
    pub connect_lazily: bool,
    pub connection_timeout: Duration,
    pub request_timeout: Duration,
}

impl ClientConfig {
    pub fn new(token: impl Into<SecretString>) -> Self {
        Self {
            token: token.into(),
            host_uri: HostUri::default(),
            connect_lazily: true,
            connection_timeout: Duration::from_secs(3),
            request_timeout: Duration::from_secs(5),
        }
    }

    pub fn with_host_uri(self, host_uri: impl Into<HostUri>) -> Self {
        Self {
            host_uri: host_uri.into(),
            ..self
        }
    }

    pub fn with_connect_lazily(self, connect_lazily: bool) -> Self {
        Self {
            connect_lazily,
            ..self
        }
    }

    pub fn with_connection_timeout(self, connection_timeout: impl Into<Duration>) -> Self {
        Self {
            connection_timeout: connection_timeout.into(),
            ..self
        }
    }

    pub fn with_request_timeout(self, request_timeout: impl Into<Duration>) -> Self {
        Self {
            request_timeout: request_timeout.into(),
            ..self
        }
    }
}

#[derive(Debug, Clone)]
pub struct Client {
    inner: ClientInner,
}

impl Client {
    pub async fn connect(config: ClientConfig) -> Result<Self, ClientError> {
        Ok(Self {
            inner: ClientInner::connect_global(config).await?,
        })
    }

    pub async fn basin_client(&self, basin: impl Into<String>) -> Result<BasinClient, ClientError> {
        Ok(BasinClient {
            inner: self.inner.connect_cell(basin).await?,
        })
    }

    /// List basins.
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

    /// Create a new basin.
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

    /// Delete a basin.
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

#[derive(Debug, Clone)]
pub struct BasinClient {
    inner: ClientInner,
}

impl BasinClient {
    pub async fn connect(
        config: ClientConfig,
        basin: impl Into<String>,
    ) -> Result<Self, ClientError> {
        // TODO: If `connect_lazily` is set to false, this will create two
        // connections. We can directly connect to basin client.
        let client = Client::connect(config).await?;
        client.basin_client(basin).await
    }

    pub fn stream_client(&self, stream: impl Into<String>) -> StreamClient {
        StreamClient {
            inner: self.inner.clone(),
            stream: stream.into(),
        }
    }

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

#[derive(Debug, Clone)]
pub struct StreamClient {
    inner: ClientInner,
    stream: String,
}

impl StreamClient {
    pub async fn connect(
        config: ClientConfig,
        basin: impl Into<String>,
        stream: impl Into<String>,
    ) -> Result<Self, ClientError> {
        BasinClient::connect(config, basin)
            .await
            .map(|client| client.stream_client(stream))
    }

    pub async fn get_next_seq_num(&self) -> Result<u64, ServiceError<GetNextSeqNumError>> {
        self.inner
            .send_retryable(GetNextSeqNumServiceRequest::new(
                self.inner.stream_service_client(),
                &self.stream,
            ))
            .await
    }

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
    pub async fn connect_global(config: ClientConfig) -> Result<Self, ClientError> {
        let uri = config.host_uri.global.clone();
        Self::connect(config, uri).await
    }

    pub async fn connect_cell(&self, basin: impl Into<String>) -> Result<Self, ClientError> {
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
                )
                .await
            }
            Some(uri) => ClientInner::connect(self.config.clone(), uri).await,
            None => Ok(Self {
                basin: Some(basin),
                ..self.clone()
            }),
        }
    }

    async fn connect(config: ClientConfig, uri: Uri) -> Result<Self, ClientError> {
        // TODO: Connection pool?
        let endpoint: Endpoint = uri.clone().into();
        let endpoint = endpoint
            .connect_timeout(config.connection_timeout)
            .timeout(config.request_timeout);
        let channel = if config.connect_lazily {
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

    pub async fn send<T: ServiceRequest>(
        &self,
        service_req: T,
    ) -> Result<T::Response, ServiceError<T::Error>> {
        send_request(service_req, &self.config.token, self.basin.as_deref()).await
    }

    pub async fn send_retryable<T: RetryableRequest>(
        &self,
        service_req: T,
    ) -> Result<T::Response, ServiceError<T::Error>> {
        let retry_fn = || async { self.send(service_req.clone()).await };

        retry_fn
            .retry(ConstantBuilder::default()) // TODO: Configure retry.
            .when(|e| service_req.should_retry(e))
            .await
    }

    pub fn account_service_client(&self) -> AccountServiceClient<Channel> {
        AccountServiceClient::new(self.channel.clone())
    }

    pub fn basin_service_client(&self) -> BasinServiceClient<Channel> {
        BasinServiceClient::new(self.channel.clone())
    }

    pub fn stream_service_client(&self) -> StreamServiceClient<Channel> {
        StreamServiceClient::new(self.channel.clone())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error(transparent)]
    TonicTransportError(#[from] tonic::transport::Error),
    #[error(transparent)]
    UriParseError(#[from] http::uri::InvalidUri),
    #[error("Missing host in URI")]
    MissingHost,
}

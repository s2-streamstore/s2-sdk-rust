use std::{fmt::Display, str::FromStr, time::Duration};

use backon::{BackoffBuilder, ConstantBuilder, Retryable};
use futures::StreamExt;
use http::{uri::Authority, HeaderValue};
use hyper_util::client::legacy::connect::HttpConnector;
use secrecy::SecretString;
use sync_docs::sync_docs;
use tokio::time::sleep;
use tonic::{
    metadata::AsciiMetadataValue,
    transport::{Channel, ClientTlsConfig, Endpoint},
};

use crate::{
    api::{
        account_service_client::AccountServiceClient, basin_service_client::BasinServiceClient,
        stream_service_client::StreamServiceClient,
    },
    service::{
        account::{
            CreateBasinServiceRequest, DeleteBasinServiceRequest, GetBasinConfigServiceRequest,
            ListBasinsServiceRequest, ReconfigureBasinServiceRequest,
        },
        basin::{
            CreateStreamServiceRequest, DeleteStreamServiceRequest, GetStreamConfigServiceRequest,
            ListStreamsServiceRequest, ReconfigureStreamServiceRequest,
        },
        send_request,
        stream::{
            AppendServiceRequest, AppendSessionServiceRequest, CheckTailServiceRequest,
            ReadServiceRequest, ReadSessionServiceRequest, ReadSessionStreamingResponse,
        },
        RetryableRequest, ServiceRequest, ServiceStreamingResponse, Streaming,
    },
    types,
};

const DEFAULT_CONNECTOR: Option<HttpConnector> = None;

/// S2 cloud environment to connect with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S2Cloud {
    /// S2 running on AWS.
    Aws,
}

impl S2Cloud {
    const AWS: &'static str = "aws";

    fn as_str(&self) -> &'static str {
        match self {
            Self::Aws => Self::AWS,
        }
    }
}

impl Display for S2Cloud {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for S2Cloud {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.eq_ignore_ascii_case(Self::AWS) {
            Ok(Self::Aws)
        } else {
            Err(s.to_owned())
        }
    }
}

/// Endpoint for connecting to an S2 basin.
#[derive(Debug, Clone)]
pub enum BasinEndpoint {
    /// Parent zone for basins.
    /// DNS is used to route to the correct cell for the basin.
    ParentZone(Authority),
    /// Direct cell endpoint.
    /// The `S2-Basin` header is included in requests to specify the basin.
    CellKnown(Authority),
}

/// Endpoints for the S2 environment.
#[derive(Debug, Clone)]
pub struct S2Endpoints {
    /// Used by `AccountService` requests.
    pub account: Authority,
    /// Used by `BasinService` and `StreamService` requests.
    pub basin: BasinEndpoint,
}

impl S2Endpoints {
    pub fn for_cloud(cloud: S2Cloud) -> Self {
        Self {
            account: format!("{cloud}.s2.dev")
                .try_into()
                .expect("valid authority"),
            basin: BasinEndpoint::ParentZone(
                format!("b.{cloud}.s2.dev")
                    .try_into()
                    .expect("valid authority"),
            ),
        }
    }

    pub fn for_cell(
        &self,
        cloud: S2Cloud,
        cell_id: impl Into<String>,
    ) -> Result<Self, http::uri::InvalidUri> {
        let cell_endpoint: Authority = format!("{}.o.{cloud}.s2.dev", cell_id.into()).try_into()?;
        Ok(Self {
            account: cell_endpoint.clone(),
            basin: BasinEndpoint::CellKnown(cell_endpoint),
        })
    }
}

/// Client configuration.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub(crate) token: SecretString,
    pub(crate) endpoints: S2Endpoints,
    pub(crate) connection_timeout: Duration,
    pub(crate) request_timeout: Duration,
    pub(crate) user_agent: HeaderValue,
    #[cfg(feature = "connector")]
    pub(crate) uri_scheme: http::uri::Scheme,
    pub(crate) retry_backoff_duration: Duration,
    pub(crate) max_attempts: usize,
}

impl ClientConfig {
    /// Initialize a default client configuration with the specified authentication token.
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            token: token.into().into(),
            endpoints: S2Endpoints::for_cloud(S2Cloud::Aws),
            connection_timeout: Duration::from_secs(3),
            request_timeout: Duration::from_secs(5),
            user_agent: "s2-sdk-rust".parse().expect("valid user agent"),
            #[cfg(feature = "connector")]
            uri_scheme: http::uri::Scheme::HTTPS,
            retry_backoff_duration: Duration::from_millis(100),
            max_attempts: 3,
        }
    }

    /// S2 endpoints to connect to.
    pub fn with_endpoints(self, host_endpoints: impl Into<S2Endpoints>) -> Self {
        Self {
            endpoints: host_endpoints.into(),
            ..self
        }
    }

    /// Timeout for connecting and transparently reconnecting. Defaults to 3s.
    pub fn with_connection_timeout(self, connection_timeout: impl Into<Duration>) -> Self {
        Self {
            connection_timeout: connection_timeout.into(),
            ..self
        }
    }

    /// Timeout for a particular request. Defaults to 5s.
    pub fn with_request_timeout(self, request_timeout: impl Into<Duration>) -> Self {
        Self {
            request_timeout: request_timeout.into(),
            ..self
        }
    }

    /// User agent. Defaults to `s2-sdk-rust`. Feel free to say hi.
    pub fn with_user_agent(self, user_agent: impl Into<HeaderValue>) -> Self {
        Self {
            user_agent: user_agent.into(),
            ..self
        }
    }

    /// URI scheme to use when connecting with a custom connector. Defaults to `https`.
    #[cfg(feature = "connector")]
    pub fn with_uri_scheme(self, uri_scheme: impl Into<http::uri::Scheme>) -> Self {
        Self {
            uri_scheme: uri_scheme.into(),
            ..self
        }
    }

    /// Backoff duration when retrying.
    /// Defaults to 100ms.
    /// A jitter is always applied.
    pub fn with_retry_backoff_duration(self, retry_backoff_duration: impl Into<Duration>) -> Self {
        Self {
            retry_backoff_duration: retry_backoff_duration.into(),
            ..self
        }
    }

    /// Maximum number of attempts per request.
    /// Setting it to 1 disables retrying.
    /// The default is to make 3 attempts.
    pub fn with_max_attempts(self, max_attempts: usize) -> Self {
        assert!(max_attempts > 0, "max attempts must be greater than 0");
        Self {
            max_attempts,
            ..self
        }
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum ClientError {
    #[error(transparent)]
    Conversion(#[from] types::ConvertError),
    #[error(transparent)]
    Service(#[from] tonic::Status),
}

/// Client for account-level operations.
#[derive(Debug, Clone)]
pub struct Client {
    inner: ClientInner,
}

impl Client {
    pub fn new(config: ClientConfig) -> Self {
        Self {
            inner: ClientInner::new(ClientKind::Account, config, DEFAULT_CONNECTOR),
        }
    }

    #[cfg(feature = "connector")]
    pub fn new_with_connector<C>(config: ClientConfig, connector: C) -> Self
    where
        C: tower_service::Service<http::Uri> + Send + 'static,
        C::Response: hyper::rt::Read + hyper::rt::Write + Send + Unpin,
        C::Future: Send,
        C::Error: std::error::Error + Send + Sync + 'static,
    {
        Self {
            inner: ClientInner::new(ClientKind::Account, config, Some(connector)),
        }
    }

    pub fn basin_client(&self, basin: types::BasinName) -> BasinClient {
        BasinClient {
            inner: self.inner.for_basin(basin),
        }
    }

    #[sync_docs]
    pub async fn list_basins(
        &self,
        req: types::ListBasinsRequest,
    ) -> Result<types::ListBasinsResponse, ClientError> {
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
    ) -> Result<types::BasinMetadata, ClientError> {
        self.inner
            .send(CreateBasinServiceRequest::new(
                self.inner.account_service_client(),
                req,
            ))
            .await
    }

    #[sync_docs]
    pub async fn delete_basin(&self, req: types::DeleteBasinRequest) -> Result<(), ClientError> {
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
        basin: types::BasinName,
    ) -> Result<types::BasinConfig, ClientError> {
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
    ) -> Result<(), ClientError> {
        self.inner
            .send_retryable(ReconfigureBasinServiceRequest::new(
                self.inner.account_service_client(),
                req,
            ))
            .await
    }
}

/// Client for basin-level operations.
#[derive(Debug, Clone)]
pub struct BasinClient {
    inner: ClientInner,
}

impl BasinClient {
    pub fn new(config: ClientConfig, basin: types::BasinName) -> Self {
        Self {
            inner: ClientInner::new(ClientKind::Basin(basin), config, DEFAULT_CONNECTOR),
        }
    }

    #[cfg(feature = "connector")]
    pub fn new_with_connector<C>(
        config: ClientConfig,
        basin: types::BasinName,
        connector: C,
    ) -> Self
    where
        C: tower_service::Service<http::Uri> + Send + 'static,
        C::Response: hyper::rt::Read + hyper::rt::Write + Send + Unpin,
        C::Future: Send,
        C::Error: std::error::Error + Send + Sync + 'static,
    {
        Self {
            inner: ClientInner::new(ClientKind::Basin(basin), config, Some(connector)),
        }
    }

    /// Create a new client for stream-level operations.
    pub fn stream_client(&self, stream: impl Into<String>) -> StreamClient {
        StreamClient {
            inner: self.inner.clone(),
            stream: stream.into(),
        }
    }

    #[sync_docs]
    pub async fn create_stream(&self, req: types::CreateStreamRequest) -> Result<(), ClientError> {
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
    ) -> Result<types::ListStreamsResponse, ClientError> {
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
    ) -> Result<types::StreamConfig, ClientError> {
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
    ) -> Result<(), ClientError> {
        self.inner
            .send(ReconfigureStreamServiceRequest::new(
                self.inner.basin_service_client(),
                req,
            ))
            .await
    }

    #[sync_docs]
    pub async fn delete_stream(&self, req: types::DeleteStreamRequest) -> Result<(), ClientError> {
        self.inner
            .send_retryable(DeleteStreamServiceRequest::new(
                self.inner.basin_service_client(),
                req,
            ))
            .await
    }
}

/// Client for stream-level operations.
#[derive(Debug, Clone)]
pub struct StreamClient {
    inner: ClientInner,
    stream: String,
}

impl StreamClient {
    pub fn new(config: ClientConfig, basin: types::BasinName, stream: impl Into<String>) -> Self {
        BasinClient::new(config, basin).stream_client(stream)
    }

    #[cfg(feature = "connector")]
    pub fn new_with_connector<C>(
        config: ClientConfig,
        basin: types::BasinName,
        stream: impl Into<String>,
        connector: C,
    ) -> Self
    where
        C: tower_service::Service<http::Uri> + Send + 'static,
        C::Response: hyper::rt::Read + hyper::rt::Write + Send + Unpin,
        C::Future: Send,
        C::Error: std::error::Error + Send + Sync + 'static,
    {
        BasinClient::new_with_connector(config, basin, connector).stream_client(stream)
    }

    #[sync_docs]
    pub async fn check_tail(&self) -> Result<u64, ClientError> {
        self.inner
            .send_retryable(CheckTailServiceRequest::new(
                self.inner.stream_service_client(),
                &self.stream,
            ))
            .await
    }

    #[sync_docs]
    pub async fn read(&self, req: types::ReadRequest) -> Result<types::ReadOutput, ClientError> {
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
    ) -> Result<Streaming<types::ReadOutput>, ClientError> {
        let request =
            ReadSessionServiceRequest::new(self.inner.stream_service_client(), &self.stream, req);
        self.inner
            .send_retryable(request.clone())
            .await
            .map(|responses| {
                Box::pin(read_resumption_stream(
                    request,
                    responses,
                    self.inner.clone(),
                )) as _
            })
    }

    #[sync_docs]
    pub async fn append(
        &self,
        req: types::AppendInput,
    ) -> Result<types::AppendOutput, ClientError> {
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
    ) -> Result<Streaming<types::AppendOutput>, ClientError>
    where
        S: 'static + Send + Unpin + futures::Stream<Item = types::AppendInput>,
    {
        self.inner
            .send(AppendSessionServiceRequest::new(
                self.inner.stream_service_client(),
                &self.stream,
                req,
            ))
            .await
            .map(|s| Box::pin(s) as _)
    }
}

#[derive(Debug, Clone)]
enum ClientKind {
    Account,
    Basin(types::BasinName),
}

impl ClientKind {
    fn to_authority(&self, endpoints: &S2Endpoints) -> Authority {
        match self {
            ClientKind::Account => endpoints.account.clone(),
            ClientKind::Basin(basin) => match &endpoints.basin {
                BasinEndpoint::ParentZone(zone) => format!("{basin}.{zone}")
                    .try_into()
                    .expect("valid authority as basin pre-validated"),
                BasinEndpoint::CellKnown(endpoint) => endpoint.clone(),
            },
        }
    }
}

#[derive(Debug, Clone)]
struct ClientInner {
    kind: ClientKind,
    channel: Channel,
    config: ClientConfig,
}

impl ClientInner {
    fn new<C>(kind: ClientKind, config: ClientConfig, connector: Option<C>) -> Self
    where
        C: tower_service::Service<http::Uri> + Send + 'static,
        C::Response: hyper::rt::Read + hyper::rt::Write + Send + Unpin,
        C::Future: Send,
        C::Error: std::error::Error + Send + Sync + 'static,
    {
        let authority = kind.to_authority(&config.endpoints);

        #[cfg(not(feature = "connector"))]
        let scheme = "https";
        #[cfg(feature = "connector")]
        let scheme = config.uri_scheme.as_str();

        let endpoint = format!("{scheme}://{authority}")
            .parse::<Endpoint>()
            .expect("previously validated endpoint scheme and authority")
            .user_agent(config.user_agent.clone())
            .expect("converting HeaderValue into HeaderValue")
            .http2_adaptive_window(true)
            .tls_config(
                ClientTlsConfig::default()
                    .with_webpki_roots()
                    .assume_http2(true),
            )
            .expect("valid TLS config")
            .connect_timeout(config.connection_timeout)
            .timeout(config.request_timeout);

        let channel = if let Some(connector) = connector {
            assert!(
                matches!(config.endpoints.basin, BasinEndpoint::CellKnown(_)),
                "custom connector only supported when connecting directly to a cell"
            );
            endpoint.connect_with_connector_lazy(connector)
        } else {
            endpoint.connect_lazy()
        };

        Self {
            kind,
            channel,
            config,
        }
    }

    fn for_basin(&self, basin: types::BasinName) -> ClientInner {
        let current_authority = self.kind.to_authority(&self.config.endpoints);
        let new_kind = ClientKind::Basin(basin);
        let new_authority = new_kind.to_authority(&self.config.endpoints);
        if current_authority == new_authority {
            self.clone()
        } else {
            // TODO: what if connector was used
            Self::new(new_kind, self.config.clone(), DEFAULT_CONNECTOR)
        }
    }

    async fn send<T: ServiceRequest>(&self, service_req: T) -> Result<T::Response, ClientError> {
        let basin_header = match (
            T::IS_BASIN_REQUEST,
            &self.kind,
            &self.config.endpoints.basin,
        ) {
            (true, ClientKind::Basin(basin), BasinEndpoint::CellKnown(_)) => {
                Some(AsciiMetadataValue::from_str(basin).expect("valid"))
            }
            _ => None,
        };
        send_request(service_req, &self.config.token, basin_header).await
    }

    async fn send_retryable_with_backoff<T: RetryableRequest>(
        &self,
        service_req: T,
        backoff_builder: impl BackoffBuilder,
    ) -> Result<T::Response, ClientError> {
        let retry_fn = || async { self.send(service_req.clone()).await };

        retry_fn
            .retry(backoff_builder)
            .when(|e| service_req.should_retry(e))
            .await
    }

    async fn send_retryable<T: RetryableRequest>(
        &self,
        service_req: T,
    ) -> Result<T::Response, ClientError> {
        self.send_retryable_with_backoff(service_req, self.backoff_builder())
            .await
    }

    fn backoff_builder(&self) -> impl BackoffBuilder {
        ConstantBuilder::default()
            .with_delay(self.config.retry_backoff_duration)
            .with_max_times(self.config.max_attempts)
            .with_jitter()
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

fn read_resumption_stream(
    mut request: ReadSessionServiceRequest,
    mut responses: ServiceStreamingResponse<ReadSessionStreamingResponse>,
    client: ClientInner,
) -> impl Send + futures::Stream<Item = Result<types::ReadOutput, ClientError>> {
    let mut backoff = None;
    async_stream::stream! {
        while let Some(item) = responses.next().await {
            match item {
                Err(e) if request.should_retry(&e) => {
                    if backoff.is_none() {
                        backoff = Some(client.backoff_builder().build());
                    }
                    if let Some(duration) = backoff.as_mut().and_then(|b| b.next()) {
                        sleep(duration).await;
                        if let Ok(new_responses) = client.send_retryable(request.clone()).await {
                            responses = new_responses;
                        } else {
                            yield Err(e);
                        }
                    } else {
                        yield Err(e);
                    }
                }
                item => {
                    if item.is_ok() {
                        backoff = None;
                    }
                    if let Ok(types::ReadOutput::Batch(types::SequencedRecordBatch { records })) = &item {
                        if let Some(record) = records.last() {
                            request.set_start_seq_num(Some(record.seq_num + 1));
                        }
                    }
                    yield item;
                }
            }
        }
    }
}

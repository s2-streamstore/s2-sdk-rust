use std::{fmt::Display, str::FromStr, time::Duration};

use backon::{BackoffBuilder, ConstantBuilder, Retryable};
use futures::StreamExt;
use http::{uri::Authority, HeaderValue};
use hyper_util::client::legacy::connect::HttpConnector;
use secrecy::SecretString;
use sync_docs::sync_docs;
use tokio::time::sleep;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

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

const DEFAULT_HTTP_CONNECTOR: Option<HttpConnector> = None;

/// S2 cloud environment to connect with.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HostCloud {
    /// S2 running on AWS.
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
}

impl Display for HostCloud {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for HostCloud {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.eq_ignore_ascii_case(Self::AWS) {
            Ok(Self::Aws)
        } else {
            Err(ParseError::new("host", s))
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HostEnv {
    #[default]
    Prod,
    Staging,
    Sandbox,
}

impl HostEnv {
    const PROD: &'static str = "prod";
    const STAGING: &'static str = "staging";
    const SANDBOX: &'static str = "sandbox";

    fn as_str(&self) -> &'static str {
        match self {
            Self::Prod => Self::PROD,
            Self::Staging => Self::STAGING,
            Self::Sandbox => Self::SANDBOX,
        }
    }
}

impl Display for HostEnv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for HostEnv {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.eq_ignore_ascii_case(Self::PROD) {
            Ok(Self::Prod)
        } else if s.eq_ignore_ascii_case(Self::STAGING) {
            Ok(Self::Staging)
        } else if s.eq_ignore_ascii_case(Self::SANDBOX) {
            Ok(Self::Sandbox)
        } else {
            Err(ParseError::new("env", s))
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BasinZone {
    CloudEnv,
    Static,
}

impl BasinZone {
    const CLOUD_ENV: &'static str = "cloud-env";
    const STATIC: &'static str = "static";

    fn as_str(&self) -> &'static str {
        match self {
            Self::CloudEnv => Self::CLOUD_ENV,
            Self::Static => Self::STATIC,
        }
    }
}

impl Display for BasinZone {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for BasinZone {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.eq_ignore_ascii_case(Self::CLOUD_ENV) {
            Ok(Self::CloudEnv)
        } else if s.eq_ignore_ascii_case(Self::STATIC) {
            Ok(Self::Static)
        } else {
            Err(ParseError::new("basin zone", s))
        }
    }
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("Invalid {0}: {1}")]
pub struct ParseError(String, String);

impl ParseError {
    fn new(what: impl Into<String>, details: impl Display) -> Self {
        Self(what.into(), details.to_string())
    }
}

/// Endpoints for the S2 environment.
#[derive(Debug, Clone)]
pub struct S2Endpoints {
    cell: Authority,
    basin_zone: Option<Authority>,
}

impl From<HostCloud> for S2Endpoints {
    fn from(cloud: HostCloud) -> Self {
        S2Endpoints::for_cloud(cloud)
    }
}

impl Default for S2Endpoints {
    fn default() -> Self {
        Self::for_cloud(HostCloud::default())
    }
}

impl S2Endpoints {
    pub fn for_cloud(cloud: HostCloud) -> Self {
        Self::from_parts(cloud, HostEnv::default(), None, None)
    }

    pub fn from_env() -> Result<Self, ParseError> {
        fn env_var<T>(
            name: &str,
            parse: impl FnOnce(String) -> Result<T, ParseError>,
        ) -> Result<Option<T>, ParseError> {
            match std::env::var(name) {
                Ok(value) => Ok(Some(parse(value)?)),
                Err(std::env::VarError::NotPresent) => Ok(None),
                Err(std::env::VarError::NotUnicode(value)) => Err(ParseError::new(
                    format!("{name} env var"),
                    value.to_string_lossy(),
                )),
            }
        }

        let cloud = env_var("S2_CLOUD", |s| HostCloud::from_str(&s))?.unwrap_or_default();
        let env = env_var("S2_ENV", |s| HostEnv::from_str(&s))?.unwrap_or_default();
        let basin_zone = env_var("S2_BASIN_ZONE", |s| BasinZone::from_str(&s))?;
        let cell_id = env_var("S2_CELL_ID", Ok)?;

        Ok(Self::from_parts(cloud, env, basin_zone, cell_id.as_deref()))
    }

    pub fn from_parts(
        cloud: HostCloud,
        env: HostEnv,
        basin_zone: Option<BasinZone>,
        cell_id: Option<&str>,
    ) -> Self {
        let env_suffix = match env {
            HostEnv::Prod => String::new(),
            env => format!("-{env}"),
        };

        let (cell_endpoint, default_basin_zone) = match cell_id {
            None => (format!("{cloud}.s2.dev"), BasinZone::CloudEnv),
            Some(cell_id) => (
                format!("{cell_id}.o{env_suffix}.{cloud}.s2.dev"),
                BasinZone::Static,
            ),
        };

        let basin_endpoint = match basin_zone.unwrap_or(default_basin_zone) {
            BasinZone::CloudEnv => Some(format!("b{env_suffix}.{cloud}.s2.dev")),
            BasinZone::Static => None,
        };

        Self {
            cell: cell_endpoint
                .parse()
                .expect("previously validated cell endpoint"),
            basin_zone: basin_endpoint
                .map(|b| b.parse().expect("previously validated basin endpoint")),
        }
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
            endpoints: S2Endpoints::default(),
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
    pub fn max_attempts(self, max_attempts: usize) -> Self {
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
            inner: ClientInner::new_cell(config, DEFAULT_HTTP_CONNECTOR),
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
            inner: ClientInner::new_cell(config, Some(connector)),
        }
    }

    pub fn basin_client(&self, basin: types::BasinName) -> BasinClient {
        BasinClient {
            inner: self.inner.new_basin(basin),
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
        Client::new(config).basin_client(basin)
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
        Client::new_with_connector(config, connector).basin_client(basin)
    }

    /// Get the client to interact with the S2 stream service API.
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
struct ClientInner {
    channel: Channel,
    basin: Option<types::BasinName>,
    config: ClientConfig,
}

impl ClientInner {
    fn new_cell<C>(config: ClientConfig, connector: Option<C>) -> Self
    where
        C: tower_service::Service<http::Uri> + Send + 'static,
        C::Response: hyper::rt::Read + hyper::rt::Write + Send + Unpin,
        C::Future: Send,
        C::Error: std::error::Error + Send + Sync + 'static,
    {
        let cell_endpoint = config.endpoints.cell.clone();
        Self::new(config, cell_endpoint, connector)
    }

    fn new_basin(&self, basin: types::BasinName) -> Self {
        match self.config.endpoints.basin_zone.clone() {
            Some(endpoint) => {
                let basin_endpoint: Authority = format!("{basin}.{endpoint}")
                    .parse()
                    .expect("previously validated basin name and endpoint");
                ClientInner::new(self.config.clone(), basin_endpoint, DEFAULT_HTTP_CONNECTOR)
            }
            None => Self {
                basin: Some(basin),
                ..self.clone()
            },
        }
    }

    fn new<C>(config: ClientConfig, endpoint: Authority, connector: Option<C>) -> Self
    where
        C: tower_service::Service<http::Uri> + Send + 'static,
        C::Response: hyper::rt::Read + hyper::rt::Write + Send + Unpin,
        C::Future: Send,
        C::Error: std::error::Error + Send + Sync + 'static,
    {
        #[cfg(not(feature = "connector"))]
        let scheme = "https";
        #[cfg(feature = "connector")]
        let scheme = config.uri_scheme.as_str();

        let endpoint = format!("{scheme}://{endpoint}")
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
                config.endpoints.basin_zone.is_none(),
                "cannot connect with connector if basin zone is provided"
            );
            endpoint.connect_with_connector_lazy(connector)
        } else {
            endpoint.connect_lazy()
        };

        Self {
            channel,
            basin: None,
            config,
        }
    }

    async fn send<T: ServiceRequest>(&self, service_req: T) -> Result<T::Response, ClientError> {
        send_request(service_req, &self.config.token, self.basin.as_ref()).await
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

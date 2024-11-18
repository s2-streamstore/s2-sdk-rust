use std::{fmt::Display, str::FromStr, time::Duration};

use backon::{ConstantBuilder, Retryable};
use http::uri::Authority;
use hyper_util::client::legacy::connect::HttpConnector;
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
            ReadServiceRequest, ReadSessionServiceRequest,
        },
        RetryableRequest, ServiceRequest, Streaming,
    },
    types,
};

const DEFAULT_HTTP_CONNECTOR: Option<HttpConnector> = None;

/// Cloud deployment to be used to connect the client with.
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

/// Endpoints for the hosted S2 environment.
#[derive(Debug, Clone)]
pub struct HostEndpoints {
    pub cell: Authority,
    pub basin_zone: Option<Authority>,
}

impl From<HostCloud> for HostEndpoints {
    fn from(cloud: HostCloud) -> Self {
        HostEndpoints::for_cloud(cloud)
    }
}

impl Default for HostEndpoints {
    fn default() -> Self {
        Self::for_cloud(HostCloud::default())
    }
}

impl HostEndpoints {
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
            cell: cell_endpoint.parse().unwrap(),
            basin_zone: basin_endpoint.map(|b| b.parse().unwrap()),
        }
    }
}

/// Client configuration to be used to connect with the host.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Auth token for the client.
    pub token: SecretString,
    /// Host URI to connect with.
    pub host_endpoints: HostEndpoints,
    /// Timeout for connecting/reconnecting.
    pub connection_timeout: Duration,
    /// Timeout for a particular request.
    pub request_timeout: Duration,
    /// User agent to be used for the client.
    pub user_agent: String,
    /// URI scheme to use to connect.
    #[cfg(feature = "connector")]
    pub uri_scheme: http::uri::Scheme,
}

impl ClientConfig {
    /// Construct a new client configuration with given auth token and other
    /// defaults.
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            token: token.into().into(),
            host_endpoints: HostEndpoints::default(),
            connection_timeout: Duration::from_secs(3),
            request_timeout: Duration::from_secs(5),
            user_agent: "s2-sdk-rust".to_string(),
            #[cfg(feature = "connector")]
            uri_scheme: http::uri::Scheme::HTTPS,
        }
    }

    /// Construct from an existing configuration with the new host URIs.
    pub fn with_host_endpoints(self, host_endpoints: impl Into<HostEndpoints>) -> Self {
        Self {
            host_endpoints: host_endpoints.into(),
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

    /// Construct from an existing configuration with the new URI scheme.
    #[cfg(feature = "connector")]
    pub fn with_uri_scheme(self, uri_scheme: impl Into<http::uri::Scheme>) -> Self {
        Self {
            uri_scheme: uri_scheme.into(),
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

/// The S2 client to interact with the API.
#[derive(Debug, Clone)]
pub struct Client {
    inner: ClientInner,
}

impl Client {
    /// Create the client to connect with the S2 API.
    pub fn new(config: ClientConfig) -> Result<Self, ConnectionError> {
        Ok(Self {
            inner: ClientInner::new_cell(config, DEFAULT_HTTP_CONNECTOR)?,
        })
    }

    #[cfg(feature = "connector")]
    pub fn new_with_connector<C>(
        config: ClientConfig,
        connector: C,
    ) -> Result<Self, ConnectionError>
    where
        C: tower_service::Service<http::Uri> + Send + 'static,
        C::Response: hyper::rt::Read + hyper::rt::Write + Send + Unpin,
        C::Future: Send,
        C::Error: std::error::Error + Send + Sync + 'static,
    {
        Ok(Self {
            inner: ClientInner::new_cell(config, Some(connector))?,
        })
    }

    /// Get the client to interact with the S2 basin service API.
    pub fn basin_client(&self, basin: impl Into<String>) -> Result<BasinClient, ConnectionError> {
        Ok(BasinClient {
            inner: self.inner.new_basin(basin)?,
        })
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
        basin: impl Into<String>,
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

/// Client to interact with the S2 basin service API.
#[derive(Debug, Clone)]
pub struct BasinClient {
    inner: ClientInner,
}

impl BasinClient {
    /// Create the client to connect with the S2 basin service API.
    pub fn new(config: ClientConfig, basin: impl Into<String>) -> Result<Self, ConnectionError> {
        let client = Client::new(config)?;
        client.basin_client(basin)
    }

    #[cfg(feature = "connector")]
    pub fn new_with_connector<C>(
        config: ClientConfig,
        basin: impl Into<String>,
        connector: C,
    ) -> Result<Self, ConnectionError>
    where
        C: tower_service::Service<http::Uri> + Send + 'static,
        C::Response: hyper::rt::Read + hyper::rt::Write + Send + Unpin,
        C::Future: Send,
        C::Error: std::error::Error + Send + Sync + 'static,
    {
        let client = Client::new_with_connector(config, connector)?;
        client.basin_client(basin)
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

/// Client to interact with the S2 stream service API.
#[derive(Debug, Clone)]
pub struct StreamClient {
    inner: ClientInner,
    stream: String,
}

impl StreamClient {
    /// Create the client to connect with the S2 stream service API.
    pub fn new(
        config: ClientConfig,
        basin: impl Into<String>,
        stream: impl Into<String>,
    ) -> Result<Self, ConnectionError> {
        BasinClient::new(config, basin).map(|client| client.stream_client(stream))
    }

    #[cfg(feature = "connector")]
    pub fn new_with_connector<C>(
        config: ClientConfig,
        basin: impl Into<String>,
        stream: impl Into<String>,
        connector: C,
    ) -> Result<Self, ConnectionError>
    where
        C: tower_service::Service<http::Uri> + Send + 'static,
        C::Response: hyper::rt::Read + hyper::rt::Write + Send + Unpin,
        C::Future: Send,
        C::Error: std::error::Error + Send + Sync + 'static,
    {
        BasinClient::new_with_connector(config, basin, connector)
            .map(|client| client.stream_client(stream))
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
        self.inner
            .send_retryable(ReadSessionServiceRequest::new(
                self.inner.stream_service_client(),
                &self.stream,
                req,
            ))
            .await
            .map(|s| Box::pin(s) as _)
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
    basin: Option<String>,
    config: ClientConfig,
}

impl ClientInner {
    fn new_cell<C>(config: ClientConfig, connector: Option<C>) -> Result<Self, ConnectionError>
    where
        C: tower_service::Service<http::Uri> + Send + 'static,
        C::Response: hyper::rt::Read + hyper::rt::Write + Send + Unpin,
        C::Future: Send,
        C::Error: std::error::Error + Send + Sync + 'static,
    {
        let cell_endpoint = config.host_endpoints.cell.clone();
        Self::new(config, cell_endpoint, connector)
    }

    fn new_basin(&self, basin: impl Into<String>) -> Result<Self, ConnectionError> {
        let basin = basin.into();

        match self.config.host_endpoints.basin_zone.clone() {
            Some(endpoint) => {
                let basin_endpoint: Authority = format!("{basin}.{endpoint}").parse()?;
                ClientInner::new(self.config.clone(), basin_endpoint, DEFAULT_HTTP_CONNECTOR)
            }
            None => Ok(Self {
                basin: Some(basin),
                ..self.clone()
            }),
        }
    }

    fn new<C>(
        config: ClientConfig,
        endpoint: Authority,
        connector: Option<C>,
    ) -> Result<Self, ConnectionError>
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

        let channel = if let Some(connector) = connector {
            assert!(
                config.host_endpoints.basin_zone.is_none(),
                "cannot connect with connector if basin zone is provided"
            );
            endpoint.connect_with_connector_lazy(connector)
        } else {
            endpoint.connect_lazy()
        };

        Ok(Self {
            channel,
            basin: None,
            config,
        })
    }

    async fn send<T: ServiceRequest>(&self, service_req: T) -> Result<T::Response, ClientError> {
        send_request(service_req, &self.config.token, self.basin.as_deref()).await
    }

    async fn send_retryable<T: RetryableRequest>(
        &self,
        service_req: T,
    ) -> Result<T::Response, ClientError> {
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
pub enum ConnectionError {
    #[error(transparent)]
    TonicTransportError(#[from] tonic::transport::Error),
    #[error(transparent)]
    UriParseError(#[from] http::uri::InvalidUri),
}

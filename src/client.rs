use std::time::Duration;

use http::{uri::Authority, Uri};
use secrecy::SecretString;
use tonic::transport::{Channel, Endpoint};
use typed_builder::TypedBuilder;

use crate::{
    api::{
        account_service_client::AccountServiceClient, basin_service_client::BasinServiceClient,
        stream_service_client::StreamServiceClient,
    },
    service::{
        account::{
            CreateBasinError, CreateBasinServiceRequest, DeleteBasinError,
            DeleteBasinServiceRequest, ListBasinsError, ListBasinsServiceRequest,
        },
        basin::{
            CreateStreamError, CreateStreamServiceRequest, DeleteStreamError,
            DeleteStreamServiceRequest, GetBasinConfigError, GetBasinConfigServiceRequest,
            GetStreamConfigError, GetStreamConfigServiceRequest, ListStreamsError,
            ListStreamsServiceRequest, ReconfigureBasinError, ReconfigureBasinServiceRequest,
            ReconfigureStreamError, ReconfigureStreamServiceRequest,
        },
        send_request,
        stream::{GetNextSeqNumError, GetNextSeqNumServiceRequest},
        ServiceError, ServiceRequest,
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

#[derive(Debug, Clone, TypedBuilder)]
pub struct HostUri {
    #[builder]
    pub global: Uri,
    #[builder(default)]
    pub cell: Option<Uri>,
    #[builder(default)]
    pub prefix_host_with_basin: bool,
}

impl Default for HostUri {
    fn default() -> Self {
        HostCloud::default().into()
    }
}

#[derive(Debug, Clone, TypedBuilder)]
pub struct ClientConfig {
    #[builder(default, setter(into))]
    pub host_uri: HostUri,
    #[builder(setter(into))]
    pub token: SecretString,
    #[builder(default)]
    pub test_connection: bool,
    #[builder(default = Duration::from_secs(3), setter(into))]
    pub connection_timeout: Duration,
    #[builder(default = Duration::from_secs(5), setter(into))]
    pub request_timeout: Duration,
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
            .send(
                ListBasinsServiceRequest::new(self.inner.account_service_client()),
                req,
            )
            .await
    }

    /// Create a new basin.
    pub async fn create_basin(
        &self,
        req: types::CreateBasinRequest,
    ) -> Result<types::CreateBasinResponse, ServiceError<CreateBasinError>> {
        self.inner
            .send(
                CreateBasinServiceRequest::new(self.inner.account_service_client()),
                req,
            )
            .await
    }

    /// Delete a basin.
    pub async fn delete_basin(
        &self,
        req: types::DeleteBasinRequest,
    ) -> Result<(), ServiceError<DeleteBasinError>> {
        let if_exists = req.if_exists;

        match self
            .inner
            .send(
                DeleteBasinServiceRequest::new(self.inner.account_service_client()),
                req,
            )
            .await
        {
            Err(ServiceError::Remote(DeleteBasinError::NotFound(_))) if if_exists => Ok(()),
            res => res,
        }
    }
}

#[derive(Debug, Clone)]
pub struct BasinClient {
    inner: ClientInner,
}

impl BasinClient {
    pub fn stream_client(&self, stream: impl Into<String>) -> StreamClient {
        StreamClient {
            inner: self.inner.clone(),
            stream: stream.into(),
        }
    }

    pub async fn get_basin_config(
        &self,
    ) -> Result<types::GetBasinConfigResponse, ServiceError<GetBasinConfigError>> {
        self.inner
            .send(
                GetBasinConfigServiceRequest::new(self.inner.basin_service_client()),
                /* request = */ (),
            )
            .await
    }

    pub async fn reconfigure_basin(
        &self,
        req: types::ReconfigureBasinRequest,
    ) -> Result<(), ServiceError<ReconfigureBasinError>> {
        self.inner
            .send(
                ReconfigureBasinServiceRequest::new(self.inner.basin_service_client()),
                req,
            )
            .await
    }

    pub async fn create_stream(
        &self,
        req: types::CreateStreamRequest,
    ) -> Result<(), ServiceError<CreateStreamError>> {
        self.inner
            .send(
                CreateStreamServiceRequest::new(self.inner.basin_service_client()),
                req,
            )
            .await
    }

    pub async fn list_streams(
        &self,
        req: types::ListStreamsRequest,
    ) -> Result<types::ListStreamsResponse, ServiceError<ListStreamsError>> {
        self.inner
            .send(
                ListStreamsServiceRequest::new(self.inner.basin_service_client()),
                req,
            )
            .await
    }

    pub async fn get_stream_config(
        &self,
        req: types::GetStreamConfigRequest,
    ) -> Result<types::GetStreamConfigResponse, ServiceError<GetStreamConfigError>> {
        self.inner
            .send(
                GetStreamConfigServiceRequest::new(self.inner.basin_service_client()),
                req,
            )
            .await
    }

    pub async fn reconfigure_stream(
        &self,
        req: types::ReconfigureStreamRequest,
    ) -> Result<(), ServiceError<ReconfigureStreamError>> {
        self.inner
            .send(
                ReconfigureStreamServiceRequest::new(self.inner.basin_service_client()),
                req,
            )
            .await
    }

    pub async fn delete_stream(
        &self,
        req: types::DeleteStreamRequest,
    ) -> Result<(), ServiceError<DeleteStreamError>> {
        let if_exists = req.if_exists;

        match self
            .inner
            .send(
                DeleteStreamServiceRequest::new(self.inner.basin_service_client()),
                req,
            )
            .await
        {
            Err(ServiceError::Remote(DeleteStreamError::NotFound(_))) if if_exists => Ok(()),
            res => res,
        }
    }
}

#[derive(Debug, Clone)]
pub struct StreamClient {
    inner: ClientInner,
    stream: String,
}

impl StreamClient {
    pub async fn get_next_seq_num(
        &self,
    ) -> Result<types::GetNextSeqNumResponse, ServiceError<GetNextSeqNumError>> {
        self.inner
            .send(
                GetNextSeqNumServiceRequest::new(self.inner.stream_service_client()),
                self.stream.clone(),
            )
            .await
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
        let channel = if config.test_connection {
            endpoint.connect().await?
        } else {
            endpoint.connect_lazy()
        };
        Ok(Self {
            channel,
            basin: None,
            config,
        })
    }

    pub async fn send<T: ServiceRequest>(
        &self,
        service: T,
        req: T::Request,
    ) -> Result<T::Response, ServiceError<T::Error>> {
        send_request(service, req, &self.config.token, self.basin.as_deref()).await
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

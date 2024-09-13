use std::time::Duration;

use http::{uri::Authority, Uri};
use secrecy::SecretString;
use tonic::transport::{Channel, Endpoint};
use typed_builder::TypedBuilder;

use crate::{
    api::{account_service_client::AccountServiceClient, basin_service_client::BasinServiceClient},
    service::{
        account::{CreateBasinError, CreateBasinServiceRequest},
        basin::{
            GetBasinConfigError, GetBasinConfigServiceRequest, ListStreamsError,
            ListStreamsServiceRequest,
        },
        send_request, ServiceError, ServiceRequest,
    },
    types,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Cloud {
    /// Localhost (to be used for testing).
    #[cfg(debug_assertions)]
    Local,
    /// S2 hosted on cloud.
    #[default]
    Aws,
}

impl From<Cloud> for ClientUrl {
    fn from(value: Cloud) -> Self {
        match value {
            #[cfg(debug_assertions)]
            Cloud::Local => ClientUrl {
                global: "http://localhost:4243".try_into().unwrap(),
                cell: None,
                prefix_host_with_basin: false,
            },
            Cloud::Aws => todo!("prod aws urls"),
        }
    }
}

#[derive(Debug, Clone, TypedBuilder)]
pub struct ClientUrl {
    #[builder]
    pub global: Uri,
    #[builder(default)]
    pub cell: Option<Uri>,
    #[builder(default)]
    pub prefix_host_with_basin: bool,
}

impl Default for ClientUrl {
    fn default() -> Self {
        Cloud::default().into()
    }
}

#[derive(Debug, Clone, TypedBuilder)]
pub struct ClientConfig {
    #[builder(default, setter(into))]
    pub url: ClientUrl,
    #[builder(setter(into))]
    pub token: SecretString,
    #[builder(default)]
    pub test_connection: bool,
    #[builder(default = Duration::from_secs(3), setter(into))]
    pub connection_timeout: Duration,
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
}

#[derive(Debug, Clone)]
pub struct BasinClient {
    inner: ClientInner,
}

impl BasinClient {
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
}

#[derive(Debug, Clone)]
struct ClientInner {
    channel: ConnectedChannel,
    basin: Option<String>,
    config: ClientConfig,
}

impl ClientInner {
    pub async fn connect_global(config: ClientConfig) -> Result<Self, ClientError> {
        let uri = config.url.global.clone();
        Self::connect(config, uri).await
    }

    pub async fn connect_cell(&self, basin: impl Into<String>) -> Result<Self, ClientError> {
        let basin = basin.into();

        match self.config.url.cell.clone() {
            Some(uri) if self.config.url.prefix_host_with_basin => {
                let host = uri.host().ok_or(ClientError::MissingHost)?;
                let port = uri.port_u16().map_or(String::new(), |p| p.to_string());
                let authority: Authority = format!("{basin}.{host}:{port}").parse()?;
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
        let endpoint = endpoint.connect_timeout(config.connection_timeout);
        let channel = if config.test_connection {
            endpoint.connect().await?
        } else {
            endpoint.connect_lazy()
        };
        Ok(Self {
            channel: ConnectedChannel {
                inner: channel,
                endpoint: uri,
            },
            basin: None,
            config,
        })
    }

    pub async fn send<T: ServiceRequest>(
        &self,
        service: T,
        req: T::Request,
    ) -> Result<T::Response, ServiceError<T::Error>> {
        send_request(
            service,
            req,
            &self.channel.endpoint,
            &self.config.token,
            self.basin.as_deref(),
        )
        .await
    }

    pub fn account_service_client(&self) -> AccountServiceClient<Channel> {
        AccountServiceClient::new(self.channel.inner.clone())
    }

    pub fn basin_service_client(&self) -> BasinServiceClient<Channel> {
        BasinServiceClient::new(self.channel.inner.clone())
    }
}

#[derive(Debug, Clone)]
struct ConnectedChannel {
    inner: Channel,
    endpoint: Uri,
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

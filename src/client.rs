use std::time::Duration;

use secrecy::SecretString;
use tonic::transport::{Channel, Endpoint};
use typed_builder::TypedBuilder;
use url::Url;

use crate::{
    api::{account_service_client::AccountServiceClient, basin_service_client::BasinServiceClient},
    service_request::{
        account::{CreateBasinError, CreateBasinServiceRequest},
        basin::{ListStreamsError, ListStreamsServiceRequest},
        send_request, ServiceError, ServiceRequest,
    },
    types,
};

#[derive(Debug, Clone, TypedBuilder)]
pub struct ClientUrl {
    #[builder]
    pub global: Url,
    #[builder(default)]
    pub cell: Option<Url>,
}

impl Default for ClientUrl {
    fn default() -> Self {
        // TODO: Update defaults to prod URLs.
        ClientUrl {
            global: "http://localhost:4243".try_into().unwrap(),
            cell: None,
        }
    }
}

#[derive(Debug, Clone, TypedBuilder)]
pub struct ClientConfig {
    #[builder(default)]
    pub uri: ClientUrl,
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
    config: ClientConfig,
}

impl ClientInner {
    pub async fn connect_global(config: ClientConfig) -> Result<Self, ClientError> {
        let uri = config.uri.global.clone();
        Self::connect(config, uri).await
    }

    pub async fn connect_cell(&self, basin: impl Into<String>) -> Result<Self, ClientError> {
        let basin = basin.into();
        let mut inner = self.clone();
        if let Some(mut url) = inner.config.uri.cell.clone() {
            let new_host = url.host_str().map(|host| format!("{basin}.{host}"));
            url.set_host(new_host.as_deref())?;
            ClientInner::connect(inner.config, url).await
        } else {
            // We need to fake the connected endpoint to pass the "Host" header.
            let mut url = inner.config.uri.global.clone();
            let new_host = url.host_str().map(|host| format!("{basin}.{host}"));
            url.set_host(new_host.as_deref())?;
            inner.channel.endpoint = url;
            Ok(inner)
        }
    }

    async fn connect(config: ClientConfig, url: Url) -> Result<Self, ClientError> {
        // TODO: Connection pool?
        let endpoint: Endpoint = url.as_str().parse()?;
        let endpoint = endpoint.connect_timeout(config.connection_timeout);
        let channel = if config.test_connection {
            endpoint.connect().await?
        } else {
            endpoint.connect_lazy()
        };
        Ok(Self {
            channel: ConnectedChannel {
                inner: channel,
                endpoint: url,
            },
            config,
        })
    }

    pub async fn send<T: ServiceRequest>(
        &self,
        service: T,
        req: T::Request,
    ) -> Result<T::Response, ServiceError<T::Error>> {
        send_request(service, req, &self.channel.endpoint, &self.config.token).await
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
    endpoint: Url,
}

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error(transparent)]
    TonicTransportError(#[from] tonic::transport::Error),
    #[error(transparent)]
    UrlParseError(#[from] url::ParseError),
}

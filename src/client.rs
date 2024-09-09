use derive_builder::Builder;
use tonic::transport::{Channel, Endpoint};
use url::Url;

use crate::{
    api::{account_service_client::AccountServiceClient, basin_service_client::BasinServiceClient},
    request::{
        send_request, CreateBasinError, CreateBasinServiceRequest, ServiceError, ServiceRequest,
    },
    types,
    util::secret_string::SecretString,
};

#[derive(Debug, Clone, Builder)]
pub struct ClientUrl {
    #[builder]
    pub global: Url,
    #[builder(default)]
    pub basin: Option<Url>,
}

impl Default for ClientUrl {
    fn default() -> Self {
        // TODO: Update defaults to prod URLs.
        ClientUrl {
            global: "http://localhost:4243".try_into().unwrap(),
            basin: None,
        }
    }
}

#[derive(Debug, Clone, Builder)]
#[builder(pattern = "owned")]
pub struct ClientConfig {
    #[builder(default)]
    pub uri: ClientUrl,
    #[builder(setter(into))]
    pub token: SecretString,
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
            inner: self.inner.connect_basin(basin).await?,
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
            .map_err(Into::into)
    }
}

#[derive(Debug, Clone)]
pub struct BasinClient {
    inner: ClientInner,
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

    pub async fn connect_basin(&self, basin: impl Into<String>) -> Result<Self, ClientError> {
        let basin = basin.into();
        let mut inner = self.clone();
        if let Some(mut url) = inner.config.uri.basin.clone() {
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
        // TODO: Configurable `connect_lazy`?
        // TODO: Connection pool?
        let endpoint: Endpoint = url.as_str().parse()?;
        let channel = endpoint.connect().await?;
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

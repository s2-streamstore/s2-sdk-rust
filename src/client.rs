use std::fmt;

use tonic::transport::{Channel, Endpoint};

use crate::{
    api::account_service_client::AccountServiceClient,
    request::{
        send_request, CreateBasinError, CreateBasinServiceRequest, ServiceError, ServiceRequest,
    },
    types,
};

#[derive(Debug, Clone)]
pub struct Client {
    inner: ClientInner,
}

impl Client {
    pub async fn connect(
        endpoint: impl Into<Endpoint>,
        token: impl Into<String>,
    ) -> Result<Self, ClientError> {
        Ok(Self {
            inner: ClientInner::connect(endpoint, token).await?,
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

#[derive(Clone)]
pub(crate) struct ClientInner {
    endpoint: Endpoint,
    channel: Channel,
    token: String,
}

impl fmt::Debug for ClientInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientInner")
            .field("endpoint", &self.endpoint)
            .field("channel", &self.channel)
            .field("token", &"***")
            .finish()
    }
}

impl ClientInner {
    pub async fn connect(
        endpoint: impl Into<Endpoint>,
        token: impl Into<String>,
    ) -> Result<Self, ClientError> {
        // TODO: Configurable `connect_lazy`?
        // TODO: Connection pool?
        let endpoint: Endpoint = endpoint.into();
        let channel = endpoint.connect().await?;
        Ok(Self {
            endpoint,
            channel,
            token: token.into(),
        })
    }

    pub async fn send<T: ServiceRequest>(
        &self,
        service: T,
        req: T::Request,
    ) -> Result<T::Response, ServiceError<T::Error>> {
        send_request(service, req, &self.endpoint, &self.token).await
    }

    pub fn account_service_client(&self) -> AccountServiceClient<Channel> {
        AccountServiceClient::new(self.channel.clone())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error(transparent)]
    TonicTransportError(#[from] tonic::transport::Error),
}

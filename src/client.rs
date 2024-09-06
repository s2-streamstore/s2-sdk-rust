use tonic::transport::{Channel, Endpoint};

use crate::{
    request::{
        send_request, CreateBasinError, CreateBasinServiceRequest, ServiceError, ServiceRequest,
    },
    types,
};

#[derive(Debug, Clone)]
pub struct Client {
    channel: Channel,
    token: String,
}

impl Client {
    pub async fn connect(
        endpoint: impl Into<Endpoint>,
        token: impl Into<String>,
    ) -> Result<Self, ClientError> {
        // TODO: Configurable `connect_lazy`?
        let channel = endpoint.into().connect().await?;
        Ok(Self {
            channel,
            token: token.into(),
        })
    }

    pub async fn create_basin(
        &self,
        req: types::CreateBasinRequest,
    ) -> Result<types::CreateBasinResponse, ServiceError<CreateBasinError>> {
        self.send(CreateBasinServiceRequest::new(self.channel()), req)
            .await
            .map_err(Into::into)
    }

    pub(crate) async fn send<T: ServiceRequest>(
        &self,
        service: T,
        req: T::Request,
    ) -> Result<T::Response, ServiceError<T::Error>> {
        send_request(service, req, &self.token).await
    }

    pub(crate) fn channel(&self) -> Channel {
        self.channel.clone()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error(transparent)]
    TonicTransportError(#[from] tonic::transport::Error),
}

use tonic::transport::{Channel, Endpoint};

use crate::{
    request::{send_request, CreateBasinServiceRequest, ServiceError},
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
        let channel = endpoint.into().connect().await?;
        Ok(Self {
            channel,
            token: token.into(),
        })
    }

    pub async fn create_basin(
        &self,
        req: types::CreateBasinRequest,
    ) -> Result<types::CreateBasinResponse, ServiceError<tonic::Status>> {
        send_request(
            CreateBasinServiceRequest::new(self.channel.clone(), &self.token),
            req,
        )
        .await
        .map_err(Into::into)
    }
}

// TODO: Think about errors.

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error(transparent)]
    TonicTransportError(#[from] tonic::transport::Error),
}

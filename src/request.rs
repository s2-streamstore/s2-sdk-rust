use backon::{ConstantBuilder, Retryable};
use tonic::{transport::Channel, IntoRequest};

use crate::{
    api::{self, account_service_client::AccountServiceClient},
    types::{self, ConvertError},
};

#[derive(Debug, Clone, thiserror::Error)]
pub enum ServiceError<T: std::error::Error> {
    #[error(transparent)]
    ConvertError(ConvertError),
    #[error(transparent)]
    RemoteError(T),
}

pub async fn send_request<T: ServiceRequest>(
    service: T,
    req: T::Req,
) -> Result<T::Resp, ServiceError<T::Error>> {
    let retry_fn = || async {
        let mut service = service.clone();
        let req = service
            .prepare_request(req.clone())
            .map_err(ServiceError::ConvertError)?;
        service.send(req).await.map_err(ServiceError::RemoteError)
    };
    // TODO: Configure retry.
    let retry = Retryable::retry(retry_fn, ConstantBuilder::default()).when(|e| match e {
        ServiceError::ConvertError(_) => false,
        ServiceError::RemoteError(e) => service.retry_if(e),
    });
    let resp = retry.await?;
    service
        .parse_response(resp)
        .map_err(ServiceError::ConvertError)
}

pub trait ServiceRequest: Clone {
    type Req: Clone;
    type ApiReq;
    type Resp;
    type ApiResp;
    type Error: std::error::Error;

    fn prepare_request(&self, req: Self::Req)
        -> Result<tonic::Request<Self::ApiReq>, ConvertError>;
    fn parse_response(
        &self,
        resp: tonic::Response<Self::ApiResp>,
    ) -> Result<Self::Resp, ConvertError>;
    async fn send(
        &mut self,
        req: tonic::Request<Self::ApiReq>,
    ) -> Result<tonic::Response<Self::ApiResp>, Self::Error>;
    fn retry_if(&self, err: &Self::Error) -> bool;
}

#[derive(Debug, Clone)]
pub struct CreateBasinServiceRequest {
    client: AccountServiceClient<Channel>,
    token: String,
}

impl CreateBasinServiceRequest {
    pub fn new(channel: Channel, token: impl Into<String>) -> Self {
        Self {
            client: AccountServiceClient::new(channel),
            token: token.into(),
        }
    }
}

impl ServiceRequest for CreateBasinServiceRequest {
    type Req = types::CreateBasinRequest;
    type ApiReq = api::CreateBasinRequest;
    type Resp = types::CreateBasinResponse;
    type ApiResp = api::CreateBasinResponse;

    // TODO: Update this to meaningful error.
    type Error = tonic::Status;

    fn prepare_request(
        &self,
        req: Self::Req,
    ) -> Result<tonic::Request<Self::ApiReq>, ConvertError> {
        let req: api::CreateBasinRequest = req.try_into()?;
        let mut req = req.into_request();
        add_auth_token_to_req(&mut req, &self.token);
        Ok(req)
    }

    fn parse_response(
        &self,
        resp: tonic::Response<Self::ApiResp>,
    ) -> Result<Self::Resp, ConvertError> {
        Ok(resp.into_inner().try_into()?)
    }

    async fn send(
        &mut self,
        req: tonic::Request<Self::ApiReq>,
    ) -> Result<tonic::Response<Self::ApiResp>, Self::Error> {
        self.client.create_basin(req).await
    }

    fn retry_if(&self, _status: &Self::Error) -> bool {
        false
    }
}

fn add_auth_token_to_req<T>(req: &mut tonic::Request<T>, token: &str) {
    req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).try_into().unwrap(),
    );
}

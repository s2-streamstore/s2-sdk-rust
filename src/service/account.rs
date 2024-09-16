use prost_types::method_options::IdempotencyLevel;
use tonic::{transport::Channel, IntoRequest};

use super::{ServiceError, ServiceRequest};
use crate::{
    api::{self, account_service_client::AccountServiceClient},
    types::{self, ConvertError},
};

#[derive(Debug, Clone)]
pub struct CreateBasinServiceRequest {
    client: AccountServiceClient<Channel>,
}

impl CreateBasinServiceRequest {
    pub fn new(client: AccountServiceClient<Channel>) -> Self {
        Self { client }
    }
}

impl ServiceRequest for CreateBasinServiceRequest {
    type Request = types::CreateBasinRequest;
    type ApiRequest = api::CreateBasinRequest;
    type Response = types::CreateBasinResponse;
    type ApiResponse = api::CreateBasinResponse;
    type Error = CreateBasinError;

    const IDEMPOTENCY_LEVEL: IdempotencyLevel = IdempotencyLevel::IdempotencyUnknown;

    fn prepare_request(
        &self,
        req: Self::Request,
    ) -> Result<tonic::Request<Self::ApiRequest>, ConvertError> {
        let req: api::CreateBasinRequest = req.try_into()?;
        Ok(req.into_request())
    }

    fn parse_response(
        &self,
        resp: tonic::Response<Self::ApiResponse>,
    ) -> Result<Self::Response, ConvertError> {
        resp.into_inner().try_into()
    }

    fn parse_status(&self, status: &tonic::Status) -> Option<Self::Error> {
        match status.code() {
            tonic::Code::InvalidArgument => Some(CreateBasinError::InvalidArgument(
                status.message().to_string(),
            )),
            tonic::Code::AlreadyExists => Some(CreateBasinError::AlreadyExists(
                status.message().to_string(),
            )),
            _ => None,
        }
    }

    async fn send(
        &mut self,
        req: tonic::Request<Self::ApiRequest>,
    ) -> Result<tonic::Response<Self::ApiResponse>, tonic::Status> {
        self.client.create_basin(req).await
    }

    fn should_retry(&self, _status: &ServiceError<Self::Error>) -> bool {
        false
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum CreateBasinError {
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
    #[error("Already exists: {0}")]
    AlreadyExists(String),
}

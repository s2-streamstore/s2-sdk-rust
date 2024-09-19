use prost_types::method_options::IdempotencyLevel;
use tonic::{transport::Channel, IntoRequest};

use super::ServiceRequest;
use crate::{
    api::{self, stream_service_client::StreamServiceClient},
    types,
};

#[derive(Debug, Clone)]
pub struct GetNextSeqNumServiceRequest {
    client: StreamServiceClient<Channel>,
}

impl GetNextSeqNumServiceRequest {
    pub fn new(client: StreamServiceClient<Channel>) -> Self {
        Self { client }
    }
}

impl ServiceRequest for GetNextSeqNumServiceRequest {
    type Request = String;
    type ApiRequest = api::GetNextSeqNumRequest;
    type Response = types::GetNextSeqNumResponse;
    type ApiResponse = api::GetNextSeqNumResponse;
    type Error = GetNextSeqNumError;

    const IDEMPOTENCY_LEVEL: IdempotencyLevel = IdempotencyLevel::NoSideEffects;

    fn prepare_request(
        &self,
        req: Self::Request,
    ) -> Result<tonic::Request<Self::ApiRequest>, types::ConvertError> {
        let req = api::GetNextSeqNumRequest { stream: req };
        Ok(req.into_request())
    }

    fn parse_response(
        &self,
        resp: tonic::Response<Self::ApiResponse>,
    ) -> Result<Self::Response, types::ConvertError> {
        Ok(resp.into_inner().into())
    }

    fn parse_status(&self, status: &tonic::Status) -> Option<Self::Error> {
        match status.code() {
            tonic::Code::NotFound => {
                Some(GetNextSeqNumError::NotFound(status.message().to_string()))
            }
            tonic::Code::InvalidArgument => Some(GetNextSeqNumError::InvalidArgument(
                status.message().to_string(),
            )),
            _ => None,
        }
    }

    async fn send(
        &mut self,
        req: tonic::Request<Self::ApiRequest>,
    ) -> Result<tonic::Response<Self::ApiResponse>, tonic::Status> {
        self.client.get_next_seq_num(req).await
    }

    fn should_retry(&self, _err: &super::ServiceError<Self::Error>) -> bool {
        false
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GetNextSeqNumError {
    #[error("Not found: {0}")]
    NotFound(String),
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
}

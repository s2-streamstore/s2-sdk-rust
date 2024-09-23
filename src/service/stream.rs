use prost_types::method_options::IdempotencyLevel;
use tonic::{transport::Channel, IntoRequest};

use crate::{
    api::{self, stream_service_client::StreamServiceClient},
    types,
};

use super::ServiceRequest;

#[derive(Debug, Clone)]
pub struct GetNextSeqNumServiceRequest {
    client: StreamServiceClient<Channel>,
    stream: String,
}

impl GetNextSeqNumServiceRequest {
    pub fn new(client: StreamServiceClient<Channel>, stream: impl Into<String>) -> Self {
        Self {
            client,
            stream: stream.into(),
        }
    }
}

impl ServiceRequest for GetNextSeqNumServiceRequest {
    type ApiRequest = api::GetNextSeqNumRequest;
    type Response = types::GetNextSeqNumResponse;
    type ApiResponse = api::GetNextSeqNumResponse;
    type Error = GetNextSeqNumError;

    const IDEMPOTENCY_LEVEL: IdempotencyLevel = IdempotencyLevel::NoSideEffects;

    fn prepare_request(&self) -> Result<tonic::Request<Self::ApiRequest>, types::ConvertError> {
        let req = api::GetNextSeqNumRequest {
            stream: self.stream.clone(),
        };
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

#[derive(Debug, Clone)]
pub struct AppendServiceRequest {
    client: StreamServiceClient<Channel>,
    stream: String,
    req: types::AppendRequest,
}

impl AppendServiceRequest {
    pub fn new(
        client: StreamServiceClient<Channel>,
        stream: impl Into<String>,
        req: types::AppendRequest,
    ) -> Self {
        Self {
            client,
            stream: stream.into(),
            req,
        }
    }
}

impl ServiceRequest for AppendServiceRequest {
    type ApiRequest = api::AppendRequest;
    type Response = types::AppendResponse;
    type ApiResponse = api::AppendResponse;
    type Error = AppendError;

    const IDEMPOTENCY_LEVEL: IdempotencyLevel = IdempotencyLevel::IdempotencyUnknown;

    fn prepare_request(&self) -> Result<tonic::Request<Self::ApiRequest>, types::ConvertError> {
        Ok(self
            .req
            .clone()
            .into_api_type(self.stream.clone())
            .into_request())
    }

    fn parse_response(
        &self,
        resp: tonic::Response<Self::ApiResponse>,
    ) -> Result<Self::Response, types::ConvertError> {
        resp.into_inner().try_into()
    }

    fn parse_status(&self, status: &tonic::Status) -> Option<Self::Error> {
        match status.code() {
            tonic::Code::NotFound => Some(AppendError::NotFound(status.message().to_string())),
            tonic::Code::InvalidArgument => {
                Some(AppendError::InvalidArgument(status.message().to_string()))
            }
            _ => None,
        }
    }

    async fn send(
        &mut self,
        req: tonic::Request<Self::ApiRequest>,
    ) -> Result<tonic::Response<Self::ApiResponse>, tonic::Status> {
        self.client.append(req).await
    }

    fn should_retry(&self, _err: &super::ServiceError<Self::Error>) -> bool {
        false
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AppendError {
    #[error("Not found: {0}")]
    NotFound(String),
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
}

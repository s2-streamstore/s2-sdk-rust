use prost_types::method_options::IdempotencyLevel;
use tonic::{transport::Channel, IntoRequest};

use super::{ServiceRequest, StreamingResponse, WithStreamingResponse};
use crate::{
    api::{self, stream_service_client::StreamServiceClient},
    types,
};

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

    fn parse_status(&self, status: &tonic::Status) -> Result<Self::Response, Option<Self::Error>> {
        Err(match status.code() {
            tonic::Code::NotFound => {
                Some(GetNextSeqNumError::NotFound(status.message().to_string()))
            }
            tonic::Code::InvalidArgument => Some(GetNextSeqNumError::InvalidArgument(
                status.message().to_string(),
            )),
            _ => None,
        })
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

    fn parse_status(&self, status: &tonic::Status) -> Result<Self::Response, Option<Self::Error>> {
        Err(match status.code() {
            tonic::Code::NotFound => Some(AppendError::NotFound(status.message().to_string())),
            tonic::Code::InvalidArgument => {
                Some(AppendError::InvalidArgument(status.message().to_string()))
            }
            _ => None,
        })
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

#[derive(Debug, Clone)]
pub struct ReadServiceRequest {
    client: StreamServiceClient<Channel>,
    stream: String,
    req: types::ReadRequest,
}

impl ReadServiceRequest {
    pub fn new(
        client: StreamServiceClient<Channel>,
        stream: impl Into<String>,
        req: types::ReadRequest,
    ) -> Self {
        Self {
            client,
            stream: stream.into(),
            req,
        }
    }
}

impl ServiceRequest for ReadServiceRequest {
    type ApiRequest = api::ReadRequest;
    type Response = types::ReadResponse;
    type ApiResponse = api::ReadResponse;
    type Error = ReadError;

    const IDEMPOTENCY_LEVEL: IdempotencyLevel = IdempotencyLevel::NoSideEffects;

    fn prepare_request(&self) -> Result<tonic::Request<Self::ApiRequest>, types::ConvertError> {
        let req = self.req.clone().try_into_api_type(self.stream.clone())?;
        Ok(req.into_request())
    }

    fn parse_response(
        &self,
        resp: tonic::Response<Self::ApiResponse>,
    ) -> Result<Self::Response, types::ConvertError> {
        resp.into_inner().try_into()
    }

    fn parse_status(&self, status: &tonic::Status) -> Result<Self::Response, Option<Self::Error>> {
        Err(match status.code() {
            tonic::Code::NotFound => Some(ReadError::NotFound(status.message().to_string())),
            tonic::Code::InvalidArgument => {
                Some(ReadError::InvalidArgument(status.message().to_string()))
            }
            _ => None,
        })
    }

    async fn send(
        &mut self,
        req: tonic::Request<Self::ApiRequest>,
    ) -> Result<tonic::Response<Self::ApiResponse>, tonic::Status> {
        self.client.read(req).await
    }

    fn should_retry(&self, _err: &super::ServiceError<Self::Error>) -> bool {
        false
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ReadError {
    #[error("Not found: {0}")]
    NotFound(String),
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
}

#[derive(Debug, Clone)]
pub struct ReadSessionServiceRequest {
    client: StreamServiceClient<Channel>,
    stream: String,
    req: types::ReadSessionRequest,
}

impl ReadSessionServiceRequest {
    pub fn new(
        client: StreamServiceClient<Channel>,
        stream: impl Into<String>,
        req: types::ReadSessionRequest,
    ) -> Self {
        Self {
            client,
            stream: stream.into(),
            req,
        }
    }
}

impl ServiceRequest for ReadSessionServiceRequest {
    type ApiRequest = api::ReadSessionRequest;
    type Response = StreamingResponse<Self>;
    type ApiResponse = tonic::Streaming<api::ReadSessionResponse>;
    type Error = ReadSessionError;

    const IDEMPOTENCY_LEVEL: IdempotencyLevel = IdempotencyLevel::NoSideEffects;

    fn prepare_request(&self) -> Result<tonic::Request<Self::ApiRequest>, types::ConvertError> {
        let req = self.req.clone().into_api_type(self.stream.clone());
        Ok(req.into_request())
    }

    fn parse_response(
        &self,
        resp: tonic::Response<Self::ApiResponse>,
    ) -> Result<Self::Response, types::ConvertError> {
        Ok(StreamingResponse::new(self.clone(), resp.into_inner()))
    }

    fn parse_status(&self, status: &tonic::Status) -> Result<Self::Response, Option<Self::Error>> {
        Err(match status.code() {
            tonic::Code::NotFound => Some(ReadSessionError::NotFound(status.message().to_string())),
            tonic::Code::InvalidArgument => Some(ReadSessionError::InvalidArgument(
                status.message().to_string(),
            )),
            _ => None,
        })
    }

    async fn send(
        &mut self,
        req: tonic::Request<Self::ApiRequest>,
    ) -> Result<tonic::Response<Self::ApiResponse>, tonic::Status> {
        self.client.read_session(req).await
    }

    fn should_retry(&self, _err: &super::ServiceError<Self::Error>) -> bool {
        false
    }
}

impl WithStreamingResponse for ReadSessionServiceRequest {
    type ResponseItem = types::ReadSessionResponse;
    type ApiResponseItem = api::ReadSessionResponse;

    fn parse_response_item(
        &self,
        resp: Self::ApiResponseItem,
    ) -> Result<Self::ResponseItem, types::ConvertError> {
        resp.try_into()
    }

    fn parse_response_item_status(
        &self,
        status: &tonic::Status,
    ) -> Result<Self::ResponseItem, Option<Self::Error>> {
        Err(match status.code() {
            tonic::Code::NotFound => Some(ReadSessionError::NotFound(status.message().to_string())),
            tonic::Code::InvalidArgument => Some(ReadSessionError::InvalidArgument(
                status.message().to_string(),
            )),
            _ => None,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ReadSessionError {
    #[error("Not found: {0}")]
    NotFound(String),
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
}

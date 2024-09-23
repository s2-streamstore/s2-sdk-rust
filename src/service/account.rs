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
    req: types::CreateBasinRequest,
}

impl CreateBasinServiceRequest {
    pub fn new(client: AccountServiceClient<Channel>, req: types::CreateBasinRequest) -> Self {
        Self { client, req }
    }
}

impl ServiceRequest for CreateBasinServiceRequest {
    type ApiRequest = api::CreateBasinRequest;
    type Response = types::CreateBasinResponse;
    type ApiResponse = api::CreateBasinResponse;
    type Error = CreateBasinError;

    const IDEMPOTENCY_LEVEL: IdempotencyLevel = IdempotencyLevel::IdempotencyUnknown;

    fn prepare_request(&self) -> Result<tonic::Request<Self::ApiRequest>, ConvertError> {
        let req: api::CreateBasinRequest = self.req.clone().try_into()?;
        Ok(req.into_request())
    }

    fn parse_response(
        &self,
        resp: tonic::Response<Self::ApiResponse>,
    ) -> Result<Self::Response, ConvertError> {
        resp.into_inner().try_into()
    }

    fn parse_status(&self, status: &tonic::Status) -> Result<Self::Response, Option<Self::Error>> {
        Err(match status.code() {
            tonic::Code::InvalidArgument => Some(CreateBasinError::InvalidArgument(
                status.message().to_string(),
            )),
            tonic::Code::AlreadyExists => Some(CreateBasinError::AlreadyExists(
                status.message().to_string(),
            )),
            _ => None,
        })
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

#[derive(Debug, Clone)]
pub struct ListBasinsServiceRequest {
    client: AccountServiceClient<Channel>,
    req: types::ListBasinsRequest,
}

impl ListBasinsServiceRequest {
    pub fn new(client: AccountServiceClient<Channel>, req: types::ListBasinsRequest) -> Self {
        Self { client, req }
    }
}

impl ServiceRequest for ListBasinsServiceRequest {
    type ApiRequest = api::ListBasinsRequest;
    type Response = types::ListBasinsResponse;
    type ApiResponse = api::ListBasinsResponse;
    type Error = ListBasinsError;

    const IDEMPOTENCY_LEVEL: IdempotencyLevel = IdempotencyLevel::NoSideEffects;

    fn prepare_request(&self) -> Result<tonic::Request<Self::ApiRequest>, ConvertError> {
        let req: api::ListBasinsRequest = self.req.clone().try_into()?;
        Ok(req.into_request())
    }

    fn parse_response(
        &self,
        resp: tonic::Response<Self::ApiResponse>,
    ) -> Result<Self::Response, ConvertError> {
        resp.into_inner().try_into()
    }

    fn parse_status(&self, status: &tonic::Status) -> Result<Self::Response, Option<Self::Error>> {
        Err(match status.code() {
            tonic::Code::InvalidArgument => Some(ListBasinsError::InvalidArgument(
                status.message().to_string(),
            )),
            _ => None,
        })
    }

    async fn send(
        &mut self,
        req: tonic::Request<Self::ApiRequest>,
    ) -> Result<tonic::Response<Self::ApiResponse>, tonic::Status> {
        self.client.list_basins(req).await
    }

    fn should_retry(&self, _status: &ServiceError<Self::Error>) -> bool {
        false
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum ListBasinsError {
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
}

#[derive(Debug, Clone)]
pub struct DeleteBasinServiceRequest {
    client: AccountServiceClient<Channel>,
    req: types::DeleteBasinRequest,
}

impl DeleteBasinServiceRequest {
    pub fn new(client: AccountServiceClient<Channel>, req: types::DeleteBasinRequest) -> Self {
        Self { client, req }
    }
}

impl ServiceRequest for DeleteBasinServiceRequest {
    type ApiRequest = api::DeleteBasinRequest;
    type Response = ();
    type ApiResponse = api::DeleteBasinResponse;
    type Error = DeleteBasinError;

    const IDEMPOTENCY_LEVEL: IdempotencyLevel = IdempotencyLevel::Idempotent;

    fn prepare_request(&self) -> Result<tonic::Request<Self::ApiRequest>, ConvertError> {
        let req: api::DeleteBasinRequest = self.req.clone().into();
        Ok(req.into_request())
    }

    fn parse_response(
        &self,
        _resp: tonic::Response<Self::ApiResponse>,
    ) -> Result<Self::Response, ConvertError> {
        Ok(())
    }

    fn parse_status(&self, status: &tonic::Status) -> Result<Self::Response, Option<Self::Error>> {
        match status.code() {
            tonic::Code::InvalidArgument => Err(Some(DeleteBasinError::InvalidArgument(
                status.message().to_string(),
            ))),
            tonic::Code::NotFound if self.req.if_exists => Ok(()),
            tonic::Code::NotFound => Err(Some(DeleteBasinError::NotFound(
                status.message().to_string(),
            ))),
            _ => Err(None),
        }
    }

    async fn send(
        &mut self,
        req: tonic::Request<Self::ApiRequest>,
    ) -> Result<tonic::Response<Self::ApiResponse>, tonic::Status> {
        self.client.delete_basin(req).await
    }

    fn should_retry(&self, _status: &ServiceError<Self::Error>) -> bool {
        false
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum DeleteBasinError {
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
    #[error("Not found: {0}")]
    NotFound(String),
}

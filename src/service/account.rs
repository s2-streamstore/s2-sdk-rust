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

#[derive(Debug, Clone)]
pub struct ListBasinsServiceRequest {
    client: AccountServiceClient<Channel>,
}

impl ListBasinsServiceRequest {
    pub fn new(client: AccountServiceClient<Channel>) -> Self {
        Self { client }
    }
}

impl ServiceRequest for ListBasinsServiceRequest {
    type Request = types::ListBasinsRequest;
    type ApiRequest = api::ListBasinsRequest;
    type Response = types::ListBasinsResponse;
    type ApiResponse = api::ListBasinsResponse;
    type Error = ListBasinsError;

    const IDEMPOTENCY_LEVEL: IdempotencyLevel = IdempotencyLevel::NoSideEffects;

    fn prepare_request(
        &self,
        req: Self::Request,
    ) -> Result<tonic::Request<Self::ApiRequest>, ConvertError> {
        let req: api::ListBasinsRequest = req.into();
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
            tonic::Code::InvalidArgument => Some(ListBasinsError::InvalidArgument(
                status.message().to_string(),
            )),
            _ => None,
        }
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
}

impl DeleteBasinServiceRequest {
    pub fn new(client: AccountServiceClient<Channel>) -> Self {
        Self { client }
    }
}

impl ServiceRequest for DeleteBasinServiceRequest {
    type Request = types::DeleteBasinRequest;
    type ApiRequest = api::DeleteBasinRequest;
    type Response = ();
    type ApiResponse = api::DeleteBasinResponse;
    type Error = DeleteBasinError;

    const IDEMPOTENCY_LEVEL: IdempotencyLevel = IdempotencyLevel::Idempotent;

    fn prepare_request(
        &self,
        req: Self::Request,
    ) -> Result<tonic::Request<Self::ApiRequest>, ConvertError> {
        let req: api::DeleteBasinRequest = req.into();
        Ok(req.into_request())
    }

    fn parse_response(
        &self,
        _resp: tonic::Response<Self::ApiResponse>,
    ) -> Result<Self::Response, ConvertError> {
        Ok(())
    }

    fn parse_status(&self, status: &tonic::Status) -> Option<Self::Error> {
        match status.code() {
            tonic::Code::InvalidArgument => Some(DeleteBasinError::InvalidArgument(
                status.message().to_string(),
            )),
            tonic::Code::NotFound => Some(DeleteBasinError::NotFound(status.message().to_string())),
            _ => None,
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

#[derive(Debug, Clone)]
pub struct GetBasinConfigServiceRequest {
    client: AccountServiceClient<Channel>,
}

impl GetBasinConfigServiceRequest {
    pub fn new(client: AccountServiceClient<Channel>) -> Self {
        Self { client }
    }
}

impl ServiceRequest for GetBasinConfigServiceRequest {
    type Request = types::GetBasinConfigRequest;
    type ApiRequest = api::GetBasinConfigRequest;
    type Response = types::GetBasinConfigResponse;
    type ApiResponse = api::GetBasinConfigResponse;
    type Error = GetBasinConfigError;

    const IDEMPOTENCY_LEVEL: IdempotencyLevel = IdempotencyLevel::NoSideEffects;

    fn prepare_request(
        &self,
        req: Self::Request,
    ) -> Result<tonic::Request<Self::ApiRequest>, types::ConvertError> {
        let req: api::GetBasinConfigRequest = req.into();
        Ok(req.into_request())
    }

    fn parse_response(
        &self,
        resp: tonic::Response<Self::ApiResponse>,
    ) -> Result<Self::Response, types::ConvertError> {
        resp.into_inner().try_into()
    }

    fn parse_status(&self, status: &tonic::Status) -> Option<Self::Error> {
        match status.code() {
            tonic::Code::NotFound => {
                Some(GetBasinConfigError::NotFound(status.message().to_string()))
            }
            _ => None,
        }
    }

    async fn send(
        &mut self,
        req: tonic::Request<Self::ApiRequest>,
    ) -> Result<tonic::Response<Self::ApiResponse>, tonic::Status> {
        self.client.get_basin_config(req).await
    }

    fn should_retry(&self, _err: &super::ServiceError<Self::Error>) -> bool {
        false
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GetBasinConfigError {
    #[error("Not found: {0}")]
    NotFound(String),
}

#[derive(Debug, Clone)]
pub struct ReconfigureBasinServiceRequest {
    client: AccountServiceClient<Channel>,
}

impl ReconfigureBasinServiceRequest {
    pub fn new(client: AccountServiceClient<Channel>) -> Self {
        Self { client }
    }
}

impl ServiceRequest for ReconfigureBasinServiceRequest {
    type Request = types::ReconfigureBasinRequest;
    type ApiRequest = api::ReconfigureBasinRequest;
    type Response = ();
    type ApiResponse = api::ReconfigureBasinResponse;
    type Error = ReconfigureBasinError;

    const IDEMPOTENCY_LEVEL: IdempotencyLevel = IdempotencyLevel::Idempotent;

    fn prepare_request(
        &self,
        req: Self::Request,
    ) -> Result<tonic::Request<Self::ApiRequest>, types::ConvertError> {
        let req: api::ReconfigureBasinRequest = req.try_into()?;
        Ok(req.into_request())
    }

    fn parse_response(
        &self,
        _resp: tonic::Response<Self::ApiResponse>,
    ) -> Result<Self::Response, types::ConvertError> {
        Ok(())
    }

    fn parse_status(&self, status: &tonic::Status) -> Option<Self::Error> {
        match status.code() {
            tonic::Code::NotFound => Some(ReconfigureBasinError::NotFound(
                status.message().to_string(),
            )),
            tonic::Code::InvalidArgument => Some(ReconfigureBasinError::InvalidArgument(
                status.message().to_string(),
            )),
            _ => None,
        }
    }

    async fn send(
        &mut self,
        req: tonic::Request<Self::ApiRequest>,
    ) -> Result<tonic::Response<Self::ApiResponse>, tonic::Status> {
        self.client.reconfigure_basin(req).await
    }

    fn should_retry(&self, _err: &super::ServiceError<Self::Error>) -> bool {
        false
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ReconfigureBasinError {
    #[error("Not found: {0}")]
    NotFound(String),
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
}

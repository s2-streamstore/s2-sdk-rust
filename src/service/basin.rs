use tonic::{transport::Channel, IntoRequest};

use super::{IdempodentRequest, ServiceRequest};
use crate::{
    api::{self, basin_service_client::BasinServiceClient},
    types,
};

#[derive(Debug, Clone)]
pub struct ListStreamsServiceRequest {
    client: BasinServiceClient<Channel>,
    req: types::ListStreamsRequest,
}

impl ListStreamsServiceRequest {
    pub fn new(client: BasinServiceClient<Channel>, req: types::ListStreamsRequest) -> Self {
        Self { client, req }
    }
}

impl ServiceRequest for ListStreamsServiceRequest {
    type ApiRequest = api::ListStreamsRequest;
    type Response = types::ListStreamsResponse;
    type ApiResponse = api::ListStreamsResponse;
    type Error = ListStreamsError;

    fn prepare_request(&mut self) -> Result<tonic::Request<Self::ApiRequest>, types::ConvertError> {
        let req: api::ListStreamsRequest = self.req.clone().try_into()?;
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
            tonic::Code::NotFound => Some(ListStreamsError::NotFound(status.message().to_string())),
            tonic::Code::InvalidArgument => Some(ListStreamsError::InvalidArgument(
                status.message().to_string(),
            )),
            _ => None,
        })
    }

    async fn send(
        &mut self,
        req: tonic::Request<Self::ApiRequest>,
    ) -> Result<tonic::Response<Self::ApiResponse>, tonic::Status> {
        self.client.list_streams(req).await
    }
}

impl IdempodentRequest for ListStreamsServiceRequest {
    const NO_SIDE_EFFECTS: bool = true;
}

#[derive(Debug, thiserror::Error)]
pub enum ListStreamsError {
    #[error("Not found: {0}")]
    NotFound(String),
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
}

#[derive(Debug, Clone)]
pub struct GetStreamConfigServiceRequest {
    client: BasinServiceClient<Channel>,
    stream: String,
}

impl GetStreamConfigServiceRequest {
    pub fn new(client: BasinServiceClient<Channel>, stream: impl Into<String>) -> Self {
        Self {
            client,
            stream: stream.into(),
        }
    }
}

impl ServiceRequest for GetStreamConfigServiceRequest {
    type ApiRequest = api::GetStreamConfigRequest;
    type Response = types::StreamConfig;
    type ApiResponse = api::GetStreamConfigResponse;
    type Error = GetStreamConfigError;

    fn prepare_request(&mut self) -> Result<tonic::Request<Self::ApiRequest>, types::ConvertError> {
        let req = api::GetStreamConfigRequest {
            stream: self.stream.clone(),
        };
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
            tonic::Code::NotFound => {
                Some(GetStreamConfigError::NotFound(status.message().to_string()))
            }
            tonic::Code::InvalidArgument => Some(GetStreamConfigError::InvalidArgument(
                status.message().to_string(),
            )),
            _ => None,
        })
    }

    async fn send(
        &mut self,
        req: tonic::Request<Self::ApiRequest>,
    ) -> Result<tonic::Response<Self::ApiResponse>, tonic::Status> {
        self.client.get_stream_config(req).await
    }
}

impl IdempodentRequest for GetStreamConfigServiceRequest {
    const NO_SIDE_EFFECTS: bool = true;
}

#[derive(Debug, thiserror::Error)]
pub enum GetStreamConfigError {
    #[error("Not found: {0}")]
    NotFound(String),
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
}

#[derive(Debug, Clone)]
pub struct CreateStreamServiceRequest {
    client: BasinServiceClient<Channel>,
    req: types::CreateStreamRequest,
}

impl CreateStreamServiceRequest {
    pub fn new(client: BasinServiceClient<Channel>, req: types::CreateStreamRequest) -> Self {
        Self { client, req }
    }
}

impl ServiceRequest for CreateStreamServiceRequest {
    type ApiRequest = api::CreateStreamRequest;
    type Response = ();
    type ApiResponse = api::CreateStreamResponse;
    type Error = CreateStreamError;

    fn prepare_request(&mut self) -> Result<tonic::Request<Self::ApiRequest>, types::ConvertError> {
        let req: api::CreateStreamRequest = self.req.clone().try_into()?;
        Ok(req.into_request())
    }

    fn parse_response(
        &self,
        _resp: tonic::Response<Self::ApiResponse>,
    ) -> Result<Self::Response, types::ConvertError> {
        Ok(())
    }

    fn parse_status(&self, status: &tonic::Status) -> Result<Self::Response, Option<Self::Error>> {
        Err(match status.code() {
            tonic::Code::AlreadyExists => Some(CreateStreamError::AlreadyExists(
                status.message().to_string(),
            )),
            tonic::Code::NotFound => {
                Some(CreateStreamError::NotFound(status.message().to_string()))
            }
            tonic::Code::InvalidArgument => Some(CreateStreamError::InvalidArgument(
                status.message().to_string(),
            )),
            _ => None,
        })
    }

    async fn send(
        &mut self,
        req: tonic::Request<Self::ApiRequest>,
    ) -> Result<tonic::Response<Self::ApiResponse>, tonic::Status> {
        self.client.create_stream(req).await
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CreateStreamError {
    #[error("Already exists: {0}")]
    AlreadyExists(String),
    #[error("Not found: {0}")]
    NotFound(String),
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
}

#[derive(Debug, Clone)]
pub struct DeleteStreamServiceRequest {
    client: BasinServiceClient<Channel>,
    req: types::DeleteStreamRequest,
}

impl DeleteStreamServiceRequest {
    pub fn new(client: BasinServiceClient<Channel>, req: types::DeleteStreamRequest) -> Self {
        Self { client, req }
    }
}

impl ServiceRequest for DeleteStreamServiceRequest {
    type ApiRequest = api::DeleteStreamRequest;
    type Response = ();
    type ApiResponse = api::DeleteStreamResponse;
    type Error = DeleteStreamError;

    fn prepare_request(&mut self) -> Result<tonic::Request<Self::ApiRequest>, types::ConvertError> {
        let req: api::DeleteStreamRequest = self.req.clone().into();
        Ok(req.into_request())
    }

    fn parse_response(
        &self,
        _resp: tonic::Response<Self::ApiResponse>,
    ) -> Result<Self::Response, types::ConvertError> {
        Ok(())
    }

    fn parse_status(&self, status: &tonic::Status) -> Result<Self::Response, Option<Self::Error>> {
        match status.code() {
            tonic::Code::NotFound if self.req.if_exists => Ok(()),
            tonic::Code::NotFound => Err(Some(DeleteStreamError::NotFound(
                status.message().to_string(),
            ))),
            tonic::Code::InvalidArgument => Err(Some(DeleteStreamError::InvalidArgument(
                status.message().to_string(),
            ))),
            _ => Err(None),
        }
    }

    async fn send(
        &mut self,
        req: tonic::Request<Self::ApiRequest>,
    ) -> Result<tonic::Response<Self::ApiResponse>, tonic::Status> {
        self.client.delete_stream(req).await
    }
}

impl IdempodentRequest for DeleteStreamServiceRequest {
    const NO_SIDE_EFFECTS: bool = false;
}

#[derive(Debug, thiserror::Error)]
pub enum DeleteStreamError {
    #[error("Not found: {0}")]
    NotFound(String),
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
}

#[derive(Debug, Clone)]
pub struct ReconfigureStreamServiceRequest {
    client: BasinServiceClient<Channel>,
    req: types::ReconfigureStreamRequest,
}

impl ReconfigureStreamServiceRequest {
    pub fn new(client: BasinServiceClient<Channel>, req: types::ReconfigureStreamRequest) -> Self {
        Self { client, req }
    }
}

impl ServiceRequest for ReconfigureStreamServiceRequest {
    type ApiRequest = api::ReconfigureStreamRequest;
    type Response = ();
    type ApiResponse = api::ReconfigureStreamResponse;
    type Error = ReconfigureStreamError;

    fn prepare_request(&mut self) -> Result<tonic::Request<Self::ApiRequest>, types::ConvertError> {
        let req: api::ReconfigureStreamRequest = self.req.clone().try_into()?;
        Ok(req.into_request())
    }

    fn parse_response(
        &self,
        _resp: tonic::Response<Self::ApiResponse>,
    ) -> Result<Self::Response, types::ConvertError> {
        Ok(())
    }

    fn parse_status(&self, status: &tonic::Status) -> Result<Self::Response, Option<Self::Error>> {
        Err(match status.code() {
            tonic::Code::NotFound => Some(ReconfigureStreamError::NotFound(
                status.message().to_string(),
            )),
            tonic::Code::InvalidArgument => Some(ReconfigureStreamError::InvalidArgument(
                status.message().to_string(),
            )),
            _ => None,
        })
    }

    async fn send(
        &mut self,
        req: tonic::Request<Self::ApiRequest>,
    ) -> Result<tonic::Response<Self::ApiResponse>, tonic::Status> {
        self.client.reconfigure_stream(req).await
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ReconfigureStreamError {
    #[error("Not found: {0}")]
    NotFound(String),
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
}

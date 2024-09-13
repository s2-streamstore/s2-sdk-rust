use tonic::{transport::Channel, IntoRequest};

use super::ServiceRequest;
use crate::{
    api::{self, basin_service_client::BasinServiceClient},
    types,
};

#[derive(Debug, Clone)]
pub struct ListStreamsServiceRequest {
    client: BasinServiceClient<Channel>,
}

impl ListStreamsServiceRequest {
    pub fn new(client: BasinServiceClient<Channel>) -> Self {
        Self { client }
    }
}

impl ServiceRequest for ListStreamsServiceRequest {
    type Request = types::ListStreamsRequest;
    type ApiRequest = api::ListStreamsRequest;
    type Response = types::ListStreamsResponse;
    type ApiResponse = api::ListStreamsResponse;
    type Error = ListStreamsError;

    const HAS_NO_SIDE_EFFECTS: bool = true;

    fn prepare_request(
        &self,
        req: Self::Request,
    ) -> Result<tonic::Request<Self::ApiRequest>, types::ConvertError> {
        let req: api::ListStreamsRequest = req.try_into()?;
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
            tonic::Code::NotFound => Some(ListStreamsError::NotFound(status.message().to_string())),
            tonic::Code::InvalidArgument => Some(ListStreamsError::InvalidArgument(
                status.message().to_string(),
            )),
            _ => None,
        }
    }

    async fn send(
        &mut self,
        req: tonic::Request<Self::ApiRequest>,
    ) -> Result<tonic::Response<Self::ApiResponse>, tonic::Status> {
        self.client.list_streams(req).await
    }

    fn should_retry(&self, _err: &super::ServiceError<Self::Error>) -> bool {
        false
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ListStreamsError {
    #[error("Not found: {0}")]
    NotFound(String),
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
}

#[derive(Debug, Clone)]
pub struct GetBasinConfigServiceRequest {
    client: BasinServiceClient<Channel>,
}

impl GetBasinConfigServiceRequest {
    pub fn new(client: BasinServiceClient<Channel>) -> Self {
        Self { client }
    }
}

impl ServiceRequest for GetBasinConfigServiceRequest {
    type Request = ();
    type ApiRequest = api::GetBasinConfigRequest;
    type Response = types::GetBasinConfigResponse;
    type ApiResponse = api::GetBasinConfigResponse;
    type Error = GetBasinConfigError;

    const HAS_NO_SIDE_EFFECTS: bool = true;

    fn prepare_request(
        &self,
        _req: Self::Request,
    ) -> Result<tonic::Request<Self::ApiRequest>, types::ConvertError> {
        Ok(api::GetBasinConfigRequest {}.into_request())
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
pub struct GetStreamConfigServiceRequest {
    client: BasinServiceClient<Channel>,
}

impl GetStreamConfigServiceRequest {
    pub fn new(client: BasinServiceClient<Channel>) -> Self {
        Self { client }
    }
}

impl ServiceRequest for GetStreamConfigServiceRequest {
    type Request = types::GetStreamConfigRequest;
    type ApiRequest = api::GetStreamConfigRequest;
    type Response = types::GetStreamConfigResponse;
    type ApiResponse = api::GetStreamConfigResponse;
    type Error = GetStreamConfigError;

    const HAS_NO_SIDE_EFFECTS: bool = true;

    fn prepare_request(
        &self,
        req: Self::Request,
    ) -> Result<tonic::Request<Self::ApiRequest>, types::ConvertError> {
        let req: api::GetStreamConfigRequest = req.into();
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
                Some(GetStreamConfigError::NotFound(status.message().to_string()))
            }
            _ => None,
        }
    }

    async fn send(
        &mut self,
        req: tonic::Request<Self::ApiRequest>,
    ) -> Result<tonic::Response<Self::ApiResponse>, tonic::Status> {
        self.client.get_stream_config(req).await
    }

    fn should_retry(&self, _err: &super::ServiceError<Self::Error>) -> bool {
        false
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GetStreamConfigError {
    #[error("Not found: {0}")]
    NotFound(String),
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
}

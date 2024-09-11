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

    const HAS_SIDE_EFFECTS: bool = false;

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
            tonic::Code::NotFound => Some(ListStreamsError::BasinDoesNotExist),
            tonic::Code::InvalidArgument => Some(ListStreamsError::StartAfterLessThanPrefix),
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
    #[error("Basin does not exist")]
    BasinDoesNotExist,
    #[error("`start_after` should be greater than or equal to the `prefix` specified")]
    StartAfterLessThanPrefix,
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

    const HAS_SIDE_EFFECTS: bool = false;

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
            tonic::Code::NotFound => Some(GetBasinConfigError::BasinDoesNotExist),
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
    #[error("Basin does not exist")]
    BasinDoesNotExist,
}

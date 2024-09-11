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
    #[error("`start_after` should be greater than or equal to the `prefix` specified")]
    StartAfterLessThanPrefix,
}

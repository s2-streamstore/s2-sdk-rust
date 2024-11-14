use tonic::{transport::Channel, IntoRequest};

use super::{ClientError, IdempotentRequest, ServiceRequest};
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

    fn prepare_request(&mut self) -> Result<tonic::Request<Self::ApiRequest>, ClientError> {
        let req: api::ListStreamsRequest = self.req.clone().try_into()?;
        Ok(req.into_request())
    }

    fn parse_response(
        &self,
        resp: tonic::Response<Self::ApiResponse>,
    ) -> Result<Self::Response, ClientError> {
        Ok(resp.into_inner().into())
    }

    async fn send(
        &mut self,
        req: tonic::Request<Self::ApiRequest>,
    ) -> Result<tonic::Response<Self::ApiResponse>, tonic::Status> {
        self.client.list_streams(req).await
    }
}

impl IdempotentRequest for ListStreamsServiceRequest {
    const NO_SIDE_EFFECTS: bool = true;
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

    fn prepare_request(&mut self) -> Result<tonic::Request<Self::ApiRequest>, ClientError> {
        let req = api::GetStreamConfigRequest {
            stream: self.stream.clone(),
        };
        Ok(req.into_request())
    }

    fn parse_response(
        &self,
        resp: tonic::Response<Self::ApiResponse>,
    ) -> Result<Self::Response, ClientError> {
        resp.into_inner().try_into()
    }

    async fn send(
        &mut self,
        req: tonic::Request<Self::ApiRequest>,
    ) -> Result<tonic::Response<Self::ApiResponse>, tonic::Status> {
        self.client.get_stream_config(req).await
    }
}

impl IdempotentRequest for GetStreamConfigServiceRequest {
    const NO_SIDE_EFFECTS: bool = true;
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

    fn prepare_request(&mut self) -> Result<tonic::Request<Self::ApiRequest>, ClientError> {
        let req: api::CreateStreamRequest = self.req.clone().try_into()?;
        Ok(req.into_request())
    }

    fn parse_response(
        &self,
        _resp: tonic::Response<Self::ApiResponse>,
    ) -> Result<Self::Response, ClientError> {
        Ok(())
    }

    async fn send(
        &mut self,
        req: tonic::Request<Self::ApiRequest>,
    ) -> Result<tonic::Response<Self::ApiResponse>, tonic::Status> {
        self.client.create_stream(req).await
    }
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

    fn prepare_request(&mut self) -> Result<tonic::Request<Self::ApiRequest>, ClientError> {
        let req: api::DeleteStreamRequest = self.req.clone().into();
        Ok(req.into_request())
    }

    fn parse_response(
        &self,
        _resp: tonic::Response<Self::ApiResponse>,
    ) -> Result<Self::Response, ClientError> {
        Ok(())
    }

    async fn send(
        &mut self,
        req: tonic::Request<Self::ApiRequest>,
    ) -> Result<tonic::Response<Self::ApiResponse>, tonic::Status> {
        self.client.delete_stream(req).await
    }
}

impl IdempotentRequest for DeleteStreamServiceRequest {
    const NO_SIDE_EFFECTS: bool = false;
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

    fn prepare_request(&mut self) -> Result<tonic::Request<Self::ApiRequest>, ClientError> {
        let req: api::ReconfigureStreamRequest = self.req.clone().try_into()?;
        Ok(req.into_request())
    }

    fn parse_response(
        &self,
        _resp: tonic::Response<Self::ApiResponse>,
    ) -> Result<Self::Response, ClientError> {
        Ok(())
    }

    async fn send(
        &mut self,
        req: tonic::Request<Self::ApiRequest>,
    ) -> Result<tonic::Response<Self::ApiResponse>, tonic::Status> {
        self.client.reconfigure_stream(req).await
    }
}

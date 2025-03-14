use prost_types::method_options::IdempotencyLevel;
use tonic::{IntoRequest, transport::Channel};

use super::{ServiceRequest, add_s2_request_token_header, gen_s2_request_token};
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
    const IDEMPOTENCY_LEVEL: IdempotencyLevel = IdempotencyLevel::NoSideEffects;

    fn prepare_request(&mut self) -> Result<tonic::Request<Self::ApiRequest>, types::ConvertError> {
        let req: api::ListStreamsRequest = self.req.clone().try_into()?;
        Ok(req.into_request())
    }

    async fn send(
        &mut self,
        req: tonic::Request<Self::ApiRequest>,
    ) -> Result<tonic::Response<Self::ApiResponse>, tonic::Status> {
        self.client.list_streams(req).await
    }

    fn parse_response(
        &self,
        resp: tonic::Response<Self::ApiResponse>,
    ) -> Result<Self::Response, types::ConvertError> {
        Ok(resp.into_inner().into())
    }
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
    const IDEMPOTENCY_LEVEL: IdempotencyLevel = IdempotencyLevel::NoSideEffects;

    fn prepare_request(&mut self) -> Result<tonic::Request<Self::ApiRequest>, types::ConvertError> {
        let req = api::GetStreamConfigRequest {
            stream: self.stream.clone(),
        };
        Ok(req.into_request())
    }

    async fn send(
        &mut self,
        req: tonic::Request<Self::ApiRequest>,
    ) -> Result<tonic::Response<Self::ApiResponse>, tonic::Status> {
        self.client.get_stream_config(req).await
    }

    fn parse_response(
        &self,
        resp: tonic::Response<Self::ApiResponse>,
    ) -> Result<Self::Response, types::ConvertError> {
        resp.into_inner().try_into()
    }
}

#[derive(Debug, Clone)]
pub struct CreateStreamServiceRequest {
    client: BasinServiceClient<Channel>,
    req: types::CreateStreamRequest,
    s2_request_token: String,
}

impl CreateStreamServiceRequest {
    pub fn new(client: BasinServiceClient<Channel>, req: types::CreateStreamRequest) -> Self {
        Self {
            client,
            req,
            s2_request_token: gen_s2_request_token(),
        }
    }
}

impl ServiceRequest for CreateStreamServiceRequest {
    type ApiRequest = api::CreateStreamRequest;
    type Response = types::StreamInfo;
    type ApiResponse = api::CreateStreamResponse;
    const IDEMPOTENCY_LEVEL: IdempotencyLevel = IdempotencyLevel::Idempotent;

    fn prepare_request(&mut self) -> Result<tonic::Request<Self::ApiRequest>, types::ConvertError> {
        let req: api::CreateStreamRequest = self.req.clone().into();
        let mut tonic_req = req.into_request();
        add_s2_request_token_header(tonic_req.metadata_mut(), &self.s2_request_token)?;
        Ok(tonic_req)
    }

    async fn send(
        &mut self,
        req: tonic::Request<Self::ApiRequest>,
    ) -> Result<tonic::Response<Self::ApiResponse>, tonic::Status> {
        self.client.create_stream(req).await
    }

    fn parse_response(
        &self,
        resp: tonic::Response<Self::ApiResponse>,
    ) -> Result<Self::Response, types::ConvertError> {
        resp.into_inner().try_into()
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
    const IDEMPOTENCY_LEVEL: IdempotencyLevel = IdempotencyLevel::Idempotent;

    fn prepare_request(&mut self) -> Result<tonic::Request<Self::ApiRequest>, types::ConvertError> {
        let req: api::DeleteStreamRequest = self.req.clone().into();
        Ok(req.into_request())
    }

    async fn send(
        &mut self,
        req: tonic::Request<Self::ApiRequest>,
    ) -> Result<tonic::Response<Self::ApiResponse>, tonic::Status> {
        match self.client.delete_stream(req).await {
            Err(status) if self.req.if_exists && status.code() == tonic::Code::NotFound => {
                Ok(tonic::Response::new(api::DeleteStreamResponse {}))
            }
            other => other,
        }
    }

    fn parse_response(
        &self,
        _resp: tonic::Response<Self::ApiResponse>,
    ) -> Result<Self::Response, types::ConvertError> {
        Ok(())
    }
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
    type Response = types::StreamConfig;
    type ApiResponse = api::ReconfigureStreamResponse;
    const IDEMPOTENCY_LEVEL: IdempotencyLevel = IdempotencyLevel::IdempotencyUnknown;

    fn prepare_request(&mut self) -> Result<tonic::Request<Self::ApiRequest>, types::ConvertError> {
        let req: api::ReconfigureStreamRequest = self.req.clone().into();
        Ok(req.into_request())
    }

    async fn send(
        &mut self,
        req: tonic::Request<Self::ApiRequest>,
    ) -> Result<tonic::Response<Self::ApiResponse>, tonic::Status> {
        self.client.reconfigure_stream(req).await
    }

    fn parse_response(
        &self,
        resp: tonic::Response<Self::ApiResponse>,
    ) -> Result<Self::Response, types::ConvertError> {
        resp.into_inner().try_into()
    }
}

use tonic::{transport::Channel, IntoRequest};

use super::{
    ClientError, IdempotentRequest, ServiceRequest, ServiceStreamingRequest,
    ServiceStreamingResponse, StreamingRequest, StreamingResponse,
};
use crate::{
    api::{self, stream_service_client::StreamServiceClient},
    types,
};

#[derive(Debug, Clone)]
pub struct CheckTailServiceRequest {
    client: StreamServiceClient<Channel>,
    stream: String,
}

impl CheckTailServiceRequest {
    pub fn new(client: StreamServiceClient<Channel>, stream: impl Into<String>) -> Self {
        Self {
            client,
            stream: stream.into(),
        }
    }
}

impl ServiceRequest for CheckTailServiceRequest {
    type ApiRequest = api::CheckTailRequest;
    type Response = u64;
    type ApiResponse = api::CheckTailResponse;

    fn prepare_request(&mut self) -> Result<tonic::Request<Self::ApiRequest>, types::ConvertError> {
        let req = api::CheckTailRequest {
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

    async fn send(
        &mut self,
        req: tonic::Request<Self::ApiRequest>,
    ) -> Result<tonic::Response<Self::ApiResponse>, tonic::Status> {
        self.client.check_tail(req).await
    }
}

impl IdempotentRequest for CheckTailServiceRequest {
    const NO_SIDE_EFFECTS: bool = true;
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
    type Response = types::ReadOutput;
    type ApiResponse = api::ReadResponse;

    fn prepare_request(&mut self) -> Result<tonic::Request<Self::ApiRequest>, types::ConvertError> {
        let req = self.req.clone().try_into_api_type(self.stream.clone())?;
        Ok(req.into_request())
    }

    fn parse_response(
        &self,
        resp: tonic::Response<Self::ApiResponse>,
    ) -> Result<Self::Response, types::ConvertError> {
        resp.into_inner().try_into().map_err(Into::into)
    }

    async fn send(
        &mut self,
        req: tonic::Request<Self::ApiRequest>,
    ) -> Result<tonic::Response<Self::ApiResponse>, tonic::Status> {
        self.client.read(req).await
    }
}

impl IdempotentRequest for ReadServiceRequest {
    const NO_SIDE_EFFECTS: bool = true;
}

#[derive(Debug, Clone)]
pub struct ReadSessionServiceRequest {
    client: StreamServiceClient<Channel>,
    stream: String,
    pub(crate) req: types::ReadSessionRequest,
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
    type Response = ServiceStreamingResponse<ReadSessionStreamingResponse>;
    type ApiResponse = tonic::Streaming<api::ReadSessionResponse>;

    fn prepare_request(&mut self) -> Result<tonic::Request<Self::ApiRequest>, types::ConvertError> {
        let req = self.req.clone().into_api_type(self.stream.clone());
        Ok(req.into_request())
    }

    fn parse_response(
        &self,
        resp: tonic::Response<Self::ApiResponse>,
    ) -> Result<Self::Response, types::ConvertError> {
        Ok(ServiceStreamingResponse::new(
            ReadSessionStreamingResponse,
            resp.into_inner(),
        ))
    }

    async fn send(
        &mut self,
        req: tonic::Request<Self::ApiRequest>,
    ) -> Result<tonic::Response<Self::ApiResponse>, tonic::Status> {
        self.client.read_session(req).await
    }
}

impl IdempotentRequest for ReadSessionServiceRequest {
    const NO_SIDE_EFFECTS: bool = true;
}

pub struct ReadSessionStreamingResponse;

impl StreamingResponse for ReadSessionStreamingResponse {
    type ResponseItem = types::ReadOutput;
    type ApiResponseItem = api::ReadSessionResponse;

    fn parse_response_item(
        &self,
        resp: Self::ApiResponseItem,
    ) -> Result<Self::ResponseItem, ClientError> {
        resp.try_into().map_err(Into::into)
    }
}

#[derive(Debug, Clone)]
pub struct AppendServiceRequest {
    client: StreamServiceClient<Channel>,
    stream: String,
    req: types::AppendInput,
}

impl AppendServiceRequest {
    pub fn new(
        client: StreamServiceClient<Channel>,
        stream: impl Into<String>,
        req: types::AppendInput,
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
    type Response = types::AppendOutput;
    type ApiResponse = api::AppendResponse;

    fn prepare_request(&mut self) -> Result<tonic::Request<Self::ApiRequest>, types::ConvertError> {
        Ok(api::AppendRequest {
            input: Some(self.req.clone().into_api_type(self.stream.clone())),
        }
        .into_request())
    }

    fn parse_response(
        &self,
        resp: tonic::Response<Self::ApiResponse>,
    ) -> Result<Self::Response, types::ConvertError> {
        resp.into_inner().try_into().map_err(Into::into)
    }

    async fn send(
        &mut self,
        req: tonic::Request<Self::ApiRequest>,
    ) -> Result<tonic::Response<Self::ApiResponse>, tonic::Status> {
        self.client.append(req).await
    }
}

pub struct AppendSessionServiceRequest<S>
where
    S: Send + futures::Stream<Item = types::AppendInput> + Unpin,
{
    client: StreamServiceClient<Channel>,
    stream: String,
    req: Option<S>,
}

impl<S> AppendSessionServiceRequest<S>
where
    S: Send + futures::Stream<Item = types::AppendInput> + Unpin,
{
    pub fn new(client: StreamServiceClient<Channel>, stream: impl Into<String>, req: S) -> Self {
        Self {
            client,
            stream: stream.into(),
            req: Some(req),
        }
    }
}

impl<S> ServiceRequest for AppendSessionServiceRequest<S>
where
    S: 'static + Send + futures::Stream<Item = types::AppendInput> + Unpin,
{
    type ApiRequest = ServiceStreamingRequest<AppendSessionStreamingRequest, S>;
    type Response = ServiceStreamingResponse<AppendSessionStreamingResponse>;
    type ApiResponse = tonic::Streaming<api::AppendSessionResponse>;

    fn prepare_request(&mut self) -> Result<tonic::Request<Self::ApiRequest>, types::ConvertError> {
        let req = ServiceStreamingRequest::new(
            AppendSessionStreamingRequest::new(&self.stream),
            self.req.take().ok_or("missing streaming append request")?,
        );
        Ok(req.into_request())
    }

    fn parse_response(
        &self,
        resp: tonic::Response<Self::ApiResponse>,
    ) -> Result<Self::Response, types::ConvertError> {
        Ok(ServiceStreamingResponse::new(
            AppendSessionStreamingResponse,
            resp.into_inner(),
        ))
    }

    async fn send(
        &mut self,
        req: tonic::Request<Self::ApiRequest>,
    ) -> Result<tonic::Response<Self::ApiResponse>, tonic::Status> {
        self.client.append_session(req).await
    }
}

pub struct AppendSessionStreamingRequest {
    stream: String,
}

impl AppendSessionStreamingRequest {
    fn new(stream: impl Into<String>) -> Self {
        Self {
            stream: stream.into(),
        }
    }
}

impl StreamingRequest for AppendSessionStreamingRequest {
    type RequestItem = types::AppendInput;
    type ApiRequestItem = api::AppendSessionRequest;

    fn prepare_request_item(&self, req: Self::RequestItem) -> Self::ApiRequestItem {
        api::AppendSessionRequest {
            input: Some(req.into_api_type(&self.stream)),
        }
    }
}

pub struct AppendSessionStreamingResponse;

impl StreamingResponse for AppendSessionStreamingResponse {
    type ResponseItem = types::AppendOutput;
    type ApiResponseItem = api::AppendSessionResponse;

    fn parse_response_item(
        &self,
        resp: Self::ApiResponseItem,
    ) -> Result<Self::ResponseItem, ClientError> {
        resp.try_into().map_err(Into::into)
    }
}

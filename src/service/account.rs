use tonic::{transport::Channel, IntoRequest};

use super::{ClientError, IdempotentRequest, ServiceRequest};
use crate::{
    api::{self, account_service_client::AccountServiceClient},
    types,
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
    type Response = types::BasinMetadata;
    type ApiResponse = api::CreateBasinResponse;

    fn prepare_request(&mut self) -> Result<tonic::Request<Self::ApiRequest>, ClientError> {
        let req: api::CreateBasinRequest = self.req.clone().try_into()?;
        Ok(req.into_request())
    }

    fn parse_response(
        &self,
        resp: tonic::Response<Self::ApiResponse>,
    ) -> Result<Self::Response, ClientError> {
        resp.into_inner().try_into().map_err(Into::into)
    }

    async fn send(
        &mut self,
        req: tonic::Request<Self::ApiRequest>,
    ) -> Result<tonic::Response<Self::ApiResponse>, tonic::Status> {
        self.client.create_basin(req).await
    }
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

    fn prepare_request(&mut self) -> Result<tonic::Request<Self::ApiRequest>, ClientError> {
        let req: api::ListBasinsRequest = self.req.clone().try_into()?;
        Ok(req.into_request())
    }

    fn parse_response(
        &self,
        resp: tonic::Response<Self::ApiResponse>,
    ) -> Result<Self::Response, ClientError> {
        resp.into_inner().try_into().map_err(Into::into)
    }

    async fn send(
        &mut self,
        req: tonic::Request<Self::ApiRequest>,
    ) -> Result<tonic::Response<Self::ApiResponse>, tonic::Status> {
        self.client.list_basins(req).await
    }
}

impl IdempotentRequest for ListBasinsServiceRequest {
    const NO_SIDE_EFFECTS: bool = true;
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

    fn prepare_request(&mut self) -> Result<tonic::Request<Self::ApiRequest>, ClientError> {
        let req: api::DeleteBasinRequest = self.req.clone().into();
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
        self.client.delete_basin(req).await
    }
}

impl IdempotentRequest for DeleteBasinServiceRequest {
    const NO_SIDE_EFFECTS: bool = false;
}

#[derive(Debug, Clone)]
pub struct GetBasinConfigServiceRequest {
    client: AccountServiceClient<Channel>,
    basin: String,
}

impl GetBasinConfigServiceRequest {
    pub fn new(client: AccountServiceClient<Channel>, basin: impl Into<String>) -> Self {
        Self {
            client,
            basin: basin.into(),
        }
    }
}

impl ServiceRequest for GetBasinConfigServiceRequest {
    type ApiRequest = api::GetBasinConfigRequest;
    type Response = types::BasinConfig;
    type ApiResponse = api::GetBasinConfigResponse;

    fn prepare_request(&mut self) -> Result<tonic::Request<Self::ApiRequest>, ClientError> {
        let req = api::GetBasinConfigRequest {
            basin: self.basin.clone(),
        };
        Ok(req.into_request())
    }

    fn parse_response(
        &self,
        resp: tonic::Response<Self::ApiResponse>,
    ) -> Result<Self::Response, ClientError> {
        resp.into_inner().try_into().map_err(Into::into)
    }

    async fn send(
        &mut self,
        req: tonic::Request<Self::ApiRequest>,
    ) -> Result<tonic::Response<Self::ApiResponse>, tonic::Status> {
        self.client.get_basin_config(req).await
    }
}

impl IdempotentRequest for GetBasinConfigServiceRequest {
    const NO_SIDE_EFFECTS: bool = true;
}

#[derive(Debug, Clone)]
pub struct ReconfigureBasinServiceRequest {
    client: AccountServiceClient<Channel>,
    req: types::ReconfigureBasinRequest,
}

impl ReconfigureBasinServiceRequest {
    pub fn new(client: AccountServiceClient<Channel>, req: types::ReconfigureBasinRequest) -> Self {
        Self { client, req }
    }
}

impl ServiceRequest for ReconfigureBasinServiceRequest {
    type ApiRequest = api::ReconfigureBasinRequest;
    type Response = ();
    type ApiResponse = api::ReconfigureBasinResponse;

    fn prepare_request(&mut self) -> Result<tonic::Request<Self::ApiRequest>, ClientError> {
        let req: api::ReconfigureBasinRequest = self.req.clone().try_into()?;
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
        self.client.reconfigure_basin(req).await
    }
}

impl IdempotentRequest for ReconfigureBasinServiceRequest {
    const NO_SIDE_EFFECTS: bool = false;
}

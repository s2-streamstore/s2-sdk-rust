use prost_types::method_options::IdempotencyLevel;
use tonic::{IntoRequest, transport::Channel};

use super::{ServiceRequest, add_s2_request_token_header, gen_s2_request_token};
use crate::{
    api::{self, account_service_client::AccountServiceClient},
    types,
};

#[derive(Debug, Clone)]
pub struct CreateBasinServiceRequest {
    client: AccountServiceClient<Channel>,
    req: types::CreateBasinRequest,
    s2_request_token: String,
}

impl CreateBasinServiceRequest {
    pub fn new(client: AccountServiceClient<Channel>, req: types::CreateBasinRequest) -> Self {
        Self {
            client,
            req,
            s2_request_token: gen_s2_request_token(),
        }
    }
}

impl ServiceRequest for CreateBasinServiceRequest {
    type ApiRequest = api::CreateBasinRequest;
    type Response = types::BasinInfo;
    type ApiResponse = api::CreateBasinResponse;
    const IDEMPOTENCY_LEVEL: IdempotencyLevel = IdempotencyLevel::Idempotent;

    fn prepare_request(&mut self) -> Result<tonic::Request<Self::ApiRequest>, types::ConvertError> {
        let req: api::CreateBasinRequest = self.req.clone().into();
        let mut tonic_req = req.into_request();
        add_s2_request_token_header(tonic_req.metadata_mut(), &self.s2_request_token)?;
        Ok(tonic_req)
    }

    async fn send(
        &mut self,
        req: tonic::Request<Self::ApiRequest>,
    ) -> Result<tonic::Response<Self::ApiResponse>, tonic::Status> {
        self.client.create_basin(req).await
    }

    fn parse_response(
        &self,
        resp: tonic::Response<Self::ApiResponse>,
    ) -> Result<Self::Response, types::ConvertError> {
        resp.into_inner().try_into()
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
    const IDEMPOTENCY_LEVEL: IdempotencyLevel = IdempotencyLevel::NoSideEffects;

    fn prepare_request(&mut self) -> Result<tonic::Request<Self::ApiRequest>, types::ConvertError> {
        let req: api::ListBasinsRequest = self.req.clone().try_into()?;
        Ok(req.into_request())
    }

    async fn send(
        &mut self,
        req: tonic::Request<Self::ApiRequest>,
    ) -> Result<tonic::Response<Self::ApiResponse>, tonic::Status> {
        self.client.list_basins(req).await
    }

    fn parse_response(
        &self,
        resp: tonic::Response<Self::ApiResponse>,
    ) -> Result<Self::Response, types::ConvertError> {
        resp.into_inner().try_into()
    }
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
    const IDEMPOTENCY_LEVEL: IdempotencyLevel = IdempotencyLevel::Idempotent;

    fn prepare_request(&mut self) -> Result<tonic::Request<Self::ApiRequest>, types::ConvertError> {
        let req: api::DeleteBasinRequest = self.req.clone().into();
        Ok(req.into_request())
    }

    async fn send(
        &mut self,
        req: tonic::Request<Self::ApiRequest>,
    ) -> Result<tonic::Response<Self::ApiResponse>, tonic::Status> {
        match self.client.delete_basin(req).await {
            Err(status) if self.req.if_exists && status.code() == tonic::Code::NotFound => {
                Ok(tonic::Response::new(api::DeleteBasinResponse {}))
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
pub struct GetBasinConfigServiceRequest {
    client: AccountServiceClient<Channel>,
    basin: types::BasinName,
}

impl GetBasinConfigServiceRequest {
    pub fn new(client: AccountServiceClient<Channel>, basin: types::BasinName) -> Self {
        Self { client, basin }
    }
}

impl ServiceRequest for GetBasinConfigServiceRequest {
    type ApiRequest = api::GetBasinConfigRequest;
    type Response = types::BasinConfig;
    type ApiResponse = api::GetBasinConfigResponse;
    const IDEMPOTENCY_LEVEL: IdempotencyLevel = IdempotencyLevel::NoSideEffects;

    fn prepare_request(&mut self) -> Result<tonic::Request<Self::ApiRequest>, types::ConvertError> {
        let req = api::GetBasinConfigRequest {
            basin: self.basin.to_string(),
        };
        Ok(req.into_request())
    }

    async fn send(
        &mut self,
        req: tonic::Request<Self::ApiRequest>,
    ) -> Result<tonic::Response<Self::ApiResponse>, tonic::Status> {
        self.client.get_basin_config(req).await
    }

    fn parse_response(
        &self,
        resp: tonic::Response<Self::ApiResponse>,
    ) -> Result<Self::Response, types::ConvertError> {
        resp.into_inner().try_into()
    }
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
    type Response = types::BasinConfig;
    type ApiResponse = api::ReconfigureBasinResponse;
    const IDEMPOTENCY_LEVEL: IdempotencyLevel = IdempotencyLevel::Idempotent;

    fn prepare_request(&mut self) -> Result<tonic::Request<Self::ApiRequest>, types::ConvertError> {
        let req: api::ReconfigureBasinRequest = self.req.clone().into();
        Ok(req.into_request())
    }

    async fn send(
        &mut self,
        req: tonic::Request<Self::ApiRequest>,
    ) -> Result<tonic::Response<Self::ApiResponse>, tonic::Status> {
        self.client.reconfigure_basin(req).await
    }

    fn parse_response(
        &self,
        resp: tonic::Response<Self::ApiResponse>,
    ) -> Result<Self::Response, types::ConvertError> {
        resp.into_inner().try_into()
    }
}

#[derive(Debug, Clone)]
pub struct IssueAccessTokenServiceRequest {
    client: AccountServiceClient<Channel>,
    info: types::AccessTokenInfo,
}

impl IssueAccessTokenServiceRequest {
    pub fn new(client: AccountServiceClient<Channel>, info: types::AccessTokenInfo) -> Self {
        Self { client, info }
    }
}

impl ServiceRequest for IssueAccessTokenServiceRequest {
    type ApiRequest = api::IssueAccessTokenRequest;
    type Response = String;
    type ApiResponse = api::IssueAccessTokenResponse;
    const IDEMPOTENCY_LEVEL: IdempotencyLevel = IdempotencyLevel::IdempotencyUnknown;

    fn prepare_request(&mut self) -> Result<tonic::Request<Self::ApiRequest>, types::ConvertError> {
        let req: api::IssueAccessTokenRequest = self.info.clone().into();
        Ok(req.into_request())
    }

    async fn send(
        &mut self,
        req: tonic::Request<Self::ApiRequest>,
    ) -> Result<tonic::Response<Self::ApiResponse>, tonic::Status> {
        self.client.issue_access_token(req).await
    }

    fn parse_response(
        &self,
        resp: tonic::Response<Self::ApiResponse>,
    ) -> Result<Self::Response, types::ConvertError> {
        Ok(resp.into_inner().into())
    }
}

#[derive(Debug, Clone)]
pub struct RevokeAccessTokenServiceRequest {
    client: AccountServiceClient<Channel>,
    id: types::AccessTokenId,
}
impl RevokeAccessTokenServiceRequest {
    pub fn new(client: AccountServiceClient<Channel>, id: types::AccessTokenId) -> Self {
        Self { client, id }
    }
}

impl ServiceRequest for RevokeAccessTokenServiceRequest {
    type ApiRequest = api::RevokeAccessTokenRequest;
    type Response = types::AccessTokenInfo;
    type ApiResponse = api::RevokeAccessTokenResponse;
    const IDEMPOTENCY_LEVEL: IdempotencyLevel = IdempotencyLevel::Idempotent;

    fn prepare_request(&mut self) -> Result<tonic::Request<Self::ApiRequest>, types::ConvertError> {
        let req: api::RevokeAccessTokenRequest = self.id.clone().into();
        Ok(req.into_request())
    }

    async fn send(
        &mut self,
        req: tonic::Request<Self::ApiRequest>,
    ) -> Result<tonic::Response<Self::ApiResponse>, tonic::Status> {
        self.client.revoke_access_token(req).await
    }

    fn parse_response(
        &self,
        resp: tonic::Response<Self::ApiResponse>,
    ) -> Result<Self::Response, types::ConvertError> {
        resp.into_inner().try_into()
    }
}

#[derive(Debug, Clone)]
pub struct ListAccessTokensServiceRequest {
    client: AccountServiceClient<Channel>,
    req: types::ListAccessTokensRequest,
}
impl ListAccessTokensServiceRequest {
    pub fn new(client: AccountServiceClient<Channel>, req: types::ListAccessTokensRequest) -> Self {
        Self { client, req }
    }
}

impl ServiceRequest for ListAccessTokensServiceRequest {
    type ApiRequest = api::ListAccessTokensRequest;
    type Response = types::ListAccessTokensResponse;
    type ApiResponse = api::ListAccessTokensResponse;
    const IDEMPOTENCY_LEVEL: IdempotencyLevel = IdempotencyLevel::NoSideEffects;

    fn prepare_request(&mut self) -> Result<tonic::Request<Self::ApiRequest>, types::ConvertError> {
        let req: api::ListAccessTokensRequest = self.req.clone().try_into()?;
        Ok(req.into_request())
    }

    async fn send(
        &mut self,
        req: tonic::Request<Self::ApiRequest>,
    ) -> Result<tonic::Response<Self::ApiResponse>, tonic::Status> {
        self.client.list_access_tokens(req).await
    }

    fn parse_response(
        &self,
        resp: tonic::Response<Self::ApiResponse>,
    ) -> Result<Self::Response, types::ConvertError> {
        resp.into_inner().try_into()
    }
}

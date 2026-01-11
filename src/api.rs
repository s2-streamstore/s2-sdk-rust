use crate::retry::RetryBackoffBuilder;
use crate::types::{
    AccessTokenId, BasinAuthority, BasinName, Compression, RetryConfig, S2Config, S2Endpoints,
    StreamName,
};
use async_compression::Level;
use async_compression::tokio::write::{GzipEncoder, ZstdEncoder};
use async_stream::try_stream;
use bytes::BytesMut;
use futures::{Stream, StreamExt};
use http::header::InvalidHeaderValue;
use http::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE};
use http::{HeaderMap, HeaderValue, StatusCode};
use prost::{self, Message};
use reqwest::{Request, Response};
use s2_api::v1::access::{
    AccessTokenInfo, IssueAccessTokenResponse, ListAccessTokensRequest, ListAccessTokensResponse,
};
use s2_api::v1::basin::{BasinInfo, CreateBasinRequest, ListBasinsRequest, ListBasinsResponse};
use s2_api::v1::config::{BasinConfig, BasinReconfiguration, StreamConfig, StreamReconfiguration};
use s2_api::v1::metrics::{
    AccountMetricSetRequest, BasinMetricSetRequest, MetricSetResponse, StreamMetricSetRequest,
};
use s2_api::v1::stream::s2s::{self, FrameDecoder, SessionMessage, TerminalMessage};
use s2_api::v1::stream::{
    AppendConditionFailed, CreateStreamRequest, ListStreamsRequest, ListStreamsResponse, ReadEnd,
    ReadStart, StreamInfo, TailResponse,
    proto::{AppendAck, AppendInput, ReadBatch},
};
use secrecy::ExposeSecret;
use std::collections::VecDeque;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio_util::codec::Decoder;
use tracing::{debug, warn};
use url::Url;

const CONTENT_TYPE_S2S: &str = "s2s/proto";
const CONTENT_TYPE_PROTO: &str = "application/protobuf";
const ACCEPT_PROTO: &str = "application/protobuf";
const S2_REQUEST_TOKEN: &str = "s2-request-token";
const S2_BASIN: &str = "s2-basin";
const RETRY_AFTER_MS_HEADER: &str = "retry-after-ms";
const SESSION_REQUEST_TIMEOUT: Duration = Duration::from_secs(u64::MAX);

#[derive(Debug, Clone)]
pub struct AccountClient {
    pub client: BaseClient,
    pub config: Arc<S2Config>,
    pub base_url: Url,
}

impl AccountClient {
    pub fn init(config: S2Config) -> Result<Self, ApiError> {
        let base_url = base_url(&config.endpoints, ClientKind::Account);
        let client = BaseClient::init(&config)?;
        Ok(Self {
            client,
            config: Arc::new(config),
            base_url,
        })
    }

    pub fn basin_client(&self, name: BasinName) -> BasinClient {
        BasinClient::init(name, self.config.clone(), self.client.clone())
    }

    pub async fn list_access_tokens(
        &self,
        request: ListAccessTokensRequest,
    ) -> Result<ListAccessTokensResponse, ApiError> {
        let url = self.base_url.join("v1/access-tokens")?;
        let request = self.get(url).query(&request).build()?;
        let response = self.request(request).send().await?;
        Ok(response.json::<ListAccessTokensResponse>().await?)
    }

    pub async fn issue_access_token(
        &self,
        info: AccessTokenInfo,
    ) -> Result<IssueAccessTokenResponse, ApiError> {
        let url = self.base_url.join("v1/access-tokens")?;
        let request = self.post(url).json(&info).build()?;
        let response = self.request(request).send().await?;
        Ok(response.json::<IssueAccessTokenResponse>().await?)
    }

    pub async fn revoke_access_token(&self, id: AccessTokenId) -> Result<(), ApiError> {
        let url = self
            .base_url
            .join(&format!("v1/access-tokens/{}", urlencoding::encode(&id)))?;
        let request = self.delete(url).build()?;
        let _response = self.request(request).send().await?;
        Ok(())
    }

    pub async fn list_basins(
        &self,
        request: ListBasinsRequest,
    ) -> Result<ListBasinsResponse, ApiError> {
        let url = self.base_url.join("v1/basins")?;
        let request = self.get(url).query(&request).build()?;
        let response = self.request(request).send().await?;
        Ok(response.json::<ListBasinsResponse>().await?)
    }

    pub async fn create_basin(
        &self,
        request: CreateBasinRequest,
        idempotency_token: String,
    ) -> Result<BasinInfo, ApiError> {
        let url = self.base_url.join("v1/basins")?;
        let request = self
            .post(url)
            .header(S2_REQUEST_TOKEN, idempotency_token)
            .json(&request)
            .build()?;
        let response = self.request(request).send().await?;
        Ok(response.json::<BasinInfo>().await?)
    }

    pub async fn get_basin_config(&self, name: BasinName) -> Result<BasinConfig, ApiError> {
        let url = self.base_url.join(&format!("v1/basins/{name}"))?;
        let request = self.get(url).build()?;
        let response = self.request(request).send().await?;
        Ok(response.json::<BasinConfig>().await?)
    }

    pub async fn reconfigure_basin(
        &self,
        name: BasinName,
        config: BasinReconfiguration,
    ) -> Result<BasinConfig, ApiError> {
        let url = self.base_url.join(&format!("v1/basins/{name}"))?;
        let request = self.patch(url).json(&config).build()?;
        let response = self.request(request).send().await?;
        Ok(response.json::<BasinConfig>().await?)
    }

    pub async fn delete_basin(
        &self,
        name: BasinName,
        ignore_not_found: bool,
    ) -> Result<(), ApiError> {
        let url = self.base_url.join(&format!("v1/basins/{name}"))?;
        let request = self.delete(url).build()?;
        self.request(request)
            .send()
            .await
            .ignore_not_found(ignore_not_found)?;
        Ok(())
    }

    pub async fn get_account_metrics(
        &self,
        request: AccountMetricSetRequest,
    ) -> Result<MetricSetResponse, ApiError> {
        let url = self.base_url.join("v1/metrics")?;
        let request = self.get(url).query(&request).build()?;
        let response = self.request(request).send().await?;
        Ok(response.json::<MetricSetResponse>().await?)
    }

    pub async fn get_basin_metrics(
        &self,
        name: BasinName,
        request: BasinMetricSetRequest,
    ) -> Result<MetricSetResponse, ApiError> {
        let url = self.base_url.join(&format!("v1/metrics/{name}"))?;
        let request = self.get(url).query(&request).build()?;
        let response = self.request(request).send().await?;
        Ok(response.json::<MetricSetResponse>().await?)
    }

    pub async fn get_stream_metrics(
        &self,
        basin_name: BasinName,
        stream_name: StreamName,
        request: StreamMetricSetRequest,
    ) -> Result<MetricSetResponse, ApiError> {
        let url = self.base_url.join(&format!(
            "v1/metrics/{basin_name}/{}",
            urlencoding::encode(&stream_name)
        ))?;
        let request = self.get(url).query(&request).build()?;
        let response = self.request(request).send().await?;
        Ok(response.json::<MetricSetResponse>().await?)
    }
}

impl Deref for AccountClient {
    type Target = BaseClient;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

#[derive(Debug, Clone)]
pub struct BasinClient {
    pub name: BasinName,
    pub client: BaseClient,
    pub config: Arc<S2Config>,
    pub base_url: Url,
}

impl BasinClient {
    pub fn init(name: BasinName, config: Arc<S2Config>, client: BaseClient) -> Self {
        let base_url = base_url(&config.endpoints, ClientKind::Basin(name.clone()));
        Self {
            name,
            client,
            config,
            base_url,
        }
    }

    fn request(&self, mut request: Request) -> RequestBuilder<'_> {
        if matches!(
            self.config.endpoints.basin_authority,
            BasinAuthority::Direct(_)
        ) {
            request.headers_mut().insert(
                S2_BASIN,
                HeaderValue::from_str(&self.name).expect("valid header value"),
            );
        }
        self.client.request(request)
    }

    pub async fn list_streams(
        &self,
        request: ListStreamsRequest,
    ) -> Result<ListStreamsResponse, ApiError> {
        let url = self.base_url.join("v1/streams")?;
        let request = self.get(url).query(&request).build()?;
        let response = self.request(request).send().await?;
        Ok(response.json::<ListStreamsResponse>().await?)
    }

    pub async fn create_stream(
        &self,
        request: CreateStreamRequest,
        idempotency_token: String,
    ) -> Result<StreamInfo, ApiError> {
        let url = self.base_url.join("v1/streams")?;
        let request = self
            .post(url)
            .header(S2_REQUEST_TOKEN, idempotency_token)
            .json(&request)
            .build()?;
        let response = self.request(request).send().await?;
        Ok(response.json::<StreamInfo>().await?)
    }

    pub async fn get_stream_config(&self, name: StreamName) -> Result<StreamConfig, ApiError> {
        let url = self
            .base_url
            .join(&format!("v1/streams/{}", urlencoding::encode(&name)))?;
        let request = self.get(url).build()?;
        let response = self.request(request).send().await?;
        Ok(response.json::<StreamConfig>().await?)
    }

    pub async fn reconfigure_stream(
        &self,
        name: StreamName,
        config: StreamReconfiguration,
    ) -> Result<StreamConfig, ApiError> {
        let url = self
            .base_url
            .join(&format!("v1/streams/{}", urlencoding::encode(&name)))?;
        let request = self.patch(url).json(&config).build()?;
        let response = self.request(request).send().await?;
        Ok(response.json::<StreamConfig>().await?)
    }

    pub async fn delete_stream(
        &self,
        name: StreamName,
        ignore_not_found: bool,
    ) -> Result<(), ApiError> {
        let url = self
            .base_url
            .join(&format!("v1/streams/{}", urlencoding::encode(&name)))?;
        let request = self.delete(url).build()?;
        self.request(request)
            .send()
            .await
            .ignore_not_found(ignore_not_found)?;
        Ok(())
    }

    pub async fn check_tail(&self, name: &StreamName) -> Result<TailResponse, ApiError> {
        let url = self.base_url.join(&format!(
            "v1/streams/{}/records/tail",
            urlencoding::encode(name)
        ))?;
        let request = self.get(url).build()?;
        let response = self.request(request).send().await?;
        Ok(response.json::<TailResponse>().await?)
    }

    pub async fn append(
        &self,
        name: &StreamName,
        input: AppendInput,
        retry_enabled: bool,
    ) -> Result<AppendAck, ApiError> {
        let url = self
            .base_url
            .join(&format!("v1/streams/{}/records", urlencoding::encode(name)))?;
        let request = self
            .post(url)
            .header(CONTENT_TYPE, CONTENT_TYPE_PROTO)
            .header(ACCEPT, ACCEPT_PROTO)
            .body(input.encode_to_vec())
            .build()?;
        let response = self
            .request(request)
            .with_retry_enabled(retry_enabled)
            .error_handler(|status, response| async move {
                if status == StatusCode::PRECONDITION_FAILED {
                    Err(ApiError::AppendConditionFailed(
                        response.json::<AppendConditionFailed>().await?,
                    ))
                } else {
                    Err(ApiError::Server(
                        status,
                        response.json::<ApiErrorResponse>().await?,
                    ))
                }
            })
            .send()
            .await?;
        Ok(AppendAck::decode(response.bytes().await?)?)
    }

    pub async fn read(
        &self,
        name: &StreamName,
        start: ReadStart,
        end: ReadEnd,
    ) -> Result<ReadBatch, ApiError> {
        let url = self
            .base_url
            .join(&format!("v1/streams/{}/records", urlencoding::encode(name)))?;
        let request = self
            .get(url)
            .header(ACCEPT, ACCEPT_PROTO)
            .query(&start)
            .query(&end)
            .build()?;
        let response = self
            .request(request)
            .error_handler(read_response_error_handler)
            .send()
            .await?;
        Ok(ReadBatch::decode(response.bytes().await?)?)
    }

    pub async fn append_session<I>(
        &self,
        name: &StreamName,
        inputs: I,
    ) -> Result<Streaming<AppendAck>, ApiError>
    where
        I: Stream<Item = AppendInput> + Send + 'static,
    {
        let url = self
            .base_url
            .join(&format!("v1/streams/{}/records", urlencoding::encode(name)))?;

        let compression = self.config.compression.into();

        let encoded_stream = inputs.map(move |input| {
            s2s::SessionMessage::regular(compression, &input).map(|msg| msg.encode())
        });

        let mut request = self
            .post(url)
            .header(CONTENT_TYPE, CONTENT_TYPE_S2S)
            .body(reqwest::Body::wrap_stream(encoded_stream))
            .timeout(SESSION_REQUEST_TIMEOUT);
        request = add_basin_header_if_required(request, &self.config.endpoints, &self.name);
        let response = request.send().await?.into_result().await?;
        let mut bytes_stream = response.bytes_stream();

        let mut buffer = BytesMut::new();
        let mut decoder = FrameDecoder;

        Ok(Box::pin(try_stream! {
            while let Some(chunk) = bytes_stream.next().await {
                let chunk = chunk?;
                buffer.extend_from_slice(&chunk);

                loop {
                    match decoder.decode(&mut buffer) {
                        Ok(Some(SessionMessage::Regular(msg))) => {
                            yield msg.try_into_proto()?;
                        }
                        Ok(Some(SessionMessage::Terminal(msg))) => {
                            Err::<(), ApiError>(msg.into())?;
                        }
                        Ok(None) => break,
                        Err(err) => Err(err)?,
                    }
                }
            }
        }))
    }

    pub async fn read_session(
        &self,
        name: &StreamName,
        start: ReadStart,
        end: ReadEnd,
    ) -> Result<Streaming<ReadBatch>, ApiError> {
        let url = self
            .base_url
            .join(&format!("v1/streams/{}/records", urlencoding::encode(name)))?;

        let mut request = self
            .client
            .get(url)
            .header(CONTENT_TYPE, CONTENT_TYPE_S2S)
            .query(&start)
            .query(&end)
            .timeout(SESSION_REQUEST_TIMEOUT);
        request = add_basin_header_if_required(request, &self.config.endpoints, &self.name);
        let response = request
            .send()
            .await?
            .into_result_with_handler(read_response_error_handler)
            .await?;
        let mut bytes_stream = response.bytes_stream();

        let mut buffer = BytesMut::new();
        let mut decoder = FrameDecoder;

        Ok(Box::pin(try_stream! {
            while let Some(chunk) = bytes_stream.next().await {
                let chunk = chunk?;
                buffer.extend_from_slice(&chunk);

                loop {
                    match decoder.decode(&mut buffer) {
                        Ok(Some(SessionMessage::Regular(msg))) => {
                            yield msg.try_into_proto()?;
                        }
                        Ok(Some(SessionMessage::Terminal(msg))) => {
                            Err::<(), ApiError>(msg.into())?;
                        }
                        Ok(None) => break,
                        Err(err) => Err(err)?,
                    }
                }
            }
        }))
    }
}

async fn read_response_error_handler(
    status: StatusCode,
    response: Response,
) -> Result<Response, ApiError> {
    if status == StatusCode::RANGE_NOT_SATISFIABLE {
        Err(ApiError::ReadUnwritten(
            response.json::<TailResponse>().await?,
        ))
    } else {
        Err(ApiError::Server(
            status,
            response.json::<ApiErrorResponse>().await?,
        ))
    }
}

impl Deref for BasinClient {
    type Target = BaseClient;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

#[derive(Debug, thiserror::Error, serde::Deserialize)]
#[error("{code}: {message}")]
pub struct ApiErrorResponse {
    pub code: String,
    pub message: String,
}

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error(transparent)]
    Client(#[from] ClientError),
    #[error(transparent)]
    Url(#[from] url::ParseError),
    #[error(transparent)]
    ProtoDecode(#[from] prost::DecodeError),
    #[error(transparent)]
    S2STerminalDecode(#[from] S2STerminalDecodeError),
    #[error(transparent)]
    InvalidHeaderValue(#[from] InvalidHeaderValue),
    #[error(transparent)]
    Compression(#[from] std::io::Error),
    #[error("append condition check failed")]
    AppendConditionFailed(AppendConditionFailed),
    #[error("read from an unwritten position")]
    ReadUnwritten(TailResponse),
    #[error("{1}")]
    Server(StatusCode, ApiErrorResponse),
}

impl ApiError {
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Server(status, _) => {
                matches!(
                    *status,
                    StatusCode::REQUEST_TIMEOUT
                        | StatusCode::CONFLICT
                        | StatusCode::TOO_MANY_REQUESTS
                        | StatusCode::INTERNAL_SERVER_ERROR
                        | StatusCode::BAD_GATEWAY
                        | StatusCode::SERVICE_UNAVAILABLE
                        | StatusCode::GATEWAY_TIMEOUT
                )
            }
            Self::Client(err) => err.is_retryable(),
            _ => false,
        }
    }
}

impl From<reqwest::Error> for ApiError {
    fn from(err: reqwest::Error) -> Self {
        ClientError::from(err).into()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("connect: {0}")]
    Connect(String),
    #[error("timeout: {0}")]
    Timeout(String),
    #[error("connection closed early: {0}")]
    ConnectionClosedEarly(String),
    #[error("request canceled: {0}")]
    RequestCanceled(String),
    #[error("unexpected eof: {0}")]
    UnexpectedEof(String),
    #[error("connection reset: {0}")]
    ConnectionReset(String),
    #[error("connection aborted: {0}")]
    ConnectionAborted(String),
    #[error("connection refused: {0}")]
    ConnectionRefused(String),
    #[error("{0}")]
    Others(String),
}

impl ClientError {
    pub fn is_retryable(&self) -> bool {
        !matches!(self, ClientError::Others(_))
    }
}

impl From<reqwest::Error> for ClientError {
    fn from(err: reqwest::Error) -> Self {
        let err_msg = err.to_string();
        if err.is_connect() {
            Self::Connect(err_msg)
        } else if err.is_timeout() {
            Self::Timeout(err_msg)
        } else if err.is_request() {
            if let Some(hyper_err) = source_err::<hyper::Error>(&err) {
                let hyper_err_msg = format!("{hyper_err} -> {err_msg}");
                if hyper_err.is_incomplete_message() {
                    Self::ConnectionClosedEarly(hyper_err_msg)
                } else if hyper_err.is_canceled() {
                    Self::RequestCanceled(hyper_err_msg)
                } else {
                    Self::Others(hyper_err_msg)
                }
            } else {
                Self::Others(err_msg)
            }
        } else if let Some(io_err) = source_err::<std::io::Error>(&err) {
            let io_err_msg = format!("{io_err} -> {err_msg}");
            if io_err.kind() == std::io::ErrorKind::UnexpectedEof {
                Self::UnexpectedEof(io_err_msg)
            } else if io_err.kind() == std::io::ErrorKind::ConnectionReset {
                Self::ConnectionReset(io_err_msg)
            } else if io_err.kind() == std::io::ErrorKind::ConnectionAborted {
                Self::ConnectionAborted(io_err_msg)
            } else if io_err.kind() == std::io::ErrorKind::ConnectionRefused {
                Self::ConnectionRefused(io_err_msg)
            } else {
                Self::Others(io_err_msg)
            }
        } else {
            Self::Others(err_msg)
        }
    }
}

fn source_err<T: std::error::Error + 'static>(err: &dyn std::error::Error) -> Option<&T> {
    let mut source = err.source();

    while let Some(err) = source {
        if let Some(err) = err.downcast_ref::<T>() {
            return Some(err);
        }

        source = err.source();
    }
    None
}

#[derive(Debug, thiserror::Error)]
pub enum S2STerminalDecodeError {
    #[error("invalid status code: {0}")]
    InvalidStatusCode(#[from] http::status::InvalidStatusCode),
    #[error("failed to parse error response: {0}")]
    JsonDecode(#[from] serde_json::Error),
}

impl From<TerminalMessage> for ApiError {
    fn from(msg: TerminalMessage) -> Self {
        let status = match StatusCode::from_u16(msg.status) {
            Ok(s) => s,
            Err(e) => return ApiError::S2STerminalDecode(e.into()),
        };
        if status == StatusCode::PRECONDITION_FAILED {
            let condition_failed = match serde_json::from_str::<AppendConditionFailed>(&msg.body) {
                Ok(condition_failed) => condition_failed,
                Err(err) => {
                    return ApiError::S2STerminalDecode(err.into());
                }
            };
            ApiError::AppendConditionFailed(condition_failed)
        } else if status == StatusCode::RANGE_NOT_SATISFIABLE {
            let tail = match serde_json::from_str::<TailResponse>(&msg.body) {
                Ok(tail) => tail,
                Err(err) => {
                    return ApiError::S2STerminalDecode(err.into());
                }
            };
            ApiError::ReadUnwritten(tail)
        } else {
            let response = match serde_json::from_str::<ApiErrorResponse>(&msg.body) {
                Ok(response) => response,
                Err(err) => {
                    return ApiError::S2STerminalDecode(err.into());
                }
            };
            ApiError::Server(status, response)
        }
    }
}

pub type Streaming<R> = Pin<Box<dyn Send + Stream<Item = Result<R, ApiError>>>>;

#[derive(Debug, Clone)]
pub struct BaseClient {
    client: reqwest::Client,
    retry_builder: RetryBackoffBuilder,
    compression: Compression,
}

impl BaseClient {
    pub fn init(config: &S2Config) -> Result<Self, ApiError> {
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            format!("Bearer {}", config.access_token.expose_secret()).try_into()?,
        );
        let mut client_builder = reqwest::ClientBuilder::new()
            .timeout(config.request_timeout)
            .connect_timeout(config.connection_timeout)
            .user_agent(config.user_agent.clone())
            .default_headers(headers);
        match config.compression {
            Compression::Gzip => {
                client_builder = client_builder.gzip(true);
            }
            Compression::Zstd => {
                client_builder = client_builder.zstd(true);
            }
            Compression::None => {}
        }
        Ok(Self {
            client: client_builder.build()?,
            retry_builder: retry_builder(&config.retry),
            compression: config.compression,
        })
    }

    async fn compress_request(&self, request: &mut Request) -> Result<(), ApiError> {
        if let Some(body) = request.body_mut() {
            let bytes = body.as_bytes().expect("should not be a stream");
            match self.compression {
                Compression::None => {}
                Compression::Gzip => {
                    let mut encoder = GzipEncoder::with_quality(Vec::new(), Level::Fastest);
                    encoder.write_all(bytes).await?;
                    encoder.shutdown().await?;
                    *body = encoder.into_inner().into();
                }
                Compression::Zstd => {
                    let mut encoder = ZstdEncoder::with_quality(Vec::new(), Level::Fastest);
                    encoder.write_all(bytes).await?;
                    encoder.shutdown().await?;
                    *body = encoder.into_inner().into();
                }
            }
        }
        Ok(())
    }

    fn request(&self, request: Request) -> RequestBuilder<'_> {
        RequestBuilder {
            client: self,
            request,
            retry_enabled: true,
            error_handler: None,
        }
    }
}

impl Deref for BaseClient {
    type Target = reqwest::Client;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

pub fn retry_builder(config: &RetryConfig) -> RetryBackoffBuilder {
    RetryBackoffBuilder::default()
        .with_min_base_delay(config.min_base_delay)
        .with_max_base_delay(config.max_base_delay)
        .with_max_retries(config.max_retries())
}

type ErrorHandlerFn = Box<
    dyn Fn(StatusCode, Response) -> Pin<Box<dyn Future<Output = Result<Response, ApiError>> + Send>>
        + Send
        + Sync,
>;

struct RequestBuilder<'a> {
    client: &'a BaseClient,
    request: Request,
    retry_enabled: bool,
    error_handler: Option<ErrorHandlerFn>,
}

impl<'a> RequestBuilder<'a> {
    fn with_retry_enabled(self, retry_enabled: bool) -> Self {
        Self {
            retry_enabled,
            ..self
        }
    }

    fn error_handler<F, Fut>(self, handler: F) -> Self
    where
        F: Fn(StatusCode, Response) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Response, ApiError>> + Send + 'static,
    {
        Self {
            error_handler: Some(Box::new(move |status, response| {
                Box::pin(handler(status, response))
            })),
            ..self
        }
    }

    async fn send(self) -> Result<Response, ApiError> {
        let mut request = self.request;
        self.client.compress_request(&mut request).await?;

        let mut retry_backoffs: Option<VecDeque<_>> = self
            .retry_enabled
            .then(|| self.client.retry_builder.build().collect());

        loop {
            let response = self
                .client
                .execute(request.try_clone().expect("body should not be a stream"))
                .await;

            let (err, retry_after) = match response {
                Ok(resp) => {
                    let retry_after: Option<Duration> = resp
                        .headers()
                        .get(RETRY_AFTER_MS_HEADER)
                        .and_then(|v| match v.to_str() {
                            Ok(s) => Some(s),
                            Err(e) => {
                                warn!(
                                    ?e,
                                    "failed to parse {RETRY_AFTER_MS_HEADER} header as string"
                                );
                                None
                            }
                        })
                        .and_then(|v| match v.parse::<u64>() {
                            Ok(ms) => Some(ms),
                            Err(e) => {
                                warn!(?e, "failed to parse {RETRY_AFTER_MS_HEADER} header as u64");
                                None
                            }
                        })
                        .map(Duration::from_millis);

                    let result = if let Some(ref handler) = self.error_handler {
                        resp.into_result_with_handler(handler).await
                    } else {
                        resp.into_result().await
                    };

                    match result {
                        Ok(resp) => {
                            return Ok(resp);
                        }
                        Err(err) if err.is_retryable() => (err, retry_after),
                        Err(err) => return Err(err),
                    }
                }
                Err(err) => (ApiError::from(err), None),
            };

            if err.is_retryable()
                && let Some(backoff) = retry_backoffs.as_mut().and_then(|b| b.pop_front())
            {
                let backoff = retry_after.unwrap_or(backoff);
                debug!(
                    %err,
                    ?backoff,
                    num_retries_remaining = retry_backoffs.as_ref().map(|b| b.len()).unwrap_or(0),
                    "retrying request"
                );
                tokio::time::sleep(backoff).await;
            } else {
                debug!(
                    %err,
                    is_retryable = err.is_retryable(),
                    retry_enabled = self.retry_enabled,
                    retries_exhausted = retry_backoffs.as_ref().is_none_or(|b| b.is_empty()),
                    "not retrying request"
                );
                return Err(err);
            }
        }
    }
}

fn add_basin_header_if_required(
    request: reqwest::RequestBuilder,
    endpoints: &S2Endpoints,
    name: &BasinName,
) -> reqwest::RequestBuilder {
    if matches!(endpoints.basin_authority, BasinAuthority::Direct(_)) {
        return request.header(
            S2_BASIN,
            HeaderValue::from_str(name).expect("valid header value"),
        );
    }
    request
}

#[derive(Debug, Clone)]
enum ClientKind {
    Account,
    Basin(BasinName),
}

fn base_url(endpoints: &S2Endpoints, kind: ClientKind) -> Url {
    let authority = match kind {
        ClientKind::Account => endpoints.account_authority.clone(),
        ClientKind::Basin(basin) => match &endpoints.basin_authority {
            BasinAuthority::ParentZone(zone) => format!("{basin}.{zone}")
                .try_into()
                .expect("valid authority as basin pre-validated"),
            BasinAuthority::Direct(endpoint) => endpoint.clone(),
        },
    };
    let scheme = &endpoints.scheme;
    Url::parse(&format!("{scheme}://{authority}")).expect("valid url")
}

trait IntoResult {
    async fn into_result(self) -> Result<Response, ApiError>;
    async fn into_result_with_handler<F, Fut>(self, handler: F) -> Result<Response, ApiError>
    where
        F: Fn(StatusCode, Response) -> Fut,
        Fut: Future<Output = Result<Response, ApiError>>;
}

impl IntoResult for Response {
    async fn into_result(self) -> Result<Response, ApiError> {
        let status = self.status();
        if status.is_success() {
            Ok(self)
        } else {
            Err(ApiError::Server(
                status,
                self.json::<ApiErrorResponse>().await?,
            ))
        }
    }

    async fn into_result_with_handler<F, Fut>(self, handler: F) -> Result<Response, ApiError>
    where
        F: Fn(StatusCode, Response) -> Fut,
        Fut: Future<Output = Result<Response, ApiError>>,
    {
        let status = self.status();
        if status.is_success() {
            Ok(self)
        } else {
            handler(status, self).await
        }
    }
}

trait IgnoreNotFound {
    fn ignore_not_found(self, enabled: bool) -> Result<(), ApiError>;
}

impl IgnoreNotFound for Result<Response, ApiError> {
    fn ignore_not_found(self, enabled: bool) -> Result<(), ApiError> {
        match self {
            Ok(_) => Ok(()),
            Err(ApiError::Server(StatusCode::NOT_FOUND, _)) if enabled => Ok(()),
            Err(err) => Err(err),
        }
    }
}

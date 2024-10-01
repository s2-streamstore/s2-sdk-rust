pub mod account;
pub mod basin;
pub mod stream;

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use backon::{ConstantBuilder, Retryable};
use futures::StreamExt;
use secrecy::{ExposeSecret, SecretString};
use tonic::metadata::{AsciiMetadataValue, MetadataMap};

use crate::types::ConvertError;

#[derive(Debug, Clone, thiserror::Error)]
pub enum ServiceError<T: std::error::Error> {
    #[error("Message conversion: {0}")]
    Convert(ConvertError),
    #[error("Internal server error")]
    Internal,
    #[error("{0} currently not supported")]
    NotSupported(String),
    #[error("User not authenticated: {0}")]
    Unauthenticated(String),
    #[error("Unavailable: {0}")]
    Unavailable(String),
    #[error("{0}")]
    Unknown(String),
    #[error(transparent)]
    Remote(T),
}

pub async fn send_request<T: ServiceRequest>(
    mut service: T,
    token: &SecretString,
    basin: Option<&str>,
) -> Result<T::Response, ServiceError<T::Error>> {
    let mut req = service.prepare_request().map_err(ServiceError::Convert)?;

    add_authorization_header(req.metadata_mut(), token);
    add_basin_header(req.metadata_mut(), basin);

    match service.send(req).await {
        Ok(resp) => service.parse_response(resp).map_err(ServiceError::Convert),
        Err(status) => match status.code() {
            tonic::Code::Internal => Err(ServiceError::Internal),
            tonic::Code::Unimplemented => {
                Err(ServiceError::NotSupported(status.message().to_string()))
            }
            tonic::Code::Unauthenticated => {
                Err(ServiceError::Unauthenticated(status.message().to_string()))
            }
            tonic::Code::Unavailable => {
                Err(ServiceError::Unavailable(status.message().to_string()))
            }
            _ => match service.parse_status(&status) {
                Ok(resp) => Ok(resp),
                Err(None) => Err(ServiceError::Unknown(status.message().to_string())),
                Err(Some(e)) => Err(ServiceError::Remote(e)),
            },
        },
    }
}

pub async fn send_retryable_request<T: RetryableRequest>(
    service: T,
    token: &SecretString,
    basin: Option<&str>,
) -> Result<T::Response, ServiceError<T::Error>> {
    let retry_fn = || async { send_request(service.clone(), token, basin).await };

    // TODO: Configure retry.
    Retryable::retry(retry_fn, ConstantBuilder::default())
        .when(|e| service.should_retry(e))
        .await
}

fn add_authorization_header(meta: &mut MetadataMap, token: &SecretString) {
    let mut val: AsciiMetadataValue = format!("Bearer {}", token.expose_secret())
        .try_into()
        .unwrap();
    val.set_sensitive(true);
    meta.insert("authorization", val);
}

fn add_basin_header(meta: &mut MetadataMap, basin: Option<&str>) {
    if let Some(basin) = basin {
        meta.insert("s2-basin", basin.parse().unwrap());
    }
}

pub trait ServiceRequest {
    /// Request parameters generated by prost.
    type ApiRequest;
    /// Response to be returned by the RPC.
    type Response;
    /// Response generated by prost to be returned.
    type ApiResponse;
    /// Error to be returned by the RPC.
    ///
    /// Shouldn't be just `tonic::Status`. Need to have meaningful errors.
    type Error: std::error::Error;

    /// Take the request parameters and generate the corresponding tonic request.
    fn prepare_request(&mut self) -> Result<tonic::Request<Self::ApiRequest>, ConvertError>;

    /// Take the tonic response and generate the response to be returned.
    fn parse_response(
        &self,
        resp: tonic::Response<Self::ApiResponse>,
    ) -> Result<Self::Response, ConvertError>;

    /// Take the tonic status and generate the error.
    fn parse_status(&self, status: &tonic::Status) -> Result<Self::Response, Option<Self::Error>>;

    /// Actually send the tonic request to receive a raw response and the parsed error.
    async fn send(
        &mut self,
        req: tonic::Request<Self::ApiRequest>,
    ) -> Result<tonic::Response<Self::ApiResponse>, tonic::Status>;
}

pub trait RetryableRequest: ServiceRequest + Clone {
    /// Return true if the request should be retried based on the error returned.
    fn should_retry(&self, err: &ServiceError<Self::Error>) -> bool;
}

pub trait IdempodentRequest: ServiceRequest + Clone {
    /// The request does not have any side effects (for sure).
    const NO_SIDE_EFFECTS: bool;
}

impl<T: IdempodentRequest> RetryableRequest for T {
    fn should_retry(&self, err: &ServiceError<Self::Error>) -> bool {
        match err {
            // Always retry on unavailable (if the request doesn't have any
            // side-effects).
            ServiceError::Unavailable(_) => true,
            ServiceError::Internal => T::NO_SIDE_EFFECTS,
            _ => false,
        }
    }
}

pub trait StreamingRequest: Unpin {
    type RequestItem;
    type ApiRequestItem;

    fn prepare_request_item(&self, req: Self::RequestItem) -> Self::ApiRequestItem;
}

pub struct ServiceStreamingRequest<R, S>
where
    R: StreamingRequest,
    S: futures::Stream<Item = R::RequestItem> + Unpin,
{
    req: R,
    stream: S,
}

impl<R, S> ServiceStreamingRequest<R, S>
where
    R: StreamingRequest,
    S: futures::Stream<Item = R::RequestItem> + Unpin,
{
    pub fn new(req: R, stream: S) -> Self {
        Self { req, stream }
    }
}

impl<R, S> futures::Stream for ServiceStreamingRequest<R, S>
where
    R: StreamingRequest,
    S: futures::Stream<Item = R::RequestItem> + Unpin,
{
    type Item = R::ApiRequestItem;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.stream.poll_next_unpin(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(req)) => Poll::Ready(Some(self.req.prepare_request_item(req))),
        }
    }
}

pub trait StreamingResponse: Unpin {
    /// Response message item to be returned by the RPC stream.
    type ResponseItem;
    /// Response message item generated by prost in the stream.
    type ApiResponseItem;
    /// Error to be returned by the RPC.
    ///
    /// Shouldn't be just `tonic::Status`. Need to have meaningful errors.
    type Error: std::error::Error;

    /// Take the tonic response message from stream item and generate stream item.
    fn parse_response_item(
        &self,
        resp: Self::ApiResponseItem,
    ) -> Result<Self::ResponseItem, ConvertError>;

    /// Take the tonic status and generate the error.
    fn parse_response_item_status(
        &self,
        status: &tonic::Status,
    ) -> Result<Self::ResponseItem, Option<Self::Error>>;
}

pub struct ServiceStreamingResponse<S: StreamingResponse> {
    req: S,
    stream: tonic::Streaming<S::ApiResponseItem>,
}

impl<S: StreamingResponse> ServiceStreamingResponse<S> {
    pub fn new(req: S, stream: tonic::Streaming<S::ApiResponseItem>) -> Self {
        Self { req, stream }
    }
}

impl<S: StreamingResponse> futures::Stream for ServiceStreamingResponse<S> {
    type Item = Result<S::ResponseItem, ServiceError<S::Error>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.stream.poll_next_unpin(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(item)) => {
                let item = match item {
                    Ok(resp) => self
                        .req
                        .parse_response_item(resp)
                        .map_err(ServiceError::Convert),
                    Err(status) => match status.code() {
                        tonic::Code::Internal => Err(ServiceError::Internal),
                        tonic::Code::Unimplemented => {
                            Err(ServiceError::NotSupported(status.message().to_string()))
                        }
                        tonic::Code::Unauthenticated => {
                            Err(ServiceError::Unauthenticated(status.message().to_string()))
                        }
                        tonic::Code::Unavailable => {
                            Err(ServiceError::Unavailable(status.message().to_string()))
                        }
                        _ => match self.req.parse_response_item_status(&status) {
                            Ok(resp) => Ok(resp),
                            Err(None) => Err(ServiceError::Unknown(status.message().to_string())),
                            Err(Some(e)) => Err(ServiceError::Remote(e)),
                        },
                    },
                };
                Poll::Ready(Some(item))
            }
        }
    }
}

/// Wrapper around `ServiceStreamingResponse` to expose publically.
pub struct Streaming<R, E: std::error::Error>(
    Box<dyn Unpin + futures::Stream<Item = Result<R, ServiceError<E>>>>,
);

impl<R, E: std::error::Error> Streaming<R, E> {
    pub(crate) fn new<S>(s: ServiceStreamingResponse<S>) -> Self
    where
        S: StreamingResponse<ResponseItem = R, Error = E> + 'static,
    {
        Self(Box::new(s))
    }
}

impl<R, E: std::error::Error> futures::Stream for Streaming<R, E> {
    type Item = Result<R, ServiceError<E>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_next_unpin(cx)
    }
}

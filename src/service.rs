pub mod account;
pub mod basin;
pub mod stream;

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::StreamExt;
use secrecy::{ExposeSecret, SecretString};
use tonic::metadata::{AsciiMetadataValue, MetadataMap};

use crate::{client::ClientError, types};

pub async fn send_request<T: ServiceRequest>(
    mut service: T,
    token: &SecretString,
    basin: Option<&str>,
) -> Result<T::Response, ClientError> {
    let req = prepare_request(&mut service, token, basin)?;
    match service.send(req).await {
        Ok(resp) => Ok(service.parse_response(resp)?),
        Err(status) => Err(ClientError::Service(status)),
    }
}

fn prepare_request<T: ServiceRequest>(
    service: &mut T,
    token: &SecretString,
    basin: Option<&str>,
) -> Result<tonic::Request<T::ApiRequest>, types::ConvertError> {
    let mut req = service.prepare_request()?;
    add_authorization_header(req.metadata_mut(), token)?;
    if let Some(basin) = basin {
        add_basin_header(req.metadata_mut(), basin)?;
    }
    Ok(req)
}

fn add_authorization_header(
    meta: &mut MetadataMap,
    token: &SecretString,
) -> Result<(), types::ConvertError> {
    let mut val: AsciiMetadataValue = format!("Bearer {}", token.expose_secret())
        .try_into()
        .map_err(|_| "failed to parse token as metadata value")?;
    val.set_sensitive(true);
    meta.insert("authorization", val);
    Ok(())
}

fn add_basin_header(meta: &mut MetadataMap, basin: &str) -> Result<(), types::ConvertError> {
    meta.insert(
        "s2-basin",
        basin
            .parse()
            .map_err(|_| "failed to parse basin as metadata value")?,
    );
    Ok(())
}

pub trait ServiceRequest {
    /// Request parameters generated by prost.
    type ApiRequest;
    /// Response to be returned by the RPC.
    type Response;
    /// Response generated by prost to be returned.
    type ApiResponse;

    /// Take the request parameters and generate the corresponding tonic request.
    fn prepare_request(&mut self) -> Result<tonic::Request<Self::ApiRequest>, types::ConvertError>;

    /// Take the tonic response and generate the response to be returned.
    fn parse_response(
        &self,
        resp: tonic::Response<Self::ApiResponse>,
    ) -> Result<Self::Response, types::ConvertError>;

    /// Actually send the tonic request to receive a raw response and the parsed error.
    async fn send(
        &mut self,
        req: tonic::Request<Self::ApiRequest>,
    ) -> Result<tonic::Response<Self::ApiResponse>, tonic::Status>;
}

pub trait RetryableRequest: ServiceRequest + Clone {
    /// Return true if the request should be retried based on the error returned.
    fn should_retry(&self, err: &ClientError) -> bool;
}

pub trait IdempotentRequest: ServiceRequest + Clone {
    /// The request does not have any side effects (for sure).
    const NO_SIDE_EFFECTS: bool;
}

impl<T: IdempotentRequest> RetryableRequest for T {
    fn should_retry(&self, err: &ClientError) -> bool {
        match err {
            // Always retry on unavailable (if the request doesn't have any
            // side-effects).
            ClientError::Service(status) => match status.code() {
                tonic::Code::Unavailable => true,
                tonic::Code::Internal => T::NO_SIDE_EFFECTS,
                _ => false,
            },
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

    /// Take the tonic response message from stream item and generate stream item.
    fn parse_response_item(
        &self,
        resp: Self::ApiResponseItem,
    ) -> Result<Self::ResponseItem, ClientError>;
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
    type Item = Result<S::ResponseItem, ClientError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.stream.poll_next_unpin(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(item)) => {
                let item = match item {
                    Ok(resp) => self.req.parse_response_item(resp),
                    Err(status) => Err(ClientError::Service(status)),
                };
                Poll::Ready(Some(item))
            }
        }
    }
}

/// Generic type for streaming response.
pub type Streaming<R> = Pin<Box<dyn futures::Stream<Item = Result<R, ClientError>>>>;

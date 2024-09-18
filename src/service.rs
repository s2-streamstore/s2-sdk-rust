pub mod account;
pub mod basin;
pub mod stream;

use backon::{ConstantBuilder, Retryable};
use prost_types::method_options::IdempotencyLevel;
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
    service: T,
    req: T::Request,
    token: &SecretString,
    basin: Option<&str>,
) -> Result<T::Response, ServiceError<T::Error>> {
    let retry_fn = || async {
        let mut service = service.clone();

        let mut req = service
            .prepare_request(req.clone())
            .map_err(ServiceError::Convert)?;

        add_authorization_header(req.metadata_mut(), token);
        add_basin_header(req.metadata_mut(), basin);

        service
            .send(req)
            .await
            .map_err(|status| match status.code() {
                tonic::Code::Internal => ServiceError::Internal,
                tonic::Code::Unimplemented => {
                    ServiceError::NotSupported(status.message().to_string())
                }
                tonic::Code::Unauthenticated => {
                    ServiceError::Unauthenticated(status.message().to_string())
                }
                tonic::Code::Unavailable => ServiceError::Unavailable(status.message().to_string()),
                _ => service
                    .parse_status(&status)
                    .map(ServiceError::Remote)
                    .unwrap_or_else(|| ServiceError::Unknown(status.message().to_string())),
            })
    };

    // TODO: Configure retry.
    let resp = Retryable::retry(retry_fn, ConstantBuilder::default())
        .when(|e| match e {
            // Always retry on unavailable (if the request doesn't have any
            // side-effects).
            ServiceError::Unavailable(_)
                if matches!(
                    T::IDEMPOTENCY_LEVEL,
                    IdempotencyLevel::NoSideEffects | IdempotencyLevel::Idempotent
                ) =>
            {
                true
            }
            e => service.should_retry(e),
        })
        .await?;

    service.parse_response(resp).map_err(ServiceError::Convert)
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

pub trait ServiceRequest: Clone {
    /// Request parameters for the RPC.
    type Request: Clone;
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

    /// The request does not have any side effects (for sure).
    const IDEMPOTENCY_LEVEL: IdempotencyLevel;

    /// Take the request parameters and generate the corresponding tonic request.
    fn prepare_request(
        &self,
        req: Self::Request,
    ) -> Result<tonic::Request<Self::ApiRequest>, ConvertError>;

    /// Take the tonic response and generate the response to be returned.
    fn parse_response(
        &self,
        resp: tonic::Response<Self::ApiResponse>,
    ) -> Result<Self::Response, ConvertError>;

    /// Take the tonic status and generate the error.
    fn parse_status(&self, status: &tonic::Status) -> Option<Self::Error>;

    /// Actually send the tonic request to receive a raw response and the parsed error.
    async fn send(
        &mut self,
        req: tonic::Request<Self::ApiRequest>,
    ) -> Result<tonic::Response<Self::ApiResponse>, tonic::Status>;

    /// Return true if the request should be retried based on the error returned.
    fn should_retry(&self, err: &ServiceError<Self::Error>) -> bool;
}

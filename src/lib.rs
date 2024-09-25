mod api;
mod service;

pub mod client;
pub mod types;

pub use futures;
pub use http::uri;
pub use secrecy::SecretString;
pub use service::StreamingResponse;

pub mod service_error {
    pub use crate::service::{
        account::{CreateBasinError, DeleteBasinError, ListBasinsError},
        basin::{
            CreateStreamError, DeleteStreamError, GetBasinConfigError, GetStreamConfigError,
            ListStreamsError, ReconfigureBasinError, ReconfigureStreamError,
        },
        stream::{
            AppendError, AppendSessionError, GetNextSeqNumError, ReadError, ReadSessionError,
        },
        ServiceError,
    };
}

mod api;
mod service;

pub mod client;
pub mod types;

pub use futures;
pub use http::uri;
pub use secrecy::SecretString;
pub use service::Streaming;

pub mod service_error {
    pub use crate::service::{
        account::{
            CreateBasinError, DeleteBasinError, GetBasinConfigError, ListBasinsError,
            ReconfigureBasinError,
        },
        basin::{
            CreateStreamError, DeleteStreamError, GetStreamConfigError, ListStreamsError,
            ReconfigureStreamError,
        },
        stream::{
            AppendError, AppendSessionError, GetNextSeqNumError, ReadError, ReadSessionError,
        },
        ServiceError,
    };
}

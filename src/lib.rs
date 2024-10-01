mod api;
mod service;

pub mod client;
pub mod types;

pub use http::uri;
pub use secrecy::SecretString;

pub mod service_error {
    pub use crate::service::{
        account::{CreateBasinError, DeleteBasinError, GetBasinConfigError, ReconfigureBasinError},
        basin::{
            CreateStreamError, DeleteStreamError, GetStreamConfigError, ListStreamsError,
            ReconfigureStreamError,
        },
        ServiceError,
    };
}

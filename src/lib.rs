mod api;
mod service;

pub mod client;
pub mod types;

pub use secrecy::SecretString;
pub use url;

pub mod service_error {
    pub use crate::service::{
        account::CreateBasinError,
        basin::{GetBasinConfigError, GetStreamConfigError, ListStreamsError},
        ServiceError,
    };
}

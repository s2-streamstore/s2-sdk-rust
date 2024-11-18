use futures::StreamExt;
use secrecy::SecretString;

use crate::{
    client::ClientError,
    service::{stream::ReadSessionStreamingResponse, RetryableRequest, ServiceStreamingResponse},
    types,
};

use super::ReadSessionServiceRequest;

pub fn resumption_stream(
    request: ReadSessionServiceRequest,
    mut response: ServiceStreamingResponse<ReadSessionStreamingResponse>,
    token: &SecretString,
    basin: Option<&str>,
) -> impl futures::Stream<Item = Result<types::ReadOutput, ClientError>> {
    async_stream::stream! {
        while let Some(item) = response.next().await {
            match item {
                Ok(item) => {
                    // TODO: Update `request.match_seq_num`.
                    yield Ok(item);
                }
                Err(e) if request.should_retry(&e) => {
                    todo!()
                }
                Err(e) => {
                    yield Err(e);
                }
            }
        }
    }
}

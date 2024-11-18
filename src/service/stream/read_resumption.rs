use backon::BackoffBuilder;
use futures::StreamExt;

use super::ReadSessionServiceRequest;
use crate::{
    client::{ClientError, ClientInner},
    service::{stream::ReadSessionStreamingResponse, RetryableRequest, ServiceStreamingResponse},
    types,
};

pub fn stream(
    mut request: ReadSessionServiceRequest,
    mut response: ServiceStreamingResponse<ReadSessionStreamingResponse>,
    client: ClientInner,
) -> impl Send + futures::Stream<Item = Result<types::ReadOutput, ClientError>> {
    let mut backoff = backon::ConstantBuilder::default().build();
    let mut last_retry_at = tokio::time::Instant::now();

    async_stream::stream! {
        while let Some(item) = response.next().await {
            match item {
                Ok(types::ReadOutput::Batch(types::SequencedRecordBatch { records })) => {
                    if let Some(record) = records.last() {
                        request.req.start_seq_num = Some(record.seq_num + 1);
                    }
                    yield Ok(types::ReadOutput::Batch(types::SequencedRecordBatch { records }));
                }
                Err(e) if request.should_retry(&e) => {
                    if let Some(duration) = backoff.next() {
                        if last_retry_at.elapsed() < duration {
                            tokio::time::sleep(last_retry_at.elapsed() - duration).await;
                        }
                        last_retry_at = tokio::time::Instant::now();

                        match client.send_retryable(request.clone()).await {
                            Ok(new_response) => {
                                response = new_response;
                            }
                            Err(_) => {
                                // Return the original stream error.
                                yield Err(e);
                            }
                        }
                    } else {
                        yield Err(e);
                    }
                }
                other => {
                    yield other;
                }
            }
        }
    }
}

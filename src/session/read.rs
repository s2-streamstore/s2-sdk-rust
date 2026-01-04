use std::{collections::VecDeque, pin::Pin, time::Duration};

use async_stream::stream;
use futures::StreamExt;
use s2_api::v1::stream::{ReadEnd, ReadStart};
use tokio::time::timeout;
use tracing::debug;

use crate::{
    api::{ApiError, BasinClient, retry_builder},
    types::{MeteredBytes, ReadBatch, S2Error, StreamName},
};

#[derive(Debug, thiserror::Error)]
pub enum ReadSessionError {
    #[error(transparent)]
    Api(#[from] ApiError),
    #[error("heartbeat timeout")]
    HeartbeatTimeout,
}

impl ReadSessionError {
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Api(err) => err.is_retryable(),
            Self::HeartbeatTimeout => true,
        }
    }
}

impl From<ReadSessionError> for S2Error {
    fn from(err: ReadSessionError) -> Self {
        match err {
            ReadSessionError::Api(api_err) => api_err.into(),
            other => S2Error::Client(other.to_string()),
        }
    }
}

pub type Streaming<R> = Pin<Box<dyn Send + futures::Stream<Item = Result<R, ReadSessionError>>>>;

pub async fn read_session(
    client: BasinClient,
    name: StreamName,
    mut start: ReadStart,
    mut end: ReadEnd,
    ignore_command_records: bool,
) -> Streaming<ReadBatch> {
    let retry_builder = retry_builder(&client.config.retry);
    let mut retry_backoffs: VecDeque<Duration> = retry_builder.build().collect();

    Box::pin(stream! {
        let mut batches: Option<Streaming<ReadBatch>> = None;

        loop {
            if batches.is_none() {
                match session_inner(
                    client.clone(),
                    name.clone(),
                    start.clone(),
                    end.clone(),
                    ignore_command_records,
                ).await {
                    Ok(b) => batches = Some(b),
                    Err(err) => {
                        if can_retry(&err, &mut retry_backoffs).await {
                            continue;
                        }
                        yield Err(err);
                        break;
                    }
                }
            }

            match batches
                .as_mut()
                .expect("batches should not be None")
                .next()
                .await
            {
                Some(Ok(batch)) => {
                    if retry_backoffs.len() < retry_builder.max_retries as usize {
                        retry_backoffs = retry_builder.build().collect();
                    }

                    if let Some(record) = batch.records.last() {
                        start.seq_num = Some(record.seq_num + 1);
                    }
                    if let Some(count) = end.count.as_mut() {
                        *count = count.saturating_sub(batch.records.len())
                    }
                    if let Some(bytes) = end.bytes.as_mut() {
                        *bytes = bytes.saturating_sub(
                            batch.records.iter().map(|r| r.metered_bytes()).sum()
                        )
                    }

                    yield Ok(batch);
                }
                Some(Err(err)) => {
                    batches = None;
                    if can_retry(&err, &mut retry_backoffs).await {
                        continue;
                    }
                    yield Err(err);
                    break;
                }
                None => break,
            }
        }
    })
}

async fn session_inner(
    client: BasinClient,
    name: StreamName,
    start: ReadStart,
    end: ReadEnd,
    ignore_command_records: bool,
) -> Result<Streaming<ReadBatch>, ReadSessionError> {
    let mut batches = client.read_session(&name, start, end).await?;
    Ok(Box::pin(stream! {
        loop {
            match timeout(Duration::from_secs(20), batches.next()).await {
                Ok(Some(batch)) => {
                    match batch {
                        Ok(batch) => {
                            let batch = ReadBatch::from_api(batch, ignore_command_records);
                            if !batch.records.is_empty() {
                                yield Ok(batch);
                            }
                        }
                        Err(err) => {
                            yield Err(err.into());
                            break;
                        }
                    }
                }
                Ok(None) => {
                    break;
                }
                Err(_) => {
                    yield Err(ReadSessionError::HeartbeatTimeout);
                    break;
                }
            }
        }
    }))
}

async fn can_retry(err: &ReadSessionError, backoffs: &mut VecDeque<Duration>) -> bool {
    if err.is_retryable()
        && let Some(backoff) = backoffs.pop_front()
    {
        debug!(
            %err,
            ?backoff,
            num_retries_remaining = backoffs.len(),
            "retrying read session"
        );
        tokio::time::sleep(backoff).await;
        true
    } else {
        debug!(
            %err,
            is_retryable = err.is_retryable(),
            retries_exhausted = backoffs.is_empty(),
            "not retrying read session"
        );
        false
    }
}

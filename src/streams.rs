use std::{
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use bytesize::ByteSize;
use futures::{FutureExt, Stream, StreamExt};
use tokio::time::Sleep;

use crate::types::{self, MeteredSize as _};

/// Options to configure [`AppendRecordStream`].
#[derive(Debug, Clone)]
pub struct AppendRecordStreamOpts {
    /// Maximum number of records in a batch.
    pub max_batch_records: usize,
    /// Maximum size of a batch in bytes.
    pub max_batch_size: ByteSize,
    /// Enforce that the sequence number issued to the first record matches.
    ///
    /// This is incremented automatically for each batch.
    pub match_seq_num: Option<u64>,
    /// Enforce a fencing token.
    pub fencing_token: Option<Vec<u8>>,
    /// Linger duration for ready records to send together as a batch.
    pub linger: Option<Duration>,
}

impl Default for AppendRecordStreamOpts {
    fn default() -> Self {
        Self {
            max_batch_records: 1000,
            max_batch_size: ByteSize::mib(1),
            match_seq_num: None,
            fencing_token: None,
            linger: None,
        }
    }
}

impl AppendRecordStreamOpts {
    /// Construct an options struct with defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Construct from existing options with the new maximum batch records.
    pub fn with_max_batch_records(self, max_batch_records: usize) -> Self {
        Self {
            max_batch_records: max_batch_records.into(),
            ..self
        }
    }

    /// Construct from existing options with the new maximum batch size.
    pub fn with_max_batch_size(self, max_batch_size: impl Into<ByteSize>) -> Self {
        Self {
            max_batch_size: max_batch_size.into(),
            ..self
        }
    }

    /// Construct from existing options with the initial match sequence number.
    pub fn with_match_seq_num(self, match_seq_num: impl Into<u64>) -> Self {
        Self {
            match_seq_num: Some(match_seq_num.into()),
            ..self
        }
    }

    /// Construct from existing options with the fencing token.
    pub fn with_fencing_token(self, fencing_token: impl Into<Vec<u8>>) -> Self {
        Self {
            fencing_token: Some(fencing_token.into()),
            ..self
        }
    }

    /// Construct from existing options with the linger time.
    pub fn with_linger(self, linger_duration: impl Into<Duration>) -> Self {
        Self {
            linger: Some(linger_duration.into()),
            ..self
        }
    }

    fn linger_sleep_fut(&self) -> Option<Pin<Box<Sleep>>> {
        self.linger
            .map(|duration| Box::pin(tokio::time::sleep(duration)))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AppendRecordStreamError {
    #[error("max_batch_size should not be more than 1 Mib")]
    BatchSizeTooLarge,
}

/// Wrapper over a stream of append records that can be sent over to
/// [`crate::client::StreamClient::append_session`].
pub struct AppendRecordStream<R, S>
where
    R: Into<types::AppendRecord>,
    S: Send + Stream<Item = R> + Unpin,
{
    stream: S,
    peeked_record: Option<types::AppendRecord>,
    terminated: bool,
    linger_sleep: Option<Pin<Box<Sleep>>>,
    opts: AppendRecordStreamOpts,
}

impl<R, S> AppendRecordStream<R, S>
where
    R: Into<types::AppendRecord>,
    S: Send + Stream<Item = R> + Unpin,
{
    /// Try constructing a new [`AppendRecordStream`] from the given stream and options.
    pub fn new(stream: S, opts: AppendRecordStreamOpts) -> Result<Self, AppendRecordStreamError> {
        if opts.max_batch_size > ByteSize::mib(1) {
            return Err(AppendRecordStreamError::BatchSizeTooLarge);
        }

        Ok(Self {
            stream,
            peeked_record: None,
            terminated: false,
            linger_sleep: opts.linger_sleep_fut(),
            opts,
        })
    }

    fn push_record_to_batch(
        &mut self,
        record: types::AppendRecord,
        batch: &mut Vec<types::AppendRecord>,
        batch_size: &mut ByteSize,
    ) {
        let record_size = record.metered_size();
        if *batch_size + record_size > self.opts.max_batch_size {
            // Set the peeked record and move on.
            self.peeked_record = Some(record);
        } else {
            *batch_size += record_size;
            batch.push(record);
        }
    }
}

impl<R, S> Stream for AppendRecordStream<R, S>
where
    R: Into<types::AppendRecord>,
    S: Send + Stream<Item = R> + Unpin,
{
    type Item = types::AppendInput;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.terminated {
            return Poll::Ready(None);
        }

        if self
            .linger_sleep
            .as_mut()
            .is_some_and(|fut| fut.poll_unpin(cx).is_pending())
        {
            return Poll::Pending;
        }

        let mut batch = Vec::with_capacity(self.opts.max_batch_records);
        let mut batch_size = ByteSize::b(0);

        if let Some(peeked) = self.peeked_record.take() {
            self.push_record_to_batch(peeked, &mut batch, &mut batch_size);
        }

        while batch.len() < self.opts.max_batch_records && self.peeked_record.is_none() {
            match self.stream.poll_next_unpin(cx) {
                Poll::Pending => break,
                Poll::Ready(None) => {
                    self.terminated = true;
                    break;
                }
                Poll::Ready(Some(record)) => {
                    self.push_record_to_batch(record.into(), &mut batch, &mut batch_size);
                }
            }
        }

        if batch.is_empty() {
            assert!(
                self.peeked_record.is_none(),
                "dangling peeked record does not fit into size limits"
            );

            if self.terminated {
                Poll::Ready(None)
            } else {
                // Since we don't have any batches to send, we want to ignore the linger
                // interval for the next poll.
                self.linger_sleep = None;
                Poll::Pending
            }
        } else {
            if self.peeked_record.is_some() {
                // Ensure we poll again to return the peeked stream (at least).
                cx.waker().wake_by_ref();
            }

            let match_seq_num = self.opts.match_seq_num;
            if let Some(m) = self.opts.match_seq_num.as_mut() {
                *m += batch.len() as u64
            }

            // Reset the linger sleep future since the old one is polled ready.
            self.linger_sleep = self.opts.linger_sleep_fut();

            Poll::Ready(Some(types::AppendInput {
                records: batch,
                match_seq_num,
                fencing_token: self.opts.fencing_token.clone(),
            }))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytesize::ByteSize;
    use futures::StreamExt as _;
    use rstest::rstest;
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::UnboundedReceiverStream;

    use crate::{
        streams::{AppendRecordStream, AppendRecordStreamOpts},
        types,
    };

    #[rstest]
    #[case(Some(2), None)]
    #[case(None, Some(ByteSize::b(30)))]
    #[case(Some(2), Some(ByteSize::b(100)))]
    #[case(Some(10), Some(ByteSize::b(30)))]
    #[tokio::test]
    async fn test_append_record_stream_batching(
        #[case] max_batch_records: Option<usize>,
        #[case] max_batch_size: Option<ByteSize>,
    ) {
        let stream_iter = (0..100).map(|i| types::AppendRecord::new(format!("r_{i}")));
        let stream = futures::stream::iter(stream_iter);

        let mut opts = AppendRecordStreamOpts::new();
        if let Some(max_batch_records) = max_batch_records {
            opts = opts.with_max_batch_records(max_batch_records);
        }
        if let Some(max_batch_size) = max_batch_size {
            opts = opts.with_max_batch_size(max_batch_size);
        }

        let batch_stream = AppendRecordStream::new(stream, opts).unwrap();

        let batches = batch_stream
            .map(|batch| batch.records)
            .collect::<Vec<_>>()
            .await;

        let mut i = 0;
        for batch in batches {
            assert_eq!(batch.len(), 2);
            for record in batch {
                assert_eq!(record.body, format!("r_{i}").into_bytes());
                i += 1;
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_append_record_stream_linger() {
        let (stream_tx, stream_rx) = mpsc::unbounded_channel::<types::AppendRecord>();
        let mut i = 0;

        let collect_batches_handle = tokio::spawn(async move {
            let batch_stream = AppendRecordStream::new(
                UnboundedReceiverStream::new(stream_rx),
                AppendRecordStreamOpts::new().with_linger(Duration::from_secs(2)),
            )
            .unwrap();

            batch_stream
                .map(|batch| {
                    batch
                        .records
                        .into_iter()
                        .map(|rec| rec.body)
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>()
                .await
        });

        let mut send_next = || {
            stream_tx
                .send(types::AppendRecord::new(format!("r_{i}")))
                .unwrap();
            i += 1;
        };

        async fn sleep_secs(secs: u64) {
            let dur = Duration::from_secs(secs) + Duration::from_millis(10);
            tokio::time::sleep(dur).await;
        }

        send_next();
        send_next();

        sleep_secs(2).await;

        send_next();

        sleep_secs(1).await;

        send_next();

        sleep_secs(1).await;

        send_next();
        std::mem::drop(stream_tx); // Should close the stream

        let batches = collect_batches_handle.await.unwrap();

        let expected_batches = vec![
            vec![b"r_0".to_owned(), b"r_1".to_owned()],
            vec![b"r_2".to_owned(), b"r_3".to_owned()],
            vec![b"r_4".to_owned()],
        ];

        assert_eq!(batches, expected_batches);
    }
}

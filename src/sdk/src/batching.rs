//! Append records batching stream.
//!
//! [`StreamClient::append_session`] accepts a stream of [`AppendInput`]s which
//! requires a user to batch records into [`AppendRecordBatch`]es. This module
//! provides a way to smartly batch [`AppendRecord`]s based on size limits and
//! linger duration.
//!
//! The stream enforces the provided fencing token (if any) and auto-increments
//! matching sequence number for concurrency control.
//!
//! # Example usage
//!
//! ```no_run
//! # use s2::client::*;
//! # use s2::types::*;
//! # use s2::batching::*;
//! # use std::time::Duration;
//! # let config = ClientConfig::new("token");
//! # let basin: BasinName = "my-favorite-basin".parse().unwrap();
//! # let stream_client = StreamClient::new(config, basin, "stream");
//! # let fencing_token = FencingToken::generate(16).unwrap();
//! let append_records_stream = futures::stream::iter([
//!     AppendRecord::new("hello").unwrap(),
//!     AppendRecord::new("bye").unwrap(),
//!     // ...
//! ]);
//!
//! let batching_opts = AppendRecordsBatchingOpts::new()
//!     .with_max_batch_records(100)
//!     .with_linger(Duration::from_millis(100))
//!     .with_fencing_token(Some(fencing_token))
//!     .with_match_seq_num(Some(10));
//!
//! let batching_stream = AppendRecordsBatchingStream::new(
//!     append_records_stream,
//!     batching_opts,
//! );
//!
//! # let _ = async move {
//! let ack_stream = stream_client.append_session(batching_stream).await?;
//! # return Ok::<(), ClientError>(()); };
//! ```
//!
//! [`StreamClient::append_session`]: crate::client::StreamClient::append_session
//! [`AppendInput`]: crate::types::AppendInput
//! [`AppendRecordBatch`]: crate::types::AppendRecordBatch
//! [`AppendRecord`]: crate::types::AppendRecord

use std::{
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use futures::{Stream, StreamExt};
use s2_types::types;

/// Options to configure batching scheme for [`AppendRecordsBatchingStream`].
#[derive(Debug, Clone)]
pub struct AppendRecordsBatchingOpts {
    max_batch_records: usize,
    #[cfg(test)]
    max_batch_bytes: u64,
    match_seq_num: Option<u64>,
    fencing_token: Option<types::FencingToken>,
    linger_duration: Duration,
}

impl Default for AppendRecordsBatchingOpts {
    fn default() -> Self {
        Self {
            max_batch_records: 1000,
            #[cfg(test)]
            max_batch_bytes: types::AppendRecordBatch::MAX_BYTES,
            match_seq_num: None,
            fencing_token: None,
            linger_duration: Duration::from_millis(5),
        }
    }
}

impl AppendRecordsBatchingOpts {
    /// Construct an options struct with defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Maximum number of records in a batch.
    pub fn with_max_batch_records(self, max_batch_records: usize) -> Self {
        assert!(
            max_batch_records > 0 && max_batch_records <= types::AppendRecordBatch::MAX_CAPACITY,
            "Batch capacity must be between 1 and 1000"
        );
        Self {
            max_batch_records,
            ..self
        }
    }

    /// Maximum size of a batch in bytes.
    #[cfg(test)]
    pub fn with_max_batch_bytes(self, max_batch_bytes: u64) -> Self {
        assert!(
            max_batch_bytes > 0 && max_batch_bytes <= types::AppendRecordBatch::MAX_BYTES,
            "Batch capacity must be between 1 byte and 1 MiB"
        );
        Self {
            max_batch_bytes,
            ..self
        }
    }

    /// Enforce that the sequence number issued to the first record matches.
    ///
    /// This is incremented automatically for each batch.
    pub fn with_match_seq_num(self, match_seq_num: Option<u64>) -> Self {
        Self {
            match_seq_num,
            ..self
        }
    }

    /// Enforce a fencing token.
    pub fn with_fencing_token(self, fencing_token: Option<types::FencingToken>) -> Self {
        Self {
            fencing_token,
            ..self
        }
    }

    /// Linger duration for records before flushing.
    ///
    /// A linger duration of 5ms is set by default. Set to `Duration::ZERO`
    /// to disable.
    pub fn with_linger(self, linger_duration: impl Into<Duration>) -> Self {
        Self {
            linger_duration: linger_duration.into(),
            ..self
        }
    }
}

/// Wrap a stream of [`AppendRecord`]s as a stream of [`AppendInput`]s by
/// smartly batching records together based on batching options provided by
/// [`AppendRecordsBatchingOpts`].
///
/// See the module level documentation for detailed usage.
///
/// [`AppendRecord`]: crate::types::AppendRecord
/// [`AppendInput`]: crate::types::AppendInput
pub struct AppendRecordsBatchingStream(Pin<Box<dyn Stream<Item = types::AppendInput> + Send>>);

impl AppendRecordsBatchingStream {
    /// Create a new batching stream.
    pub fn new<R, S>(stream: S, opts: AppendRecordsBatchingOpts) -> Self
    where
        R: 'static + Into<types::AppendRecord>,
        S: 'static + Send + Stream<Item = R> + Unpin,
    {
        Self(Box::pin(append_records_batching_stream(stream, opts)))
    }
}

impl Stream for AppendRecordsBatchingStream {
    type Item = types::AppendInput;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_next_unpin(cx)
    }
}

fn append_records_batching_stream<R, S>(
    mut stream: S,
    opts: AppendRecordsBatchingOpts,
) -> impl Stream<Item = types::AppendInput> + Send
where
    R: Into<types::AppendRecord>,
    S: 'static + Send + Stream<Item = R> + Unpin,
{
    async_stream::stream! {
        let mut terminated = false;
        let mut batch_builder = BatchBuilder::new(&opts);

        let batch_deadline = tokio::time::sleep(Duration::ZERO);
        tokio::pin!(batch_deadline);

        while !terminated {
            while !batch_builder.is_full() {
                if batch_builder.len() == 1 {
                    // Start the timer when the first record is added.
                    batch_deadline
                        .as_mut()
                        .reset(tokio::time::Instant::now() + opts.linger_duration);
                }

                tokio::select! {
                    biased;
                    next = stream.next() => {
                        if let Some(record) = next {
                            batch_builder.push(record);
                        } else {
                            terminated = true;
                            break;
                        }
                    },
                    _ = &mut batch_deadline, if !batch_builder.is_empty() => {
                        break;
                    }
                };
            }

            if let Some(input) = batch_builder.flush() {
                yield input;
            }
        }
    }
}

struct BatchBuilder<'a> {
    opts: &'a AppendRecordsBatchingOpts,
    peeked_record: Option<types::AppendRecord>,
    next_match_seq_num: Option<u64>,
    batch: types::AppendRecordBatch,
}

impl<'a> BatchBuilder<'a> {
    pub fn new<'b: 'a>(opts: &'b AppendRecordsBatchingOpts) -> Self {
        Self {
            peeked_record: None,
            next_match_seq_num: opts.match_seq_num,
            batch: Self::new_batch(opts),
            opts,
        }
    }

    #[cfg(not(test))]
    fn new_batch(opts: &AppendRecordsBatchingOpts) -> types::AppendRecordBatch {
        types::AppendRecordBatch::with_max_capacity(opts.max_batch_records)
    }

    #[cfg(test)]
    fn new_batch(opts: &AppendRecordsBatchingOpts) -> types::AppendRecordBatch {
        types::AppendRecordBatch::with_max_capacity_and_bytes(
            opts.max_batch_records,
            opts.max_batch_bytes,
        )
    }

    pub fn push(&mut self, record: impl Into<types::AppendRecord>) {
        if let Err(record) = self.batch.push(record) {
            let ret = self.peeked_record.replace(record);
            assert_eq!(ret, None);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.batch.is_empty()
    }

    pub fn len(&self) -> usize {
        self.batch.len()
    }

    pub fn is_full(&self) -> bool {
        self.batch.is_full() || self.peeked_record.is_some()
    }

    pub fn flush(&mut self) -> Option<types::AppendInput> {
        let ret = if self.batch.is_empty() {
            None
        } else {
            let match_seq_num = self.next_match_seq_num;
            if let Some(next_match_seq_num) = self.next_match_seq_num.as_mut() {
                *next_match_seq_num += self.batch.len() as u64;
            }

            let records = std::mem::replace(&mut self.batch, Self::new_batch(self.opts));
            Some(types::AppendInput {
                records,
                match_seq_num,
                fencing_token: self.opts.fencing_token.clone(),
            })
        };

        if let Some(record) = self.peeked_record.take() {
            self.push(record);
        }

        // If the peeked record could not be moved into the batch, it doesn't
        // fit size limits. This shouldn't happen though, since each append
        // record is validated for size before creating it.
        assert_eq!(self.peeked_record, None);

        ret
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytes::Bytes;
    use futures::StreamExt as _;
    use rstest::rstest;
    use s2_types::types::{self, AppendInput, AppendRecordBatch};
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::UnboundedReceiverStream;

    use super::{AppendRecordsBatchingOpts, AppendRecordsBatchingStream};

    #[rstest]
    #[case(Some(2), None)]
    #[case(None, Some(30))]
    #[case(Some(2), Some(100))]
    #[case(Some(10), Some(30))]
    #[tokio::test]
    async fn test_append_record_batching_mechanics(
        #[case] max_batch_records: Option<usize>,
        #[case] max_batch_bytes: Option<u64>,
    ) {
        let stream_iter = (0..100)
            .map(|i| {
                let body = format!("r_{i}");
                if let Some(max_batch_size) = max_batch_bytes {
                    types::AppendRecord::with_max_bytes(max_batch_size, body)
                } else {
                    types::AppendRecord::new(body)
                }
                .unwrap()
            })
            .collect::<Vec<_>>();
        let stream = futures::stream::iter(stream_iter);

        let mut opts = AppendRecordsBatchingOpts::new().with_linger(Duration::ZERO);
        if let Some(max_batch_records) = max_batch_records {
            opts = opts.with_max_batch_records(max_batch_records);
        }
        if let Some(max_batch_size) = max_batch_bytes {
            opts = opts.with_max_batch_bytes(max_batch_size);
        }

        let batch_stream = AppendRecordsBatchingStream::new(stream, opts);

        let batches = batch_stream
            .map(|batch| batch.records)
            .collect::<Vec<_>>()
            .await;

        let mut i = 0;
        for batch in batches {
            assert_eq!(batch.len(), 2);
            for record in batch {
                assert_eq!(record.into_parts().body, format!("r_{i}").into_bytes());
                i += 1;
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_append_record_batching_linger() {
        let (stream_tx, stream_rx) = mpsc::unbounded_channel::<types::AppendRecord>();
        let mut i = 0;

        let size_limit = 40;

        let collect_batches_handle = tokio::spawn(async move {
            let batch_stream = AppendRecordsBatchingStream::new(
                UnboundedReceiverStream::new(stream_rx),
                AppendRecordsBatchingOpts::new()
                    .with_linger(Duration::from_secs(2))
                    .with_max_batch_records(3)
                    .with_max_batch_bytes(size_limit),
            );

            batch_stream
                .map(|batch| {
                    batch
                        .records
                        .into_iter()
                        .map(|rec| rec.into_parts().body)
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>()
                .await
        });

        let mut send_next = |padding: Option<&str>| {
            let mut record =
                types::AppendRecord::with_max_bytes(size_limit, format!("r_{i}")).unwrap();
            if let Some(padding) = padding {
                // The padding exists just to increase the size of record in
                // order to test the size limits.
                record = record
                    .with_headers(vec![types::Header::new("padding", padding.to_owned())])
                    .unwrap();
            }
            stream_tx.send(record).unwrap();
            i += 1;
        };

        async fn sleep_secs(secs: u64) {
            let dur = Duration::from_secs(secs) + Duration::from_millis(10);
            tokio::time::sleep(dur).await;
        }

        send_next(None);
        send_next(None);

        sleep_secs(2).await;

        send_next(None);

        // Waiting for a short time before sending next record.
        sleep_secs(1).await;

        send_next(None);

        sleep_secs(1).await;

        // Checking batch count limits here. The first 3 records should be
        // flushed immediately.
        send_next(None);
        send_next(None);
        send_next(None);
        send_next(None);

        // Waiting for a long time before sending any records.
        sleep_secs(200).await;

        // Checking size limits here. The first record should be flushed
        // immediately.
        send_next(Some("large string"));
        send_next(None);

        std::mem::drop(stream_tx); // Should close the stream

        let batches = collect_batches_handle.await.unwrap();

        let expected_batches: Vec<Vec<Bytes>> = vec![
            vec!["r_0".into(), "r_1".into()],
            vec!["r_2".into(), "r_3".into()],
            vec!["r_4".into(), "r_5".into(), "r_6".into()],
            vec!["r_7".into()],
            vec!["r_8".into()],
            vec!["r_9".into()],
        ];

        assert_eq!(batches, expected_batches);
    }

    #[tokio::test]
    #[should_panic]
    async fn test_append_record_batching_panic_size_limits() {
        let size_limit = 1;
        let record =
            types::AppendRecord::with_max_bytes(size_limit, "too long to fit into size limits")
                .unwrap();

        let mut batch_stream = AppendRecordsBatchingStream::new(
            futures::stream::iter([record]),
            AppendRecordsBatchingOpts::new().with_max_batch_bytes(size_limit),
        );

        let _ = batch_stream.next().await;
    }

    #[tokio::test]
    async fn test_append_record_batching_append_input_opts() {
        let test_record = types::AppendRecord::new("a").unwrap();

        let total_records = 12;
        let test_records = (0..total_records)
            .map(|_| test_record.clone())
            .collect::<Vec<_>>();

        let expected_fencing_token = types::FencingToken::new("hello").unwrap();
        let mut expected_match_seq_num = 10;

        let num_batch_records = 3;

        let batch_stream = AppendRecordsBatchingStream::new(
            futures::stream::iter(test_records),
            AppendRecordsBatchingOpts::new()
                .with_max_batch_records(num_batch_records)
                .with_fencing_token(Some(expected_fencing_token.clone()))
                .with_match_seq_num(Some(expected_match_seq_num)),
        );

        let batches = batch_stream.collect::<Vec<_>>().await;

        assert_eq!(batches.len(), total_records / num_batch_records);

        let expected_batch =
            AppendRecordBatch::try_from_iter((0..num_batch_records).map(|_| test_record.clone()))
                .unwrap();

        for input in batches {
            let AppendInput {
                records,
                match_seq_num,
                fencing_token,
            } = input;
            assert_eq!(records, expected_batch);
            assert_eq!(fencing_token.as_ref(), Some(&expected_fencing_token));
            assert_eq!(match_seq_num, Some(expected_match_seq_num));
            expected_match_seq_num += num_batch_records as u64;
        }
    }
}

use std::{
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use bytesize::ByteSize;
use futures::{Stream, StreamExt};

use crate::types::{self, MeteredSize as _};

/// Options to configure [`AppendRecordStream`].
#[derive(Debug, Clone)]
pub struct AppendRecordStreamOpts {
    max_batch_records: usize,
    max_batch_size: ByteSize,
    match_seq_num: Option<u64>,
    fencing_token: Option<Vec<u8>>,
    linger_duration: Duration,
}

impl Default for AppendRecordStreamOpts {
    fn default() -> Self {
        Self {
            max_batch_records: 1000,
            max_batch_size: ByteSize::mib(1),
            match_seq_num: None,
            fencing_token: None,
            linger_duration: Duration::from_millis(5),
        }
    }
}

impl AppendRecordStreamOpts {
    /// Construct an options struct with defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Maximum number of records in a batch.
    pub fn with_max_batch_records(
        self,
        max_batch_records: usize,
    ) -> Result<Self, AppendRecordStreamOptsError> {
        if max_batch_records > 1000 {
            return Err(AppendRecordStreamOptsError::BatchRecordsCountTooLarge);
        }

        Ok(Self {
            max_batch_records,
            ..self
        })
    }

    /// Maximum size of a batch in bytes.
    pub fn with_max_batch_size(
        self,
        max_batch_size: impl Into<ByteSize>,
    ) -> Result<Self, AppendRecordStreamOptsError> {
        let max_batch_size = max_batch_size.into();

        if max_batch_size > ByteSize::mib(1) {
            return Err(AppendRecordStreamOptsError::BatchSizeTooLarge);
        }

        Ok(Self {
            max_batch_size,
            ..self
        })
    }

    /// Enforce that the sequence number issued to the first record matches.
    ///
    /// This is incremented automatically for each batch.
    pub fn with_match_seq_num(self, match_seq_num: impl Into<u64>) -> Self {
        Self {
            match_seq_num: Some(match_seq_num.into()),
            ..self
        }
    }

    /// Enforce a fencing token.
    pub fn with_fencing_token(self, fencing_token: impl Into<Vec<u8>>) -> Self {
        Self {
            fencing_token: Some(fencing_token.into()),
            ..self
        }
    }

    /// Linger duration for records before flushing.
    ///
    /// A linger of 5ms is set by default. Set to `Duration::ZERO` to disable.
    pub fn with_linger(self, linger_duration: impl Into<Duration>) -> Self {
        Self {
            linger_duration: linger_duration.into(),
            ..self
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AppendRecordStreamOptsError {
    #[error("max_batch_records should not be more then 1000")]
    BatchRecordsCountTooLarge,
    #[error("max_batch_size should not be more than 1 Mib")]
    BatchSizeTooLarge,
}

/// Wrapper over a stream of append records that can be sent over to
/// [`crate::client::StreamClient::append_session`].
pub struct AppendRecordStream {
    inner: Pin<Box<dyn Stream<Item = types::AppendInput> + Send>>,
}

impl AppendRecordStream {
    /// Try constructing a new [`AppendRecordStream`] from the given stream and options.
    pub fn new<R, S>(stream: S, opts: AppendRecordStreamOpts) -> Self
    where
        R: Into<types::AppendRecord>,
        S: 'static + Send + Stream<Item = R> + Unpin,
    {
        let inner = Box::pin(async_stream::stream! {
            let mut terminated = false;
            let mut peeked_record: Option<types::AppendRecord> = None;
            let mut stream = stream;
            let mut next_match_seq_num = opts.match_seq_num;
            let mut next_linger_duration = opts.linger_duration;

            while !terminated {
                let mut batch = Vec::with_capacity(opts.max_batch_records);
                let mut batch_size = ByteSize::b(0);

                if let Some(record) = peeked_record.take() {
                    Self::push_record_to_batch(
                        &opts,
                        record,
                        &mut batch,
                        &mut batch_size,
                        &mut peeked_record,
                    );
                }

                let linger_timeout = tokio::time::sleep(next_linger_duration);
                tokio::pin!(linger_timeout);

                while batch.len() < opts.max_batch_records && peeked_record.is_none() {
                    // Before we go to the select statement, we want to ensure
                    // that the outer loop does not keep the CPU busy in case
                    // the next_linger_duration is 0. So we await the first
                    // record here in case the batch is empty.
                    if batch.is_empty() {
                        if let Some(record) = stream.next().await {
                            Self::push_record_to_batch(
                                &opts,
                                record.into(),
                                &mut batch,
                                &mut batch_size,
                                &mut peeked_record,
                            );
                        } else {
                            terminated = true;
                            break;
                        }
                    } else {
                        tokio::select! {
                            biased;
                            next = stream.next() => {
                                if let Some(record) = next {
                                    Self::push_record_to_batch(
                                        &opts,
                                        record.into(),
                                        &mut batch,
                                        &mut batch_size,
                                        &mut peeked_record,
                                    );
                                } else {
                                    terminated = true;
                                    break;
                                }
                            },
                            _ = &mut linger_timeout => {
                                break;
                            }
                        };
                    }
                }

                // Some kind of limit has reached at this point.
                next_linger_duration = if batch.is_empty() {
                    assert!(
                        peeked_record.is_none(),
                        "dangling peeked record does not fit into size limits"
                    );

                    // Next linger duration should be none.
                    Duration::ZERO
                } else {
                    let batch_size = batch.len() as u64;

                    yield types::AppendInput {
                        records: batch,
                        match_seq_num: next_match_seq_num,
                        fencing_token: opts.fencing_token.clone(),
                    };

                    if let Some(m) = next_match_seq_num.as_mut() {
                        *m += batch_size;
                    }

                    // Next linger durationis same as the original.
                    opts.linger_duration
                };
            }
        });

        Self { inner }
    }

    fn push_record_to_batch(
        opts: &AppendRecordStreamOpts,
        record: types::AppendRecord,
        batch: &mut Vec<types::AppendRecord>,
        batch_size: &mut ByteSize,
        peeked_record: &mut Option<types::AppendRecord>,
    ) {
        assert!(peeked_record.is_none());
        let record_size = record.metered_size();
        if *batch_size + record_size > opts.max_batch_size {
            // Set the peeked record and move on.
            let _ = std::mem::replace(peeked_record, Some(record));
        } else {
            *batch_size += record_size;
            batch.push(record);
        }
    }
}

impl Stream for AppendRecordStream {
    type Item = types::AppendInput;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
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

        let mut opts = AppendRecordStreamOpts::new().with_linger(Duration::ZERO);
        if let Some(max_batch_records) = max_batch_records {
            opts = opts.with_max_batch_records(max_batch_records).unwrap();
        }
        if let Some(max_batch_size) = max_batch_size {
            opts = opts.with_max_batch_size(max_batch_size).unwrap();
        }

        let batch_stream = AppendRecordStream::new(stream, opts);

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
                AppendRecordStreamOpts::new()
                    .with_linger(Duration::from_secs(2))
                    .with_max_batch_records(3)
                    .unwrap()
                    .with_max_batch_size(ByteSize::b(40))
                    .unwrap(),
            );

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

        let mut send_next = |padding: Option<&str>| {
            let mut record = types::AppendRecord::new(format!("r_{i}"));
            if let Some(padding) = padding {
                // The padding exists just to increase the size of record in
                // order to test the size limits.
                record = record.with_headers(vec![types::Header::new("padding", padding)]);
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

        let expected_batches = vec![
            vec![b"r_0".to_owned(), b"r_1".to_owned()],
            vec![b"r_2".to_owned(), b"r_3".to_owned()],
            vec![b"r_4".to_owned(), b"r_5".to_owned(), b"r_6".to_owned()],
            vec![b"r_7".to_owned()],
            vec![b"r_8".to_owned()],
            vec![b"r_9".to_owned()],
        ];

        assert_eq!(batches, expected_batches);
    }
}

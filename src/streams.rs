use std::{
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use bytesize::ByteSize;
use futures::{Stream, StreamExt};
use tokio::time::{Interval, MissedTickBehavior};

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
    /// Linger time for ready records to send together as a batch.
    pub linger_time: Option<Duration>,
}

impl Default for AppendRecordStreamOpts {
    fn default() -> Self {
        Self {
            max_batch_records: 1000,
            max_batch_size: ByteSize::mib(1),
            match_seq_num: None,
            fencing_token: None,
            linger_time: None,
        }
    }
}

impl AppendRecordStreamOpts {
    /// Construct an options struct with defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Construct from existing options with the new maximum batch records.
    pub fn with_max_batch_records(self, max_batch_records: impl Into<usize>) -> Self {
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
    pub fn with_linger_time(self, linger_time: impl Into<Duration>) -> Self {
        Self {
            linger_time: Some(linger_time.into()),
            ..self
        }
    }

    fn new_linger_interval(&self) -> Option<Interval> {
        self.linger_time.map(|duration| {
            let mut int = tokio::time::interval(duration);
            int.set_missed_tick_behavior(MissedTickBehavior::Delay);
            int
        })
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
    linger_interval: Option<Interval>,
    ignore_linger: bool,
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
            linger_interval: opts.new_linger_interval(),
            ignore_linger: false,
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

        if self.ignore_linger {
            self.ignore_linger = false;
        } else {
            if self
                .linger_interval
                .as_mut()
                .is_some_and(|int| int.poll_tick(cx).is_pending())
            {
                return Poll::Pending;
            }
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
                self.ignore_linger = true;
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

            Poll::Ready(Some(types::AppendInput {
                records: batch,
                match_seq_num,
                fencing_token: self.opts.fencing_token.clone(),
            }))
        }
    }
}

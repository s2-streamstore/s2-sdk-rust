use std::{
    pin::Pin,
    task::{Context, Poll},
};

use bytesize::ByteSize;
use futures::{Stream, StreamExt};

use crate::types;

#[derive(Debug, Clone)]
pub struct AppendRecordStreamOpts {
    pub max_batch_records: usize,
    pub max_batch_size: ByteSize,
    // AppendInput params:
    pub match_seq_num: Option<u64>,
    pub fencing_token: Option<Vec<u8>>,
}

impl Default for AppendRecordStreamOpts {
    fn default() -> Self {
        Self {
            max_batch_records: 1000,
            max_batch_size: ByteSize::mib(1),
            match_seq_num: None,
            fencing_token: None,
        }
    }
}

impl AppendRecordStreamOpts {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_max_batch_records(self, max_batch_records: impl Into<usize>) -> Self {
        Self {
            max_batch_records: max_batch_records.into(),
            ..self
        }
    }

    pub fn with_max_batch_size(self, max_batch_size: impl Into<ByteSize>) -> Self {
        Self {
            max_batch_size: max_batch_size.into(),
            ..self
        }
    }

    pub fn with_match_seq_num(self, match_seq_num: impl Into<u64>) -> Self {
        Self {
            match_seq_num: Some(match_seq_num.into()),
            ..self
        }
    }

    pub fn with_fencing_token(self, fencing_token: impl Into<Vec<u8>>) -> Self {
        Self {
            fencing_token: Some(fencing_token.into()),
            ..self
        }
    }
}

pub struct AppendRecordStream<S>
where
    S: 'static + Send + Stream<Item = types::AppendRecord> + Unpin,
{
    stream: S,
    peeked: Option<types::AppendRecord>,
    terminated: bool,
    opts: AppendRecordStreamOpts,
}

impl<S> AppendRecordStream<S>
where
    S: 'static + Send + Stream<Item = types::AppendRecord> + Unpin,
{
    pub fn new(stream: S, opts: AppendRecordStreamOpts) -> Self {
        Self {
            stream,
            peeked: None,
            terminated: false,
            opts,
        }
    }

    fn push_record_to_batch(
        &self,
        record: types::AppendRecord,
        batch: &mut Vec<types::AppendRecord>,
        batch_size: &mut ByteSize,
    ) -> Option<types::AppendRecord> {
        let record_size = ByteSize::b(
            (record.body.len()
                + record
                    .headers
                    .iter()
                    .map(|h| h.name.len() + h.value.len())
                    .sum::<usize>()) as u64,
        );
        if *batch_size + record_size > self.opts.max_batch_size {
            Some(record)
        } else {
            *batch_size += record_size;
            batch.push(record);
            None
        }
    }
}

impl<S> Stream for AppendRecordStream<S>
where
    S: 'static + Send + Stream<Item = types::AppendRecord> + Unpin,
{
    type Item = types::AppendInput;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.terminated {
            return Poll::Ready(None);
        }

        let mut batch = Vec::with_capacity(self.opts.max_batch_records);
        let mut batch_size = ByteSize::b(0);

        if let Some(peeked) = self.peeked.take() {
            assert!(
                self.push_record_to_batch(peeked, &mut batch, &mut batch_size)
                    .is_none(),
                "peeked record cannot be pushed due to large size"
            );
        }

        while batch.len() < self.opts.max_batch_records && self.peeked.is_none() {
            match self.stream.poll_next_unpin(cx) {
                Poll::Pending => break,
                Poll::Ready(None) => {
                    self.terminated = true;
                    break;
                }
                Poll::Ready(Some(record)) => {
                    self.peeked = self.push_record_to_batch(record, &mut batch, &mut batch_size);
                }
            }
        }

        if batch.is_empty() {
            assert!(
                self.peeked.is_none(),
                "dangling peeked record does not fit into size limits"
            );

            if self.terminated {
                Poll::Ready(None)
            } else {
                Poll::Pending
            }
        } else {
            if self.peeked.is_some() {
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

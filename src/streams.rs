use std::{
    pin::Pin,
    task::{Context, Poll},
};

use bytesize::ByteSize;
use futures::{Stream, StreamExt};

use crate::types;

#[derive(Debug, Clone)]
pub struct AppendRecordBatchingScheme {
    pub max_batch_records: usize,
    pub max_batch_size: ByteSize,
}

impl Default for AppendRecordBatchingScheme {
    fn default() -> Self {
        Self {
            max_batch_records: 1000,
            max_batch_size: ByteSize::mib(1),
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
    scheme: AppendRecordBatchingScheme,
}

impl<S> AppendRecordStream<S>
where
    S: 'static + Send + Stream<Item = types::AppendRecord> + Unpin,
{
    pub fn new(stream: S, scheme: AppendRecordBatchingScheme) -> Self {
        Self {
            stream,
            peeked: None,
            terminated: false,
            scheme,
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
        if *batch_size + record_size > self.scheme.max_batch_size {
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

        let mut batch = Vec::with_capacity(self.scheme.max_batch_records);
        let mut batch_size = ByteSize::b(0);

        if let Some(peeked) = self.peeked.take() {
            assert!(
                self.push_record_to_batch(peeked, &mut batch, &mut batch_size)
                    .is_none(),
                "peeked record cannot be pushed due to large size"
            );
        }

        while batch.len() < self.scheme.max_batch_records && self.peeked.is_none() {
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

            Poll::Ready(Some(types::AppendInput::new(batch)))
        }
    }
}

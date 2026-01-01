//! High-level producer for appending records to streams.
//!
//! See [`Producer`].

use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{FutureExt, StreamExt, stream::FuturesUnordered};
use s2_common::caps::RECORD_BATCH_MAX;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::task::AbortOnDropHandle;

use crate::{
    batching::{AppendInputs, AppendRecordBatches, BatchingConfig},
    session::AppendSession,
    types::{AppendAck, AppendInput, AppendRecord, FencingToken, MeteredBytes, S2Error},
};

/// A [Future] that resolves to an acknowledgement once the record is appended.
pub struct RecordSubmitTicket {
    rx: oneshot::Receiver<Result<IndexedAppendAck, S2Error>>,
}

impl Future for RecordSubmitTicket {
    type Output = Result<IndexedAppendAck, S2Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.rx).poll(cx) {
            Poll::Ready(Ok(res)) => Poll::Ready(res),
            Poll::Ready(Err(_)) => Poll::Ready(Err(ProducerError::Dropped.into())),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Acknowledgement for an appended record.
#[derive(Debug, Clone)]
pub struct IndexedAppendAck {
    /// Sequence number assigned to the record.
    pub seq_num: u64,
    /// Acknowledgement for the containing batch.
    pub batch: AppendAck,
}

#[derive(Debug, Clone, Default)]
#[non_exhaustive]
/// Configuration for [Producer].
pub struct ProducerConfig {
    /// Configuration for batching records into [AppendInput]s before appending.
    pub batching: BatchingConfig,
    /// Fencing token for all [AppendInput]s.
    ///
    /// Defaults to `None`.
    pub fencing_token: Option<FencingToken>,
    /// Match sequence number for the initial [AppendInput]. It will be auto-incremented for the
    /// subsequent ones.
    ///
    /// Defaults to `None`.
    pub match_seq_num: Option<u64>,
}

impl ProducerConfig {
    /// Create a new [ProducerConfig] with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the configuration for batching records into [AppendInput]s before appending.
    pub fn with_batching(self, batching: BatchingConfig) -> Self {
        Self { batching, ..self }
    }

    /// Set the fencing token for all [AppendInput]s.
    pub fn with_fencing_token(self, fencing_token: FencingToken) -> Self {
        Self {
            fencing_token: Some(fencing_token),
            ..self
        }
    }

    /// Set the match sequence number for the initial [AppendInput]. It will be auto-incremented
    /// for the subsequent ones.
    pub fn with_match_seq_num(self, match_seq_num: u64) -> Self {
        Self {
            match_seq_num: Some(match_seq_num),
            ..self
        }
    }
}

/// High-level interface for submitting individual [AppendRecord]s.
///
/// Wraps an [AppendSession] and handles batching records into [AppendInput]s automatically based on
/// the provided [configuration](ProducerConfig).
pub struct Producer {
    cmd_tx: mpsc::Sender<Command>,
    _handle: AbortOnDropHandle<()>,
}

impl Producer {
    /// Create a new [Producer].
    pub fn new(session: AppendSession, config: ProducerConfig) -> Self {
        let (cmd_tx, cmd_rx) = mpsc::channel::<Command>(RECORD_BATCH_MAX.count);
        let _handle = AbortOnDropHandle::new(tokio::spawn(Self::run(session, config, cmd_rx)));
        Self { cmd_tx, _handle }
    }

    /// Submit a record for appending.
    ///
    /// **Note**: You must call [Producer::close] if want to ensure all submitted records are
    /// appended.
    pub async fn submit(&self, record: AppendRecord) -> Result<RecordSubmitTicket, S2Error> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::Submit { record, ack_tx })
            .await
            .map_err(|_| ProducerError::Closed)?;
        Ok(RecordSubmitTicket { rx: ack_rx })
    }

    /// Close the producer and wait for all submitted records to be appended.
    pub async fn close(self) -> Result<(), S2Error> {
        let (done_tx, done_rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::Close { done_tx })
            .await
            .map_err(|_| ProducerError::Closed)?;
        done_rx.await.map_err(|_| ProducerError::Closed)?
    }

    async fn run(
        session: AppendSession,
        config: ProducerConfig,
        mut cmd_rx: mpsc::Receiver<Command>,
    ) {
        let (record_tx, record_rx) = mpsc::channel::<AppendRecord>(RECORD_BATCH_MAX.count);
        let mut inputs = AppendInputs {
            batches: AppendRecordBatches::new(ReceiverStream::new(record_rx), config.batching),
            fencing_token: config.fencing_token,
            match_seq_num: config.match_seq_num,
        };

        let mut ack_txs: VecDeque<oneshot::Sender<Result<IndexedAppendAck, S2Error>>> =
            VecDeque::new();
        let mut claimable_tickets: FuturesUnordered<_> = FuturesUnordered::new();
        let mut close_tx: Option<oneshot::Sender<Result<(), S2Error>>> = None;
        let mut closing = false;
        let mut stashed_submission: Option<StashedSubmission> = None;
        let mut stashed_input: Option<AppendInput> = None;
        let mut stashed_input_bytes = 0;

        loop {
            tokio::select! {
                record_tx_permit = record_tx.reserve(), if stashed_submission.is_some() => {
                    let submission = stashed_submission
                        .take()
                        .expect("stashed_submission should not be None");
                    ack_txs.push_back(submission.ack_tx);
                    record_tx_permit
                        .expect("record_rx should not be closed")
                        .send(submission.record);
                }

                cmd = cmd_rx.recv(), if stashed_submission.is_none() => {
                    match cmd {
                        Some(Command::Submit { record, ack_tx }) => {
                            if closing {
                                let _ = ack_tx.send(
                                    Err(ProducerError::Closing.into())
                                );
                            } else {
                                stashed_submission = Some(StashedSubmission { record, ack_tx });
                            }
                        }
                        Some(Command::Close { done_tx }) => {
                            closing = true;
                            close_tx = Some(done_tx);
                        }
                        None => {
                            for ack_tx in ack_txs.drain(..) {
                                let _ = ack_tx.send(Err(ProducerError::Dropped.into()));
                            }
                            return;
                        }
                    }
                }

                input = inputs.next(), if stashed_input.is_none() => {
                    match input.expect("record_tx should not be closed") {
                        Ok(input) => {
                            stashed_input_bytes = input.records.metered_bytes() as u32;
                            stashed_input = Some(input);
                        }
                        Err(err) => {
                            for ack_tx in ack_txs.drain(..) {
                                let _ = ack_tx.send(Err(err.clone().into()));
                            }
                            if let Some(submission) = stashed_submission.take() {
                                let _ = submission.ack_tx.send(Err(err.clone().into()));
                            }
                            return;
                        }
                    }
                }

                session_permit = session.reserve(stashed_input_bytes), if stashed_input.is_some() => {
                    match session_permit {
                        Ok(session_permit) => {
                            let input = stashed_input
                                .take()
                                .expect("stashed_input should not be None");
                            let batch_len = input.records.len();
                            let ticket = session_permit.send(input);
                            claimable_tickets.push(ticket.map({
                                let ack_txs = ack_txs.drain(..batch_len).collect::<Vec<_>>();
                                |batch_ack| (batch_ack, ack_txs)
                            }));
                        }
                        Err(err) => {
                            for ack_tx in ack_txs.drain(..) {
                                let _ = ack_tx.send(Err(err.clone()));
                            }
                            if let Some(submission) = stashed_submission.take() {
                                let _ = submission.ack_tx.send(Err(err.clone()));
                            }
                            return;
                        }
                    }
                }

                Some((batch_ack, ack_txs)) = claimable_tickets.next() => {
                    dispatch_acks(batch_ack, ack_txs);
                }
            }

            if closing
                && ack_txs.is_empty()
                && claimable_tickets.is_empty()
                && stashed_submission.is_none()
                && stashed_input.is_none()
            {
                break;
            }
        }

        let session_close_res = session.close().await;

        if let Some(done_tx) = close_tx.take() {
            let _ = done_tx.send(session_close_res);
        }
    }
}

#[derive(Debug, Clone, thiserror::Error)]
enum ProducerError {
    #[error("producer already closed")]
    Closed,
    #[error("producer is closing")]
    Closing,
    #[error("producer dropped without calling close")]
    Dropped,
}

impl From<ProducerError> for S2Error {
    fn from(err: ProducerError) -> Self {
        S2Error::Client(err.to_string())
    }
}

enum Command {
    Submit {
        record: AppendRecord,
        ack_tx: oneshot::Sender<Result<IndexedAppendAck, S2Error>>,
    },
    Close {
        done_tx: oneshot::Sender<Result<(), S2Error>>,
    },
}

struct StashedSubmission {
    record: AppendRecord,
    ack_tx: oneshot::Sender<Result<IndexedAppendAck, S2Error>>,
}

fn dispatch_acks(
    batch_ack: Result<AppendAck, S2Error>,
    ack_txs: Vec<oneshot::Sender<Result<IndexedAppendAck, S2Error>>>,
) {
    match batch_ack {
        Ok(batch_ack) => {
            for (offset, ack_tx) in ack_txs.into_iter().enumerate() {
                let seq_num = batch_ack.start.seq_num + offset as u64;
                let _ = ack_tx.send(Ok(IndexedAppendAck {
                    seq_num,
                    batch: batch_ack.clone(),
                }));
            }
        }
        Err(err) => {
            for ack_tx in ack_txs {
                let _ = ack_tx.send(Err(err.clone()));
            }
        }
    }
}

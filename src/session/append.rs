use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use futures::StreamExt;
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot},
    time::Instant,
};
use tokio_muxt::{CoalesceMode, MuxTimer};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::task::AbortOnDropHandle;
use tracing::debug;

use crate::{
    api::{ApiError, BasinClient, Streaming, retry_builder},
    retry::RetryBackoffBuilder,
    types::{
        AppendAck, AppendInput, AppendRetryPolicy, MeteredBytes, S2Error, StreamName,
        StreamPosition,
    },
};

#[derive(Debug, thiserror::Error)]
pub enum AppendSessionError {
    #[error(transparent)]
    Api(#[from] ApiError),
    #[error("append acknowledgement timed out")]
    AckTimeout,
    #[error("server disconnected")]
    ServerDisconnected,
    #[error("response stream closed early while appends in flight")]
    StreamClosedEarly,
    #[error("session already closed")]
    SessionClosed,
    #[error("session is closing")]
    SessionClosing,
    #[error("session dropped without calling close")]
    SessionDropped,
}

impl AppendSessionError {
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Api(err) => err.is_retryable(),
            Self::AckTimeout => true,
            Self::ServerDisconnected => true,
            _ => false,
        }
    }
}

impl From<AppendSessionError> for S2Error {
    fn from(err: AppendSessionError) -> Self {
        match err {
            AppendSessionError::Api(api_err) => api_err.into(),
            other => S2Error::Client(other.to_string()),
        }
    }
}

/// A [Future] that resolves to an acknowledgement once the batch of records is appended.
pub struct BatchSubmitTicket {
    rx: oneshot::Receiver<Result<AppendAck, S2Error>>,
}

impl Future for BatchSubmitTicket {
    type Output = Result<AppendAck, S2Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.rx).poll(cx) {
            Poll::Ready(Ok(res)) => Poll::Ready(res),
            Poll::Ready(Err(_)) => Poll::Ready(Err(AppendSessionError::SessionDropped.into())),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
/// Configuration for an [AppendSession].
pub struct AppendSessionConfig {
    /// Limit on total metered bytes of unacknowledged [AppendInput]s held in memory.
    ///
    /// Defaults to `10MiB`.
    pub max_inflight_bytes: u32,
    /// Limit on number of unacknowledged [AppendInput]s held in memory.
    ///
    /// Defaults to `None`.
    pub max_inflight_batches: Option<u32>,
}

impl Default for AppendSessionConfig {
    fn default() -> Self {
        Self {
            max_inflight_bytes: 10 * 1024 * 1024,
            max_inflight_batches: None,
        }
    }
}

impl AppendSessionConfig {
    /// Create a new [AppendSessionConfig] with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the limit on total metered bytes of unacknowledged [AppendInput]s held in memory.
    pub fn with_max_inflight_bytes(self, max_inflight_bytes: u32) -> Self {
        Self {
            max_inflight_bytes,
            ..self
        }
    }

    /// Set the limit on number of unacknowledged [AppendInput]s held in memory.
    pub fn with_max_inflight_batches(self, max_inflight_batches: u32) -> Self {
        Self {
            max_inflight_batches: Some(max_inflight_batches),
            ..self
        }
    }
}

struct SessionState {
    cmd_rx: mpsc::Receiver<Command>,
    inflight_appends: VecDeque<InflightAppend>,
    inflight_bytes: usize,
    close_tx: Option<oneshot::Sender<Result<(), S2Error>>>,
    closing: bool,
    total_records: usize,
    total_acked_records: usize,
    prev_ack_end: Option<StreamPosition>,
}

/// A session for high-throughput appending with backpressure control. It can be created from
/// [append_session](crate::S2Stream::append_session).
///
/// Supports pipelining multiple [AppendInput]s while preserving submission order.
pub struct AppendSession {
    cmd_tx: mpsc::Sender<Command>,
    permits: InflightPermits,
    _handle: AbortOnDropHandle<()>,
}

impl AppendSession {
    pub(crate) fn new(
        client: BasinClient,
        stream: StreamName,
        config: AppendSessionConfig,
    ) -> Self {
        let buffer_size = config.max_inflight_batches.unwrap_or(100) as usize;
        let (cmd_tx, cmd_rx) = mpsc::channel(buffer_size);
        let permits = InflightPermits::new(config.max_inflight_batches, config.max_inflight_bytes);
        let retry_builder = retry_builder(&client.config.retry);
        let handle = AbortOnDropHandle::new(tokio::spawn(Self::run_session_with_retry(
            client,
            stream,
            cmd_rx,
            retry_builder,
            buffer_size,
        )));
        Self {
            cmd_tx,
            permits,
            _handle: handle,
        }
    }

    pub(crate) async fn reserve(&self, bytes: u32) -> Result<AppendSessionPermit<'_>, S2Error> {
        let inflight_permit = self.permits.acquire(bytes).await;
        let cmd_tx_permit = self
            .cmd_tx
            .reserve()
            .await
            .map_err(|_| AppendSessionError::SessionClosed)?;
        Ok(AppendSessionPermit {
            inflight_permit,
            cmd_tx_permit,
        })
    }

    /// Submit a batch of records for appending.
    ///
    /// **Note**: You must call [AppendSession::close] if want to ensure all submitted batches are
    /// appended.
    pub async fn submit(&self, input: AppendInput) -> Result<BatchSubmitTicket, S2Error> {
        let permit = self.reserve(input.records.metered_bytes() as u32).await?;
        Ok(permit.send(input))
    }

    /// Close the session and wait for all submitted batch of records to be appended.
    pub async fn close(self) -> Result<(), S2Error> {
        let (done_tx, done_rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::Close { done_tx })
            .await
            .map_err(|_| AppendSessionError::SessionClosed)?;
        done_rx
            .await
            .map_err(|_| AppendSessionError::SessionClosed)??;
        Ok(())
    }

    async fn run_session_with_retry(
        client: BasinClient,
        stream: StreamName,
        cmd_rx: mpsc::Receiver<Command>,
        retry_builder: RetryBackoffBuilder,
        buffer_size: usize,
    ) {
        let mut state = SessionState {
            cmd_rx,
            inflight_appends: VecDeque::new(),
            inflight_bytes: 0,
            close_tx: None,
            closing: false,
            total_records: 0,
            total_acked_records: 0,
            prev_ack_end: None,
        };
        let mut prev_total_acked_records = 0;
        let mut retry_backoffs: VecDeque<Duration> = retry_builder.build().collect();

        loop {
            let result = Self::run_session(&client, &stream, &mut state, buffer_size).await;

            match result {
                Ok(()) => {
                    break;
                }
                Err(err) => {
                    if prev_total_acked_records < state.total_acked_records {
                        prev_total_acked_records = state.total_acked_records;
                        retry_backoffs = retry_builder.build().collect();
                    }

                    let retry_policy_compliant = retry_policy_compliant(
                        client.config.retry.append_retry_policy,
                        &state.inflight_appends,
                    );

                    if retry_policy_compliant
                        && err.is_retryable()
                        && let Some(backoff) = retry_backoffs.pop_front()
                    {
                        debug!(
                            %err,
                            ?backoff,
                            num_retries_remaining = retry_backoffs.len(),
                            "retrying append session"
                        );
                        tokio::time::sleep(backoff).await;
                    } else {
                        debug!(
                            %err,
                            retry_policy_compliant,
                            retries_exhausted = retry_backoffs.is_empty(),
                            "not retrying append session"
                        );

                        let err: S2Error = err.into();
                        for inflight_append in state.inflight_appends.drain(..) {
                            let _ = inflight_append.ack_tx.send(Err(err.clone()));
                        }

                        if let Some(done_tx) = state.close_tx.take() {
                            let _ = done_tx.send(Err(err));
                        }
                        break;
                    }
                }
            }
        }

        if let Some(done_tx) = state.close_tx.take() {
            let _ = done_tx.send(Ok(()));
        }
    }

    async fn run_session(
        client: &BasinClient,
        stream: &StreamName,
        state: &mut SessionState,
        buffer_size: usize,
    ) -> Result<(), AppendSessionError> {
        let (input_tx, mut acks) = Self::connect(client, stream, buffer_size).await?;
        let ack_timeout = client.config.request_timeout;

        if !state.inflight_appends.is_empty() {
            Self::resend(state, &input_tx, &mut acks, ack_timeout).await?;

            assert!(state.inflight_appends.is_empty());
            assert_eq!(state.inflight_bytes, 0);
        }

        let timer = MuxTimer::<N_TIMER_VARIANTS>::default();
        tokio::pin!(timer);

        let mut stashed_submission: Option<StashedSubmission> = None;

        loop {
            tokio::select! {
                (event_ord, _deadline) = &mut timer, if timer.is_armed() => {
                    match TimerEvent::from(event_ord) {
                        TimerEvent::AckDeadline => {
                            return Err(AppendSessionError::AckTimeout);
                        }
                    }
                }

                input_tx_permit = input_tx.reserve(), if stashed_submission.is_some() => {
                    let input_tx_permit = input_tx_permit
                        .map_err(|_| AppendSessionError::ServerDisconnected)?;
                    let submission = stashed_submission
                        .take()
                        .expect("stashed_submission should not be None");

                    input_tx_permit.send(submission.input.clone());

                    state.total_records += submission.input.records.len();
                    state.inflight_bytes += submission.permit.num_bytes_permits();

                    timer.as_mut().fire_at(
                        TimerEvent::AckDeadline,
                        submission.since + ack_timeout,
                        CoalesceMode::Earliest,
                    );
                    state.inflight_appends.push_back(submission.into());
                }

                cmd = state.cmd_rx.recv(), if stashed_submission.is_none() => {
                    match cmd {
                        Some(Command::Submit { input, ack_tx, permit }) => {
                            if state.closing {
                                let _ = ack_tx.send(
                                    Err(AppendSessionError::SessionClosing.into())
                                );
                            } else {
                                stashed_submission = Some(StashedSubmission {
                                    input,
                                    ack_tx,
                                    permit,
                                    since: Instant::now(),
                                });
                            }
                        }
                        Some(Command::Close { done_tx }) => {
                            state.closing = true;
                            state.close_tx = Some(done_tx);
                        }
                        None => {
                            return Err(AppendSessionError::SessionDropped);
                        }
                    }
                }

                ack = acks.next() => {
                    match ack {
                        Some(Ok(ack)) => {
                            process_ack(
                                ack,
                                state,
                                timer.as_mut(),
                                ack_timeout,
                            );
                        }
                        Some(Err(err)) => {
                            return Err(err.into());
                        }
                        None => {
                            if !state.inflight_appends.is_empty() || stashed_submission.is_some() {
                                return Err(AppendSessionError::StreamClosedEarly);
                            }
                            break;
                        }
                    }
                }
            }

            if state.closing && state.inflight_appends.is_empty() && stashed_submission.is_none() {
                break;
            }
        }

        assert!(state.inflight_appends.is_empty());
        assert_eq!(state.inflight_bytes, 0);
        assert!(stashed_submission.is_none());

        Ok(())
    }

    async fn resend(
        state: &mut SessionState,
        input_tx: &mpsc::Sender<AppendInput>,
        acks: &mut Streaming<AppendAck>,
        ack_timeout: Duration,
    ) -> Result<(), AppendSessionError> {
        debug!(
            inflight_appends_len = state.inflight_appends.len(),
            inflight_bytes = state.inflight_bytes,
            "resending inflight appends"
        );

        let mut resend_index = 0;
        let mut resend_finished = false;

        let timer = MuxTimer::<N_TIMER_VARIANTS>::default();
        tokio::pin!(timer);

        while !state.inflight_appends.is_empty() {
            tokio::select! {
                (event_ord, _deadline) = &mut timer, if timer.is_armed() => {
                    match TimerEvent::from(event_ord) {
                        TimerEvent::AckDeadline => {
                            return Err(AppendSessionError::AckTimeout);
                        }
                    }
                }

                input_tx_permit = input_tx.reserve(), if !resend_finished => {
                    let input_tx_permit = input_tx_permit
                        .map_err(|_| AppendSessionError::ServerDisconnected)?;

                    if let Some(inflight_append) = state.inflight_appends.get_mut(resend_index) {
                        inflight_append.since = Instant::now();
                        timer.as_mut().fire_at(
                            TimerEvent::AckDeadline,
                            inflight_append.since + ack_timeout,
                            CoalesceMode::Earliest,
                        );
                        input_tx_permit.send(inflight_append.input.clone());
                        resend_index += 1;
                    } else {
                        resend_finished = true;
                    }
                }

                ack = acks.next() => {
                    match ack {
                        Some(Ok(ack)) => {
                            process_ack(
                                ack,
                                state,
                                timer.as_mut(),
                                ack_timeout,
                            );
                            resend_index -= 1;
                        }
                        Some(Err(err)) => {
                            return Err(err.into());
                        }
                        None => {
                            return Err(AppendSessionError::StreamClosedEarly);
                        }
                    }
                }
            }
        }

        assert_eq!(
            resend_index, 0,
            "resend_index should be 0 after resend completes"
        );
        debug!("finished resending inflight appends");
        Ok(())
    }

    async fn connect(
        client: &BasinClient,
        stream: &StreamName,
        buffer_size: usize,
    ) -> Result<(mpsc::Sender<AppendInput>, Streaming<AppendAck>), AppendSessionError> {
        let (input_tx, input_rx) = mpsc::channel::<AppendInput>(buffer_size);
        let ack_stream = Box::pin(
            client
                .append_session(stream, ReceiverStream::new(input_rx).map(|i| i.into()))
                .await?
                .map(|ack| match ack {
                    Ok(ack) => Ok(ack.into()),
                    Err(err) => Err(err),
                }),
        );
        Ok((input_tx, ack_stream))
    }
}

pub(crate) struct AppendSessionPermit<'a> {
    inflight_permit: InflightPermit,
    cmd_tx_permit: mpsc::Permit<'a, Command>,
}

impl AppendSessionPermit<'_> {
    pub(crate) fn send(self, input: AppendInput) -> BatchSubmitTicket {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.cmd_tx_permit.send(Command::Submit {
            input,
            ack_tx,
            permit: self.inflight_permit,
        });
        BatchSubmitTicket { rx: ack_rx }
    }
}

struct InflightPermit {
    _count: Option<OwnedSemaphorePermit>,
    bytes: OwnedSemaphorePermit,
}

impl InflightPermit {
    fn num_bytes_permits(&self) -> usize {
        self.bytes.num_permits()
    }
}

struct InflightPermits {
    count: Option<Arc<Semaphore>>,
    bytes: Arc<Semaphore>,
}

impl InflightPermits {
    fn new(count_permits: Option<u32>, bytes_permits: u32) -> Self {
        Self {
            count: count_permits.map(|permits| Arc::new(Semaphore::new(permits as usize))),
            bytes: Arc::new(Semaphore::new(bytes_permits as usize)),
        }
    }

    async fn acquire(&self, bytes: u32) -> InflightPermit {
        InflightPermit {
            _count: if let Some(count) = self.count.as_ref() {
                Some(
                    count
                        .clone()
                        .acquire_many_owned(1)
                        .await
                        .expect("semaphore should not be closed"),
                )
            } else {
                None
            },
            bytes: self
                .bytes
                .clone()
                .acquire_many_owned(bytes)
                .await
                .expect("semaphore should not be closed"),
        }
    }
}

struct StashedSubmission {
    input: AppendInput,
    ack_tx: oneshot::Sender<Result<AppendAck, S2Error>>,
    permit: InflightPermit,
    since: Instant,
}

struct InflightAppend {
    input: AppendInput,
    ack_tx: oneshot::Sender<Result<AppendAck, S2Error>>,
    since: Instant,
    permit: InflightPermit,
}

impl From<StashedSubmission> for InflightAppend {
    fn from(value: StashedSubmission) -> Self {
        Self {
            input: value.input,
            ack_tx: value.ack_tx,
            permit: value.permit,
            since: value.since,
        }
    }
}

fn retry_policy_compliant(
    policy: AppendRetryPolicy,
    inflight_appends: &VecDeque<InflightAppend>,
) -> bool {
    if policy == AppendRetryPolicy::All {
        return true;
    }
    inflight_appends
        .iter()
        .all(|ia| policy.is_compliant(&ia.input))
}

enum Command {
    Submit {
        input: AppendInput,
        ack_tx: oneshot::Sender<Result<AppendAck, S2Error>>,
        permit: InflightPermit,
    },
    Close {
        done_tx: oneshot::Sender<Result<(), S2Error>>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TimerEvent {
    AckDeadline,
}

const N_TIMER_VARIANTS: usize = 1;

impl From<TimerEvent> for usize {
    fn from(event: TimerEvent) -> Self {
        match event {
            TimerEvent::AckDeadline => 0,
        }
    }
}

impl From<usize> for TimerEvent {
    fn from(value: usize) -> Self {
        match value {
            0 => TimerEvent::AckDeadline,
            _ => panic!("invalid ordinal"),
        }
    }
}

fn process_ack(
    ack: AppendAck,
    state: &mut SessionState,
    timer: Pin<&mut MuxTimer<N_TIMER_VARIANTS>>,
    ack_timeout: Duration,
) {
    let corresponding_append = state
        .inflight_appends
        .pop_front()
        .expect("corresponding append should be present for an ack");

    assert!(
        ack.end.seq_num >= ack.start.seq_num,
        "ack end seq_num should be greater than or equal to start seq_num"
    );

    if let Some(end) = state.prev_ack_end {
        assert!(
            ack.end.seq_num > end.seq_num,
            "ack end seq_num should be greater than previous ack end"
        );
    }

    let num_acked_records = (ack.end.seq_num - ack.start.seq_num) as usize;
    assert_eq!(
        num_acked_records,
        corresponding_append.input.records.len(),
        "ack record count should match submitted batch size"
    );

    state.total_acked_records += num_acked_records;
    state.inflight_bytes -= corresponding_append.permit.num_bytes_permits();
    state.prev_ack_end = Some(ack.end);

    let _ = corresponding_append.ack_tx.send(Ok(ack));

    if let Some(oldest_append) = state.inflight_appends.front() {
        timer.fire_at(
            TimerEvent::AckDeadline,
            oldest_append.since + ack_timeout,
            CoalesceMode::Latest,
        );
    } else {
        timer.cancel(TimerEvent::AckDeadline);
        assert_eq!(
            state.total_records, state.total_acked_records,
            "all records should be acked when inflight is empty"
        );
    }
}

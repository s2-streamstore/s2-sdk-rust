use std::{
    collections::VecDeque,
    ops::{DerefMut, Range},
    sync::Arc,
    time::Duration,
};

use futures::StreamExt;
use tokio::{
    sync::{Mutex, mpsc, mpsc::Permit},
    time::Instant,
};
use tokio_muxt::{CoalesceMode, MuxTimer};
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;
use tonic_side_effect::FrameSignal;
use tracing::{debug, warn};

use crate::{
    client::{AppendRetryPolicy, ClientError, StreamClient},
    service::{
        ServiceStreamingResponse,
        stream::{AppendSessionServiceRequest, AppendSessionStreamingResponse},
    },
    types::{self, MeteredBytes, RETRY_AFTER_MS_METADATA_KEY},
};

async fn connect(
    stream_client: &StreamClient,
    frame_signal: FrameSignal,
    compression: bool,
) -> Result<
    (
        mpsc::Sender<types::AppendInput>,
        ServiceStreamingResponse<AppendSessionStreamingResponse>,
    ),
    ClientError,
> {
    frame_signal.reset();
    let (input_tx, input_rx) = mpsc::channel(10);
    let service_req = AppendSessionServiceRequest::new(
        stream_client
            .inner
            .frame_monitoring_stream_service_client(frame_signal.clone()),
        &stream_client.stream,
        ReceiverStream::new(input_rx),
        compression,
    );

    Ok((input_tx, stream_client.inner.send(service_req).await?))
}

struct InflightBatch {
    start: Instant,
    metered_bytes: u64,
    inner: types::AppendInput,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TimerEvent {
    MetricUpdate,
    BatchDeadline,
}

const N_TIMER_VARIANTS: usize = 2;
const MAX_BATCH_SIZE: u64 = 1024 * 1024;

impl From<TimerEvent> for usize {
    fn from(event: TimerEvent) -> Self {
        match event {
            TimerEvent::MetricUpdate => 0,
            TimerEvent::BatchDeadline => 1,
        }
    }
}

impl From<usize> for TimerEvent {
    fn from(value: usize) -> Self {
        match value {
            0 => TimerEvent::MetricUpdate,
            1 => TimerEvent::BatchDeadline,
            _ => panic!("invalid ordinal"),
        }
    }
}

struct AppendState<S>
where
    S: 'static + Send + Unpin + futures::Stream<Item = types::AppendInput>,
{
    /// Append batches which are "inflight" currently, and have not yet received acknowledgement.
    inflight: VecDeque<InflightBatch>,

    /// Size of `inflight` queue in `metered_bytes`.
    inflight_size: u64,

    /// Stream of `AppendInput` from client.
    request_stream: S,

    /// Number of records received from client, over the course of this append session.
    total_records: usize,

    /// Number of acknowledged records from S2, over the course of this append session.
    total_records_acknowledged: usize,

    /// Used to temporarily store the most recent `AppendInput` from the client stream.
    stashed_request: Option<types::AppendInput>,
}

/// Handle S2 acknowledgment by forwarding it to the client,
/// and updating the inflight data.
fn ack_and_pop(
    s2_ack: types::AppendAck,
    inflight: &mut VecDeque<InflightBatch>,
    inflight_size: &mut u64,
    permit: Permit<'_, Result<types::AppendAck, ClientError>>,
) -> Range<u64> {
    let corresponding_batch = inflight.pop_front().expect("inflight should not be empty");

    assert_eq!(
        (s2_ack.end.seq_num - s2_ack.start.seq_num) as usize,
        corresponding_batch.inner.records.len(),
        "number of acknowledged records from S2 should equal amount from first inflight batch"
    );

    *inflight_size -= corresponding_batch.metered_bytes;
    let ack_range = s2_ack.start.seq_num..s2_ack.end.seq_num;

    permit.send(Ok(s2_ack));

    ack_range
}

async fn resend(
    request_timeout: Duration,
    inflight: &mut VecDeque<InflightBatch>,
    inflight_size: &mut u64,
    s2_input_tx: mpsc::Sender<types::AppendInput>,
    s2_ack_stream: &mut ServiceStreamingResponse<AppendSessionStreamingResponse>,
    total_records_acknowledged: &mut usize,
    output_tx: mpsc::Sender<Result<types::AppendAck, ClientError>>,
) -> Result<(), ClientError> {
    debug!(
        inflight_len = inflight.len(),
        inflight_bytes = inflight_size,
        "resending"
    );

    let mut resend_index = 0;
    let mut resend_tx_finished = false;
    let mut stashed_ack = None;

    let timer = MuxTimer::<N_TIMER_VARIANTS>::default();
    tokio::pin!(timer);

    while !inflight.is_empty() {
        tokio::select! {
            (event_ord, _deadline) = &mut timer, if timer.is_armed() => {
                match TimerEvent::from(event_ord) {
                    TimerEvent::BatchDeadline => Err(ClientError::Service(Status::cancelled("client: hit deadline (`request_timeout`) waiting for an append acknowledgement")))?,
                    _ => unreachable!("only batch deadline timer in resend mode")
                }
            }

            s2_permit = s2_input_tx.reserve(), if !resend_tx_finished => {
                let s2_permit = s2_permit.map_err(|_| ClientError::Service(Status::unavailable("client: s2 server disconnected")))?;
                match inflight.get(resend_index) {
                    Some(batch) => {
                        timer.as_mut().fire_at(TimerEvent::BatchDeadline, batch.start + request_timeout, CoalesceMode::Earliest);
                        s2_permit.send(batch.inner.clone());
                        resend_index += 1;
                    },
                    None => resend_tx_finished = true
                }
            },

            next_ack = s2_ack_stream.next(), if stashed_ack.is_none() => {
                stashed_ack = Some(next_ack.ok_or(ClientError::Service(Status::internal("client: response stream closed early")))?);
            }

            client_permit = output_tx.reserve(), if stashed_ack.is_some() => {
                let ack_range = ack_and_pop(
                    stashed_ack.take().expect("stashed ack")?,
                    inflight,
                    inflight_size,
                    client_permit.map_err(|_| ClientError::Service(Status::cancelled("client: disconnected")))?
                );

                *total_records_acknowledged += (ack_range.end - ack_range.start) as usize;
                resend_index -= 1;

                // Adjust next timer.
                match inflight.front() {
                    Some(batch) => timer.as_mut().fire_at(
                        TimerEvent::BatchDeadline,
                        batch.start + request_timeout,
                        CoalesceMode::Latest
                    ),
                    None => timer.as_mut().cancel(TimerEvent::BatchDeadline),
                };
            }
        }
    }

    assert!(stashed_ack.is_none());
    assert_eq!(resend_index, 0);

    debug!("finished resending");

    Ok(())
}

async fn session_inner<S>(
    state: Arc<Mutex<AppendState<S>>>,
    frame_signal: FrameSignal,
    stream_client: StreamClient,
    output_tx: mpsc::Sender<Result<types::AppendAck, ClientError>>,
    compression: bool,
) -> Result<(), ClientError>
where
    S: 'static + Send + Unpin + futures::Stream<Item = types::AppendInput>,
{
    let mut lock = state.lock().await;
    let AppendState {
        inflight,
        inflight_size,
        request_stream,
        total_records,
        total_records_acknowledged,
        stashed_request,
    } = lock.deref_mut();

    assert!(*inflight_size <= stream_client.inner.config.max_append_inflight_bytes);

    let (s2_input_tx, mut s2_ack_stream) =
        connect(&stream_client, frame_signal.clone(), compression).await?;
    let batch_ack_deadline = stream_client.inner.config.request_timeout;

    if !inflight.is_empty() {
        resend(
            batch_ack_deadline,
            inflight,
            inflight_size,
            s2_input_tx.clone(),
            &mut s2_ack_stream,
            total_records_acknowledged,
            output_tx.clone(),
        )
        .await?;

        frame_signal.reset();

        assert!(inflight.is_empty());
        assert_eq!(*inflight_size, 0);
        assert_eq!(total_records, total_records_acknowledged);
    }

    let timer = MuxTimer::<N_TIMER_VARIANTS>::default();
    tokio::pin!(timer);

    let mut client_input_terminated = false;
    let mut stashed_ack = None;

    while !(client_input_terminated && inflight.is_empty()) {
        tokio::select! {
            (event_ord, _deadline) = &mut timer, if timer.is_armed() => {
                match TimerEvent::from(event_ord) {
                    TimerEvent::MetricUpdate => todo!(),
                    TimerEvent::BatchDeadline => Err(ClientError::Service(Status::cancelled("client: hit deadline (`request_timeout`) waiting for an append acknowledgement")))?
                }
            }

            next_request = request_stream.next(), if
                stashed_request.is_none() &&
                !client_input_terminated &&
                *inflight_size + MAX_BATCH_SIZE <= stream_client.inner.config.max_append_inflight_bytes
            => {
                match next_request {
                    Some(append_input) => *stashed_request = Some(append_input),
                    None => client_input_terminated = true
                }
            }

            s2_permit = s2_input_tx.reserve(), if stashed_request.is_some() => {
                let s2_permit = s2_permit.map_err(|_| ClientError::Service(Status::unavailable("client: s2 server disconnected")))?;
                let append_input = stashed_request.take().expect("stashed request");
                let metered_bytes = append_input.metered_bytes();
                let start = Instant::now();

                *inflight_size += metered_bytes;
                *total_records += append_input.records.len();
                inflight.push_back(InflightBatch {
                    start,
                    metered_bytes,
                    inner: append_input.clone()
                });

                s2_permit.send(append_input);

                timer.as_mut().fire_at(TimerEvent::BatchDeadline, start + batch_ack_deadline, CoalesceMode::Earliest);
            }

            next_ack = s2_ack_stream.next(), if stashed_ack.is_none() => {
                stashed_ack = Some(next_ack.ok_or(ClientError::Service(Status::internal("client: response stream closed early")))?);
            }

            client_permit = output_tx.reserve(), if stashed_ack.is_some() => {
                let ack_range = ack_and_pop(
                    stashed_ack.take().expect("stashed ack")?,
                    inflight,
                    inflight_size,
                    client_permit.map_err(|_| ClientError::Service(Status::cancelled("client: disconnected")))?
                );

                *total_records_acknowledged += (ack_range.end - ack_range.start) as usize;

                // Safe to reset frame signal whenever we reach a sync point between
                // records received and acknowledged.
                if inflight.is_empty() {
                    assert_eq!(total_records, total_records_acknowledged);
                    frame_signal.reset()
                }

                // Adjust next timer.
                match inflight.front() {
                    Some(batch) => timer.as_mut().fire_at(
                        TimerEvent::BatchDeadline,
                        batch.start + batch_ack_deadline,
                        CoalesceMode::Latest
                    ),
                    None => timer.as_mut().cancel(TimerEvent::BatchDeadline),
                };
            }
        }
    }

    assert!(stashed_ack.is_none());
    assert!(stashed_request.is_none());
    assert!(client_input_terminated);

    assert_eq!(total_records, total_records_acknowledged);
    assert_eq!(inflight.len(), 0);
    assert_eq!(*inflight_size, 0);

    Ok(())
}

pub(crate) async fn manage_session<S>(
    stream_client: StreamClient,
    input: S,
    output_tx: mpsc::Sender<Result<types::AppendAck, ClientError>>,
    compression: bool,
) where
    S: 'static + Send + Unpin + futures::Stream<Item = types::AppendInput>,
{
    let state = Arc::new(Mutex::new(AppendState {
        inflight: Default::default(),
        inflight_size: Default::default(),
        request_stream: input,
        total_records: 0,
        total_records_acknowledged: 0,
        stashed_request: None,
    }));

    let frame_signal = FrameSignal::new();
    let mut attempts = 1;
    let mut acks_out: usize = 0;
    loop {
        match session_inner(
            state.clone(),
            frame_signal.clone(),
            stream_client.clone(),
            output_tx.clone(),
            compression,
        )
        .await
        {
            Ok(()) => return,
            Err(e) => {
                let new_acks_out = state.lock().await.total_records_acknowledged;
                if acks_out < new_acks_out {
                    // Progress has been made during the last attempt, so reset the retry counter.
                    acks_out = new_acks_out;
                    attempts = 1;
                }

                let now = Instant::now();
                let remaining_attempts = attempts < stream_client.inner.config.max_attempts;
                let (enough_time, retryable_error, retry_backoff_duration) = {
                    let mut retry_backoff_duration =
                        stream_client.inner.config.retry_backoff_duration;
                    let retryable_error = match &e {
                        ClientError::Service(status) => {
                            if let Some(value) = status.metadata().get(RETRY_AFTER_MS_METADATA_KEY)
                            {
                                if let Some(retry_after_ms) = value
                                    .to_str()
                                    .ok()
                                    .map(|v| v.parse())
                                    .transpose()
                                    .ok()
                                    .flatten()
                                {
                                    retry_backoff_duration = Duration::from_millis(retry_after_ms);
                                } else {
                                    warn!(
                                        "Failed to convert {RETRY_AFTER_MS_METADATA_KEY} metadata to u64.
                                        Falling back to default backoff duration: {:?}",
                                        retry_backoff_duration
                                    );
                                }
                            }
                            matches!(
                                status.code(),
                                tonic::Code::Unavailable
                                    | tonic::Code::DeadlineExceeded
                                    | tonic::Code::Unknown
                                    | tonic::Code::ResourceExhausted
                            )
                        }
                        ClientError::Conversion(_) => false,
                    };
                    let enough_time = state
                        .lock()
                        .await
                        .inflight
                        .front()
                        .map(|state| {
                            let next_deadline =
                                state.start + stream_client.inner.config.request_timeout;
                            now + retry_backoff_duration < next_deadline
                        })
                        .unwrap_or(true);
                    (enough_time, retryable_error, retry_backoff_duration)
                };
                let policy_compliant = {
                    match stream_client.inner.config.append_retry_policy {
                        AppendRetryPolicy::All => true,
                        AppendRetryPolicy::NoSideEffects => {
                            // If no request frame has been produced, we conclude that the failing
                            // append never left this host, so it is safe to retry.
                            !frame_signal.is_signalled()
                        }
                    }
                };

                if remaining_attempts && enough_time && retryable_error && policy_compliant {
                    tokio::time::sleep(retry_backoff_duration).await;
                    attempts += 1;
                    debug!(attempts, ?e, "retrying");
                } else {
                    debug!(
                        ?e,
                        remaining_attempts,
                        enough_time,
                        retryable_error,
                        policy_compliant,
                        "not retrying"
                    );
                    _ = output_tx.send(Err(e)).await;
                    return;
                }
            }
        }
    }
}

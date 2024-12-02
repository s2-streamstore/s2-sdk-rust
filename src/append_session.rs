use std::{
    collections::VecDeque,
    ops::{DerefMut, RangeTo},
    sync::Arc,
    time::Duration,
};

use futures::StreamExt;
use tokio::{
    sync::{mpsc, mpsc::Permit, Mutex},
    time::Instant,
};
use tokio_muxt::{CoalesceMode, MuxTimer};
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;
use tonic_side_effect::FrameSignal;
use tracing::debug;

use crate::{
    client::{AppendRetryPolicy, ClientError, StreamClient},
    service::{
        stream::{AppendSessionServiceRequest, AppendSessionStreamingResponse},
        ServiceStreamingResponse,
    },
    types,
    types::MeteredBytes,
};

async fn connect(
    stream_client: &StreamClient,
    frame_signal: FrameSignal,
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
    inflight: VecDeque<InflightBatch>,
    inflight_size: u64,
    request_stream: S,
    last_acked_seqnum: Option<RangeTo<u64>>,
}

fn ack_and_pop(
    channel_ack: types::AppendOutput,
    inflight: &mut VecDeque<InflightBatch>,
    inflight_size: &mut u64,
    permit: Permit<'_, Result<types::AppendOutput, ClientError>>,
) -> Result<RangeTo<u64>, ClientError> {
    let n_acked_records = channel_ack.end_seq_num - channel_ack.start_seq_num;
    let corresponding_batch = inflight.pop_front().expect("inflight should not be empty");

    assert_eq!(
        n_acked_records as usize,
        corresponding_batch.inner.records.len(),
        "number of acknowledged records should equal amount in first inflight batch"
    );

    *inflight_size -= corresponding_batch.metered_bytes;
    let end_seq_num = channel_ack.end_seq_num;

    permit.send(Ok(channel_ack));

    Ok(..end_seq_num)
}

async fn resend(
    request_timeout: Duration,
    inflight: &mut VecDeque<InflightBatch>,
    inflight_size: &mut u64,
    channel_input_tx: mpsc::Sender<types::AppendInput>,
    channel_ack_stream: &mut ServiceStreamingResponse<AppendSessionStreamingResponse>,
    last_acked_seqnum: &mut Option<RangeTo<u64>>,
    output_tx: mpsc::Sender<Result<types::AppendOutput, ClientError>>,
) -> Result<(), ClientError> {
    debug!(
        inflight_len = inflight.len(),
        inflight_bytes = inflight_size,
        "resending"
    );
    let mut resend_index = 0;
    let mut resend_tx_finished = false;
    let timer = MuxTimer::<N_TIMER_VARIANTS>::default();
    tokio::pin!(timer);

    while !inflight.is_empty() {
        tokio::select! {
            (event_ord, _) = &mut timer, if timer.is_armed() => {
                match TimerEvent::from(event_ord) {
                    TimerEvent::BatchDeadline => {
                        Err(ClientError::Service(Status::cancelled("sdk hit deadline (`request_timeout`) waiting for an append acknowledgement")))?
                    }
                    _ => unreachable!("only batch deadline timer in resend mode")
                }
            }
            Ok(permit) = channel_input_tx.reserve(), if !resend_tx_finished => {
                match inflight.get(resend_index) {
                    Some(batch) => {
                        timer.as_mut().fire_at(TimerEvent::BatchDeadline, batch.start + request_timeout, CoalesceMode::Earliest);
                        permit.send(batch.inner.clone());
                        resend_index += 1;
                    },
                    None => resend_tx_finished = true
                }
            },
            Some(ack) = channel_ack_stream.next() => {
                let ack_until = ack_and_pop(
                    ack?,
                    inflight,
                    inflight_size,
                    output_tx.reserve().await.map_err(|_| ClientError::Service(Status::cancelled("client disconnected")))?
                )?;

                *last_acked_seqnum = Some(ack_until);
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
    Ok(())
}

async fn session_inner<S>(
    state: Arc<Mutex<AppendState<S>>>,
    frame_signal: FrameSignal,
    stream_client: StreamClient,
    output_tx: mpsc::Sender<Result<types::AppendOutput, ClientError>>,
) -> Result<(), ClientError>
where
    S: 'static + Send + Unpin + futures::Stream<Item = types::AppendInput>,
{
    let mut lock = state.lock().await;
    let AppendState {
        inflight,
        inflight_size,
        request_stream,
        last_acked_seqnum,
    } = lock.deref_mut();

    assert!(*inflight_size <= stream_client.inner.config.max_append_inflight_bytes);
    let (input_tx, mut ack_stream) = connect(&stream_client, frame_signal.clone()).await?;
    let batch_ack_deadline = stream_client.inner.config.request_timeout;

    if !inflight.is_empty() {
        resend(
            batch_ack_deadline,
            inflight,
            inflight_size,
            input_tx.clone(),
            &mut ack_stream,
            last_acked_seqnum,
            output_tx.clone(),
        )
        .await?;

        assert_eq!(inflight.len(), 0);
        assert_eq!(*inflight_size, 0);
        frame_signal.reset();

        debug!("finished resending");
    }
    let timer = MuxTimer::<N_TIMER_VARIANTS>::default();
    tokio::pin!(timer);
    let mut input_terminated = false;

    while !(input_terminated && inflight.is_empty()) {
        tokio::select! {
            (event_ord, _deadline) = &mut timer,
                if timer.is_armed()
            => {
                match TimerEvent::from(event_ord) {
                    TimerEvent::MetricUpdate => {
                        todo!()
                    }
                    TimerEvent::BatchDeadline =>
                        Err(ClientError::Service(Status::cancelled("sdk hit deadline (`request_timeout`) waiting for an append acknowledgement")))?
                }
            }
            client_input = request_stream.next(),
                if !input_terminated && *inflight_size + MAX_BATCH_SIZE <= stream_client.inner.config.max_append_inflight_bytes
            => {
                match client_input {
                    Some(append_input) => {
                        let metered_bytes = append_input.metered_bytes();
                        *inflight_size += metered_bytes;
                        let start = Instant::now();
                        inflight.push_back(InflightBatch {
                            start,
                            metered_bytes,
                            inner: append_input.clone()
                        });
                        timer.as_mut().fire_at(TimerEvent::BatchDeadline, start + batch_ack_deadline, CoalesceMode::Earliest);
                        input_tx.send(append_input)
                            .await
                            .map_err(|_| ClientError::Service(Status::unavailable("server disconnected")))?;
                    }
                    None => input_terminated = true,
                }
            },
            Some(ack) = ack_stream.next() => {
                let ack_until = ack_and_pop(
                    ack?,
                    inflight,
                    inflight_size,
                    output_tx.reserve().await.map_err(|_| ClientError::Service(Status::cancelled("client disconnected")))?
                )?;

                *last_acked_seqnum = Some(ack_until);

                if inflight.is_empty() {
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

            },

            else => break,
        }
    }

    assert!(input_terminated);
    assert_eq!(inflight.len(), 0);
    assert_eq!(*inflight_size, 0);

    Ok(())
}

pub(crate) async fn manage_session<S>(
    stream_client: StreamClient,
    input: S,
    output_tx: mpsc::Sender<Result<types::AppendOutput, ClientError>>,
) where
    S: 'static + Send + Unpin + futures::Stream<Item = types::AppendInput>,
{
    let state = Arc::new(Mutex::new(AppendState {
        inflight: Default::default(),
        inflight_size: Default::default(),
        request_stream: input,
        last_acked_seqnum: None,
    }));

    let frame_signal = FrameSignal::new();
    let mut attempts = 1;
    let mut last_acked_seqnum: Option<RangeTo<u64>> = None;
    loop {
        match session_inner(
            state.clone(),
            frame_signal.clone(),
            stream_client.clone(),
            output_tx.clone(),
        )
        .await
        {
            Ok(()) => return,
            Err(e) => {
                let new_last_acked_seqnum = state.lock().await.last_acked_seqnum;
                if last_acked_seqnum != new_last_acked_seqnum {
                    // Progress has been made during the last attempt, so reset the retry counter.
                    last_acked_seqnum = new_last_acked_seqnum;
                    attempts = 1;
                }

                let now = Instant::now();
                let remaining_attempts = attempts < stream_client.inner.config.max_attempts;
                let enough_time = {
                    state
                        .lock()
                        .await
                        .inflight
                        .front()
                        .map(|state| {
                            let next_deadline =
                                state.start + stream_client.inner.config.request_timeout;
                            now + stream_client.inner.config.retry_backoff_duration < next_deadline
                        })
                        .unwrap_or(true)
                };
                let retryable_error = {
                    match &e {
                        ClientError::Service(status) => {
                            matches!(
                                status.code(),
                                tonic::Code::Unavailable
                                    | tonic::Code::DeadlineExceeded
                                    | tonic::Code::Unknown
                            )
                        }
                        ClientError::Conversion(_) => false,
                    }
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
                    tokio::time::sleep(stream_client.inner.config.retry_backoff_duration).await;
                    attempts += 1;
                    debug!(attempts, ?e, "retrying");
                } else {
                    debug!(
                        remaining_attempts,
                        enough_time, retryable_error, policy_compliant, "not retrying"
                    );
                    _ = output_tx.send(Err(e)).await;
                    return;
                }
            }
        }
    }
}

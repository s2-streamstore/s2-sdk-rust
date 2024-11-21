use crate::client::{AppendRetryPolicy, ClientError, StreamClient};
use crate::service::stream::{AppendSessionServiceRequest, AppendSessionStreamingResponse};
use crate::service::ServiceStreamingResponse;
use crate::types::MeteredSize;
use crate::types;
use bytesize::ByteSize;
use enum_ordinalize::Ordinalize;
use futures::StreamExt;
use std::collections::VecDeque;
use std::ops::DerefMut;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};
use tokio::time::Instant;
use tokio_muxt::{CoalesceMode, MuxTimer};
use tokio_stream::wrappers::ReceiverStream;
use tonic_side_effect::FrameSignal;
use tracing::{trace, warn};

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
    // Signal can be reset as we are creating a new connection anyway.
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
    metered_size: ByteSize,
    inner: types::AppendInput,
}

#[derive(Ordinalize, Debug, Clone, Copy, PartialEq, Eq)]
enum TimerEvent {
    MetricUpdate,
    BatchDeadline,
}

impl From<TimerEvent> for usize {
    fn from(event: TimerEvent) -> Self {
        event.ordinal() as usize
    }
}

struct AppendState<S>
where
    S: 'static + Send + Unpin + futures::Stream<Item = types::AppendInput>,
{
    inflight: VecDeque<InflightBatch>,
    inflight_size: u64,
    request_stream: S,
}

async fn recover(
    request_timeout: Duration,
    inflight: &mut VecDeque<InflightBatch>,
    inflight_size: &mut u64,
    channel_input_tx: mpsc::Sender<types::AppendInput>,
    channel_ack_stream: &mut ServiceStreamingResponse<AppendSessionStreamingResponse>,
    output_tx: mpsc::Sender<Result<types::AppendOutput, ClientError>>,
) -> Result<(), ClientError> {
    trace!(
        inflight_len = inflight.len(),
        inflight_bytes = inflight_size,
        "recover"
    );
    let mut recovery_index = 0;
    let mut recovery_tx_finished = false;

    let timer = MuxTimer::<{ TimerEvent::VARIANT_COUNT }>::default();
    tokio::pin!(timer);

    while !inflight.is_empty() {
        tokio::select! {
            (event_ord, _) = &mut timer, if timer.is_armed() => {
                match TimerEvent::from_ordinal(event_ord as i8).expect("valid event ordinal") {
                    TimerEvent::BatchDeadline => {
                        Err(ClientError::LocalDeadline("deadline for append acknowledgement hit".to_string()))?
                    }
                    _ => unreachable!("only batch deadline timer in recovery mode")
                }
            }
            Ok(permit) = channel_input_tx.reserve(), if !recovery_tx_finished => {
                match inflight.get(recovery_index) {
                    Some(batch) => {
                        timer.as_mut().fire_at(TimerEvent::BatchDeadline, batch.start + request_timeout, CoalesceMode::Earliest);
                        permit.send(batch.inner.clone());
                        recovery_index += 1;
                    },
                    None => recovery_tx_finished = true
                }
            },
            Some(ack) = channel_ack_stream.next() => {
                let ack = ack?;
                let n_acked_records = ack.end_seq_num - ack.start_seq_num;
                let recovery_batch = inflight
                    .pop_front()
                    .expect("inflight should not be empty");
                assert_eq!(
                    n_acked_records as usize,
                    recovery_batch.inner.records.len(),
                    "number of acknowledged should equal amount in recovery batch"
                );
                output_tx
                    .send(Ok(ack))
                    .await
                    .map_err(|_| ClientError::LostUser)?;

                // Adjust next timer.
                match inflight.front() {
                    Some(batch) => timer.as_mut().fire_at(
                        TimerEvent::BatchDeadline,
                        batch.start + request_timeout,
                        CoalesceMode::Latest
                    ),
                    None => timer.as_mut().cancel(TimerEvent::BatchDeadline),
                };

                *inflight_size -= recovery_batch.metered_size.0;
                recovery_index -= 1;
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
        ref mut inflight,
        ref mut inflight_size,
        ref mut request_stream,
    } = lock.deref_mut();

    assert!(inflight.len() <= stream_client.inner.config.max_append_batches_inflight);

    let (input_tx, mut ack_stream) = connect(&stream_client, frame_signal.clone()).await?;
    let batch_ack_deadline = stream_client.inner.config.request_timeout;

    // Recovery.
    if !inflight.is_empty() {
        recover(
            batch_ack_deadline,
            inflight,
            inflight_size,
            input_tx.clone(),
            &mut ack_stream,
            output_tx.clone(),
        )
        .await?;
        frame_signal.reset();

        assert_eq!(inflight.len(), 0);
        assert_eq!(*inflight_size, 0);
        trace!("recovery finished");
    }

    let timer = MuxTimer::<{ TimerEvent::VARIANT_COUNT }>::default();
    tokio::pin!(timer);


    let mut input_terminated = false;
    while !input_terminated {
        tokio::select! {
            (event_ord, deadline) = &mut timer,
                if timer.is_armed()
            => {
                match TimerEvent::from_ordinal(event_ord as i8).expect("valid event ordinal") {
                    TimerEvent::MetricUpdate => {
                        //TODO
                    }
                    TimerEvent::BatchDeadline => {
                        let first_batch_start = inflight.front().map(|s| s.start);
                        warn!(?deadline, ?first_batch_start, "hitting batch deadline!");
                        Err(ClientError::LocalDeadline("deadline for append acknowledgement hit".to_string()))?
                    }
                }

            }
            client_input = request_stream.next(),
                if !input_terminated && inflight.len() < stream_client.inner.config.max_append_batches_inflight
            => {
                match client_input {
                    None => input_terminated = true,
                    Some(append_input) => {
                        let metered_size = append_input.metered_size();
                        *inflight_size += metered_size.0;
                        let enqueue_time = Instant::now();
                        inflight.push_back(InflightBatch {
                            start: enqueue_time,
                            metered_size,
                            inner: append_input.clone()
                        });
                        timer.as_mut().fire_at(TimerEvent::BatchDeadline, enqueue_time + batch_ack_deadline, CoalesceMode::Earliest);
                        input_tx.send(append_input)
                            .await
                            .map_err(|_| ClientError::Service(tonic::Status::unavailable("frontend input_tx disconnected")))?;
                    }
                }
            },
            ack = ack_stream.next() => {
                match ack  {
                    Some(ack) => {
                        let ack = ack?;
                        let n_acked_records = ack.end_seq_num - ack.start_seq_num;
                        let corresponding_batch = inflight.pop_front()
                            .expect("inflight should not be empty");
                        assert_eq!(
                            n_acked_records as usize,
                            corresponding_batch.inner.records.len(),
                            "number of acknowledged should equal amount in recovery batch"
                        );
                        output_tx.send(Ok(ack)).await.map_err(|_| ClientError::LostUser)?;

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

                        *inflight_size -= corresponding_batch.metered_size.0;
                    }
                    None => break,
                }
            },
            else => {
                break;
            }
        }
    }

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
    }));

    let frame_signal = FrameSignal::new();
    let mut attempts = 1;
    let mut resp = session_inner(
        state.clone(),
        frame_signal.clone(),
        stream_client.clone(),
        output_tx.clone(),
    )
    .await;

    while let Err(e) = resp {
        let now = Instant::now();
        let remaining_attempts = attempts < stream_client.inner.config.max_attempts;
        let enough_time = {
            state
                .lock()
                .await
                .inflight
                .front()
                .map(|state| {
                    let next_deadline = state.start + stream_client.inner.config.request_timeout;
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
                ClientError::Conversion(_)
                | ClientError::LocalDeadline(_)
                | ClientError::LostUser => false,
            }
        };
        let policy_compliant = {
            match stream_client.inner.config.append_retry_policy {
                AppendRetryPolicy::All => true,
                AppendRetryPolicy::NoSideEffects => {
                    // If no request frame has been produced, we conclude that the failing append
                    // never left this host, so it is safe to retry.
                    !frame_signal.is_signalled()
                }
            }
        };

        if remaining_attempts && enough_time && retryable_error && policy_compliant {
            tokio::time::sleep(stream_client.inner.config.retry_backoff_duration).await;
            attempts += 1;
            resp = session_inner(
                state.clone(),
                frame_signal.clone(),
                stream_client.clone(),
                output_tx.clone(),
            )
            .await;
        } else {
            trace!(
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

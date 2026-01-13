mod common;

use std::time::Duration;

use assert_matches::assert_matches;
use common::S2Stream;
use futures::StreamExt;
use s2::{
    append_session::AppendSessionConfig, batching::BatchingConfig, producer::ProducerConfig,
    types::*,
};
use test_context::test_context;
use time::OffsetDateTime;

#[test_context(S2Stream)]
#[tokio_shared_rt::test(shared)]
async fn tail_of_new_stream(stream: &S2Stream) -> Result<(), S2Error> {
    let tail = stream.check_tail().await?;

    assert_eq!(tail.seq_num, 0);

    Ok(())
}

#[test_context(S2Stream)]
#[tokio_shared_rt::test(shared)]
async fn single_append(stream: &S2Stream) -> Result<(), S2Error> {
    let input = AppendInput::new(AppendRecordBatch::try_from_iter([AppendRecord::new(
        "lorem",
    )?])?);

    let ack = stream.append(input).await?;

    assert_eq!(ack.start.seq_num, 0);
    assert_eq!(ack.end.seq_num, 1);
    assert_eq!(ack.tail.seq_num, 1);

    Ok(())
}

#[test_context(S2Stream)]
#[tokio_shared_rt::test(shared)]
async fn multiple_appends(stream: &S2Stream) -> Result<(), S2Error> {
    let input1 = AppendInput::new(AppendRecordBatch::try_from_iter([AppendRecord::new(
        "lorem",
    )?])?);
    let input2 = AppendInput::new(AppendRecordBatch::try_from_iter([AppendRecord::new(
        "ipsum",
    )?])?);
    let input3 = AppendInput::new(AppendRecordBatch::try_from_iter([
        AppendRecord::new("dolor")?,
        AppendRecord::new("sit")?,
    ])?);

    let ack1 = stream.append(input1).await?;
    let ack2 = stream.append(input2).await?;
    let ack3 = stream.append(input3).await?;

    assert_eq!(ack1.start.seq_num, 0);
    assert_eq!(ack1.end.seq_num, 1);
    assert_eq!(ack2.start.seq_num, 1);
    assert_eq!(ack2.end.seq_num, 2);
    assert_eq!(ack3.start.seq_num, 2);
    assert_eq!(ack3.end.seq_num, 4);

    Ok(())
}

#[test_context(S2Stream)]
#[tokio_shared_rt::test(shared)]
async fn append_with_headers(stream: &S2Stream) -> Result<(), S2Error> {
    let headers = vec![Header::new("key1", "value1"), Header::new("key2", "value2")];
    let input = AppendInput::new(AppendRecordBatch::try_from_iter([AppendRecord::new(
        "lorem",
    )?
    .with_headers(headers.clone())?])?);

    let ack = stream.append(input).await?;

    assert_eq!(ack.start.seq_num, 0);

    let batch = stream.read(ReadInput::new()).await?;

    assert_eq!(batch.records.len(), 1);
    assert_eq!(batch.records[0].headers.len(), headers.len());

    Ok(())
}

#[test_context(S2Stream)]
#[tokio_shared_rt::test(shared)]
async fn append_with_match_seq_num(stream: &S2Stream) -> Result<(), S2Error> {
    let input1 = AppendInput::new(AppendRecordBatch::try_from_iter([
        AppendRecord::new("lorem")?,
        AppendRecord::new("ipsum")?,
    ])?)
    .with_match_seq_num(0);

    let ack1 = stream.append(input1).await?;

    assert_eq!(ack1.start.seq_num, 0);
    assert_eq!(ack1.end.seq_num, 2);

    let input2 = AppendInput::new(AppendRecordBatch::try_from_iter([AppendRecord::new(
        "dolor",
    )?])?)
    .with_match_seq_num(2);

    let ack2 = stream.append(input2).await?;

    assert_eq!(ack2.start.seq_num, 2);
    assert_eq!(ack2.end.seq_num, 3);

    Ok(())
}

#[test_context(S2Stream)]
#[tokio_shared_rt::test(shared)]
async fn read_with_count_limit_partial(stream: &S2Stream) -> Result<(), S2Error> {
    let input = AppendInput::new(AppendRecordBatch::try_from_iter([
        AppendRecord::new("lorem")?,
        AppendRecord::new("ipsum")?,
        AppendRecord::new("dolor")?,
    ])?);

    let ack = stream.append(input).await?;

    assert_eq!(ack.start.seq_num, 0);
    assert_eq!(ack.end.seq_num, 3);

    let batch = stream
        .read(
            ReadInput::new()
                .with_start(ReadStart::new().with_from(ReadFrom::SeqNum(1)))
                .with_stop(ReadStop::new().with_limits(ReadLimits::new().with_count(2))),
        )
        .await?;

    assert_eq!(batch.records.len(), 2);
    assert_eq!(batch.records[0].seq_num, 1);
    assert_eq!(batch.records[0].body, "ipsum");
    assert_eq!(batch.records[1].seq_num, 2);
    assert_eq!(batch.records[1].body, "dolor");

    Ok(())
}

#[test_context(S2Stream)]
#[tokio_shared_rt::test(shared)]
async fn read_with_count_limit_exact(stream: &S2Stream) -> Result<(), S2Error> {
    let input = AppendInput::new(AppendRecordBatch::try_from_iter([
        AppendRecord::new("lorem")?,
        AppendRecord::new("ipsum")?,
        AppendRecord::new("dolor")?,
    ])?);

    let ack = stream.append(input).await?;

    assert_eq!(ack.start.seq_num, 0);
    assert_eq!(ack.end.seq_num, 3);

    let batch = stream
        .read(
            ReadInput::new()
                .with_stop(ReadStop::new().with_limits(ReadLimits::new().with_count(3))),
        )
        .await?;

    assert_eq!(batch.records.len(), 3);
    assert_eq!(batch.records[0].seq_num, 0);
    assert_eq!(batch.records[0].body, "lorem");
    assert_eq!(batch.records[2].seq_num, 2);
    assert_eq!(batch.records[2].body, "dolor");

    Ok(())
}

#[test_context(S2Stream)]
#[tokio_shared_rt::test(shared)]
async fn read_with_count_limit_exceeds(stream: &S2Stream) -> Result<(), S2Error> {
    let input = AppendInput::new(AppendRecordBatch::try_from_iter([
        AppendRecord::new("lorem")?,
        AppendRecord::new("ipsum")?,
    ])?);

    let ack = stream.append(input).await?;

    assert_eq!(ack.start.seq_num, 0);
    assert_eq!(ack.end.seq_num, 2);

    let batch = stream
        .read(
            ReadInput::new()
                .with_start(ReadStart::new().with_from(ReadFrom::SeqNum(0)))
                .with_stop(ReadStop::new().with_limits(ReadLimits::new().with_count(5))),
        )
        .await?;

    assert_eq!(batch.records.len(), 2);

    Ok(())
}

#[test_context(S2Stream)]
#[tokio_shared_rt::test(shared)]
async fn read_with_bytes_limit_partial(stream: &S2Stream) -> Result<(), S2Error> {
    let input = AppendInput::new(AppendRecordBatch::try_from_iter([
        AppendRecord::new("lorem")?,
        AppendRecord::new("ipsum")?,
        AppendRecord::new("dolor")?,
    ])?);
    let bytes_limit = input.records.metered_bytes() - 5;

    let ack = stream.append(input).await?;

    assert_eq!(ack.start.seq_num, 0);
    assert_eq!(ack.end.seq_num, 3);

    let batch = stream
        .read(
            ReadInput::new()
                .with_stop(ReadStop::new().with_limits(ReadLimits::new().with_bytes(bytes_limit))),
        )
        .await?;

    assert_eq!(batch.records.len(), 2);

    Ok(())
}

#[test_context(S2Stream)]
#[tokio_shared_rt::test(shared)]
async fn read_with_bytes_limit_exact(stream: &S2Stream) -> Result<(), S2Error> {
    let input = AppendInput::new(AppendRecordBatch::try_from_iter([AppendRecord::new(
        "lorem",
    )?])?);
    let bytes_limit = input.records.metered_bytes();

    let ack = stream.append(input).await?;

    assert_eq!(ack.start.seq_num, 0);
    assert_eq!(ack.end.seq_num, 1);

    let batch = stream
        .read(
            ReadInput::new()
                .with_stop(ReadStop::new().with_limits(ReadLimits::new().with_bytes(bytes_limit))),
        )
        .await?;

    assert_eq!(batch.records.len(), 1);
    assert_eq!(batch.records[0].body, "lorem");

    Ok(())
}

#[test_context(S2Stream)]
#[tokio_shared_rt::test(shared)]
async fn read_with_bytes_limit_exceeds(stream: &S2Stream) -> Result<(), S2Error> {
    let input = AppendInput::new(AppendRecordBatch::try_from_iter([
        AppendRecord::new("lorem")?,
        AppendRecord::new("ipsum")?,
    ])?);

    let ack = stream.append(input).await?;

    assert_eq!(ack.start.seq_num, 0);
    assert_eq!(ack.end.seq_num, 2);

    let batch = stream
        .read(
            ReadInput::new()
                .with_stop(ReadStop::new().with_limits(ReadLimits::new().with_bytes(1000))),
        )
        .await?;

    assert_eq!(batch.records.len(), 2);

    Ok(())
}

#[test_context(S2Stream)]
#[tokio_shared_rt::test(shared)]
async fn read_from_seq_num_with_bytes_limit(stream: &S2Stream) -> Result<(), S2Error> {
    let input = AppendInput::new(AppendRecordBatch::try_from_iter([
        AppendRecord::new("lorem")?,
        AppendRecord::new("ipsum")?,
        AppendRecord::new("dolor")?,
        AppendRecord::new("sit")?,
    ])?);
    let bytes_limit = input.records[1].metered_bytes() + input.records[2].metered_bytes();

    let ack = stream.append(input).await?;

    assert_eq!(ack.start.seq_num, 0);
    assert_eq!(ack.end.seq_num, 4);

    let batch = stream
        .read(
            ReadInput::new()
                .with_start(ReadStart::new().with_from(ReadFrom::SeqNum(1)))
                .with_stop(ReadStop::new().with_limits(ReadLimits::new().with_bytes(bytes_limit))),
        )
        .await?;

    assert_eq!(batch.records.len(), 2);
    assert_eq!(batch.records[0].seq_num, 1);
    assert_eq!(batch.records[0].body, "ipsum");
    assert_eq!(batch.records[1].seq_num, 2);
    assert_eq!(batch.records[1].body, "dolor");

    Ok(())
}

#[test_context(S2Stream)]
#[tokio_shared_rt::test(shared)]
async fn read_with_zero_bytes_limit(stream: &S2Stream) -> Result<(), S2Error> {
    let input = AppendInput::new(AppendRecordBatch::try_from_iter([AppendRecord::new(
        "lorem",
    )?])?);

    let ack = stream.append(input).await?;

    assert_eq!(ack.start.seq_num, 0);
    assert_eq!(ack.end.seq_num, 1);

    let batch = stream
        .read(
            ReadInput::new()
                .with_stop(ReadStop::new().with_limits(ReadLimits::new().with_bytes(0))),
        )
        .await?;

    assert_eq!(batch.records.len(), 0);

    Ok(())
}

#[test_context(S2Stream)]
#[tokio_shared_rt::test(shared)]
async fn read_with_unbounded_limit(stream: &S2Stream) -> Result<(), S2Error> {
    let input = AppendInput::new(AppendRecordBatch::try_from_iter([
        AppendRecord::new("lorem")?,
        AppendRecord::new("ipsum")?,
    ])?);

    let ack = stream.append(input).await?;

    assert_eq!(ack.start.seq_num, 0);
    assert_eq!(ack.end.seq_num, 2);

    let batch = stream
        .read(ReadInput::new().with_stop(ReadStop::new().with_limits(ReadLimits::new())))
        .await?;

    assert_eq!(batch.records.len(), 2);

    Ok(())
}

#[test_context(S2Stream)]
#[tokio_shared_rt::test(shared)]
async fn append_empty_body_record(stream: &S2Stream) -> Result<(), S2Error> {
    let input = AppendInput::new(AppendRecordBatch::try_from_iter([AppendRecord::new("")?])?);

    let ack = stream.append(input).await?;

    assert_eq!(ack.start.seq_num, 0);
    assert_eq!(ack.end.seq_num, 1);

    Ok(())
}

#[test_context(S2Stream)]
#[tokio_shared_rt::test(shared)]
async fn append_with_mismatched_seq_num_errors(stream: &S2Stream) -> Result<(), S2Error> {
    let input1 = AppendInput::new(AppendRecordBatch::try_from_iter([AppendRecord::new(
        "lorem",
    )?])?);

    let ack = stream.append(input1).await?;

    assert_eq!(ack.start.seq_num, 0);
    assert_eq!(ack.end.seq_num, 1);

    let input2 = AppendInput::new(AppendRecordBatch::try_from_iter([AppendRecord::new(
        "ipsum",
    )?])?)
    .with_match_seq_num(0);

    let result = stream.append(input2).await;

    assert_matches!(
        result,
        Err(S2Error::AppendConditionFailed(
            AppendConditionFailed::SeqNumMismatch(1)
        ))
    );

    Ok(())
}

#[test_context(S2Stream)]
#[tokio_shared_rt::test(shared)]
async fn append_session_with_mismatched_seq_num_errors(stream: &S2Stream) -> Result<(), S2Error> {
    let session = stream.append_session(AppendSessionConfig::new());

    let input1 = AppendInput::new(AppendRecordBatch::try_from_iter([AppendRecord::new(
        "lorem",
    )?])?);

    let ack = session.submit(input1).await?.await?;

    assert_eq!(ack.start.seq_num, 0);
    assert_eq!(ack.end.seq_num, 1);

    let input2 = AppendInput::new(AppendRecordBatch::try_from_iter([AppendRecord::new(
        "ipsum",
    )?])?)
    .with_match_seq_num(0);

    let result = session.submit(input2).await?.await;

    assert_matches!(
        result,
        Err(S2Error::AppendConditionFailed(
            AppendConditionFailed::SeqNumMismatch(1)
        ))
    );

    Ok(())
}

#[test_context(S2Stream)]
#[tokio_shared_rt::test(shared)]
async fn append_with_mismatched_fencing_token_errors(stream: &S2Stream) -> Result<(), S2Error> {
    let fencing_token_1 = FencingToken::generate(30).expect("valid fencing token");
    let input1 = AppendInput::new(AppendRecordBatch::try_from_iter([CommandRecord::fence(
        fencing_token_1.clone(),
    )
    .into()])?);

    let ack = stream.append(input1).await?;

    assert_eq!(ack.start.seq_num, 0);
    assert_eq!(ack.end.seq_num, 1);

    let fencing_token_2 = FencingToken::generate(30).expect("valid fencing token");
    let input2 = AppendInput::new(AppendRecordBatch::try_from_iter([AppendRecord::new(
        "ipsum",
    )?])?)
    .with_fencing_token(fencing_token_2);

    let result = stream.append(input2).await;

    assert_matches!(
        result,
        Err(S2Error::AppendConditionFailed(AppendConditionFailed::FencingTokenMismatch(fencing_token))) => {
            assert_eq!(fencing_token, fencing_token_1)
        }
    );

    Ok(())
}

#[test_context(S2Stream)]
#[tokio_shared_rt::test(shared)]
async fn append_session_with_mismatched_fencing_token_errors(
    stream: &S2Stream,
) -> Result<(), S2Error> {
    let session = stream.append_session(AppendSessionConfig::new());

    let fencing_token_1 = FencingToken::generate(30).expect("valid fencing token");
    let input1 = AppendInput::new(AppendRecordBatch::try_from_iter([CommandRecord::fence(
        fencing_token_1.clone(),
    )
    .into()])?);

    let ack = session.submit(input1).await?.await?;

    assert_eq!(ack.start.seq_num, 0);
    assert_eq!(ack.end.seq_num, 1);

    let fencing_token_2 = FencingToken::generate(30).expect("valid fencing token");
    let input2 = AppendInput::new(AppendRecordBatch::try_from_iter([AppendRecord::new(
        "ipsum",
    )?])?)
    .with_fencing_token(fencing_token_2);

    let result = session.submit(input2).await?.await;

    assert_matches!(
        result,
        Err(S2Error::AppendConditionFailed(AppendConditionFailed::FencingTokenMismatch(fencing_token))) => {
            assert_eq!(fencing_token, fencing_token_1)
        }
    );

    Ok(())
}

#[test_context(S2Stream)]
#[tokio_shared_rt::test(shared)]
async fn read_empty_stream_errors(stream: &S2Stream) -> Result<(), S2Error> {
    let result = stream.read(ReadInput::new()).await;

    assert_matches!(
        result,
        Err(S2Error::ReadUnwritten(StreamPosition {
            seq_num: 0,
            timestamp: 0,
            ..
        }))
    );

    Ok(())
}

#[test_context(S2Stream)]
#[tokio_shared_rt::test(shared)]
async fn read_beyond_tail_errors(stream: &S2Stream) -> Result<(), S2Error> {
    let input = AppendInput::new(AppendRecordBatch::try_from_iter([AppendRecord::new(
        "lorem",
    )?])?);

    let ack = stream.append(input).await?;

    assert_eq!(ack.start.seq_num, 0);
    assert_eq!(ack.end.seq_num, 1);

    let result = stream
        .read(ReadInput::new().with_start(ReadStart::new().with_from(ReadFrom::SeqNum(10))))
        .await;

    assert_matches!(
        result,
        Err(S2Error::ReadUnwritten(StreamPosition { seq_num: 1, .. }))
    );

    Ok(())
}

#[test_context(S2Stream)]
#[tokio_shared_rt::test(shared)]
async fn read_beyond_tail_with_clamp_to_tail_errors(stream: &S2Stream) -> Result<(), S2Error> {
    let input = AppendInput::new(AppendRecordBatch::try_from_iter([AppendRecord::new(
        "lorem",
    )?])?);

    let ack = stream.append(input).await?;

    assert_eq!(ack.start.seq_num, 0);
    assert_eq!(ack.end.seq_num, 1);

    let result = stream
        .read(
            ReadInput::new().with_start(
                ReadStart::new()
                    .with_from(ReadFrom::SeqNum(10))
                    .with_clamp_to_tail(true),
            ),
        )
        .await;

    assert_matches!(
        result,
        Err(S2Error::ReadUnwritten(StreamPosition { seq_num: 1, .. }))
    );

    Ok(())
}

#[test_context(S2Stream)]
#[tokio_shared_rt::test(shared)]
async fn read_beyond_tail_with_clamp_to_tail_and_wait_returns_empty_batch(
    stream: &S2Stream,
) -> Result<(), S2Error> {
    let input = AppendInput::new(AppendRecordBatch::try_from_iter([AppendRecord::new(
        "lorem",
    )?])?);

    let ack = stream.append(input).await?;

    assert_eq!(ack.start.seq_num, 0);
    assert_eq!(ack.end.seq_num, 1);

    let batch = stream
        .read(
            ReadInput::new()
                .with_start(
                    ReadStart::new()
                        .with_from(ReadFrom::SeqNum(10))
                        .with_clamp_to_tail(true),
                )
                .with_stop(ReadStop::new().with_wait(1)),
        )
        .await?;

    assert!(batch.records.is_empty());

    Ok(())
}

#[test_context(S2Stream)]
#[tokio_shared_rt::test(shared)]
async fn read_session_beyond_tail_errors(stream: &S2Stream) -> Result<(), S2Error> {
    let input = AppendInput::new(AppendRecordBatch::try_from_iter([AppendRecord::new(
        "lorem",
    )?])?);

    let ack = stream.append(input).await?;

    assert_eq!(ack.start.seq_num, 0);
    assert_eq!(ack.end.seq_num, 1);

    let result = stream
        .read_session(ReadInput::new().with_start(ReadStart::new().with_from(ReadFrom::SeqNum(10))))
        .await;

    assert!(result.is_err());
    assert_matches!(
        result.err().expect("should be err"),
        S2Error::ReadUnwritten(StreamPosition { seq_num: 1, .. })
    );

    Ok(())
}

#[test_context(S2Stream)]
#[tokio_shared_rt::test(shared)]
async fn read_session_beyond_tail_with_clamp_to_tail(stream: &S2Stream) -> Result<(), S2Error> {
    let input = AppendInput::new(AppendRecordBatch::try_from_iter([AppendRecord::new(
        "lorem",
    )?])?);

    let ack = stream.append(input).await?;

    assert_eq!(ack.start.seq_num, 0);
    assert_eq!(ack.end.seq_num, 1);

    let mut batches = stream
        .read_session(
            ReadInput::new().with_start(
                ReadStart::new()
                    .with_from(ReadFrom::SeqNum(10))
                    .with_clamp_to_tail(true),
            ),
        )
        .await?;

    let result = tokio::time::timeout(Duration::from_secs(1), batches.next()).await;
    assert_matches!(result, Err(tokio::time::error::Elapsed { .. }));

    Ok(())
}

#[test_context(S2Stream)]
#[tokio_shared_rt::test(shared)]
async fn append_with_empty_header_value(stream: &S2Stream) -> Result<(), S2Error> {
    let input = AppendInput::new(AppendRecordBatch::try_from_iter([AppendRecord::new(
        "lorem",
    )?
    .with_headers([Header::new("key1", ""), Header::new("key2", "")])?])?);

    let ack = stream.append(input).await?;

    assert_eq!(ack.start.seq_num, 0);
    assert_eq!(ack.end.seq_num, 1);

    let batch = stream.read(ReadInput::new()).await?;

    assert_eq!(batch.records.len(), 1);
    assert_eq!(batch.records[0].headers.len(), 2);

    Ok(())
}

#[test_context(S2Stream)]
#[tokio_shared_rt::test(shared)]
async fn append_mixed_records_with_and_without_headers(stream: &S2Stream) -> Result<(), S2Error> {
    let input = AppendInput::new(AppendRecordBatch::try_from_iter([
        AppendRecord::new("lorem")?.with_headers([Header::new("key1", "value1")])?,
        AppendRecord::new("ipsum")?
            .with_headers([Header::new("key2", ""), Header::new("key3", "value3")])?,
        AppendRecord::new("dolor")?,
    ])?);

    let ack = stream.append(input).await?;

    assert_eq!(ack.start.seq_num, 0);
    assert_eq!(ack.end.seq_num, 3);

    let batch = stream.read(ReadInput::new()).await?;

    assert_eq!(batch.records.len(), 3);
    assert_eq!(batch.records[0].headers.len(), 1);
    assert_eq!(batch.records[1].headers.len(), 2);
    assert_eq!(batch.records[2].headers.len(), 0);

    Ok(())
}

#[test_context(S2Stream)]
#[tokio_shared_rt::test(shared)]
async fn check_tail_after_multiple_appends(stream: &S2Stream) -> Result<(), S2Error> {
    let input1 = AppendInput::new(AppendRecordBatch::try_from_iter([AppendRecord::new(
        "lorem",
    )?])?);

    let ack1 = stream.append(input1).await?;

    assert_eq!(ack1.start.seq_num, 0);
    assert_eq!(ack1.end.seq_num, 1);

    let tail1 = stream.check_tail().await?;
    assert_eq!(tail1.seq_num, 1);

    let input2 = AppendInput::new(AppendRecordBatch::try_from_iter([
        AppendRecord::new("ipsum")?,
        AppendRecord::new("dolor")?,
    ])?);

    let ack2 = stream.append(input2).await?;

    assert_eq!(ack2.start.seq_num, 1);
    assert_eq!(ack2.end.seq_num, 3);

    let tail2 = stream.check_tail().await?;
    assert_eq!(tail2.seq_num, 3);

    Ok(())
}

#[test_context(S2Stream)]
#[tokio_shared_rt::test(shared)]
async fn read_from_timestamp(stream: &S2Stream) -> Result<(), S2Error> {
    let base_timestamp = OffsetDateTime::now_utc().unix_timestamp() as u64;
    let input = AppendInput::new(AppendRecordBatch::try_from_iter([
        AppendRecord::new("lorem")?.with_timestamp(base_timestamp),
        AppendRecord::new("ipsum")?.with_timestamp(base_timestamp + 1),
        AppendRecord::new("dolor")?.with_timestamp(base_timestamp + 2),
        AppendRecord::new("sit")?.with_timestamp(base_timestamp + 3),
    ])?);

    let ack = stream.append(input).await?;

    assert_eq!(ack.start.seq_num, 0);
    assert_eq!(ack.start.timestamp, base_timestamp);
    assert_eq!(ack.end.seq_num, 4);
    assert_eq!(ack.end.timestamp, base_timestamp + 3);

    let batch = stream
        .read(
            ReadInput::new()
                .with_start(ReadStart::new().with_from(ReadFrom::Timestamp(base_timestamp + 1))),
        )
        .await?;

    assert_eq!(batch.records.len(), 3);
    assert_eq!(batch.records[0].seq_num, 1);
    assert_eq!(batch.records[0].timestamp, base_timestamp + 1);
    assert_eq!(batch.records[1].seq_num, 2);
    assert_eq!(batch.records[1].timestamp, base_timestamp + 2);
    assert_eq!(batch.records[2].seq_num, 3);
    assert_eq!(batch.records[2].timestamp, base_timestamp + 3);

    Ok(())
}

#[test_context(S2Stream)]
#[tokio_shared_rt::test(shared)]
async fn read_from_timestamp_with_count_limit(stream: &S2Stream) -> Result<(), S2Error> {
    let base_timestamp = OffsetDateTime::now_utc().unix_timestamp() as u64;
    let input = AppendInput::new(AppendRecordBatch::try_from_iter([
        AppendRecord::new("lorem")?.with_timestamp(base_timestamp),
        AppendRecord::new("ipsum")?.with_timestamp(base_timestamp + 1),
        AppendRecord::new("dolor")?.with_timestamp(base_timestamp + 2),
        AppendRecord::new("sit")?.with_timestamp(base_timestamp + 3),
    ])?);

    let ack = stream.append(input).await?;

    assert_eq!(ack.start.seq_num, 0);
    assert_eq!(ack.start.timestamp, base_timestamp);
    assert_eq!(ack.end.seq_num, 4);
    assert_eq!(ack.end.timestamp, base_timestamp + 3);

    let batch = stream
        .read(
            ReadInput::new()
                .with_start(ReadStart::new().with_from(ReadFrom::Timestamp(base_timestamp + 2)))
                .with_stop(ReadStop::new().with_limits(ReadLimits::new().with_count(1))),
        )
        .await?;

    assert_eq!(batch.records.len(), 1);
    assert_eq!(batch.records[0].seq_num, 2);
    assert_eq!(batch.records[0].timestamp, base_timestamp + 2);

    Ok(())
}

#[test_context(S2Stream)]
#[tokio_shared_rt::test(shared)]
async fn read_from_timestamp_with_bytes_limit(stream: &S2Stream) -> Result<(), S2Error> {
    let base_timestamp = OffsetDateTime::now_utc().unix_timestamp() as u64;
    let input = AppendInput::new(AppendRecordBatch::try_from_iter([
        AppendRecord::new("lorem")?.with_timestamp(base_timestamp),
        AppendRecord::new("ipsum")?.with_timestamp(base_timestamp + 1),
        AppendRecord::new("dolor")?.with_timestamp(base_timestamp + 2),
        AppendRecord::new("sit")?.with_timestamp(base_timestamp + 3),
    ])?);
    let bytes_limit = input.records[1].metered_bytes() + 5;

    let ack = stream.append(input).await?;

    assert_eq!(ack.start.seq_num, 0);
    assert_eq!(ack.start.timestamp, base_timestamp);
    assert_eq!(ack.end.seq_num, 4);
    assert_eq!(ack.end.timestamp, base_timestamp + 3);

    let batch = stream
        .read(
            ReadInput::new()
                .with_start(ReadStart::new().with_from(ReadFrom::Timestamp(base_timestamp + 1)))
                .with_stop(ReadStop::new().with_limits(ReadLimits::new().with_bytes(bytes_limit))),
        )
        .await?;

    assert_eq!(batch.records.len(), 1);
    assert_eq!(batch.records[0].seq_num, 1);
    assert_eq!(batch.records[0].timestamp, base_timestamp + 1);

    Ok(())
}

#[test_context(S2Stream)]
#[tokio_shared_rt::test(shared)]
async fn read_from_timestamp_in_future_errors(stream: &S2Stream) -> Result<(), S2Error> {
    let base_timestamp = OffsetDateTime::now_utc().unix_timestamp() as u64;
    let input = AppendInput::new(AppendRecordBatch::try_from_iter([
        AppendRecord::new("lorem")?.with_timestamp(base_timestamp),
        AppendRecord::new("ipsum")?.with_timestamp(base_timestamp + 1),
    ])?);

    let ack = stream.append(input).await?;

    assert_eq!(ack.start.seq_num, 0);
    assert_eq!(ack.start.timestamp, base_timestamp);
    assert_eq!(ack.end.seq_num, 2);
    assert_eq!(ack.end.timestamp, base_timestamp + 1);

    let result = stream
        .read(
            ReadInput::new()
                .with_start(ReadStart::new().with_from(ReadFrom::Timestamp(base_timestamp + 100))),
        )
        .await;

    assert_matches!(result, Err(S2Error::ReadUnwritten(tail)) => {
        assert_eq!(tail.seq_num, 2);
        assert_eq!(tail.timestamp, base_timestamp + 1);
    });

    Ok(())
}

#[test_context(S2Stream)]
#[tokio_shared_rt::test(shared)]
async fn append_session_close_delivers_all_acks(stream: &S2Stream) -> Result<(), S2Error> {
    let session = stream.append_session(AppendSessionConfig::default());

    let ticket1 = session
        .submit(AppendInput::new(AppendRecordBatch::try_from_iter([
            AppendRecord::new("lorem")?,
            AppendRecord::new("ipsum")?,
        ])?))
        .await?;
    let ticket2 = session
        .submit(AppendInput::new(AppendRecordBatch::try_from_iter([
            AppendRecord::new("dolor")?,
        ])?))
        .await?;
    let ticket3 = session
        .submit(AppendInput::new(AppendRecordBatch::try_from_iter([
            AppendRecord::new("sit")?,
        ])?))
        .await?;

    session.close().await?;

    let ack1 = ticket1.await?;
    let ack2 = ticket2.await?;
    let ack3 = ticket3.await?;

    assert_eq!(ack1.start.seq_num, 0);
    assert_eq!(ack1.end.seq_num, 2);
    assert_eq!(ack2.start.seq_num, 2);
    assert_eq!(ack2.end.seq_num, 3);
    assert_eq!(ack3.start.seq_num, 3);
    assert_eq!(ack3.end.seq_num, 4);

    Ok(())
}

#[test_context(S2Stream)]
#[tokio_shared_rt::test(shared)]
async fn batch_submit_ticket_drop_should_not_affect_others(
    stream: &S2Stream,
) -> Result<(), S2Error> {
    let session = stream.append_session(AppendSessionConfig::default());

    let _ticket1 = session
        .submit(AppendInput::new(AppendRecordBatch::try_from_iter([
            AppendRecord::new("lorem")?,
            AppendRecord::new("ipsum")?,
        ])?))
        .await?;
    drop(_ticket1);

    let ticket2 = session
        .submit(AppendInput::new(AppendRecordBatch::try_from_iter([
            AppendRecord::new("dolor")?,
        ])?))
        .await?;

    session.close().await?;

    let ack2 = ticket2.await?;
    assert_eq!(ack2.start.seq_num, 2);
    assert_eq!(ack2.end.seq_num, 3);

    Ok(())
}

#[test_context(S2Stream)]
#[tokio_shared_rt::test(shared)]
async fn producer_delivers_all_acks(stream: &S2Stream) -> Result<(), S2Error> {
    let producer = stream.producer(ProducerConfig::default());

    let ack1 = producer.submit(AppendRecord::new("lorem")?).await?.await?;
    let ack2 = producer.submit(AppendRecord::new("ipsum")?).await?.await?;
    let ack3 = producer.submit(AppendRecord::new("dolor")?).await?.await?;

    assert_eq!(ack1.seq_num, 0);
    assert_eq!(ack2.seq_num, 1);
    assert_eq!(ack3.seq_num, 2);

    Ok(())
}

#[test_context(S2Stream)]
#[tokio_shared_rt::test(shared)]
async fn producer_close_delivers_all_indexed_acks_from_same_ack(
    stream: &S2Stream,
) -> Result<(), S2Error> {
    let producer = stream.producer(ProducerConfig::default());

    let ticket1 = producer.submit(AppendRecord::new("lorem")?).await?;
    let ticket2 = producer.submit(AppendRecord::new("ipsum")?).await?;
    let ticket3 = producer.submit(AppendRecord::new("dolor")?).await?;

    producer.close().await?;

    let ack1 = ticket1.await?;
    let ack2 = ticket2.await?;
    let ack3 = ticket3.await?;

    assert_eq!(ack1.seq_num, 0);
    assert_eq!(ack2.seq_num, 1);
    assert_eq!(ack3.seq_num, 2);

    assert_eq!(ack1.batch, ack2.batch);
    assert_eq!(ack2.batch, ack3.batch);

    Ok(())
}

#[test_context(S2Stream)]
#[tokio_shared_rt::test(shared)]
async fn producer_close_delivers_all_indexed_acks_from_different_acks(
    stream: &S2Stream,
) -> Result<(), S2Error> {
    let producer = stream.producer(
        ProducerConfig::default()
            .with_batching(BatchingConfig::default().with_max_batch_records(1)?),
    );

    let ticket1 = producer.submit(AppendRecord::new("lorem")?).await?;
    let ticket2 = producer.submit(AppendRecord::new("ipsum")?).await?;
    let ticket3 = producer.submit(AppendRecord::new("dolor")?).await?;

    producer.close().await?;

    let ack1 = ticket1.await?;
    let ack2 = ticket2.await?;
    let ack3 = ticket3.await?;

    assert_eq!(ack1.seq_num, 0);
    assert_eq!(ack2.seq_num, 1);
    assert_eq!(ack3.seq_num, 2);

    assert_ne!(ack1.batch, ack2.batch);
    assert_ne!(ack2.batch, ack3.batch);

    Ok(())
}

#[test_context(S2Stream)]
#[tokio_shared_rt::test(shared)]
async fn producer_drop_errors_all_claimable_tickets(stream: &S2Stream) -> Result<(), S2Error> {
    let producer = stream.producer(
        ProducerConfig::default()
            .with_batching(BatchingConfig::default().with_linger(Duration::from_secs(1))),
    );

    let ticket1 = producer.submit(AppendRecord::new("lorem")?).await?;
    let ticket2 = producer.submit(AppendRecord::new("ipsum")?).await?;

    drop(producer);

    let result1 = ticket1.await;
    let result2 = ticket2.await;

    assert_matches!(result1, Err(S2Error::Client(msg)) => {
        assert_eq!(msg, "producer dropped without calling close");
    });
    assert_matches!(result2, Err(S2Error::Client(msg)) => {
        assert_eq!(msg, "producer dropped without calling close");
    });

    Ok(())
}

#[test_context(S2Stream)]
#[tokio_shared_rt::test(shared)]
async fn producer_drop_errors_no_claimable_tickets(stream: &S2Stream) -> Result<(), S2Error> {
    let producer = stream.producer(ProducerConfig::default());

    let ticket1 = producer.submit(AppendRecord::new("lorem")?).await?;
    let ticket2 = producer.submit(AppendRecord::new("ipsum")?).await?;

    tokio::time::sleep(Duration::from_secs(1)).await;

    drop(producer);

    let ack1 = ticket1.await?;
    let ack2 = ticket2.await?;

    assert_eq!(ack1.seq_num, 0);
    assert_eq!(ack2.seq_num, 1);

    Ok(())
}

#[test_context(S2Stream)]
#[tokio_shared_rt::test(shared)]
async fn record_submit_ticket_drop_should_not_affect_others(
    stream: &S2Stream,
) -> Result<(), S2Error> {
    let producer = stream.producer(ProducerConfig::default());

    let _ticket1 = producer.submit(AppendRecord::new("lorem")?).await?;
    drop(_ticket1);

    let ticket2 = producer.submit(AppendRecord::new("ipsum")?).await?;

    producer.close().await?;

    let ack2 = ticket2.await?;
    assert_eq!(ack2.seq_num, 1);

    Ok(())
}

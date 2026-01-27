mod common;

use assert_matches::assert_matches;
use common::{S2Stream, s2, unique_basin_name, unique_stream_name};
use s2_sdk::types::*;
use test_context::test_context;
use time::OffsetDateTime;

fn now_epoch_secs() -> u32 {
    OffsetDateTime::now_utc().unix_timestamp() as u32
}

fn one_hour_ago() -> u32 {
    now_epoch_secs().saturating_sub(3600)
}

fn one_day_ago() -> u32 {
    now_epoch_secs().saturating_sub(86400)
}

fn one_week_ago() -> u32 {
    now_epoch_secs().saturating_sub(7 * 86400)
}

#[tokio::test]
async fn account_metrics_active_basins() -> Result<(), S2Error> {
    let s2 = s2();
    let now = now_epoch_secs();
    let start = one_hour_ago();

    let metrics = s2
        .get_account_metrics(GetAccountMetricsInput::new(AccountMetricSet::ActiveBasins(
            TimeRange::new(start, now),
        )))
        .await?;

    assert!(!metrics.is_empty());
    assert_matches!(&metrics[0], Metric::Label(label) => {
        assert_eq!(label.name, "active_basins");
    });

    Ok(())
}

#[tokio::test]
async fn account_metrics_account_ops() -> Result<(), S2Error> {
    let s2 = s2();
    let now = now_epoch_secs();
    let start = one_hour_ago();

    let metrics = s2
        .get_account_metrics(GetAccountMetricsInput::new(AccountMetricSet::AccountOps(
            TimeRangeAndInterval::new(start, now),
        )))
        .await?;

    for metric in &metrics {
        assert_matches!(metric, Metric::Accumulation(acc) => {
            assert_eq!(acc.unit, MetricUnit::Operations);
        });
    }

    Ok(())
}

#[tokio::test]
async fn account_metrics_account_ops_with_minute_interval() -> Result<(), S2Error> {
    let s2 = s2();
    let now = now_epoch_secs();
    let start = one_hour_ago();

    let metrics = s2
        .get_account_metrics(GetAccountMetricsInput::new(AccountMetricSet::AccountOps(
            TimeRangeAndInterval::new(start, now).with_interval(TimeseriesInterval::Minute),
        )))
        .await?;

    for metric in &metrics {
        assert_matches!(metric, Metric::Accumulation(acc) => {
            assert_eq!(acc.interval, TimeseriesInterval::Minute);
        });
    }

    Ok(())
}

#[tokio::test]
async fn account_metrics_account_ops_with_hour_interval() -> Result<(), S2Error> {
    let s2 = s2();
    let now = now_epoch_secs();
    let start = one_day_ago();

    let metrics = s2
        .get_account_metrics(GetAccountMetricsInput::new(AccountMetricSet::AccountOps(
            TimeRangeAndInterval::new(start, now).with_interval(TimeseriesInterval::Hour),
        )))
        .await?;

    for metric in &metrics {
        assert_matches!(metric, Metric::Accumulation(acc) => {
            assert_eq!(acc.interval, TimeseriesInterval::Hour);
        });
    }

    Ok(())
}

#[tokio::test]
async fn account_metrics_account_ops_with_day_interval() -> Result<(), S2Error> {
    let s2 = s2();
    let now = now_epoch_secs();
    let start = one_week_ago();

    let metrics = s2
        .get_account_metrics(GetAccountMetricsInput::new(AccountMetricSet::AccountOps(
            TimeRangeAndInterval::new(start, now).with_interval(TimeseriesInterval::Day),
        )))
        .await?;

    for metric in &metrics {
        assert_matches!(metric, Metric::Accumulation(acc) => {
            assert_eq!(acc.interval, TimeseriesInterval::Day);
        });
    }

    Ok(())
}

#[tokio::test]
async fn basin_metrics_storage() -> Result<(), S2Error> {
    let s2 = s2();
    let basin_name = unique_basin_name();
    let now = now_epoch_secs();
    let start = one_hour_ago();

    s2.create_basin(CreateBasinInput::new(basin_name.clone()))
        .await?;

    let metrics = s2
        .get_basin_metrics(GetBasinMetricsInput::new(
            basin_name.clone(),
            BasinMetricSet::Storage(TimeRange::new(start, now)),
        ))
        .await?;

    for metric in &metrics {
        assert_matches!(metric, Metric::Gauge(gauge) => {
            assert_eq!(gauge.unit, MetricUnit::Bytes);
        });
    }

    s2.delete_basin(DeleteBasinInput::new(basin_name)).await?;

    Ok(())
}

#[tokio::test]
async fn basin_metrics_append_ops() -> Result<(), S2Error> {
    let s2 = s2();
    let basin_name = unique_basin_name();
    let now = now_epoch_secs();
    let start = one_hour_ago();

    s2.create_basin(CreateBasinInput::new(basin_name.clone()))
        .await?;

    let metrics = s2
        .get_basin_metrics(GetBasinMetricsInput::new(
            basin_name.clone(),
            BasinMetricSet::AppendOps(TimeRangeAndInterval::new(start, now)),
        ))
        .await?;

    for metric in &metrics {
        assert_matches!(metric, Metric::Accumulation(acc) => {
            assert_eq!(acc.unit, MetricUnit::Operations);
        });
    }

    s2.delete_basin(DeleteBasinInput::new(basin_name)).await?;

    Ok(())
}

#[tokio::test]
async fn basin_metrics_read_ops() -> Result<(), S2Error> {
    let s2 = s2();
    let basin_name = unique_basin_name();
    let now = now_epoch_secs();
    let start = one_hour_ago();

    s2.create_basin(CreateBasinInput::new(basin_name.clone()))
        .await?;

    let metrics = s2
        .get_basin_metrics(GetBasinMetricsInput::new(
            basin_name.clone(),
            BasinMetricSet::ReadOps(TimeRangeAndInterval::new(start, now)),
        ))
        .await?;

    for metric in &metrics {
        assert_matches!(metric, Metric::Accumulation(acc) => {
            assert_eq!(acc.unit, MetricUnit::Operations);
        });
    }

    s2.delete_basin(DeleteBasinInput::new(basin_name)).await?;

    Ok(())
}

#[tokio::test]
async fn basin_metrics_read_throughput() -> Result<(), S2Error> {
    let s2 = s2();
    let basin_name = unique_basin_name();
    let now = now_epoch_secs();
    let start = one_hour_ago();

    s2.create_basin(CreateBasinInput::new(basin_name.clone()))
        .await?;

    let metrics = s2
        .get_basin_metrics(GetBasinMetricsInput::new(
            basin_name.clone(),
            BasinMetricSet::ReadThroughput(TimeRangeAndInterval::new(start, now)),
        ))
        .await?;

    for metric in &metrics {
        assert_matches!(metric, Metric::Accumulation(acc) => {
            assert_eq!(acc.unit, MetricUnit::Bytes);
        });
    }

    s2.delete_basin(DeleteBasinInput::new(basin_name)).await?;

    Ok(())
}

#[tokio::test]
async fn basin_metrics_append_throughput() -> Result<(), S2Error> {
    let s2 = s2();
    let basin_name = unique_basin_name();
    let now = now_epoch_secs();
    let start = one_hour_ago();

    s2.create_basin(CreateBasinInput::new(basin_name.clone()))
        .await?;

    let metrics = s2
        .get_basin_metrics(GetBasinMetricsInput::new(
            basin_name.clone(),
            BasinMetricSet::AppendThroughput(TimeRangeAndInterval::new(start, now)),
        ))
        .await?;

    for metric in &metrics {
        assert_matches!(metric, Metric::Accumulation(acc) => {
            assert_eq!(acc.unit, MetricUnit::Bytes);
        });
    }

    s2.delete_basin(DeleteBasinInput::new(basin_name)).await?;

    Ok(())
}

#[tokio::test]
async fn basin_metrics_basin_ops() -> Result<(), S2Error> {
    let s2 = s2();
    let basin_name = unique_basin_name();
    let now = now_epoch_secs();
    let start = one_hour_ago();

    s2.create_basin(CreateBasinInput::new(basin_name.clone()))
        .await?;

    let metrics = s2
        .get_basin_metrics(GetBasinMetricsInput::new(
            basin_name.clone(),
            BasinMetricSet::BasinOps(TimeRangeAndInterval::new(start, now)),
        ))
        .await?;

    for metric in &metrics {
        assert_matches!(metric, Metric::Accumulation(acc) => {
            assert_eq!(acc.unit, MetricUnit::Operations);
        });
    }

    s2.delete_basin(DeleteBasinInput::new(basin_name)).await?;

    Ok(())
}

#[test_context(S2Stream)]
#[tokio_shared_rt::test(shared)]
async fn stream_metrics_storage(stream: &S2Stream) -> Result<(), S2Error> {
    let s2 = s2();
    let now = now_epoch_secs();
    let start = one_hour_ago();

    let input = AppendInput::new(AppendRecordBatch::try_from_iter([AppendRecord::new(
        "test data for metrics",
    )?])?);
    stream.append(input).await?;

    let metrics = s2
        .get_stream_metrics(GetStreamMetricsInput::new(
            stream.basin_name(),
            stream.stream_name(),
            StreamMetricSet::Storage(TimeRange::new(start, now)),
        ))
        .await?;

    for metric in &metrics {
        assert_matches!(metric, Metric::Gauge(gauge) => {
            assert_eq!(gauge.unit, MetricUnit::Bytes);
        });
    }

    Ok(())
}

#[tokio::test]
async fn basin_metrics_for_nonexistent_basin_errors() -> Result<(), S2Error> {
    let s2 = s2();
    let basin_name = unique_basin_name();
    let now = now_epoch_secs();
    let start = one_hour_ago();

    let result = s2
        .get_basin_metrics(GetBasinMetricsInput::new(
            basin_name,
            BasinMetricSet::Storage(TimeRange::new(start, now)),
        ))
        .await;

    assert_matches!(result, Err(S2Error::Server(err)) => {
        assert!(err.code == "permission_denied" || err.code == "basin_not_found");
    });

    Ok(())
}

#[tokio::test]
async fn stream_metrics_for_nonexistent_stream_errors() -> Result<(), S2Error> {
    let s2 = s2();
    let basin_name = unique_basin_name();
    let stream_name = unique_stream_name();
    let now = now_epoch_secs();
    let start = one_hour_ago();

    s2.create_basin(CreateBasinInput::new(basin_name.clone()))
        .await?;

    let result = s2
        .get_stream_metrics(GetStreamMetricsInput::new(
            basin_name.clone(),
            stream_name,
            StreamMetricSet::Storage(TimeRange::new(start, now)),
        ))
        .await;

    assert_matches!(result, Err(S2Error::Server(err)) => {
        assert!(err.code == "permission_denied" || err.code == "stream_not_found");
    });

    s2.delete_basin(DeleteBasinInput::new(basin_name)).await?;

    Ok(())
}

#[tokio::test]
async fn account_metrics_empty_time_range_returns_empty_data() -> Result<(), S2Error> {
    let s2 = s2();
    let far_past = 946684800; // 2000-01-01
    let far_past_end = far_past + 3600;

    let metrics = s2
        .get_account_metrics(GetAccountMetricsInput::new(AccountMetricSet::ActiveBasins(
            TimeRange::new(far_past, far_past_end),
        )))
        .await?;

    assert_matches!(&metrics[0], Metric::Label(label) => {
        assert!(label.values.is_empty());
    });

    Ok(())
}

#[tokio::test]
async fn basin_metrics_storage_with_hour_interval() -> Result<(), S2Error> {
    let s2 = s2();
    let basin_name = unique_basin_name();
    let now = now_epoch_secs();
    let start = one_day_ago();

    s2.create_basin(CreateBasinInput::new(basin_name.clone()))
        .await?;

    let metrics = s2
        .get_basin_metrics(GetBasinMetricsInput::new(
            basin_name.clone(),
            BasinMetricSet::Storage(TimeRange::new(start, now)),
        ))
        .await?;

    for metric in &metrics {
        assert_matches!(metric, Metric::Gauge(gauge) => {
            assert_eq!(gauge.unit, MetricUnit::Bytes);
        });
    }

    s2.delete_basin(DeleteBasinInput::new(basin_name)).await?;

    Ok(())
}

#[tokio::test]
async fn basin_metrics_append_ops_with_minute_interval() -> Result<(), S2Error> {
    let s2 = s2();
    let basin_name = unique_basin_name();
    let now = now_epoch_secs();
    let start = one_hour_ago();

    s2.create_basin(CreateBasinInput::new(basin_name.clone()))
        .await?;

    let metrics = s2
        .get_basin_metrics(GetBasinMetricsInput::new(
            basin_name.clone(),
            BasinMetricSet::AppendOps(
                TimeRangeAndInterval::new(start, now).with_interval(TimeseriesInterval::Minute),
            ),
        ))
        .await?;

    for metric in &metrics {
        assert_matches!(metric, Metric::Accumulation(acc) => {
            assert_eq!(acc.interval, TimeseriesInterval::Minute);
        });
    }

    s2.delete_basin(DeleteBasinInput::new(basin_name)).await?;

    Ok(())
}

#[tokio::test]
async fn basin_metrics_read_ops_with_hour_interval() -> Result<(), S2Error> {
    let s2 = s2();
    let basin_name = unique_basin_name();
    let now = now_epoch_secs();
    let start = one_day_ago();

    s2.create_basin(CreateBasinInput::new(basin_name.clone()))
        .await?;

    let metrics = s2
        .get_basin_metrics(GetBasinMetricsInput::new(
            basin_name.clone(),
            BasinMetricSet::ReadOps(
                TimeRangeAndInterval::new(start, now).with_interval(TimeseriesInterval::Hour),
            ),
        ))
        .await?;

    for metric in &metrics {
        assert_matches!(metric, Metric::Accumulation(acc) => {
            assert_eq!(acc.interval, TimeseriesInterval::Hour);
        });
    }

    s2.delete_basin(DeleteBasinInput::new(basin_name)).await?;

    Ok(())
}

#[tokio::test]
async fn basin_metrics_basin_ops_with_day_interval() -> Result<(), S2Error> {
    let s2 = s2();
    let basin_name = unique_basin_name();
    let now = now_epoch_secs();
    let start = one_week_ago();

    s2.create_basin(CreateBasinInput::new(basin_name.clone()))
        .await?;

    let metrics = s2
        .get_basin_metrics(GetBasinMetricsInput::new(
            basin_name.clone(),
            BasinMetricSet::BasinOps(
                TimeRangeAndInterval::new(start, now).with_interval(TimeseriesInterval::Day),
            ),
        ))
        .await?;

    for metric in &metrics {
        assert_matches!(metric, Metric::Accumulation(acc) => {
            assert_eq!(acc.interval, TimeseriesInterval::Day);
        });
    }

    s2.delete_basin(DeleteBasinInput::new(basin_name)).await?;

    Ok(())
}

#[test_context(S2Stream)]
#[tokio_shared_rt::test(shared)]
async fn stream_metrics_storage_with_minute_interval(stream: &S2Stream) -> Result<(), S2Error> {
    let s2 = s2();
    let now = now_epoch_secs();
    let start = one_hour_ago();

    let input = AppendInput::new(AppendRecordBatch::try_from_iter([AppendRecord::new(
        "test data for metrics with interval",
    )?])?);
    stream.append(input).await?;

    let metrics = s2
        .get_stream_metrics(GetStreamMetricsInput::new(
            stream.basin_name(),
            stream.stream_name(),
            StreamMetricSet::Storage(TimeRange::new(start, now)),
        ))
        .await?;

    for metric in &metrics {
        assert_matches!(metric, Metric::Gauge(gauge) => {
            assert_eq!(gauge.unit, MetricUnit::Bytes);
        });
    }

    Ok(())
}

#[tokio::test]
async fn basin_metrics_empty_time_range_returns_empty_data() -> Result<(), S2Error> {
    let s2 = s2();
    let basin_name = unique_basin_name();
    let far_past = 946684800; // 2000-01-01
    let far_past_end = far_past + 3600;

    s2.create_basin(CreateBasinInput::new(basin_name.clone()))
        .await?;

    let metrics = s2
        .get_basin_metrics(GetBasinMetricsInput::new(
            basin_name.clone(),
            BasinMetricSet::Storage(TimeRange::new(far_past, far_past_end)),
        ))
        .await?;

    for metric in &metrics {
        assert_matches!(metric, Metric::Gauge(gauge) => {
            assert!(gauge.values.is_empty() || gauge.values.iter().all(|(_, v)| *v == 0.0));
        });
    }

    s2.delete_basin(DeleteBasinInput::new(basin_name)).await?;

    Ok(())
}

#[tokio::test]
async fn basin_metrics_read_throughput_with_hour_interval() -> Result<(), S2Error> {
    let s2 = s2();
    let basin_name = unique_basin_name();
    let now = now_epoch_secs();
    let start = one_day_ago();

    s2.create_basin(CreateBasinInput::new(basin_name.clone()))
        .await?;

    let metrics = s2
        .get_basin_metrics(GetBasinMetricsInput::new(
            basin_name.clone(),
            BasinMetricSet::ReadThroughput(
                TimeRangeAndInterval::new(start, now).with_interval(TimeseriesInterval::Hour),
            ),
        ))
        .await?;

    for metric in &metrics {
        assert_matches!(metric, Metric::Accumulation(acc) => {
            assert_eq!(acc.unit, MetricUnit::Bytes);
            assert_eq!(acc.interval, TimeseriesInterval::Hour);
        });
    }

    s2.delete_basin(DeleteBasinInput::new(basin_name)).await?;

    Ok(())
}

#[tokio::test]
async fn basin_metrics_append_throughput_with_minute_interval() -> Result<(), S2Error> {
    let s2 = s2();
    let basin_name = unique_basin_name();
    let now = now_epoch_secs();
    let start = one_hour_ago();

    s2.create_basin(CreateBasinInput::new(basin_name.clone()))
        .await?;

    let metrics = s2
        .get_basin_metrics(GetBasinMetricsInput::new(
            basin_name.clone(),
            BasinMetricSet::AppendThroughput(
                TimeRangeAndInterval::new(start, now).with_interval(TimeseriesInterval::Minute),
            ),
        ))
        .await?;

    for metric in &metrics {
        assert_matches!(metric, Metric::Accumulation(acc) => {
            assert_eq!(acc.unit, MetricUnit::Bytes);
            assert_eq!(acc.interval, TimeseriesInterval::Minute);
        });
    }

    s2.delete_basin(DeleteBasinInput::new(basin_name)).await?;

    Ok(())
}

mod common;

use std::time::Duration;

use assert_matches::assert_matches;
use common::{S2Basin, unique_stream_name, uuid};
use s2_sdk::types::*;
use test_context::test_context;

#[test_context(S2Basin)]
#[tokio_shared_rt::test(shared)]
async fn create_list_and_delete_stream(basin: &S2Basin) -> Result<(), S2Error> {
    let stream_name = unique_stream_name();

    let stream_info = basin
        .create_stream(CreateStreamInput::new(stream_name.clone()))
        .await?;

    assert_eq!(stream_info.name, stream_name);

    let page = basin.list_streams(ListStreamsInput::new()).await?;

    assert_eq!(page.values, vec![stream_info]);
    assert!(!page.has_more);

    basin
        .delete_stream(DeleteStreamInput::new(stream_name))
        .await?;

    let page = basin.list_streams(ListStreamsInput::new()).await?;

    assert_matches!(
        page.values.as_slice(),
        [] | [StreamInfo {
            deleted_at: Some(_),
            ..
        }]
    );

    Ok(())
}

#[test_context(S2Basin)]
#[tokio_shared_rt::test(shared)]
async fn stream_config_roundtrip(basin: &S2Basin) -> Result<(), S2Error> {
    let stream_name = unique_stream_name();
    let config = StreamConfig::new()
        .with_storage_class(StorageClass::Standard)
        .with_retention_policy(RetentionPolicy::Age(3600))
        .with_timestamping(TimestampingConfig::new().with_mode(TimestampingMode::ClientRequire));

    basin
        .create_stream(CreateStreamInput::new(stream_name.clone()).with_config(config.clone()))
        .await?;

    let retrieved_config = basin.get_stream_config(stream_name.clone()).await?;

    assert_matches!(
        retrieved_config,
        StreamConfig {
            storage_class: Some(StorageClass::Standard),
            retention_policy: Some(RetentionPolicy::Age(3600)),
            timestamping: Some(TimestampingConfig {
                mode: Some(TimestampingMode::ClientRequire),
                ..
            }),
            ..
        }
    );

    Ok(())
}

#[test_context(S2Basin)]
#[tokio_shared_rt::test(shared)]
async fn reconfigure_stream(basin: &S2Basin) -> Result<(), S2Error> {
    let stream_name: StreamName = unique_stream_name();

    basin
        .create_stream(CreateStreamInput::new(stream_name.clone()))
        .await?;

    let new_config = StreamReconfiguration::new().with_delete_on_empty(
        DeleteOnEmptyReconfiguration::new().with_min_age(Duration::from_hours(12)),
    );

    let updated_config = basin
        .reconfigure_stream(ReconfigureStreamInput::new(stream_name.clone(), new_config))
        .await?;

    assert_matches!(
        updated_config.delete_on_empty,
        Some(DeleteOnEmptyConfig {
            min_age_secs: 43200,
            ..
        })
    );

    Ok(())
}

#[test_context(S2Basin)]
#[tokio_shared_rt::test(shared)]
async fn list_streams_with_limit(basin: &S2Basin) -> Result<(), S2Error> {
    let stream_name_1 = unique_stream_name();
    let stream_name_2 = unique_stream_name();
    let stream_name_3 = unique_stream_name();

    let stream_info_1 = basin
        .create_stream(CreateStreamInput::new(stream_name_1.clone()))
        .await?;

    let _stream_info_2 = basin
        .create_stream(CreateStreamInput::new(stream_name_2.clone()))
        .await?;
    let _stream_info_3 = basin
        .create_stream(CreateStreamInput::new(stream_name_3.clone()))
        .await?;

    let page = basin
        .list_streams(ListStreamsInput::new().with_limit(1))
        .await?;

    assert_eq!(page.values, vec![stream_info_1]);
    assert!(page.has_more);

    Ok(())
}

#[test_context(S2Basin)]
#[tokio_shared_rt::test(shared)]
async fn list_streams_with_prefix(basin: &S2Basin) -> Result<(), S2Error> {
    let stream_name_1: StreamName = "users/eu/0001".parse().expect("valid stream name");
    let stream_name_2: StreamName = "users/ca/0001".parse().expect("valid stream name");
    let stream_name_3: StreamName = "users/ca/0002".parse().expect("valid stream name");

    let _stream_info_1 = basin
        .create_stream(CreateStreamInput::new(stream_name_1.clone()))
        .await?;
    let stream_info_2 = basin
        .create_stream(CreateStreamInput::new(stream_name_2.clone()))
        .await?;
    let stream_info_3 = basin
        .create_stream(CreateStreamInput::new(stream_name_3.clone()))
        .await?;

    let page = basin
        .list_streams(
            ListStreamsInput::new().with_prefix("users/ca/".parse().expect("valid prefix")),
        )
        .await?;

    assert_eq!(page.values, vec![stream_info_2, stream_info_3]);
    assert!(!page.has_more);

    Ok(())
}

#[test_context(S2Basin)]
#[tokio_shared_rt::test(shared)]
async fn list_streams_with_start_after(basin: &S2Basin) -> Result<(), S2Error> {
    let stream_name_1 = unique_stream_name();
    let stream_name_2 = unique_stream_name();

    let _stream_info_1 = basin
        .create_stream(CreateStreamInput::new(stream_name_1.clone()))
        .await?;
    let stream_info_2 = basin
        .create_stream(CreateStreamInput::new(stream_name_2.clone()))
        .await?;

    let page = basin
        .list_streams(
            ListStreamsInput::new()
                .with_start_after(stream_name_1.parse().expect("valid start after")),
        )
        .await?;

    assert_eq!(page.values, vec![stream_info_2]);
    assert!(!page.has_more);

    Ok(())
}

#[test_context(S2Basin)]
#[tokio_shared_rt::test(shared)]
async fn list_streams_with_start_after_returns_empty_page(basin: &S2Basin) -> Result<(), S2Error> {
    let stream_name_1 = unique_stream_name();
    let stream_name_2 = unique_stream_name();

    basin
        .create_stream(CreateStreamInput::new(stream_name_1.clone()))
        .await?;
    basin
        .create_stream(CreateStreamInput::new(stream_name_2.clone()))
        .await?;

    let page = basin
        .list_streams(
            ListStreamsInput::new()
                .with_start_after(stream_name_2.parse().expect("valid start after")),
        )
        .await?;

    assert_eq!(page.values.len(), 0);
    assert!(!page.has_more);

    Ok(())
}

#[test_context(S2Basin)]
#[tokio_shared_rt::test(shared)]
async fn list_streams_with_start_after_less_than_prefix_errors(
    basin: &S2Basin,
) -> Result<(), S2Error> {
    let prefix = uuid();
    let stream_name_1: StreamName = format!("{}-a-a", prefix)
        .parse()
        .expect("valid stream name");
    let stream_name_2: StreamName = format!("{}-a-b", prefix)
        .parse()
        .expect("valid stream name");
    let stream_name_3: StreamName = format!("{}-b-a", prefix)
        .parse()
        .expect("valid stream name");

    basin
        .create_stream(CreateStreamInput::new(stream_name_1.clone()))
        .await?;
    basin
        .create_stream(CreateStreamInput::new(stream_name_2.clone()))
        .await?;
    basin
        .create_stream(CreateStreamInput::new(stream_name_3.clone()))
        .await?;

    let result = basin
        .list_streams(
            ListStreamsInput::new()
                .with_prefix(format!("{}-b", prefix).parse().expect("valid prefix"))
                .with_start_after(format!("{}-a", prefix).parse().expect("valid start after")),
        )
        .await;

    assert_matches!(
        result,
        Err(S2Error::Server(ErrorResponse { code, message, .. })) => {
            assert_eq!(code, "invalid");
            assert_eq!(message, "`start_after` must be greater than or equal to the `prefix`");
        }
    );

    Ok(())
}

#[test_context(S2Basin)]
#[tokio_shared_rt::test(shared)]
async fn delete_nonexistent_stream_errors(basin: &S2Basin) -> Result<(), S2Error> {
    let result = basin
        .delete_stream(DeleteStreamInput::new(unique_stream_name()))
        .await;

    assert_matches!(
        result,
        Err(S2Error::Server(ErrorResponse {code, message: _, ..})) => {
            assert_eq!(code, "stream_not_found")
        }
    );

    Ok(())
}

#[test_context(S2Basin)]
#[tokio_shared_rt::test(shared)]
async fn delete_nonexistent_stream_with_ignore(basin: &S2Basin) -> Result<(), S2Error> {
    let result = basin
        .delete_stream(DeleteStreamInput::new(unique_stream_name()).with_ignore_not_found(true))
        .await;

    assert_matches!(result, Ok(()));

    Ok(())
}

#[test_context(S2Basin)]
#[tokio_shared_rt::test(shared)]
async fn get_stream_config(basin: &S2Basin) -> Result<(), S2Error> {
    let stream_name = unique_stream_name();

    let config = StreamConfig::new().with_storage_class(StorageClass::Express);

    basin
        .create_stream(CreateStreamInput::new(stream_name.clone()).with_config(config))
        .await?;

    let retrieved_config = basin.get_stream_config(stream_name.clone()).await?;

    assert_matches!(retrieved_config.storage_class, Some(StorageClass::Express));

    Ok(())
}

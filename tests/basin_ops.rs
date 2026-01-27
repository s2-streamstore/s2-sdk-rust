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

#[test_context(S2Basin)]
#[tokio_shared_rt::test(shared)]
async fn get_nonexistent_stream_config_errors(basin: &S2Basin) -> Result<(), S2Error> {
    let stream_name = unique_stream_name();

    let result = basin.get_stream_config(stream_name).await;

    assert_matches!(result, Err(S2Error::Server(ErrorResponse { code, .. })) => {
        assert_eq!(code, "stream_not_found");
    });

    Ok(())
}

#[test_context(S2Basin)]
#[tokio_shared_rt::test(shared)]
async fn create_stream_already_exists_errors(basin: &S2Basin) -> Result<(), S2Error> {
    let stream_name = unique_stream_name();

    basin
        .create_stream(CreateStreamInput::new(stream_name.clone()))
        .await?;

    let result = basin
        .create_stream(CreateStreamInput::new(stream_name.clone()))
        .await;

    assert_matches!(result, Err(S2Error::Server(ErrorResponse { code, .. })) => {
        assert_eq!(code, "resource_already_exists");
    });

    Ok(())
}

#[test_context(S2Basin)]
#[tokio_shared_rt::test(shared)]
async fn create_stream_with_storage_class_standard(basin: &S2Basin) -> Result<(), S2Error> {
    let stream_name = unique_stream_name();

    basin
        .create_stream(
            CreateStreamInput::new(stream_name.clone())
                .with_config(StreamConfig::new().with_storage_class(StorageClass::Standard)),
        )
        .await?;

    let config = basin.get_stream_config(stream_name).await?;
    assert_matches!(config.storage_class, Some(StorageClass::Standard));

    Ok(())
}

#[test_context(S2Basin)]
#[tokio_shared_rt::test(shared)]
async fn create_stream_with_retention_policy_age(basin: &S2Basin) -> Result<(), S2Error> {
    let stream_name = unique_stream_name();

    basin
        .create_stream(
            CreateStreamInput::new(stream_name.clone()).with_config(
                StreamConfig::new().with_retention_policy(RetentionPolicy::Age(86400)),
            ),
        )
        .await?;

    let config = basin.get_stream_config(stream_name).await?;
    assert_matches!(config.retention_policy, Some(RetentionPolicy::Age(86400)));

    Ok(())
}

#[test_context(S2Basin)]
#[tokio_shared_rt::test(shared)]
async fn create_stream_with_timestamping_mode_arrival(basin: &S2Basin) -> Result<(), S2Error> {
    let stream_name = unique_stream_name();

    basin
        .create_stream(
            CreateStreamInput::new(stream_name.clone()).with_config(
                StreamConfig::new().with_timestamping(
                    TimestampingConfig::new().with_mode(TimestampingMode::Arrival),
                ),
            ),
        )
        .await?;

    let config = basin.get_stream_config(stream_name).await?;
    assert_matches!(
        config.timestamping,
        Some(TimestampingConfig {
            mode: Some(TimestampingMode::Arrival),
            ..
        })
    );

    Ok(())
}

#[test_context(S2Basin)]
#[tokio_shared_rt::test(shared)]
async fn create_stream_with_timestamping_mode_client_require(
    basin: &S2Basin,
) -> Result<(), S2Error> {
    let stream_name = unique_stream_name();

    basin
        .create_stream(CreateStreamInput::new(stream_name.clone()).with_config(
            StreamConfig::new().with_timestamping(
                TimestampingConfig::new().with_mode(TimestampingMode::ClientRequire),
            ),
        ))
        .await?;

    let config = basin.get_stream_config(stream_name).await?;
    assert_matches!(
        config.timestamping,
        Some(TimestampingConfig {
            mode: Some(TimestampingMode::ClientRequire),
            ..
        })
    );

    Ok(())
}

#[test_context(S2Basin)]
#[tokio_shared_rt::test(shared)]
async fn create_stream_with_timestamping_uncapped(basin: &S2Basin) -> Result<(), S2Error> {
    let stream_name = unique_stream_name();

    basin
        .create_stream(CreateStreamInput::new(stream_name.clone()).with_config(
            StreamConfig::new().with_timestamping(TimestampingConfig::new().with_uncapped(true)),
        ))
        .await?;

    let config = basin.get_stream_config(stream_name).await?;
    assert_matches!(
        config.timestamping,
        Some(TimestampingConfig { uncapped: true, .. })
    );

    Ok(())
}

#[test_context(S2Basin)]
#[tokio_shared_rt::test(shared)]
async fn create_stream_with_delete_on_empty(basin: &S2Basin) -> Result<(), S2Error> {
    let stream_name = unique_stream_name();

    basin
        .create_stream(CreateStreamInput::new(stream_name.clone()).with_config(
            StreamConfig::new().with_delete_on_empty(
                DeleteOnEmptyConfig::new().with_min_age(Duration::from_secs(3600)),
            ),
        ))
        .await?;

    let config = basin.get_stream_config(stream_name).await?;
    assert_matches!(
        config.delete_on_empty,
        Some(DeleteOnEmptyConfig {
            min_age_secs: 3600,
            ..
        })
    );

    Ok(())
}

#[test_context(S2Basin)]
#[tokio_shared_rt::test(shared)]
async fn create_stream_with_unicode_name(basin: &S2Basin) -> Result<(), S2Error> {
    let stream_name: StreamName = "test-stream-日本語".parse().expect("valid stream name");

    let stream_info = basin
        .create_stream(CreateStreamInput::new(stream_name.clone()))
        .await?;

    assert_eq!(stream_info.name, stream_name);

    Ok(())
}

#[test_context(S2Basin)]
#[tokio_shared_rt::test(shared)]
async fn reconfigure_stream_change_storage_class(basin: &S2Basin) -> Result<(), S2Error> {
    let stream_name = unique_stream_name();

    basin
        .create_stream(CreateStreamInput::new(stream_name.clone()))
        .await?;

    let config = basin
        .reconfigure_stream(ReconfigureStreamInput::new(
            stream_name.clone(),
            StreamReconfiguration::new().with_storage_class(StorageClass::Standard),
        ))
        .await?;

    assert_matches!(config.storage_class, Some(StorageClass::Standard));

    Ok(())
}

#[test_context(S2Basin)]
#[tokio_shared_rt::test(shared)]
async fn reconfigure_stream_change_retention_to_age(basin: &S2Basin) -> Result<(), S2Error> {
    let stream_name = unique_stream_name();

    basin
        .create_stream(CreateStreamInput::new(stream_name.clone()))
        .await?;

    let config = basin
        .reconfigure_stream(ReconfigureStreamInput::new(
            stream_name.clone(),
            StreamReconfiguration::new().with_retention_policy(RetentionPolicy::Age(3600)),
        ))
        .await?;

    assert_matches!(config.retention_policy, Some(RetentionPolicy::Age(3600)));

    Ok(())
}

#[test_context(S2Basin)]
#[tokio_shared_rt::test(shared)]
async fn reconfigure_stream_change_timestamping_mode(basin: &S2Basin) -> Result<(), S2Error> {
    let stream_name = unique_stream_name();

    basin
        .create_stream(CreateStreamInput::new(stream_name.clone()))
        .await?;

    let config = basin
        .reconfigure_stream(ReconfigureStreamInput::new(
            stream_name.clone(),
            StreamReconfiguration::new().with_timestamping(
                TimestampingReconfiguration::new().with_mode(TimestampingMode::Arrival),
            ),
        ))
        .await?;

    assert_matches!(
        config.timestamping,
        Some(TimestampingConfig {
            mode: Some(TimestampingMode::Arrival),
            ..
        })
    );

    Ok(())
}

#[test_context(S2Basin)]
#[tokio_shared_rt::test(shared)]
async fn reconfigure_stream_empty_reconfiguration(basin: &S2Basin) -> Result<(), S2Error> {
    let stream_name = unique_stream_name();

    basin
        .create_stream(CreateStreamInput::new(stream_name.clone()))
        .await?;

    let config_before = basin.get_stream_config(stream_name.clone()).await?;

    let config_after = basin
        .reconfigure_stream(ReconfigureStreamInput::new(
            stream_name,
            StreamReconfiguration::new(),
        ))
        .await?;

    assert_eq!(config_before.storage_class, config_after.storage_class);

    Ok(())
}

#[test_context(S2Basin)]
#[tokio_shared_rt::test(shared)]
async fn reconfigure_nonexistent_stream_errors(basin: &S2Basin) -> Result<(), S2Error> {
    let stream_name = unique_stream_name();

    let result = basin
        .reconfigure_stream(ReconfigureStreamInput::new(
            stream_name,
            StreamReconfiguration::new().with_storage_class(StorageClass::Standard),
        ))
        .await;

    assert_matches!(result, Err(S2Error::Server(ErrorResponse { code, .. })) => {
        assert_eq!(code, "stream_not_found");
    });

    Ok(())
}

#[test_context(S2Basin)]
#[tokio_shared_rt::test(shared)]
async fn create_stream_with_storage_class_express(basin: &S2Basin) -> Result<(), S2Error> {
    let stream_name = unique_stream_name();

    let result = basin
        .create_stream(
            CreateStreamInput::new(stream_name.clone())
                .with_config(StreamConfig::new().with_storage_class(StorageClass::Express)),
        )
        .await;

    match result {
        Ok(_) => {
            let config = basin.get_stream_config(stream_name).await?;
            assert_matches!(config.storage_class, Some(StorageClass::Express));
        }
        Err(S2Error::Server(ErrorResponse { code, message, .. }))
            if code == "invalid" && message.contains("free tier") =>
        {
            // Express storage class not available on free tier
        }
        Err(e) => return Err(e),
    }

    Ok(())
}

#[test_context(S2Basin)]
#[tokio_shared_rt::test(shared)]
async fn create_stream_with_retention_policy_infinite(basin: &S2Basin) -> Result<(), S2Error> {
    let stream_name = unique_stream_name();

    let result = basin
        .create_stream(
            CreateStreamInput::new(stream_name.clone())
                .with_config(StreamConfig::new().with_retention_policy(RetentionPolicy::Infinite)),
        )
        .await;

    match result {
        Ok(_) => {
            let config = basin.get_stream_config(stream_name).await?;
            assert_matches!(config.retention_policy, Some(RetentionPolicy::Infinite));
        }
        Err(S2Error::Server(ErrorResponse { code, message, .. }))
            if code == "invalid" && message.contains("free tier") =>
        {
            // Infinite retention not available on free tier
        }
        Err(e) => return Err(e),
    }

    Ok(())
}

#[test_context(S2Basin)]
#[tokio_shared_rt::test(shared)]
async fn list_streams_with_empty_prefix(basin: &S2Basin) -> Result<(), S2Error> {
    let stream_name = unique_stream_name();

    basin
        .create_stream(CreateStreamInput::new(stream_name.clone()))
        .await?;

    let page = basin
        .list_streams(ListStreamsInput::new().with_prefix("".parse().expect("valid prefix")))
        .await?;

    assert!(!page.values.is_empty());

    Ok(())
}

#[test_context(S2Basin)]
#[tokio_shared_rt::test(shared)]
async fn list_streams_with_limit_zero(basin: &S2Basin) -> Result<(), S2Error> {
    let stream_name = unique_stream_name();

    basin
        .create_stream(CreateStreamInput::new(stream_name.clone()))
        .await?;

    let page = basin
        .list_streams(ListStreamsInput::new().with_limit(0))
        .await?;

    assert!(page.values.len() <= 1000);

    Ok(())
}

#[test_context(S2Basin)]
#[tokio_shared_rt::test(shared)]
async fn list_streams_with_limit_over_max(basin: &S2Basin) -> Result<(), S2Error> {
    let stream_name = unique_stream_name();

    basin
        .create_stream(CreateStreamInput::new(stream_name.clone()))
        .await?;

    let page = basin
        .list_streams(ListStreamsInput::new().with_limit(1001))
        .await?;

    assert!(page.values.len() <= 1000);

    Ok(())
}

#[test_context(S2Basin)]
#[tokio_shared_rt::test(shared)]
async fn list_streams_pagination(basin: &S2Basin) -> Result<(), S2Error> {
    let prefix = uuid();
    let stream_names: Vec<StreamName> = (0..5)
        .map(|i| {
            format!("{}-{:03}", prefix, i)
                .parse()
                .expect("valid stream name")
        })
        .collect();

    for stream_name in &stream_names {
        basin
            .create_stream(CreateStreamInput::new(stream_name.clone()))
            .await?;
    }

    let mut all_streams = Vec::new();
    let mut start_after: Option<StreamNameStartAfter> = None;

    loop {
        let mut input = ListStreamsInput::new().with_prefix(prefix.parse().expect("valid prefix"));
        input = input.with_limit(2);
        if let Some(ref sa) = start_after {
            input = input.with_start_after(sa.clone());
        }

        let page = basin.list_streams(input).await?;
        all_streams.extend(page.values.clone());

        if !page.has_more {
            break;
        }
        start_after = page
            .values
            .last()
            .map(|s| s.name.as_ref().parse().expect("valid start_after"));
    }

    assert_eq!(all_streams.len(), 5);

    Ok(())
}

#[test_context(S2Basin)]
#[tokio_shared_rt::test(shared)]
async fn reconfigure_stream_change_retention_to_infinite(basin: &S2Basin) -> Result<(), S2Error> {
    let stream_name = unique_stream_name();

    basin
        .create_stream(CreateStreamInput::new(stream_name.clone()))
        .await?;

    let result = basin
        .reconfigure_stream(ReconfigureStreamInput::new(
            stream_name.clone(),
            StreamReconfiguration::new().with_retention_policy(RetentionPolicy::Infinite),
        ))
        .await;

    match result {
        Ok(config) => {
            assert_matches!(config.retention_policy, Some(RetentionPolicy::Infinite));
        }
        Err(S2Error::Server(ErrorResponse { code, message, .. }))
            if code == "invalid" && message.contains("free tier") =>
        {
            // Infinite retention not available on free tier
        }
        Err(e) => return Err(e),
    }

    Ok(())
}

#[test_context(S2Basin)]
#[tokio_shared_rt::test(shared)]
async fn reconfigure_stream_change_storage_class_to_express(
    basin: &S2Basin,
) -> Result<(), S2Error> {
    let stream_name = unique_stream_name();

    basin
        .create_stream(CreateStreamInput::new(stream_name.clone()))
        .await?;

    let result = basin
        .reconfigure_stream(ReconfigureStreamInput::new(
            stream_name.clone(),
            StreamReconfiguration::new().with_storage_class(StorageClass::Express),
        ))
        .await;

    match result {
        Ok(config) => {
            assert_matches!(config.storage_class, Some(StorageClass::Express));
        }
        Err(S2Error::Server(ErrorResponse { code, message, .. }))
            if code == "invalid" && message.contains("free tier") =>
        {
            // Express storage class not available on free tier
        }
        Err(e) => return Err(e),
    }

    Ok(())
}

#[test_context(S2Basin)]
#[tokio_shared_rt::test(shared)]
async fn reconfigure_stream_change_timestamping_uncapped(basin: &S2Basin) -> Result<(), S2Error> {
    let stream_name = unique_stream_name();

    basin
        .create_stream(CreateStreamInput::new(stream_name.clone()))
        .await?;

    let config = basin
        .reconfigure_stream(ReconfigureStreamInput::new(
            stream_name.clone(),
            StreamReconfiguration::new()
                .with_timestamping(TimestampingReconfiguration::new().with_uncapped(true)),
        ))
        .await?;

    assert_matches!(
        config.timestamping,
        Some(TimestampingConfig { uncapped: true, .. })
    );

    Ok(())
}

#[test_context(S2Basin)]
#[tokio_shared_rt::test(shared)]
async fn reconfigure_stream_change_delete_on_empty(basin: &S2Basin) -> Result<(), S2Error> {
    let stream_name = unique_stream_name();

    basin
        .create_stream(CreateStreamInput::new(stream_name.clone()))
        .await?;

    let config = basin
        .reconfigure_stream(ReconfigureStreamInput::new(
            stream_name.clone(),
            StreamReconfiguration::new().with_delete_on_empty(
                DeleteOnEmptyReconfiguration::new().with_min_age(Duration::from_secs(3600)),
            ),
        ))
        .await?;

    assert_matches!(
        config.delete_on_empty,
        Some(DeleteOnEmptyConfig {
            min_age_secs: 3600,
            ..
        })
    );

    Ok(())
}

#[test_context(S2Basin)]
#[tokio_shared_rt::test(shared)]
async fn reconfigure_stream_disable_delete_on_empty(basin: &S2Basin) -> Result<(), S2Error> {
    let stream_name = unique_stream_name();

    basin
        .create_stream(CreateStreamInput::new(stream_name.clone()).with_config(
            StreamConfig::new().with_delete_on_empty(
                DeleteOnEmptyConfig::new().with_min_age(Duration::from_secs(3600)),
            ),
        ))
        .await?;

    let config = basin
        .reconfigure_stream(ReconfigureStreamInput::new(
            stream_name.clone(),
            StreamReconfiguration::new().with_delete_on_empty(
                DeleteOnEmptyReconfiguration::new().with_min_age(Duration::from_secs(0)),
            ),
        ))
        .await?;

    assert_matches!(config.delete_on_empty, None);

    Ok(())
}

#[test_context(S2Basin)]
#[tokio_shared_rt::test(shared)]
async fn create_stream_with_timestamping_mode_client_prefer(
    basin: &S2Basin,
) -> Result<(), S2Error> {
    let stream_name = unique_stream_name();

    basin
        .create_stream(CreateStreamInput::new(stream_name.clone()).with_config(
            StreamConfig::new().with_timestamping(
                TimestampingConfig::new().with_mode(TimestampingMode::ClientPrefer),
            ),
        ))
        .await?;

    let config = basin.get_stream_config(stream_name).await?;
    assert_matches!(
        config.timestamping,
        Some(TimestampingConfig {
            mode: Some(TimestampingMode::ClientPrefer),
            ..
        }) | None
    );

    Ok(())
}

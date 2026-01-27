mod common;

use std::time::Duration;

use assert_matches::assert_matches;
use common::{s2, unique_basin_name, uuid};
use s2_sdk::types::*;

#[tokio::test]
async fn create_list_and_delete_basin() -> Result<(), S2Error> {
    let s2 = s2();
    let basin_name = unique_basin_name();

    let basin_info = s2
        .create_basin(CreateBasinInput::new(basin_name.clone()))
        .await?;

    assert_eq!(basin_info.name, basin_name);

    let page = s2
        .list_basins(ListBasinsInput::new().with_prefix(basin_name.clone().into()))
        .await?;

    assert_eq!(page.values, vec![basin_info]);
    assert!(!page.has_more);

    s2.delete_basin(DeleteBasinInput::new(basin_name.clone()))
        .await?;

    let page = s2
        .list_basins(ListBasinsInput::new().with_prefix(basin_name.into()))
        .await?;

    assert_matches!(
        page.values.as_slice(),
        [] | [BasinInfo {
            state: BasinState::Deleting,
            ..
        }]
    );

    Ok(())
}

#[tokio::test]
async fn basin_config_roundtrip() -> Result<(), S2Error> {
    let s2 = s2();
    let basin_name = unique_basin_name();
    let config = BasinConfig::new()
        .with_default_stream_config(
            StreamConfig::new()
                .with_storage_class(StorageClass::Express)
                .with_delete_on_empty(
                    DeleteOnEmptyConfig::new().with_min_age(Duration::from_secs(60)),
                ),
        )
        .with_create_stream_on_read(true);

    s2.create_basin(CreateBasinInput::new(basin_name.clone()).with_config(config.clone()))
        .await?;

    let retrieved_config = s2.get_basin_config(basin_name.clone()).await?;
    assert_matches!(
        retrieved_config,
        BasinConfig {
            default_stream_config: Some(StreamConfig {
                storage_class: Some(StorageClass::Express),
                delete_on_empty: Some(DeleteOnEmptyConfig {
                    min_age_secs: 60,
                    ..
                }),
                ..
            }),
            create_stream_on_read: true,
            ..
        }
    );

    s2.delete_basin(DeleteBasinInput::new(basin_name)).await?;

    Ok(())
}

#[tokio::test]
async fn reconfigure_basin() -> Result<(), S2Error> {
    let s2 = s2();
    let basin_name = unique_basin_name();

    s2.create_basin(
        CreateBasinInput::new(basin_name.clone())
            .with_config(BasinConfig::new().with_create_stream_on_append(true)),
    )
    .await?;

    let new_config = BasinReconfiguration::new()
        .with_default_stream_config(
            StreamReconfiguration::new().with_storage_class(StorageClass::Standard),
        )
        .with_create_stream_on_append(false);

    let updated_config = s2
        .reconfigure_basin(ReconfigureBasinInput::new(basin_name.clone(), new_config))
        .await?;

    assert_matches!(
        updated_config,
        BasinConfig {
            default_stream_config: Some(StreamConfig {
                storage_class: Some(StorageClass::Standard),
                ..
            }),
            create_stream_on_append: false,
            ..
        }
    );

    s2.delete_basin(DeleteBasinInput::new(basin_name)).await?;

    Ok(())
}

#[tokio::test]
async fn list_basins_with_limit() -> Result<(), S2Error> {
    let s2 = s2();
    let basin_name_1 = unique_basin_name();
    let basin_name_2 = unique_basin_name();

    s2.create_basin(CreateBasinInput::new(basin_name_1.clone()))
        .await?;
    s2.create_basin(CreateBasinInput::new(basin_name_2.clone()))
        .await?;

    let page = s2.list_basins(ListBasinsInput::new().with_limit(1)).await?;

    assert_eq!(page.values.len(), 1);
    assert!(page.has_more);

    s2.delete_basin(DeleteBasinInput::new(basin_name_1)).await?;
    s2.delete_basin(DeleteBasinInput::new(basin_name_2)).await?;

    Ok(())
}

#[tokio::test]
async fn list_basins_with_prefix() -> Result<(), S2Error> {
    let s2 = s2();

    let prefix_1: BasinNamePrefix = uuid().parse().expect("valid basin name prefix");
    let prefix_2: BasinNamePrefix = uuid().parse().expect("valid basin name prefix");
    let basin_name_1: BasinName = format!("{}-a", prefix_1).parse().expect("valid basin name");
    let basin_name_2: BasinName = format!("{}-b", prefix_2).parse().expect("valid basin name");

    s2.create_basin(CreateBasinInput::new(basin_name_1.clone()))
        .await?;
    s2.create_basin(CreateBasinInput::new(basin_name_2.clone()))
        .await?;

    let page = s2
        .list_basins(ListBasinsInput::new().with_prefix(prefix_1))
        .await?;

    assert_eq!(page.values.len(), 1);
    assert_matches!(page.values.first(), Some(b) => {
        assert_eq!(b.name, basin_name_1)
    });

    s2.delete_basin(DeleteBasinInput::new(basin_name_1)).await?;
    s2.delete_basin(DeleteBasinInput::new(basin_name_2)).await?;

    Ok(())
}

#[tokio::test]
async fn list_basins_with_prefix_and_start_after() -> Result<(), S2Error> {
    let s2 = s2();

    let prefix: BasinNamePrefix = uuid().parse().expect("valid prefix");
    let basin_name_1: BasinName = format!("{}-a", prefix).parse().expect("valid basin name");
    let basin_name_2: BasinName = format!("{}-b", prefix).parse().expect("valid basin name");

    s2.create_basin(CreateBasinInput::new(basin_name_1.clone()))
        .await?;
    s2.create_basin(CreateBasinInput::new(basin_name_2.clone()))
        .await?;

    let page = s2
        .list_basins(
            ListBasinsInput::new()
                .with_prefix(prefix)
                .with_start_after(basin_name_1.as_ref().parse().expect("valid start after")),
        )
        .await?;

    assert_eq!(page.values.len(), 1);
    assert_eq!(page.values[0].name, basin_name_2);

    s2.delete_basin(DeleteBasinInput::new(basin_name_1)).await?;
    s2.delete_basin(DeleteBasinInput::new(basin_name_2)).await?;

    Ok(())
}

#[tokio::test]
async fn delete_nonexistent_basin_errors() -> Result<(), S2Error> {
    let s2 = s2();
    let result = s2
        .delete_basin(DeleteBasinInput::new(unique_basin_name()))
        .await;

    assert_matches!(
        result,
        Err(S2Error::Server(ErrorResponse { code, message: _, .. })) => {
            assert_eq!(code, "basin_not_found")
        }
    );

    Ok(())
}

#[tokio::test]
async fn delete_nonexistent_basin_with_ignore() -> Result<(), S2Error> {
    let s2 = s2();
    let result = s2
        .delete_basin(DeleteBasinInput::new(unique_basin_name()).with_ignore_not_found(true))
        .await;

    assert_matches!(result, Ok(()));

    Ok(())
}

#[tokio::test]
async fn get_basin_config() -> Result<(), S2Error> {
    let s2 = s2();
    let basin_name = unique_basin_name();

    let config = BasinConfig::new()
        .with_default_stream_config(StreamConfig::new().with_storage_class(StorageClass::Express));

    s2.create_basin(CreateBasinInput::new(basin_name.clone()).with_config(config))
        .await?;

    let retrieved_config = s2.get_basin_config(basin_name.clone()).await?;

    assert_matches!(
        retrieved_config.default_stream_config,
        Some(StreamConfig {
            storage_class: Some(StorageClass::Express),
            ..
        })
    );

    s2.delete_basin(DeleteBasinInput::new(basin_name)).await?;

    Ok(())
}

#[tokio::test]
async fn issue_list_and_revoke_access_token() -> Result<(), S2Error> {
    let s2 = s2();
    let token_id: AccessTokenId = uuid().parse().expect("valid token id");

    let _token = s2
        .issue_access_token(IssueAccessTokenInput::new(
            token_id.clone(),
            AccessTokenScopeInput::from_op_group_perms(OperationGroupPermissions::read_write_all()),
        ))
        .await?;

    let page = s2
        .list_access_tokens(ListAccessTokensInput::new().with_prefix(token_id.clone().into()))
        .await?;

    assert!(page.values.iter().any(|t| t.id == token_id));

    s2.revoke_access_token(token_id.clone()).await?;

    let page = s2.list_access_tokens(ListAccessTokensInput::new()).await?;

    assert!(!page.values.iter().any(|t| t.id == token_id));

    Ok(())
}

#[tokio::test]
async fn issue_access_token_with_expiration_and_auto_prefix_streams() -> Result<(), S2Error> {
    let s2 = s2();
    let token_id: AccessTokenId = uuid().parse().expect("valid token id");

    let expires_at: S2DateTime =
        (time::OffsetDateTime::now_utc() + time::Duration::hours(1)).into();

    let token = s2
        .issue_access_token(
            IssueAccessTokenInput::new(
                token_id.clone(),
                AccessTokenScopeInput::from_op_group_perms(
                    OperationGroupPermissions::read_write_all(),
                )
                .with_streams(StreamMatcher::Prefix(
                    "namespace".parse().expect("valid prefix"),
                )),
            )
            .with_expires_at(expires_at)
            .with_auto_prefix_streams(true),
        )
        .await?;

    assert!(!token.is_empty());

    let page = s2.list_access_tokens(ListAccessTokensInput::new()).await?;

    let issued_token = page
        .values
        .iter()
        .find(|t| t.id == token_id)
        .expect("token should be present");
    assert_eq!(issued_token.expires_at, expires_at);
    assert!(issued_token.auto_prefix_streams);

    s2.revoke_access_token(token_id).await?;

    Ok(())
}

#[tokio::test]
async fn issue_access_token_with_auto_prefix_streams_but_without_prefix_errors()
-> Result<(), S2Error> {
    let s2 = s2();
    let token_id: AccessTokenId = uuid().parse().expect("valid token id");

    let result = s2
        .issue_access_token(
            IssueAccessTokenInput::new(
                token_id.clone(),
                AccessTokenScopeInput::from_op_group_perms(
                    OperationGroupPermissions::read_write_all(),
                ),
            )
            .with_auto_prefix_streams(true),
        )
        .await;

    assert_matches!(result, Err(S2Error::Server(ErrorResponse { code, message, .. })) => {
        assert_eq!(code, "invalid");
        assert_eq!(message, "Auto prefixing is only allowed for streams with prefix matching");
    });
    Ok(())
}

#[tokio::test]
async fn issue_access_token_with_no_permitted_ops_errors() -> Result<(), S2Error> {
    let s2 = s2();
    let token_id: AccessTokenId = uuid().parse().expect("valid token id");

    let result_matches = |result: Result<String, S2Error>| {
        assert_matches!(result, Err(S2Error::Server(ErrorResponse { code, message, .. })) => {
            assert_eq!(code, "invalid");
            assert_eq!(message, "Access token permissions cannot be empty");
        });
    };

    let result = s2
        .issue_access_token(IssueAccessTokenInput::new(
            token_id.clone(),
            AccessTokenScopeInput::from_op_group_perms(OperationGroupPermissions::new()),
        ))
        .await;

    result_matches(result);

    let result = s2
        .issue_access_token(IssueAccessTokenInput::new(
            token_id.clone(),
            AccessTokenScopeInput::from_ops(vec![]),
        ))
        .await;

    result_matches(result);

    Ok(())
}

#[tokio::test]
async fn list_access_tokens_with_limit() -> Result<(), S2Error> {
    let s2 = s2();

    let page = s2
        .list_access_tokens(ListAccessTokensInput::new().with_limit(1))
        .await?;

    assert_eq!(page.values.len(), 1);

    Ok(())
}

#[tokio::test]
async fn list_access_tokens_with_prefix() -> Result<(), S2Error> {
    let s2 = s2();
    let prefix = format!("{}", uuid::Uuid::new_v4().simple());
    let token_id_1: AccessTokenId = format!("{}-a", prefix).parse().expect("valid token id");
    let token_id_2: AccessTokenId = format!("{}-b", prefix).parse().expect("valid token id");
    let token_id_3: AccessTokenId = format!("{}-c", uuid::Uuid::new_v4().simple())
        .parse()
        .expect("valid token id");

    let scope =
        AccessTokenScopeInput::from_op_group_perms(OperationGroupPermissions::read_write_all());

    s2.issue_access_token(IssueAccessTokenInput::new(
        token_id_1.clone(),
        scope.clone(),
    ))
    .await?;
    s2.issue_access_token(IssueAccessTokenInput::new(
        token_id_2.clone(),
        scope.clone(),
    ))
    .await?;
    s2.issue_access_token(IssueAccessTokenInput::new(token_id_3.clone(), scope))
        .await?;

    let page = s2
        .list_access_tokens(
            ListAccessTokensInput::new().with_prefix(prefix.parse().expect("valid prefix")),
        )
        .await?;

    assert_eq!(page.values.len(), 2);
    assert!(page.values.iter().any(|t| t.id == token_id_1));
    assert!(page.values.iter().any(|t| t.id == token_id_2));

    s2.revoke_access_token(token_id_1).await?;
    s2.revoke_access_token(token_id_2).await?;
    s2.revoke_access_token(token_id_3).await?;

    Ok(())
}

#[tokio::test]
async fn issue_access_token_with_specific_ops() -> Result<(), S2Error> {
    let s2 = s2();
    let token_id: AccessTokenId = uuid().parse().expect("valid token id");

    let token = s2
        .issue_access_token(IssueAccessTokenInput::new(
            token_id.clone(),
            AccessTokenScopeInput::from_ops(vec![Operation::ListBasins, Operation::ListStreams]),
        ))
        .await?;

    assert!(!token.is_empty());

    let page = s2
        .list_access_tokens(ListAccessTokensInput::new().with_prefix(token_id.clone().into()))
        .await?;

    assert!(page.values.iter().any(|t| t.id == token_id));

    s2.revoke_access_token(token_id).await?;

    Ok(())
}

#[tokio::test]
async fn issue_access_token_with_op_groups_read_only() -> Result<(), S2Error> {
    let s2 = s2();
    let token_id: AccessTokenId = uuid().parse().expect("valid token id");

    let token = s2
        .issue_access_token(IssueAccessTokenInput::new(
            token_id.clone(),
            AccessTokenScopeInput::from_op_group_perms(
                OperationGroupPermissions::new().with_account(ReadWritePermissions::read_only()),
            ),
        ))
        .await?;

    assert!(!token.is_empty());

    s2.revoke_access_token(token_id).await?;

    Ok(())
}

#[tokio::test]
async fn issue_access_token_with_op_groups_read_write() -> Result<(), S2Error> {
    let s2 = s2();
    let token_id: AccessTokenId = uuid().parse().expect("valid token id");

    let token = s2
        .issue_access_token(IssueAccessTokenInput::new(
            token_id.clone(),
            AccessTokenScopeInput::from_op_group_perms(
                OperationGroupPermissions::new().with_stream(ReadWritePermissions::read_write()),
            ),
        ))
        .await?;

    assert!(!token.is_empty());

    s2.revoke_access_token(token_id).await?;

    Ok(())
}

#[tokio::test]
async fn issue_access_token_with_basin_exact_scope() -> Result<(), S2Error> {
    let s2 = s2();
    let token_id: AccessTokenId = uuid().parse().expect("valid token id");
    let basin_name: BasinName = "my-test-basin".parse().expect("valid basin name");

    let token = s2
        .issue_access_token(IssueAccessTokenInput::new(
            token_id.clone(),
            AccessTokenScopeInput::from_op_group_perms(OperationGroupPermissions::read_write_all())
                .with_basins(BasinMatcher::Exact(basin_name)),
        ))
        .await?;

    assert!(!token.is_empty());

    s2.revoke_access_token(token_id).await?;

    Ok(())
}

#[tokio::test]
async fn issue_access_token_with_basin_prefix_scope() -> Result<(), S2Error> {
    let s2 = s2();
    let token_id: AccessTokenId = uuid().parse().expect("valid token id");

    let token = s2
        .issue_access_token(IssueAccessTokenInput::new(
            token_id.clone(),
            AccessTokenScopeInput::from_op_group_perms(OperationGroupPermissions::read_write_all())
                .with_basins(BasinMatcher::Prefix("test-".parse().expect("valid prefix"))),
        ))
        .await?;

    assert!(!token.is_empty());

    s2.revoke_access_token(token_id).await?;

    Ok(())
}

#[tokio::test]
async fn issue_access_token_with_stream_exact_scope() -> Result<(), S2Error> {
    let s2 = s2();
    let token_id: AccessTokenId = uuid().parse().expect("valid token id");

    let token = s2
        .issue_access_token(IssueAccessTokenInput::new(
            token_id.clone(),
            AccessTokenScopeInput::from_op_group_perms(OperationGroupPermissions::read_write_all())
                .with_streams(StreamMatcher::Exact(
                    "my-stream".parse().expect("valid stream name"),
                )),
        ))
        .await?;

    assert!(!token.is_empty());

    s2.revoke_access_token(token_id).await?;

    Ok(())
}

#[tokio::test]
async fn issue_access_token_with_stream_prefix_scope() -> Result<(), S2Error> {
    let s2 = s2();
    let token_id: AccessTokenId = uuid().parse().expect("valid token id");

    let token = s2
        .issue_access_token(IssueAccessTokenInput::new(
            token_id.clone(),
            AccessTokenScopeInput::from_op_group_perms(OperationGroupPermissions::read_write_all())
                .with_streams(StreamMatcher::Prefix(
                    "logs-".parse().expect("valid prefix"),
                )),
        ))
        .await?;

    assert!(!token.is_empty());

    s2.revoke_access_token(token_id).await?;

    Ok(())
}

#[tokio::test]
async fn issue_duplicate_token_id_errors() -> Result<(), S2Error> {
    let s2 = s2();
    let token_id: AccessTokenId = uuid().parse().expect("valid token id");
    let scope =
        AccessTokenScopeInput::from_op_group_perms(OperationGroupPermissions::read_write_all());

    s2.issue_access_token(IssueAccessTokenInput::new(token_id.clone(), scope.clone()))
        .await?;

    let result = s2
        .issue_access_token(IssueAccessTokenInput::new(token_id.clone(), scope))
        .await;

    assert_matches!(result, Err(S2Error::Server(ErrorResponse { code, .. })) => {
        assert_eq!(code, "resource_already_exists");
    });

    s2.revoke_access_token(token_id).await?;

    Ok(())
}

#[tokio::test]
async fn revoke_nonexistent_token_errors() -> Result<(), S2Error> {
    let s2 = s2();
    let token_id: AccessTokenId = uuid().parse().expect("valid token id");

    let result = s2.revoke_access_token(token_id).await;

    assert_matches!(result, Err(S2Error::Server(ErrorResponse { code, .. })) => {
        assert_eq!(code, "access_token_not_found");
    });

    Ok(())
}

#[tokio::test]
async fn revoke_already_revoked_token_errors() -> Result<(), S2Error> {
    let s2 = s2();
    let token_id: AccessTokenId = uuid().parse().expect("valid token id");
    let scope =
        AccessTokenScopeInput::from_op_group_perms(OperationGroupPermissions::read_write_all());

    s2.issue_access_token(IssueAccessTokenInput::new(token_id.clone(), scope))
        .await?;

    s2.revoke_access_token(token_id.clone()).await?;

    let result = s2.revoke_access_token(token_id).await;

    assert_matches!(result, Err(S2Error::Server(ErrorResponse { code, .. })) => {
        assert_eq!(code, "access_token_not_found");
    });

    Ok(())
}

#[tokio::test]
async fn create_basin_already_exists_errors() -> Result<(), S2Error> {
    let s2 = s2();
    let basin_name = unique_basin_name();

    s2.create_basin(CreateBasinInput::new(basin_name.clone()))
        .await?;

    let result = s2
        .create_basin(CreateBasinInput::new(basin_name.clone()))
        .await;

    assert_matches!(result, Err(S2Error::Server(ErrorResponse { code, .. })) => {
        assert_eq!(code, "resource_already_exists");
    });

    s2.delete_basin(DeleteBasinInput::new(basin_name)).await?;

    Ok(())
}

#[tokio::test]
async fn get_nonexistent_basin_config_errors() -> Result<(), S2Error> {
    let s2 = s2();
    let basin_name = unique_basin_name();

    let result = s2.get_basin_config(basin_name).await;

    assert_matches!(result, Err(S2Error::Server(ErrorResponse { code, .. })) => {
        assert_eq!(code, "basin_not_found");
    });

    Ok(())
}

#[tokio::test]
async fn reconfigure_nonexistent_basin_errors() -> Result<(), S2Error> {
    let s2 = s2();
    let basin_name = unique_basin_name();

    let result = s2
        .reconfigure_basin(ReconfigureBasinInput::new(
            basin_name,
            BasinReconfiguration::new().with_create_stream_on_append(true),
        ))
        .await;

    assert_matches!(result, Err(S2Error::Server(ErrorResponse { code, .. })) => {
        assert_eq!(code, "basin_not_found");
    });

    Ok(())
}

#[tokio::test]
async fn create_basin_with_create_stream_on_append() -> Result<(), S2Error> {
    let s2 = s2();
    let basin_name = unique_basin_name();

    s2.create_basin(
        CreateBasinInput::new(basin_name.clone())
            .with_config(BasinConfig::new().with_create_stream_on_append(true)),
    )
    .await?;

    let config = s2.get_basin_config(basin_name.clone()).await?;
    assert!(config.create_stream_on_append);

    s2.delete_basin(DeleteBasinInput::new(basin_name)).await?;

    Ok(())
}

#[tokio::test]
async fn create_basin_with_create_stream_on_read() -> Result<(), S2Error> {
    let s2 = s2();
    let basin_name = unique_basin_name();

    s2.create_basin(
        CreateBasinInput::new(basin_name.clone())
            .with_config(BasinConfig::new().with_create_stream_on_read(true)),
    )
    .await?;

    let config = s2.get_basin_config(basin_name.clone()).await?;
    assert!(config.create_stream_on_read);

    s2.delete_basin(DeleteBasinInput::new(basin_name)).await?;

    Ok(())
}

#[tokio::test]
async fn create_basin_with_storage_class_standard() -> Result<(), S2Error> {
    let s2 = s2();
    let basin_name = unique_basin_name();

    s2.create_basin(CreateBasinInput::new(basin_name.clone()).with_config(
        BasinConfig::new().with_default_stream_config(
            StreamConfig::new().with_storage_class(StorageClass::Standard),
        ),
    ))
    .await?;

    let config = s2.get_basin_config(basin_name.clone()).await?;
    assert_matches!(
        config.default_stream_config,
        Some(StreamConfig {
            storage_class: Some(StorageClass::Standard),
            ..
        })
    );

    s2.delete_basin(DeleteBasinInput::new(basin_name)).await?;

    Ok(())
}

#[tokio::test]
async fn create_basin_with_retention_policy_age() -> Result<(), S2Error> {
    let s2 = s2();
    let basin_name = unique_basin_name();

    s2.create_basin(CreateBasinInput::new(basin_name.clone()).with_config(
        BasinConfig::new().with_default_stream_config(
            StreamConfig::new().with_retention_policy(RetentionPolicy::Age(86400)),
        ),
    ))
    .await?;

    let config = s2.get_basin_config(basin_name.clone()).await?;
    assert_matches!(
        config.default_stream_config,
        Some(StreamConfig {
            retention_policy: Some(RetentionPolicy::Age(86400)),
            ..
        })
    );

    s2.delete_basin(DeleteBasinInput::new(basin_name)).await?;

    Ok(())
}

#[tokio::test]
async fn create_basin_with_timestamping_mode_arrival() -> Result<(), S2Error> {
    let s2 = s2();
    let basin_name = unique_basin_name();

    s2.create_basin(
        CreateBasinInput::new(basin_name.clone()).with_config(
            BasinConfig::new().with_default_stream_config(
                StreamConfig::new().with_timestamping(
                    TimestampingConfig::new().with_mode(TimestampingMode::Arrival),
                ),
            ),
        ),
    )
    .await?;

    let config = s2.get_basin_config(basin_name.clone()).await?;
    assert_matches!(
        config.default_stream_config,
        Some(StreamConfig {
            timestamping: Some(TimestampingConfig {
                mode: Some(TimestampingMode::Arrival),
                ..
            }),
            ..
        })
    );

    s2.delete_basin(DeleteBasinInput::new(basin_name)).await?;

    Ok(())
}

#[tokio::test]
async fn create_basin_with_timestamping_mode_client_require() -> Result<(), S2Error> {
    let s2 = s2();
    let basin_name = unique_basin_name();

    s2.create_basin(CreateBasinInput::new(basin_name.clone()).with_config(
        BasinConfig::new().with_default_stream_config(StreamConfig::new().with_timestamping(
            TimestampingConfig::new().with_mode(TimestampingMode::ClientRequire),
        )),
    ))
    .await?;

    let config = s2.get_basin_config(basin_name.clone()).await?;
    assert_matches!(
        config.default_stream_config,
        Some(StreamConfig {
            timestamping: Some(TimestampingConfig {
                mode: Some(TimestampingMode::ClientRequire),
                ..
            }),
            ..
        })
    );

    s2.delete_basin(DeleteBasinInput::new(basin_name)).await?;

    Ok(())
}

#[tokio::test]
async fn reconfigure_basin_enable_create_stream_on_append() -> Result<(), S2Error> {
    let s2 = s2();
    let basin_name = unique_basin_name();

    s2.create_basin(CreateBasinInput::new(basin_name.clone()))
        .await?;

    let config = s2
        .reconfigure_basin(ReconfigureBasinInput::new(
            basin_name.clone(),
            BasinReconfiguration::new().with_create_stream_on_append(true),
        ))
        .await?;

    assert!(config.create_stream_on_append);

    s2.delete_basin(DeleteBasinInput::new(basin_name)).await?;

    Ok(())
}

#[tokio::test]
async fn reconfigure_basin_change_retention_to_age() -> Result<(), S2Error> {
    let s2 = s2();
    let basin_name = unique_basin_name();

    s2.create_basin(CreateBasinInput::new(basin_name.clone()))
        .await?;

    let config = s2
        .reconfigure_basin(ReconfigureBasinInput::new(
            basin_name.clone(),
            BasinReconfiguration::new().with_default_stream_config(
                StreamReconfiguration::new().with_retention_policy(RetentionPolicy::Age(3600)),
            ),
        ))
        .await?;

    assert_matches!(
        config.default_stream_config,
        Some(StreamConfig {
            retention_policy: Some(RetentionPolicy::Age(3600)),
            ..
        })
    );

    s2.delete_basin(DeleteBasinInput::new(basin_name)).await?;

    Ok(())
}

#[tokio::test]
async fn reconfigure_basin_change_timestamping_mode() -> Result<(), S2Error> {
    let s2 = s2();
    let basin_name = unique_basin_name();

    s2.create_basin(CreateBasinInput::new(basin_name.clone()))
        .await?;

    let config = s2
        .reconfigure_basin(ReconfigureBasinInput::new(
            basin_name.clone(),
            BasinReconfiguration::new().with_default_stream_config(
                StreamReconfiguration::new().with_timestamping(
                    TimestampingReconfiguration::new().with_mode(TimestampingMode::Arrival),
                ),
            ),
        ))
        .await?;

    assert_matches!(
        config.default_stream_config,
        Some(StreamConfig {
            timestamping: Some(TimestampingConfig {
                mode: Some(TimestampingMode::Arrival),
                ..
            }),
            ..
        })
    );

    s2.delete_basin(DeleteBasinInput::new(basin_name)).await?;

    Ok(())
}

#[tokio::test]
async fn reconfigure_basin_empty_reconfiguration() -> Result<(), S2Error> {
    let s2 = s2();
    let basin_name = unique_basin_name();

    s2.create_basin(CreateBasinInput::new(basin_name.clone()))
        .await?;

    let config_before = s2.get_basin_config(basin_name.clone()).await?;

    let config_after = s2
        .reconfigure_basin(ReconfigureBasinInput::new(
            basin_name.clone(),
            BasinReconfiguration::new(),
        ))
        .await?;

    assert_eq!(
        config_before.create_stream_on_append,
        config_after.create_stream_on_append
    );
    assert_eq!(
        config_before.create_stream_on_read,
        config_after.create_stream_on_read
    );

    s2.delete_basin(DeleteBasinInput::new(basin_name)).await?;

    Ok(())
}

#[tokio::test]
async fn list_access_tokens_with_start_after() -> Result<(), S2Error> {
    let s2 = s2();
    let prefix = format!("{}", uuid::Uuid::new_v4().simple());
    let token_id_1: AccessTokenId = format!("{}-aaa", prefix).parse().expect("valid token id");
    let token_id_2: AccessTokenId = format!("{}-bbb", prefix).parse().expect("valid token id");
    let token_id_3: AccessTokenId = format!("{}-ccc", prefix).parse().expect("valid token id");

    let scope =
        AccessTokenScopeInput::from_op_group_perms(OperationGroupPermissions::read_write_all());

    s2.issue_access_token(IssueAccessTokenInput::new(
        token_id_1.clone(),
        scope.clone(),
    ))
    .await?;
    s2.issue_access_token(IssueAccessTokenInput::new(
        token_id_2.clone(),
        scope.clone(),
    ))
    .await?;
    s2.issue_access_token(IssueAccessTokenInput::new(token_id_3.clone(), scope))
        .await?;

    let page = s2
        .list_access_tokens(
            ListAccessTokensInput::new()
                .with_prefix(prefix.parse().expect("valid prefix"))
                .with_start_after(
                    format!("{}-aaa", prefix)
                        .parse()
                        .expect("valid start after"),
                ),
        )
        .await?;

    assert_eq!(page.values.len(), 2);
    assert!(page.values.iter().any(|t| t.id == token_id_2));
    assert!(page.values.iter().any(|t| t.id == token_id_3));

    s2.revoke_access_token(token_id_1).await?;
    s2.revoke_access_token(token_id_2).await?;
    s2.revoke_access_token(token_id_3).await?;

    Ok(())
}

#[tokio::test]
async fn list_access_tokens_pagination() -> Result<(), S2Error> {
    let s2 = s2();
    let prefix = format!("{}", uuid::Uuid::new_v4().simple());
    let token_ids: Vec<AccessTokenId> = (0..5)
        .map(|i| {
            format!("{}-{:03}", prefix, i)
                .parse()
                .expect("valid token id")
        })
        .collect();

    let scope =
        AccessTokenScopeInput::from_op_group_perms(OperationGroupPermissions::read_write_all());

    for token_id in &token_ids {
        s2.issue_access_token(IssueAccessTokenInput::new(token_id.clone(), scope.clone()))
            .await?;
    }

    let mut all_tokens = Vec::new();
    let mut start_after: Option<AccessTokenIdStartAfter> = None;

    loop {
        let mut input =
            ListAccessTokensInput::new().with_prefix(prefix.parse().expect("valid prefix"));
        input = input.with_limit(2);
        if let Some(ref sa) = start_after {
            input = input.with_start_after(sa.clone());
        }

        let page = s2.list_access_tokens(input).await?;
        all_tokens.extend(page.values.clone());

        if !page.has_more {
            break;
        }
        start_after = page
            .values
            .last()
            .map(|t| t.id.as_ref().parse().expect("valid start_after"));
    }

    assert_eq!(all_tokens.len(), 5);

    for token_id in &token_ids {
        s2.revoke_access_token(token_id.clone()).await?;
    }

    Ok(())
}

#[tokio::test]
async fn issue_access_token_with_all_scopes_combined() -> Result<(), S2Error> {
    let s2 = s2();
    let token_id: AccessTokenId = uuid().parse().expect("valid token id");

    let token = s2
        .issue_access_token(IssueAccessTokenInput::new(
            token_id.clone(),
            AccessTokenScopeInput::from_op_group_perms(OperationGroupPermissions::read_write_all())
                .with_basins(BasinMatcher::Prefix("test-".parse().expect("valid prefix")))
                .with_streams(StreamMatcher::Prefix(
                    "logs-".parse().expect("valid prefix"),
                )),
        ))
        .await?;

    assert!(!token.is_empty());

    s2.revoke_access_token(token_id).await?;

    Ok(())
}

#[tokio::test]
async fn list_basins_with_empty_prefix() -> Result<(), S2Error> {
    let s2 = s2();

    let page = s2
        .list_basins(ListBasinsInput::new().with_prefix("".parse().expect("valid prefix")))
        .await?;

    assert!(page.values.len() <= 1000);

    Ok(())
}

#[tokio::test]
async fn list_basins_with_limit_zero() -> Result<(), S2Error> {
    let s2 = s2();

    let page = s2.list_basins(ListBasinsInput::new().with_limit(0)).await?;

    assert!(page.values.len() <= 1000);

    Ok(())
}

#[tokio::test]
async fn list_basins_with_limit_over_max() -> Result<(), S2Error> {
    let s2 = s2();

    let page = s2
        .list_basins(ListBasinsInput::new().with_limit(1001))
        .await?;

    assert!(page.values.len() <= 1000);

    Ok(())
}

#[tokio::test]
async fn list_basins_with_invalid_start_after_less_than_prefix_errors() -> Result<(), S2Error> {
    let s2 = s2();

    let result = s2
        .list_basins(
            ListBasinsInput::new()
                .with_prefix("z".parse().expect("valid prefix"))
                .with_start_after("a".parse().expect("valid start after")),
        )
        .await;

    assert_matches!(result, Err(S2Error::Server(ErrorResponse { code, .. })) => {
        assert_eq!(code, "invalid");
    });

    Ok(())
}

#[tokio::test]
async fn create_basin_with_storage_class_express() -> Result<(), S2Error> {
    let s2 = s2();
    let basin_name = unique_basin_name();

    let result = s2
        .create_basin(CreateBasinInput::new(basin_name.clone()).with_config(
            BasinConfig::new().with_default_stream_config(
                StreamConfig::new().with_storage_class(StorageClass::Express),
            ),
        ))
        .await;

    match result {
        Ok(_) => {
            let config = s2.get_basin_config(basin_name.clone()).await?;
            assert_matches!(
                config.default_stream_config,
                Some(StreamConfig {
                    storage_class: Some(StorageClass::Express),
                    ..
                })
            );
            s2.delete_basin(DeleteBasinInput::new(basin_name)).await?;
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

#[tokio::test]
async fn create_basin_with_retention_policy_infinite() -> Result<(), S2Error> {
    let s2 = s2();
    let basin_name = unique_basin_name();

    let result = s2
        .create_basin(CreateBasinInput::new(basin_name.clone()).with_config(
            BasinConfig::new().with_default_stream_config(
                StreamConfig::new().with_retention_policy(RetentionPolicy::Infinite),
            ),
        ))
        .await;

    match result {
        Ok(_) => {
            let config = s2.get_basin_config(basin_name.clone()).await?;
            assert_matches!(
                config.default_stream_config,
                Some(StreamConfig {
                    retention_policy: Some(RetentionPolicy::Infinite),
                    ..
                })
            );
            s2.delete_basin(DeleteBasinInput::new(basin_name)).await?;
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

#[tokio::test]
async fn create_basin_with_timestamping_uncapped_true() -> Result<(), S2Error> {
    let s2 = s2();
    let basin_name = unique_basin_name();

    s2.create_basin(CreateBasinInput::new(basin_name.clone()).with_config(
        BasinConfig::new().with_default_stream_config(
            StreamConfig::new().with_timestamping(TimestampingConfig::new().with_uncapped(true)),
        ),
    ))
    .await?;

    let config = s2.get_basin_config(basin_name.clone()).await?;
    assert_matches!(
        config.default_stream_config,
        Some(StreamConfig {
            timestamping: Some(TimestampingConfig { uncapped: true, .. }),
            ..
        })
    );

    s2.delete_basin(DeleteBasinInput::new(basin_name)).await?;

    Ok(())
}

#[tokio::test]
async fn create_basin_with_delete_on_empty() -> Result<(), S2Error> {
    let s2 = s2();
    let basin_name = unique_basin_name();

    s2.create_basin(CreateBasinInput::new(basin_name.clone()).with_config(
        BasinConfig::new().with_default_stream_config(StreamConfig::new().with_delete_on_empty(
            DeleteOnEmptyConfig::new().with_min_age(Duration::from_secs(3600)),
        )),
    ))
    .await?;

    let config = s2.get_basin_config(basin_name.clone()).await?;
    assert_matches!(
        config.default_stream_config,
        Some(StreamConfig {
            delete_on_empty: Some(DeleteOnEmptyConfig {
                min_age_secs: 3600,
                ..
            }),
            ..
        })
    );

    s2.delete_basin(DeleteBasinInput::new(basin_name)).await?;

    Ok(())
}

#[tokio::test]
async fn reconfigure_basin_change_retention_to_infinite() -> Result<(), S2Error> {
    let s2 = s2();
    let basin_name = unique_basin_name();

    s2.create_basin(CreateBasinInput::new(basin_name.clone()))
        .await?;

    let result = s2
        .reconfigure_basin(ReconfigureBasinInput::new(
            basin_name.clone(),
            BasinReconfiguration::new().with_default_stream_config(
                StreamReconfiguration::new().with_retention_policy(RetentionPolicy::Infinite),
            ),
        ))
        .await;

    match result {
        Ok(config) => {
            assert_matches!(
                config.default_stream_config,
                Some(StreamConfig {
                    retention_policy: Some(RetentionPolicy::Infinite),
                    ..
                })
            );
        }
        Err(S2Error::Server(ErrorResponse { code, message, .. }))
            if code == "invalid" && message.contains("free tier") =>
        {
            // Infinite retention not available on free tier
        }
        Err(e) => return Err(e),
    }

    s2.delete_basin(DeleteBasinInput::new(basin_name)).await?;

    Ok(())
}

#[tokio::test]
async fn reconfigure_basin_change_storage_class_to_express() -> Result<(), S2Error> {
    let s2 = s2();
    let basin_name = unique_basin_name();

    s2.create_basin(CreateBasinInput::new(basin_name.clone()))
        .await?;

    let result = s2
        .reconfigure_basin(ReconfigureBasinInput::new(
            basin_name.clone(),
            BasinReconfiguration::new().with_default_stream_config(
                StreamReconfiguration::new().with_storage_class(StorageClass::Express),
            ),
        ))
        .await;

    match result {
        Ok(config) => {
            assert_matches!(
                config.default_stream_config,
                Some(StreamConfig {
                    storage_class: Some(StorageClass::Express),
                    ..
                })
            );
        }
        Err(S2Error::Server(ErrorResponse { code, message, .. }))
            if code == "invalid" && message.contains("free tier") =>
        {
            // Express storage class not available on free tier
        }
        Err(e) => return Err(e),
    }

    s2.delete_basin(DeleteBasinInput::new(basin_name)).await?;

    Ok(())
}

#[tokio::test]
async fn reconfigure_basin_change_timestamping_uncapped() -> Result<(), S2Error> {
    let s2 = s2();
    let basin_name = unique_basin_name();

    s2.create_basin(CreateBasinInput::new(basin_name.clone()))
        .await?;

    let config = s2
        .reconfigure_basin(ReconfigureBasinInput::new(
            basin_name.clone(),
            BasinReconfiguration::new().with_default_stream_config(
                StreamReconfiguration::new()
                    .with_timestamping(TimestampingReconfiguration::new().with_uncapped(true)),
            ),
        ))
        .await?;

    assert_matches!(
        config.default_stream_config,
        Some(StreamConfig {
            timestamping: Some(TimestampingConfig { uncapped: true, .. }),
            ..
        })
    );

    s2.delete_basin(DeleteBasinInput::new(basin_name)).await?;

    Ok(())
}

#[tokio::test]
async fn reconfigure_basin_change_delete_on_empty() -> Result<(), S2Error> {
    let s2 = s2();
    let basin_name = unique_basin_name();

    s2.create_basin(CreateBasinInput::new(basin_name.clone()))
        .await?;

    let config = s2
        .reconfigure_basin(ReconfigureBasinInput::new(
            basin_name.clone(),
            BasinReconfiguration::new().with_default_stream_config(
                StreamReconfiguration::new().with_delete_on_empty(
                    DeleteOnEmptyReconfiguration::new().with_min_age(Duration::from_secs(3600)),
                ),
            ),
        ))
        .await?;

    assert_matches!(
        config.default_stream_config,
        Some(StreamConfig {
            delete_on_empty: Some(DeleteOnEmptyConfig {
                min_age_secs: 3600,
                ..
            }),
            ..
        })
    );

    s2.delete_basin(DeleteBasinInput::new(basin_name)).await?;

    Ok(())
}

#[tokio::test]
async fn reconfigure_basin_disable_delete_on_empty() -> Result<(), S2Error> {
    let s2 = s2();
    let basin_name = unique_basin_name();

    s2.create_basin(CreateBasinInput::new(basin_name.clone()).with_config(
        BasinConfig::new().with_default_stream_config(StreamConfig::new().with_delete_on_empty(
            DeleteOnEmptyConfig::new().with_min_age(Duration::from_secs(3600)),
        )),
    ))
    .await?;

    let config = s2
        .reconfigure_basin(ReconfigureBasinInput::new(
            basin_name.clone(),
            BasinReconfiguration::new().with_default_stream_config(
                StreamReconfiguration::new().with_delete_on_empty(
                    DeleteOnEmptyReconfiguration::new().with_min_age(Duration::from_secs(0)),
                ),
            ),
        ))
        .await?;

    assert_matches!(
        config.default_stream_config,
        Some(StreamConfig {
            delete_on_empty: None,
            ..
        }) | None
    );

    s2.delete_basin(DeleteBasinInput::new(basin_name)).await?;

    Ok(())
}

#[tokio::test]
async fn reconfigure_basin_enable_create_stream_on_read() -> Result<(), S2Error> {
    let s2 = s2();
    let basin_name = unique_basin_name();

    s2.create_basin(CreateBasinInput::new(basin_name.clone()))
        .await?;

    let config = s2
        .reconfigure_basin(ReconfigureBasinInput::new(
            basin_name.clone(),
            BasinReconfiguration::new().with_create_stream_on_read(true),
        ))
        .await?;

    assert!(config.create_stream_on_read);

    s2.delete_basin(DeleteBasinInput::new(basin_name)).await?;

    Ok(())
}

#[tokio::test]
async fn reconfigure_basin_disable_create_stream_on_append() -> Result<(), S2Error> {
    let s2 = s2();
    let basin_name = unique_basin_name();

    s2.create_basin(
        CreateBasinInput::new(basin_name.clone())
            .with_config(BasinConfig::new().with_create_stream_on_append(true)),
    )
    .await?;

    let config = s2
        .reconfigure_basin(ReconfigureBasinInput::new(
            basin_name.clone(),
            BasinReconfiguration::new().with_create_stream_on_append(false),
        ))
        .await?;

    assert!(!config.create_stream_on_append);

    s2.delete_basin(DeleteBasinInput::new(basin_name)).await?;

    Ok(())
}

#[tokio::test]
async fn delete_basin_with_streams() -> Result<(), S2Error> {
    let s2 = s2();
    let basin_name = unique_basin_name();

    s2.create_basin(CreateBasinInput::new(basin_name.clone()))
        .await?;

    let basin = s2.basin(basin_name.clone());
    basin
        .create_stream(CreateStreamInput::new(
            "test-stream".parse().expect("valid stream name"),
        ))
        .await?;

    s2.delete_basin(DeleteBasinInput::new(basin_name)).await?;

    Ok(())
}

#[tokio::test]
async fn list_basins_shows_deleting_state() -> Result<(), S2Error> {
    let s2 = s2();
    let basin_name = unique_basin_name();

    s2.create_basin(CreateBasinInput::new(basin_name.clone()))
        .await?;

    s2.delete_basin(DeleteBasinInput::new(basin_name.clone()))
        .await?;

    let page = s2
        .list_basins(
            ListBasinsInput::new().with_prefix(basin_name.as_ref().parse().expect("valid prefix")),
        )
        .await?;

    assert_matches!(
        page.values.as_slice(),
        [] | [BasinInfo {
            state: BasinState::Deleting,
            ..
        }]
    );

    Ok(())
}

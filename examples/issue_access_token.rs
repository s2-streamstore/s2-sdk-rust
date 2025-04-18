use s2::{
    client::{Client, ClientConfig},
    types::{
        AccessTokenId, AccessTokenInfo, AccessTokenScope, Operation, PermittedOperationGroups,
        ReadWritePermissions, ResourceSet,
    },
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let token = std::env::var("S2_ACCESS_TOKEN")?;
    let config = ClientConfig::new(token);
    let client = Client::new(config);

    let access_token_id: AccessTokenId = "my-access-token".parse()?;
    let access_token_info = AccessTokenInfo::new(access_token_id).with_scope(
        AccessTokenScope::new()
            .with_op_groups(
                PermittedOperationGroups::new()
                    .with_account(ReadWritePermissions::new().with_read(true)),
            )
            .with_op(Operation::CreateStream)
            .with_streams(ResourceSet::Prefix("my-stream-prefix".to_string()))
            .with_basins(ResourceSet::Exact("my-perfect-basin".to_string())),
    );
    let token = client.issue_access_token(access_token_info).await?;

    println!("Access token: {token}");

    Ok(())
}

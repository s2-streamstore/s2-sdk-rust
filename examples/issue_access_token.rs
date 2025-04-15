use s2::{
    client::{Client, ClientConfig},
    types::{AccessTokenId, AccessTokenInfo},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let token = std::env::var("S2_ACCESS_TOKEN")?;
    let config = ClientConfig::new(token);
    let client = Client::new(config);

    let access_token_id: AccessTokenId = "my-access-token".parse()?;
    let token = client
        .issue_access_token(AccessTokenInfo::new(access_token_id))
        .await?;

    println!("Access token: {token}");

    Ok(())
}

use s2::{
    client::{Client, ClientConfig},
    types::{AccessTokenInfo, IssueAccessTokenRequest, IssueAccessTokenResponse},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let token = std::env::var("S2_ACCESS_TOKEN")?;
    let config = ClientConfig::new(token);
    let client = Client::new(config);

    let access_token_info = AccessTokenInfo::new("my-access-token");
    let issue_access_token_req = IssueAccessTokenRequest::new(access_token_info);

    let IssueAccessTokenResponse { token } =
        client.issue_access_token(issue_access_token_req).await?;

    println!("Access token: {token}");

    Ok(())
}

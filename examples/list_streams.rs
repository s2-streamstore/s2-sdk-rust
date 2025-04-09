use s2::{
    client::{BasinClient, ClientConfig},
    types::{BasinName, ListStreamsRequest},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let token = std::env::var("S2_ACCESS_TOKEN")?;
    let config = ClientConfig::new(token);
    let basin: BasinName = "my-favorite-basin".parse()?;
    let basin_client = BasinClient::new(config, basin);

    let prefix = "my-";
    let list_streams_request = ListStreamsRequest::new().with_prefix(prefix);

    let list_streams_response = basin_client.list_streams(list_streams_request).await?;

    println!("{list_streams_response:#?}");

    Ok(())
}

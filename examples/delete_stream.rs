use streamstore::{
    client::{BasinClient, ClientConfig},
    types::{BasinName, DeleteStreamRequest},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let token = std::env::var("S2_AUTH_TOKEN")?;
    let config = ClientConfig::new(token);
    let basin: BasinName = "my-basin".parse()?;
    let basin_client = BasinClient::new(config, basin);

    let stream = "my-stream";

    let delete_stream_request = DeleteStreamRequest::new(stream);

    basin_client.delete_stream(delete_stream_request).await?;

    Ok(())
}

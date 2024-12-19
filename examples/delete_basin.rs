use streamstore::{
    client::{Client, ClientConfig},
    types::{BasinName, DeleteBasinRequest},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let token = std::env::var("S2_AUTH_TOKEN")?;
    let config = ClientConfig::new(token);
    let client = Client::new(config);

    let basin: BasinName = "my-favorite-basin".parse()?;

    let delete_basin_request = DeleteBasinRequest::new(basin)
        // Don't error if the basin doesn't exist.
        .with_if_exists(true);

    client.delete_basin(delete_basin_request).await?;

    Ok(())
}

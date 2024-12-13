use std::time::Duration;

use streamstore::{
    client::{Client, ClientConfig},
    types::{BasinConfig, BasinName, CreateBasinRequest, RetentionPolicy, StreamConfig},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let token = std::env::var("S2_AUTH_TOKEN")?;
    let config = ClientConfig::new(token);
    let client = Client::new(config);

    let basin: BasinName = "my-basin".parse()?;

    let default_stream_config = StreamConfig::new().with_retention_policy(RetentionPolicy::Age(
        // Set the default retention age to 10 days.
        Duration::from_secs(10 * 24 * 60 * 60),
    ));

    let basin_config = BasinConfig {
        default_stream_config: Some(default_stream_config),
    };

    let create_basin_request = CreateBasinRequest::new(basin).with_config(basin_config);

    let created_basin = client.create_basin(create_basin_request).await?;

    println!("{created_basin:#?}");

    Ok(())
}

use std::time::Duration;

use s2::{
    client::{BasinClient, ClientConfig},
    types::{BasinName, ReconfigureStreamRequest, RetentionPolicy, StreamConfig},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let token = std::env::var("S2_AUTH_TOKEN")?;
    let config = ClientConfig::new(token);
    let basin: BasinName = "my-favorite-basin".parse()?;
    let basin_client = BasinClient::new(config, basin);

    let stream = "my-favorite-stream";

    let stream_config_updates = StreamConfig::new().with_retention_policy(RetentionPolicy::Age(
        // Change to retention policy to 1 day
        Duration::from_secs(24 * 60 * 60),
    ));

    let reconfigure_stream_request = ReconfigureStreamRequest::new(stream)
        .with_config(stream_config_updates)
        // Field mask specifies which fields to update.
        .with_mask(vec!["retention_policy".to_string()]);

    let updated_stream_config = basin_client
        .reconfigure_stream(reconfigure_stream_request)
        .await?;

    println!("{updated_stream_config:#?}");

    Ok(())
}

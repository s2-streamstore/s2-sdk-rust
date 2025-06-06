use s2::{
    client::{Client, ClientConfig},
    types::{BasinConfig, BasinName, ReconfigureBasinRequest, StorageClass, StreamConfig},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let token = std::env::var("S2_ACCESS_TOKEN")?;
    let config = ClientConfig::new(token);
    let client = Client::new(config);

    let basin: BasinName = "my-favorite-basin".parse()?;

    let default_stream_config_updates =
        StreamConfig::new().with_storage_class(StorageClass::Standard);
    let basin_config_updates = BasinConfig::new()
        .with_default_stream_config(default_stream_config_updates)
        .with_create_stream_on_append(true)
        .with_create_stream_on_read(true);

    let reconfigure_basin_request = ReconfigureBasinRequest::new(basin)
        .with_config(basin_config_updates)
        // Field mask specifies which fields to update.
        .with_mask(vec!["default_stream_config.retention_policy".to_string()]);

    let updated_basin_config = client.reconfigure_basin(reconfigure_basin_request).await?;

    println!("{updated_basin_config:#?}");

    Ok(())
}

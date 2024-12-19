use streamstore::{
    client::{Client, ClientConfig},
    types::{BasinName, CreateStreamRequest, StorageClass, StreamConfig},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let token = std::env::var("S2_AUTH_TOKEN")?;
    let config = ClientConfig::new(token);
    let client = Client::new(config);

    let basin: BasinName = "my-favorite-basin".parse()?;
    let basin_client = client.basin_client(basin);

    let stream = "my-favorite-stream";

    let stream_config = StreamConfig::new().with_storage_class(StorageClass::Express);

    let create_stream_request = CreateStreamRequest::new(stream).with_config(stream_config);

    let created_stream = basin_client.create_stream(create_stream_request).await?;
    println!("{created_stream:#?}");

    let stream_config = basin_client.get_stream_config(stream).await?;
    println!("{stream_config:#?}");

    Ok(())
}

use streamstore::{
    client::{ClientConfig, StreamClient},
    types::{BasinName, ReadLimit, ReadRequest},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let token = std::env::var("S2_AUTH_TOKEN")?;
    let config = ClientConfig::new(token);
    let basin: BasinName = "my-favorite-basin".parse()?;
    let stream = "my-favorite-stream";
    let stream_client = StreamClient::new(config, basin, stream);

    let tail = stream_client.check_tail().await?;
    if tail == 0 {
        println!("Empty stream");
        return Ok(());
    }

    let latest_seq_num = tail - 1;

    let read_limit = ReadLimit { count: 1, bytes: 0 };
    let read_request = ReadRequest::new(latest_seq_num).with_limit(read_limit);
    let latest_record = stream_client.read(read_request).await?;

    println!("{latest_record:#?}");

    Ok(())
}

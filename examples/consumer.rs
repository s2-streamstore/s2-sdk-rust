use futures::StreamExt;
use s2::{
    client::{ClientConfig, StreamClient},
    types::{BasinName, ReadSessionRequest},
};
use tokio::select;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let token = std::env::var("S2_ACCESS_TOKEN")?;
    let config = ClientConfig::new(token);
    let basin: BasinName = "my-favorite-basin".parse()?;
    let stream = "my-favorite-stream";
    let stream_client = StreamClient::new(config, basin, stream);

    let start_seq_num = 0;
    let read_session_request = ReadSessionRequest::new(start_seq_num);
    let mut read_stream = stream_client.read_session(read_session_request).await?;

    loop {
        select! {
            next_batch = read_stream.next() => {
                let Some(next_batch) = next_batch else { break };
                let next_batch = next_batch?;
                println!("{next_batch:?}");
            }
            _ = tokio::signal::ctrl_c() => break,
        }
    }

    Ok(())
}

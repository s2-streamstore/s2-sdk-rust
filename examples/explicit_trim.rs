use streamstore::{
    client::{ClientConfig, StreamClient},
    types::{AppendInput, AppendRecordBatch, BasinName, CommandRecord},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let token = std::env::var("S2_AUTH_TOKEN")?;
    let config = ClientConfig::new(token);
    let basin: BasinName = "my-basin".parse()?;
    let stream = "my-stream";
    let stream_client = StreamClient::new(config, basin, stream);

    let tail = stream_client.check_tail().await?;

    let latest_seq_num = tail - 1;
    let trim_request = CommandRecord::trim(latest_seq_num);

    let append_record_batch = AppendRecordBatch::try_from_iter([trim_request])
        .expect("valid batch with 1 command record");
    let append_input = AppendInput::new(append_record_batch);
    let _ = stream_client.append(append_input).await?;

    Ok(())
}

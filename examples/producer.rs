use futures::StreamExt;
use streamstore::{
    batching::{AppendRecordsBatchingOpts, AppendRecordsBatchingStream},
    client::{ClientConfig, StreamClient},
    types::{AppendInput, AppendRecord, AppendRecordBatch, BasinName, CommandRecord, FencingToken},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let token = std::env::var("S2_AUTH_TOKEN")?;
    let config = ClientConfig::new(token);
    let basin: BasinName = "my-favorite-basin".parse()?;
    let stream = "my-favorite-stream";
    let stream_client = StreamClient::new(config, basin, stream);

    let fencing_token = FencingToken::generate(16).expect("valid fencing token with 16 bytes");

    // Set the fencing token.
    let fencing_token_record: AppendRecord = CommandRecord::fence(fencing_token.clone()).into();
    let fencing_token_batch = AppendRecordBatch::try_from_iter([fencing_token_record])
        .expect("valid batch with 1 append record");
    let fencing_token_append_input = AppendInput::new(fencing_token_batch);
    let set_fencing_token = stream_client.append(fencing_token_append_input).await?;

    let match_seq_num = set_fencing_token.next_seq_num; // Tail

    // Stream of records
    let append_stream = futures::stream::iter([
        AppendRecord::new("record_1")?,
        AppendRecord::new("record_2")?,
    ]);

    let append_records_batching_opts = AppendRecordsBatchingOpts::new()
        .with_fencing_token(Some(fencing_token))
        .with_match_seq_num(Some(match_seq_num));

    let append_session_request =
        AppendRecordsBatchingStream::new(append_stream, append_records_batching_opts);

    let mut append_session_stream = stream_client.append_session(append_session_request).await?;

    while let Some(next) = append_session_stream.next().await {
        let next = next?;
        println!("{next:#?}");
    }

    Ok(())
}

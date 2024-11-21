use crate::client::{ClientError, StreamClient};
use crate::service::stream::AppendSessionServiceRequest;
use crate::{types, Streaming};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::Status;

async fn run<S>(
    stream_client: StreamClient,
    input: S,
    output_tx: mpsc::Sender<Result<types::AppendOutput, ClientError>>,
) where
    S: 'static + Send + Unpin + futures::Stream<Item = types::AppendInput>,
{
    let mut resp = stream_client
        .inner
        .send(AppendSessionServiceRequest::new(
            stream_client.inner.stream_service_client(),
            &stream_client.stream,
            input,
        ))
        .await
        .unwrap();

    while let Some(x) = resp.next().await {
        let _ = output_tx.send(x).await;
    }
}

pub async fn append_session<S>(
    stream_client: &StreamClient,
    req: S,
) -> Result<Streaming<types::AppendOutput>, ClientError>
where
    S: 'static + Send + Unpin + futures::Stream<Item = types::AppendInput>,
{
    let (response_tx, response_rx) = mpsc::channel(10);
    let _ = tokio::spawn(run(stream_client.clone(), req, response_tx));

    let s = ReceiverStream::new(response_rx);

    Ok(Box::pin(s))

    //Err(ClientError::Service(Status::internal("")))
}

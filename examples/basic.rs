use std::time::Duration;

use futures::StreamExt;
use streamstore::{
    client::{Client, ClientConfig, HostEndpoints},
    service_error::{CreateBasinError, CreateStreamError, ServiceError},
    streams::AppendRecordStream,
    types::{
        AppendInput, AppendRecord, CreateBasinRequest, CreateStreamRequest, DeleteBasinRequest,
        DeleteStreamRequest, ListBasinsRequest, ListStreamsRequest, ReadSessionRequest,
    },
};

#[tokio::main]
async fn main() {
    let token = std::env::var("S2_AUTH_TOKEN").unwrap();

    let config = ClientConfig::new(token)
        .with_host_endpoint(HostEndpoints::from_env())
        .with_request_timeout(Duration::from_secs(10));

    println!("Connecting with {config:#?}");

    let client = Client::connect(config).await.unwrap();

    let basin = "s2-sdk-example-basin";

    let create_basin_req = CreateBasinRequest::new(basin);

    match client.create_basin(create_basin_req).await {
        Ok(created_basin) => {
            println!("Basin created: {created_basin:#?}");
        }
        Err(ServiceError::Remote(CreateBasinError::AlreadyExists(e))) => {
            println!("WARN: {}", e);
        }
        Err(other) => exit_with_err(other),
    };

    let list_basins_req = ListBasinsRequest::new();

    match client.list_basins(list_basins_req).await {
        Ok(basins_list) => {
            println!(
                "List of basins: {:#?}{}",
                basins_list.basins,
                if basins_list.has_more {
                    " ... and more ..."
                } else {
                    ""
                }
            )
        }
        Err(err) => exit_with_err(err),
    };

    match client.get_basin_config(basin).await {
        Ok(config) => {
            println!("Basin config: {config:#?}");
        }
        Err(err) => exit_with_err(err),
    };

    let stream = "s2-sdk-example-stream";

    let create_stream_req = CreateStreamRequest::new(stream);

    let basin_client = client.basin_client(basin).await.unwrap();

    match basin_client.create_stream(create_stream_req).await {
        Ok(()) => {
            println!("Stream created");
        }
        Err(ServiceError::Remote(CreateStreamError::AlreadyExists(e))) => {
            println!("WARN: {}", e);
        }
        Err(other) => exit_with_err(other),
    };

    let list_streams_req = ListStreamsRequest::new();

    match basin_client.list_streams(list_streams_req).await {
        Ok(streams_list) => {
            println!(
                "List of streams: {:#?}{}",
                streams_list.streams,
                if streams_list.has_more {
                    " ... and more ..."
                } else {
                    ""
                }
            )
        }
        Err(err) => exit_with_err(err),
    }

    match basin_client.get_stream_config(stream).await {
        Ok(config) => {
            println!("Stream config: {config:#?}");
        }
        Err(err) => exit_with_err(err),
    };

    let stream_client = basin_client.stream_client(stream);

    let records = [
        AppendRecord::new(b"hello world"),
        AppendRecord::new(b"bye world"),
    ];

    let append_input = AppendInput::new(records.clone());

    match stream_client.append(append_input.clone()).await {
        Ok(resp) => {
            println!("Appended: {resp:#?}");
        }
        Err(err) => exit_with_err(err),
    };

    let append_session_req =
        AppendRecordStream::new(futures::stream::iter(records), Default::default()).unwrap();

    match stream_client.append_session(append_session_req).await {
        Ok(mut stream) => {
            println!("Appended in session: {:#?}", stream.next().await);
        }
        Err(err) => exit_with_err(err),
    };

    match stream_client.check_tail().await {
        Ok(next_seq_num) => {
            println!("Next seq num: {next_seq_num:#?}");
        }
        Err(err) => exit_with_err(err),
    };

    let read_session_req = ReadSessionRequest::default();

    match stream_client.read_session(read_session_req).await {
        Ok(mut stream) => {
            println!("Read session: {:#?}", stream.next().await);
        }
        Err(err) => exit_with_err(err),
    };

    let delete_stream_req = DeleteStreamRequest::new(stream).with_if_exists(true);

    match basin_client.delete_stream(delete_stream_req).await {
        Ok(()) => {
            println!("Stream deleted!")
        }
        Err(err) => exit_with_err(err),
    };

    let delete_basin_req = DeleteBasinRequest::new(basin).with_if_exists(false);

    match client.delete_basin(delete_basin_req).await {
        Ok(()) => {
            println!("Basin deleted!")
        }
        Err(err) => exit_with_err(err),
    };
}

fn exit_with_err<E: std::fmt::Display>(err: E) {
    println!("Error: {err}");
    std::process::exit(1);
}

use s2::{
    client::{Client, ClientConfig, HostCloud},
    service_error::{CreateBasinError, CreateStreamError, ServiceError},
    types::{
        CreateBasinRequest, CreateStreamRequest, DeleteBasinRequest, DeleteStreamRequest,
        GetBasinConfigRequest, GetStreamConfigRequest, ListBasinsRequest, ListStreamsRequest,
    },
};

#[tokio::main]
async fn main() {
    let token = std::env::var("S2_AUTH_TOKEN").unwrap();

    let config = ClientConfig::builder()
        .host_uri(HostCloud::Local)
        .token(token)
        .build();

    println!("Connecting with {config:#?}");

    let client = Client::connect(config).await.unwrap();

    let basin = "s2-sdk-example-basin";
    let create_basin_req = CreateBasinRequest::builder().basin(basin).build();

    match client.create_basin(create_basin_req).await {
        Ok(created_basin) => {
            println!("Basin created: {created_basin:#?}");
        }
        Err(ServiceError::Remote(CreateBasinError::AlreadyExists(e))) => {
            println!("WARN: {}", e);
        }
        Err(other) => exit_with_err(other),
    };

    let list_basins_req = ListBasinsRequest::builder().build();

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

    let get_basin_config_req = GetBasinConfigRequest::builder().basin(basin).build();
    match client.get_basin_config(get_basin_config_req).await {
        Ok(config) => {
            println!("Basin config: {config:#?}");
        }
        Err(err) => exit_with_err(err),
    };

    let stream = "s2-sdk-example-stream";

    let create_stream_req = CreateStreamRequest::builder().stream(stream).build();
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

    let list_streams_req = ListStreamsRequest::builder().build();

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

    let get_stream_config_req = GetStreamConfigRequest::builder().stream(stream).build();

    match basin_client.get_stream_config(get_stream_config_req).await {
        Ok(config) => {
            println!("Stream config: {config:#?}");
        }
        Err(err) => exit_with_err(err),
    };

    let stream_client = basin_client.stream_client(stream);

    match stream_client.get_next_seq_num().await {
        Ok(next_seq_num) => {
            println!("Next seq num: {next_seq_num:#?}");
        }
        Err(err) => exit_with_err(err),
    };

    let delete_stream_req = DeleteStreamRequest::builder()
        .stream(stream)
        .if_exists(true)
        .build();

    match basin_client.delete_stream(delete_stream_req).await {
        Ok(()) => {
            println!("Stream deleted!")
        }
        Err(err) => exit_with_err(err),
    };

    let delete_basin_req = DeleteBasinRequest::builder()
        .basin(basin)
        .if_exists(true)
        .build();

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

use s2::{
    client::{Client, ClientConfigBuilder},
    types,
};

fn handle_response<T: std::fmt::Debug, E: std::fmt::Display>(response: Result<T, E>) {
    match response {
        Ok(val) => println!("OK: {val:#?}"),
        Err(e) => {
            println!("Error: {e}");
            std::process::exit(1);
        }
    }
}

#[tokio::main]
async fn main() {
    let token = std::env::var("S2_AUTH_TOKEN").unwrap();
    let config = ClientConfigBuilder::default().token(token).build().unwrap();

    let client = Client::connect(config).await.unwrap();

    let basin = "my-test-basin";

    let create_basin_req = types::CreateBasinRequestBuilder::default()
        .basin(basin)
        .build()
        .unwrap();

    handle_response(client.create_basin(create_basin_req).await);

    let basin_client = client.basin_client(basin).await.unwrap();

    let list_streams_req = types::ListStreamsRequestBuilder::default().build().unwrap();

    handle_response(basin_client.list_streams(list_streams_req).await);
}

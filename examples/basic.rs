use s2::{
    client::{Client, ClientConfig},
    types::{CreateBasinRequest, ListStreamsRequest},
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
    let config = ClientConfig::builder().token(token).build();
    let client = Client::connect(config).await.unwrap();

    let basin = "my-test-basin";
    let create_basin_req = CreateBasinRequest::builder().basin(basin).build();
    handle_response(client.create_basin(create_basin_req).await);

    let basin_client = client.basin_client(basin).await.unwrap();
    let list_streams_req = ListStreamsRequest::builder().build();
    handle_response(basin_client.list_streams(list_streams_req).await);
}

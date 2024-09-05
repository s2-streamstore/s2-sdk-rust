use s2::{client::Client, types};
use tonic::transport::Endpoint;

#[tokio::main]
async fn main() {
    let client = Client::connect(
        Endpoint::from_static("http://localhost:4243"),
        std::env::var("AUTH_TOKEN").unwrap(),
    )
    .await
    .unwrap();

    let create_basin_req = types::CreateBasinRequestBuilder::default()
        .basin("my-favorite-basin")
        .build()
        .unwrap();

    let created_basin = client.create_basin(create_basin_req).await.unwrap();

    println!("{created_basin:?}");
}

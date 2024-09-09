use s2::{
    client::{Client, ClientConfigBuilder},
    types,
};

#[tokio::main]
async fn main() {
    let token = std::env::var("S2_AUTH_TOKEN").unwrap();
    let config = ClientConfigBuilder::default().token(token).build().unwrap();

    let client = Client::connect(config).await.unwrap();

    let create_basin_req = types::CreateBasinRequestBuilder::default()
        .basin("my-favorite-basin")
        .build()
        .unwrap();

    match client.create_basin(create_basin_req).await {
        Ok(created) => println!("Basin created: {:?}", created.basin),
        Err(e) => {
            println!("Error: {e}");
            std::process::exit(1);
        }
    };
}

use s2::{
    client::{Client, ClientConfig},
    types::ListBasinsRequest,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let token = std::env::var("S2_ACCESS_TOKEN")?;
    let config = ClientConfig::new(token);
    let client = Client::new(config);

    let mut all_basins = Vec::new();

    let mut has_more = true;
    let mut start_after: Option<String> = None;

    while has_more {
        let mut list_basins_request = ListBasinsRequest::new();
        if let Some(start_after) = start_after.take() {
            list_basins_request = list_basins_request.with_start_after(start_after);
        }

        let list_basins_response = client.list_basins(list_basins_request).await?;

        all_basins.extend(list_basins_response.basins);

        start_after = all_basins.last().map(|b| b.name.clone());
        has_more = list_basins_response.has_more;
    }

    println!("{all_basins:#?}");

    Ok(())
}

use s2::{
    S2,
    types::{BasinNameStartAfter, ListBasinsInput, S2Config},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let access_token =
        std::env::var("S2_ACCESS_TOKEN").map_err(|_| "S2_ACCESS_TOKEN env var not set")?;

    let config = S2Config::new(access_token);
    let s2 = S2::new(config)?;

    let mut all_basins = Vec::new();

    let mut has_more = true;
    let mut start_after: Option<BasinNameStartAfter> = None;

    while has_more {
        let mut input = ListBasinsInput::new();
        if let Some(start_after) = start_after.take() {
            input = input.with_start_after(start_after);
        }

        let page = s2.list_basins(input).await?;

        all_basins.extend(page.values);

        start_after = all_basins.last().map(|b| b.name.clone().into());
        has_more = page.has_more;
    }

    println!("{all_basins:#?}");

    Ok(())
}

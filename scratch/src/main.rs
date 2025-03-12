use google_cloud_spanner::client::Client;
use google_cloud_spanner::client::ClientConfig;
use google_cloud_spanner::client::Error;
// use google_cloud_spanner::mutation::insert;
use google_cloud_spanner::statement::Statement;
// use google_cloud_spanner::value::CommitTimestamp;

#[tokio::main]
async fn main() -> Result<(), Error> {
    const DATABASE: &str = "projects/mobingi-main/instances/alphaus-prod/databases/main";

    // Create spanner client
    let config = ClientConfig::default().with_auth().await.unwrap();
    let client = Client::new(DATABASE, config).await.unwrap();

    // Insert
    // let mutation = insert(
    //     "Guild",
    //     &["GuildId", "OwnerUserID", "UpdatedAt"],
    //     &[&"guildId", &"ownerId", &CommitTimestamp::new()],
    // );

    // let commit_timestamp = client.apply(vec![mutation]).await?;

    // Read with query
    let mut stmt = Statement::new("select mask from mask_id where name = @name");
    stmt.add_param("name", &"000000009586");
    let mut tx = client.single().await?;
    let mut iter = tx.query(stmt).await?;
    while let Some(row) = iter.next().await? {
        let m = row.column_by_name::<String>("mask");
        print!("mask={:?}", m)
    }

    // Remove all the sessions.
    client.close().await;
    Ok(())
}

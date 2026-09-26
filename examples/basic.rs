use std::time::Duration;

use resume::{Result, run_one, submit};
use serde_json::json;
use tokio_postgres::NoTls;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let (mut client, connection) =
        tokio_postgres::connect(&std::env::var("DATABASE_URL")?, NoTls).await?;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("connection: {error}");
        }
    });
    client
        .batch_execute(
            "create table if not exists greetings (job_id bigint primary key, name text)",
        )
        .await?;
    submit(
        &client,
        "greet",
        "ada",
        &json!({"name": "Ada"}),
        3,
        Duration::from_secs(1),
    )
    .await?;
    loop {
        let result = run_one(&mut client, "greet", async |job, steps| {
            steps
                .step("greet", async |tx| {
                    let name = job.input["name"].as_str().ok_or("missing name")?;
                    tx.execute("insert into greetings values ($1, $2)", &[&job.id, &name])
                        .await?;
                    Ok(json!({"greeted": name}))
                })
                .await?;
            Ok(())
        })
        .await;
        match result {
            Ok(true) => continue,
            Ok(false) => {}
            Err(error) if client.is_closed() => return Err(error),
            Err(error) => eprintln!("attempt: {error}"),
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

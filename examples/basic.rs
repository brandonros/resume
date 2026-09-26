use std::time::Duration;

use resume::{Result, Retry, submit, work};
use serde_json::json;
use tokio_postgres::{NoTls, error::SqlState};

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
    submit(&client, "greet", "ada", &json!({"name": "Ada"})).await?;
    work(
        &mut client,
        "greet",
        async |job, steps| {
            steps
                .step("greet", async |tx| {
                    let name = job.input["name"].as_str().ok_or("missing name")?;
                    tx.execute("insert into greetings values ($1, $2)", &[&job.id, &name])
                        .await?;
                    Ok(json!({"greeted": name}))
                })
                .await?;
            Ok(json!(null))
        },
        |job, error| {
            // Retry known transient database failures. Invalid input, unknown
            // outcomes, and unclassified errors pause for inspection.
            let transient = error
                .downcast_ref::<tokio_postgres::Error>()
                .and_then(|error| error.code())
                .is_some_and(|code| {
                    *code == SqlState::T_R_SERIALIZATION_FAILURE
                        || *code == SqlState::T_R_DEADLOCK_DETECTED
                });
            if transient && job.failures < 3 {
                Retry::After(Duration::from_secs(1))
            } else {
                eprintln!("job {} paused: {error}", job.id);
                Retry::Stop
            }
        },
    )
    .await
}

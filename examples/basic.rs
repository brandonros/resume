use std::time::Duration;

use resume::{Result, Retry, run_one, submit};
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
    loop {
        let result = run_one(
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
                Ok(())
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
                if transient && job.attempt < 4 {
                    Retry::After(Duration::from_secs(1))
                } else {
                    Retry::Stop
                }
            },
        )
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

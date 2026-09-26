//! Run against a disposable database with schema.sql installed:
//! DATABASE_URL=postgresql://... cargo run --example recovery
use resume::{Job, Result, Retry, Steps, run_one, submit};
use serde_json::{Value, json};
use tokio_postgres::{Client, NoTls};

async fn connect() -> Result<Client> {
    let (client, connection) =
        tokio_postgres::connect(&std::env::var("DATABASE_URL")?, NoTls).await?;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("connection: {error}");
        }
    });
    Ok(client)
}

async fn checkout(job: &Job, steps: &mut Steps<'_>, service: &Client) -> Result<()> {
    steps
        .step("create-order", async |tx| {
            // A replayed INSERT would fail the primary key constraint.
            tx.execute(
                "insert into recovery_orders (job_id) values ($1)",
                &[&job.id],
            )
            .await?;
            Ok(json!(null))
        })
        .await?;
    let receipt = steps
        .step_once("charge", async || {
            // Separate connection: the charge commits independently of the workflow.
            // No deduplication here, so repeating this call would charge twice.
            service
                .execute("insert into charges (job_id) values ($1)", &[&job.id])
                .await?;
            Err("charge committed, but its response was lost".into())
        })
        .await?;
    steps
        .step("mark-paid", async |tx| {
            tx.execute(
                "update recovery_orders set receipt = $2 where job_id = $1",
                &[&job.id, &receipt],
            )
            .await?;
            Ok(json!(null))
        })
        .await?;
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let mut client = connect().await?;
    let service = connect().await?;
    client
        .batch_execute(
            "create table if not exists recovery_orders (job_id bigint primary key, receipt jsonb)",
        )
        .await?;
    // A fake external ledger, kept only for this demo's lifetime.
    service
        .batch_execute("create temporary table charges (id bigint generated always as identity, job_id bigint)")
        .await?;
    let run: i64 = client
        .query_one("select txid_current()::bigint", &[])
        .await?
        .get(0);
    let workflow = format!("recovery-{run}");
    let id = submit(&client, &workflow, "order", &json!(null)).await?;

    let error = run_one(
        &mut client,
        &workflow,
        async |job, steps| checkout(job, steps, &service).await,
        |_, _| Retry::Stop,
    )
    .await
    .expect_err("the simulated response must be lost");
    assert_eq!(
        error.to_string(),
        "charge committed, but its response was lost"
    );
    let row = client
        .query_one(
            "select j.paused and not j.leased and not j.completed, s.output is null,
                o.receipt is null
         from resume.jobs j join resume.steps s on s.job_id = j.id and s.key = 'charge'
         join recovery_orders o on o.job_id = j.id where j.id = $1",
            &[&id],
        )
        .await?;
    assert!((0..3).all(|i| row.get::<_, bool>(i)));
    println!("Job {id}: order saved, charge outcome unknown, job paused.");

    let error = client
        .execute("select resume.requeue($1)", &[&id])
        .await
        .unwrap_err();
    assert_eq!(error.as_db_error().unwrap().code().code(), "55000");
    println!("Requeue refused while the charge is unresolved.");

    // Operator verification: consult the service's ledger, never guess from the error.
    let charges = service
        .query("select id from charges where job_id = $1", &[&id])
        .await?;
    assert_eq!(charges.len(), 1);
    let verified = json!({"charge_id": charges[0].get::<_, i64>(0)});
    client
        .execute(
            "select resume.resolve_step($1, 'charge', $2)",
            &[&id, &verified],
        )
        .await?;
    assert!(
        !run_one(
            &mut client,
            &workflow,
            async |_, _| panic!("resolution must not restart a job"),
            |_, _| Retry::Stop,
        )
        .await?
    );
    println!("Verified {verified}; resolution recorded, job still paused.");

    client.execute("select resume.requeue($1)", &[&id]).await?;
    assert!(
        run_one(
            &mut client,
            &workflow,
            async |job, steps| checkout(job, steps, &service).await,
            |_, _| Retry::Stop,
        )
        .await?
    );
    let row = client
        .query_one(
            "select j.completed, j.attempt, o.receipt,
                (select count(*) from resume.steps where job_id = j.id)
         from resume.jobs j join recovery_orders o on o.job_id = j.id where j.id = $1",
            &[&id],
        )
        .await?;
    assert!(row.get::<_, bool>(0));
    assert_eq!(row.get::<_, i64>(1), 2);
    assert_eq!(row.get::<_, Value>(2), verified);
    assert_eq!(row.get::<_, i64>(3), 3);
    let calls: i64 = service
        .query_one("select count(*) from charges where job_id = $1", &[&id])
        .await?
        .get(0);
    assert_eq!(calls, 1);
    println!(
        "Completed on attempt 2: saved steps replayed, order paid, exactly one charge invocation."
    );
    Ok(())
}

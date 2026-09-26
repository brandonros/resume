//! Durable event import. Set DATABASE_URL and install schema.sql, then:
//! cargo run --example importer -- init
//! cargo run --example importer -- load import-1 examples/importer/sample.jsonl
//! cargo run --example importer -- drain
//! cargo run --example importer -- status import-1
//! `work` runs continuously. Input is snapshotted at submission, capped at 2 MiB.
use std::io::Read;
use std::time::Duration;

use resume::{Error, Job, Result, Retry, Steps, run_one, submit, work};
use serde_json::{Value, json};
use tokio_postgres::{Client, NoTls, error::SqlState};

const IMPORT: &str = "import-app:import:v1";
const BATCH: &str = "import-app:batch:v1";
const BATCH_SIZE: usize = 100;
const MAX_BYTES: usize = 2 * 1024 * 1024;

async fn connect() -> Result<Client> {
    let (client, connection) =
        tokio_postgres::connect(&std::env::var("DATABASE_URL")?, NoTls).await?;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("database connection: {error}");
        }
    });
    Ok(client)
}

fn retry(job: &Job, error: &Error) -> Retry {
    let transient = error
        .downcast_ref::<tokio_postgres::Error>()
        .and_then(|error| error.code())
        .is_some_and(|code| {
            *code == SqlState::T_R_SERIALIZATION_FAILURE || *code == SqlState::T_R_DEADLOCK_DETECTED
        });
    eprintln!("job {}: {error:?}", job.id);
    if transient && job.failures < 3 {
        Retry::After(Duration::from_secs(1))
    } else {
        Retry::Stop
    }
}

async fn enqueue(client: &Client, key: &str, source: &str) -> Result<i64> {
    if source.len() > MAX_BYTES {
        return Err("import exceeds 2 MiB; split the source file".into());
    }
    submit(client, IMPORT, key, &json!({"source": source})).await
}

async fn import(job: &Job, steps: &mut Steps<'_>) -> Result<Value> {
    let lines: Vec<_> = job.input["source"]
        .as_str()
        .ok_or("missing source")?
        .lines()
        .collect();
    let mut children = Vec::new();
    for (n, batch) in lines.chunks(BATCH_SIZE).enumerate() {
        children.push(
            steps
                .spawn(
                    &format!("batch-{n}"),
                    BATCH,
                    &json!({"start": n * BATCH_SIZE + 1, "lines": batch}),
                )
                .await?,
        );
    }
    let (mut accepted, mut rejected) = (0, 0);
    for child in children {
        let output = steps.wait_for(child).await?;
        accepted += output["accepted"].as_u64().ok_or("invalid batch output")?;
        rejected += output["rejected"].as_u64().ok_or("invalid batch output")?;
    }
    Ok(json!({"accepted": accepted, "rejected": rejected}))
}

fn parse_event(raw: &str) -> std::result::Result<Value, String> {
    let event: Value = serde_json::from_str(raw).map_err(|error| error.to_string())?;
    if event["id"].as_str().is_none_or(str::is_empty)
        || event["account"].as_str().is_none_or(str::is_empty)
        || event["amount_cents"].as_i64().is_none()
    {
        return Err("expected nonempty id/account strings and integer amount_cents".into());
    }
    Ok(event)
}

async fn batch(job: &Job, steps: &mut Steps<'_>) -> Result<Value> {
    let lines = job.input["lines"].as_array().ok_or("missing lines")?;
    let start = job.input["start"].as_i64().ok_or("missing starting line")?;
    steps
        .step("import-rows", async |tx| {
            let (mut accepted, mut rejected) = (0, 0);
            for (n, raw) in lines.iter().enumerate() {
                let raw = raw.as_str().ok_or("line is not a string")?;
                let reason = match parse_event(raw) {
                    Err(reason) => Some(reason),
                    Ok(event) => {
                        let id = event["id"].as_str().unwrap();
                        // Identical IDs+payloads are accepted duplicates. Conflicts are rejected.
                        let saved = tx
                            .query_opt(
                                "insert into import_app.events values ($1, $2)
                        on conflict (id) do update set id = excluded.id
                        where events.payload = excluded.payload returning id",
                                &[&id, &event],
                            )
                            .await?;
                        saved
                            .is_none()
                            .then(|| "event id already has different content".to_owned())
                    }
                };
                if let Some(reason) = reason {
                    let line = start + n as i64;
                    tx.execute(
                        "insert into import_app.rejections values ($1, $2, $3, $4)",
                        &[&job.id, &line, &raw, &reason],
                    )
                    .await?;
                    rejected += 1;
                } else {
                    accepted += 1;
                }
            }
            Ok(json!({"accepted": accepted, "rejected": rejected}))
        })
        .await
}

async fn status(client: &Client, key: &str) -> Result<Value> {
    Ok(client.query_one("select jsonb_build_object('status', s.status, 'result', j.output,
        'error', s.last_error, 'children', coalesce((
            select jsonb_agg(jsonb_build_object('id', c.id, 'status', c.status,
                'failures', c.failures, 'error', c.last_error) order by c.id)
            from resume.job_status c where c.parent_id = j.id
        ), '[]'::jsonb), 'rejections', coalesce((
            select jsonb_agg(jsonb_build_object('line', r.line, 'reason', r.reason) order by r.line)
            from import_app.rejections r join resume.jobs c on c.id = r.job_id where c.parent_id = j.id
        ), '[]'::jsonb)) from resume.jobs j join resume.job_status s on s.id = j.id
        where j.workflow = $1 and j.key = $2", &[&IMPORT, &key]).await?.get(0))
}

// Idle does not mean complete: scheduled retries and paused children need later attention.
async fn drain(client: &mut Client) -> Result<()> {
    loop {
        let parent = run_one(client, IMPORT, import, retry).await?;
        let child = run_one(client, BATCH, batch, retry).await?;
        if !parent && !child {
            return Ok(());
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let mut client = connect().await?;
    match args
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .as_slice()
    {
        ["init"] => client.batch_execute(include_str!("schema.sql")).await?,
        ["load", key, path] => {
            let mut source = String::new();
            std::fs::File::open(path)?
                .take((MAX_BYTES + 1) as u64)
                .read_to_string(&mut source)?;
            println!("job {}", enqueue(&client, key, &source).await?);
        }
        ["status", key] => println!(
            "{}",
            serde_json::to_string_pretty(&status(&client, key).await?)?
        ),
        ["drain"] => drain(&mut client).await?,
        ["work"] => {
            let mut batches = connect().await?;
            tokio::try_join!(
                work(&mut client, IMPORT, import, retry),
                work(&mut batches, BATCH, batch, retry)
            )?;
        }
        _ => return Err("usage: importer init | load KEY FILE | status KEY | drain | work".into()),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore = "requires a disposable database with schema.sql installed"]
    async fn snapshots_replay_batches_and_retain_bad_rows() -> Result<()> {
        let mut client = connect().await?;
        client.batch_execute(include_str!("schema.sql")).await?;
        let mut source = String::new();
        for n in 0..205 {
            source.push_str(&format!(
                "{{\"id\":\"test-{n}\",\"account\":\"alice\",\"amount_cents\":100}}\n"
            ));
        }
        source
            .push_str("not json\n{\"id\":\"test-0\",\"account\":\"alice\",\"amount_cents\":999}\n");
        let id = enqueue(&client, "test", &source).await?;
        assert_eq!(id, enqueue(&client, "test", &source).await?);
        assert!(enqueue(&client, "test", "changed file").await.is_err());
        // Save a batch, then pause it before job completion; status must explain the wait.
        assert!(run_one(&mut client, IMPORT, import, retry).await?);
        assert!(
            run_one(
                &mut client,
                BATCH,
                async |job, steps| {
                    batch(job, steps).await?;
                    Err("interrupted after batch commit".into())
                },
                |_, _| Retry::Stop
            )
            .await
            .is_err()
        );
        let waiting = status(&client, "test").await?;
        assert_eq!(waiting["status"], "waiting");
        let paused = waiting["children"]
            .as_array()
            .unwrap()
            .iter()
            .find(|child| child["status"] == "paused")
            .expect("paused child is visible");
        assert_eq!(paused["failures"], 1);
        assert_eq!(paused["error"], "interrupted after batch commit");
        let child_id = paused["id"].as_i64().unwrap();
        client
            .execute("select resume.requeue($1)", &[&child_id])
            .await?;
        drain(&mut client).await?;
        let result = status(&client, "test").await?;
        assert_eq!(result["status"], "completed");
        assert_eq!(result["result"], json!({"accepted":205,"rejected":2}));
        assert_eq!(result["rejections"].as_array().unwrap().len(), 2);
        assert_eq!(
            client
                .query_one("select count(*) from import_app.events", &[])
                .await?
                .get::<_, i64>(0),
            205
        );
        // A different import containing identical events is accepted without duplicating rows.
        enqueue(&client, "duplicate", &source).await?;
        drain(&mut client).await?;
        assert_eq!(
            status(&client, "duplicate").await?["result"],
            result["result"]
        );
        assert_eq!(
            client
                .query_one("select count(*) from import_app.events", &[])
                .await?
                .get::<_, i64>(0),
            205
        );
        Ok(())
    }
}

//! Bounded concurrency exercise, not a throughput benchmark.
//! RESUME_STRESS_PARENTS=32 RESUME_STRESS_WORKERS=8 cargo test --test stress -- --ignored --nocapture
use std::time::{Duration, Instant};

use resume::{Job, Result, Retry, Steps, run_one, submit};
use serde_json::{Value, json};
use tokio_postgres::{Client, NoTls};

const PARENT: &str = "stress:parent";
const CHILD: &str = "stress:child";
const CHILDREN: i64 = 8;

async fn connect() -> Result<Client> {
    let (client, connection) =
        tokio_postgres::connect(&std::env::var("DATABASE_URL")?, NoTls).await?;
    tokio::spawn(async move { connection.await.expect("stress connection failed") });
    Ok(client)
}

async fn parent(job: &Job, steps: &mut Steps<'_>) -> Result<Value> {
    let mut children = Vec::new();
    for n in 0..CHILDREN {
        children.push(
            steps
                .spawn(
                    &format!("child-{n}"),
                    CHILD,
                    &json!({"parent": job.id, "n": n}),
                )
                .await?,
        );
    }
    let mut sum = 0;
    for child in children {
        sum += steps
            .wait_for(child)
            .await?
            .as_i64()
            .ok_or("invalid child output")?;
    }
    Ok(json!(sum))
}

async fn child(job: &Job, steps: &mut Steps<'_>) -> Result<Value> {
    let output = steps
        .step("effect", async |tx| {
            tx.execute("insert into stress_effects values ($1)", &[&job.id])
                .await?;
            Ok(job.input["n"].clone())
        })
        .await?;
    if job.failures == 0 {
        return Err("injected failure after committed effect".into());
    }
    Ok(output)
}

async fn worker(children: bool) -> Result<()> {
    let mut client = connect().await?;
    loop {
        let result = if children {
            run_one(&mut client, CHILD, child, |_, _| {
                Retry::After(Duration::ZERO)
            })
            .await
        } else {
            run_one(&mut client, PARENT, parent, |_, _| Retry::Stop).await
        };
        match result {
            Ok(true) => continue,
            Err(error)
                if children && error.to_string() == "injected failure after committed effect" =>
            {
                continue;
            }
            Err(error) => return Err(error),
            Ok(false) => {}
        }
        let unfinished: bool = client.query_one(
            "select exists(select 1 from resume.jobs where workflow in ($1, $2) and not completed)",
            &[&PARENT, &CHILD],
        ).await?.get(0);
        if !unfinished {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[tokio::test]
#[ignore = "requires a disposable database with schema.sql installed"]
async fn concurrent_fanout_retries_and_joins_preserve_every_effect_once() -> Result<()> {
    let parents: i64 = std::env::var("RESUME_STRESS_PARENTS")
        .unwrap_or_else(|_| "32".into())
        .parse()?;
    let workers: usize = std::env::var("RESUME_STRESS_WORKERS")
        .unwrap_or_else(|_| "8".into())
        .parse()?;
    assert!(parents > 0 && workers >= 2);
    let client = connect().await?;
    client
        .batch_execute("create table stress_effects (job_id bigint primary key)")
        .await?;
    for n in 0..parents {
        let key = n.to_string();
        let id = submit(&client, PARENT, &key, &json!(null)).await?;
        assert_eq!(id, submit(&client, PARENT, &key, &json!(null)).await?);
    }
    let started = Instant::now();
    let mut tasks = tokio::task::JoinSet::new();
    for n in 0..workers {
        tasks.spawn(worker(n % 2 == 0));
    }
    tokio::time::timeout(Duration::from_secs(90), async {
        while let Some(result) = tasks.join_next().await {
            result??;
        }
        Ok::<_, resume::Error>(())
    })
    .await??;
    let row = client.query_one("select count(*), bool_and(completed and failures = 0 and output = '28'::jsonb), max(attempt)
        from resume.jobs where workflow = $1", &[&PARENT]).await?;
    assert_eq!(row.get::<_, i64>(0), parents);
    assert!(row.get::<_, bool>(1));
    let max_parent_attempts: i64 = row.get(2);
    let row = client
        .query_one(
            "select count(*), bool_and(completed and failures = 1 and attempt = 2)
        from resume.jobs where workflow = $1",
            &[&CHILD],
        )
        .await?;
    assert_eq!(row.get::<_, i64>(0), parents * CHILDREN);
    assert!(row.get::<_, bool>(1));
    assert_eq!(
        client
            .query_one("select count(*) from stress_effects", &[])
            .await?
            .get::<_, i64>(0),
        parents * CHILDREN
    );
    assert_eq!(client.query_one("select count(*) from resume.steps s join resume.jobs j on j.id = s.job_id where j.workflow in ($1, $2)", &[&PARENT, &CHILD]).await?.get::<_, i64>(0), parents * CHILDREN * 2);
    eprintln!(
        "{} parents, {} children, {} workers, {} injected failures; max parent claims {}; elapsed {:?}",
        parents,
        parents * CHILDREN,
        workers,
        parents * CHILDREN,
        max_parent_attempts,
        started.elapsed()
    );
    Ok(())
}

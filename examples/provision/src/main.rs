mod mock_cloud;

use std::time::{Duration, Instant};

use harness::{Pool, check_history, check_invariants, pending};
use mock_cloud::Cloud;
use resume::{
    Permanent, Producer, Result, RetryPolicy, Run, Worker, lock_resource, shutdown_signal,
};
use serde_json::json;
use tokio_postgres::{Client, NoTls};

/// The version of this workflow's input and steps; producers and workers must agree.
pub const VERSION: &str = "1";

/// VMs each team may have.
const QUOTA: i64 = 3;
const LEASE: Duration = Duration::from_secs(5);
const STEP_TIMEOUT: Duration = Duration::from_secs(3);
const INVARIANTS: &str = include_str!("../invariants.sql");

async fn provision(client: &mut Client, run: &Run, cloud: &mut Cloud, locked: bool) -> Result<()> {
    let team = run.input["team"].as_str().ok_or("team must be a string")?;
    let request_id = &run.idempotency_key;

    // 1. Create a VM unless this request already has one or the team is at its quota. Counting
    //    and creating are separate calls to the cloud, so two requests for the same team could
    //    both see room and both create. The lock makes them take turns until this step
    //    commits, by which time the new VM is visible to the next request's count.
    let vm = run
        .ensure(
            client,
            "ensure_vm",
            cloud,
            async |cloud, tx| {
                if locked {
                    lock_resource(tx, &format!("team:{team}")).await?;
                }
                // Look for this request's VM first, so a retry after a crash finds it instead
                // of counting it against the quota.
                if let Some(vm) = cloud.find_vm(request_id).await? {
                    return Ok(Some(json!(vm)));
                }
                if cloud.count_vms(team).await? >= QUOTA {
                    let error = format!("team {team} is at its quota of {QUOTA} VMs");
                    return Err(Permanent(error).into());
                }
                Ok(None)
            },
            async |cloud, _| Ok(json!(cloud.create_vm(team, request_id).await?)),
        )
        .await?;

    // 2. Record the sandbox in our own database.
    run.step(client, "record_sandbox", async |tx| {
        tx.execute(
            "insert into provision.sandboxes (request_id, team, vm_id) values ($1, $2, $3)",
            &[request_id, &team, &vm.as_i64()],
        )
        .await?;
        Ok(json!(null))
    })
    .await?;

    tracing::info!("sandbox ready on VM {vm}");
    Ok(())
}

/// Submits `requests` requests spread across `teams` teams, then lets `workers` workers race
/// to provision them, crashing now and then after creating a VM. Checks the history and the
/// invariants, and prints how many VMs each team ended up with.
async fn race(
    client: &Client,
    teams: usize,
    requests: usize,
    workers: usize,
    locked: bool,
) -> Result<bool> {
    const LIMIT: Duration = Duration::from_secs(120);

    client
        .batch_execute(
            "truncate provision.sandboxes, cloud.vms restart identity;
             delete from resume.runs where workflow = 'provision';",
        )
        .await?;
    let producer = Producer::new(client, "provision", VERSION).retry(RetryPolicy {
        max_attempts: 8,
        delay: Duration::from_millis(100),
        max_delay: Duration::from_secs(1),
    });
    for i in 0..requests {
        producer
            .submit(
                &format!("r{i}"),
                &json!({"team": format!("t{}", i % teams)}),
            )
            .await?;
    }

    let name = if locked { "locked" } else { "unlocked" };
    let mut pool = Pool::new(&format!("provision-{name}"))?;
    let unlocked = if locked { "0" } else { "1" };
    let start = Instant::now();
    loop {
        // Replace workers that crashed.
        pool.reap();
        while pool.workers.len() < workers {
            let seed = (pool.spawned + 1).to_string();
            pool.spawn(&[
                ("SEED", &seed),
                ("LATENCY_MS", "30"),
                ("CRASH_CHANCE", "0.03"),
                ("UNLOCKED", unlocked),
            ])?;
        }
        if pending(client, "provision").await? == 0 {
            break;
        }
        if start.elapsed() > LIMIT {
            println!("stopped waiting after {LIMIT:?}");
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    pool.stop().await;

    println!(
        "{name}: {requests} requests, {teams} teams, {workers} workers, {:.1}s, {} workers started",
        start.elapsed().as_secs_f64(),
        pool.spawned
    );
    let rows = client
        .query(
            "select team, count(*) from cloud.vms group by team order by team",
            &[],
        )
        .await?;
    let counts: Vec<String> = rows
        .iter()
        .map(|row| {
            Ok(format!(
                "{} {}",
                row.try_get::<_, &str>(0)?,
                row.try_get::<_, i64>(1)?
            ))
        })
        .collect::<Result<_>>()?;
    println!("VMs per team (quota {QUOTA}): {}", counts.join(", "));
    let history = check_history(&pool.logs)?;
    Ok(check_invariants(client, "provision", INVARIANTS).await? && history)
}

async fn connect(database_url: &str) -> Result<Client> {
    let (client, connection) = tokio_postgres::connect(database_url, NoTls).await?;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            tracing::error!("postgres: {error}");
        }
    });
    Ok(client)
}

fn env_or<T: std::str::FromStr>(name: &str, default: T) -> Result<T>
where
    T::Err: std::error::Error + Send + Sync + 'static,
{
    match std::env::var(name) {
        Ok(value) => Ok(value.parse()?),
        Err(_) => Ok(default),
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_target(false)
        .with_writer(std::io::stderr)
        .init();

    let database_url = std::env::var("DATABASE_URL")?;
    let client = connect(&database_url).await?;
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("submit") => {
            let request_id = args.next().ok_or("expected submit <request_id> <team>")?;
            let team = args.next().ok_or("expected submit <request_id> <team>")?;
            let run = Producer::new(&client, "provision", VERSION)
                .submit(&request_id, &json!({"team": team}))
                .await?;
            tracing::info!("submitted run {} for request {request_id}", run.id);
            Ok(())
        }
        Some("work") | None => {
            let mut cloud = Cloud::new(
                connect(&database_url).await?,
                env_or("SEED", 0)?,
                env_or("LATENCY_MS", 0)?,
                env_or("CRASH_CHANCE", 0.0)?,
            );
            // UNLOCKED=1 drops the lock, to show what it prevents.
            let locked = env_or("UNLOCKED", 0)? == 0;
            Worker::new(client, "provision", VERSION)
                .lease(LEASE)
                .step_timeout(STEP_TIMEOUT)
                .run(shutdown_signal(), async |client, run| {
                    provision(client, run, &mut cloud, locked).await
                })
                .await
        }
        Some("race") => {
            let teams = args.next().map_or(Ok(3), |s| s.parse())?;
            let requests = args.next().map_or(Ok(50), |s| s.parse())?;
            let workers = args.next().map_or(Ok(20), |s| s.parse())?;
            let locked = args.next().as_deref() != Some("unlocked");
            if race(&client, teams, requests, workers, locked).await? {
                Ok(())
            } else {
                Err("invariants violated".into())
            }
        }
        Some("check") => {
            if check_invariants(&client, "provision", INVARIANTS).await? {
                Ok(())
            } else {
                Err("invariants violated".into())
            }
        }
        _ => Err(
            "expected submit <request_id> <team>, work, race [teams] [requests] [workers] \
             [locked|unlocked], or check"
                .into(),
        ),
    }
}

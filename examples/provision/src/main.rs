mod mock_cloud;

use std::time::{Duration, Instant};

use harness::{Pool, check_invariants};
use mock_cloud::Cloud;
use resume::{
    Execution, Job, Permanent, Producer, Result, RetryPolicy, Worker, lock_resource,
    shutdown_signal,
};
use serde_json::json;
use tokio_postgres::Client;

pub const VERSION: &str = "1";

/// VMs each team may have.
const QUOTA: i64 = 3;
const LEASE: Duration = Duration::from_secs(5);
const STEP_TIMEOUT: Duration = Duration::from_secs(3);
const INVARIANTS: &str = include_str!("../invariants.sql");

async fn provision(
    run: &Job,
    execution: &mut Execution<'_>,
    cloud: &mut Cloud,
    locked: bool,
) -> Result<()> {
    let team = run.input["team"].as_str().ok_or("team must be a string")?;
    let request_id = &run.idempotency_key;

    // Hold the team lock across the cloud's separate count and create calls, so concurrent
    // requests cannot both see room and exceed the quota.
    let vm = execution
        .step("ensure_vm", async |tx| {
            if locked {
                lock_resource(tx, &format!("team:{team}")).await?;
            }
            // Look for this request's VM first, so a retry after a crash finds it instead
            // of counting it against the quota.
            if let Some(vm) = cloud.find_vm(request_id).await? {
                return Ok(json!(vm));
            }
            if cloud.count_vms(team).await? >= QUOTA {
                let error = format!("team {team} is at its quota of {QUOTA} VMs");
                return Err(Permanent(error).into());
            }
            Ok(json!(cloud.create_vm(team, request_id).await?))
        })
        .await?;

    execution
        .step("record_sandbox", async |tx| {
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

    harness::reset(client, "provision", "provision.sandboxes, cloud.vms").await?;
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
    harness::drive(
        client,
        "provision",
        LIMIT,
        Duration::from_millis(100),
        async || {
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
            Ok(())
        },
    )
    .await?;
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
    harness::verify(client, "provision", INVARIANTS, &pool.logs).await
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let (database_url, client) = harness::start().await?;
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
                harness::connect(&database_url).await?,
                harness::env_or("SEED", 0)?,
                harness::env_or("LATENCY_MS", 0)?,
                harness::env_or("CRASH_CHANCE", 0.0)?,
            );
            // UNLOCKED=1 drops the lock, to show what it prevents.
            let locked = harness::env_or("UNLOCKED", 0)? == 0;
            Worker::new(client, "provision", VERSION)
                .lease(LEASE)
                .step_timeout(STEP_TIMEOUT)
                .run(shutdown_signal(), async |run, execution| {
                    provision(run, execution, &mut cloud, locked).await
                })
                .await
        }
        Some("race") => {
            let teams = args.next().map_or(Ok(3), |s| s.parse())?;
            let requests = args.next().map_or(Ok(50), |s| s.parse())?;
            let workers = args.next().map_or(Ok(20), |s| s.parse())?;
            let locked = args.next().as_deref() != Some("unlocked");
            harness::passed(race(&client, teams, requests, workers, locked).await?)
        }
        Some("check") => harness::passed(check_invariants(&client, "provision", INVARIANTS).await?),
        _ => Err(
            "expected submit <request_id> <team>, work, race [teams] [requests] [workers] \
             [locked|unlocked], or check"
                .into(),
        ),
    }
}

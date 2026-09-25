use std::fs::{self, File};
use std::path::PathBuf;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use resume::{Producer, Result, RetryPolicy};
use serde_json::json;
use tokio_postgres::Client;

use crate::LEASE;
use crate::mock_vendors::{CALLS, KINDS};
use crate::rng::Rng;

const INVARIANTS: &str = include_str!("../invariants.sql");

/// Enough attempts that random faults rarely use them all, with short waits between them.
const RETRY: RetryPolicy = RetryPolicy {
    max_attempts: 8,
    delay: Duration::from_millis(100),
    max_delay: Duration::from_secs(1),
};

/// Onboards one customer per fault point, each fault firing once, and prints how each ended.
/// Then checks the invariants.
pub async fn each(client: &Client) -> Result<bool> {
    reset(client).await?;
    let producer = Producer::new(client, "onboard").retry(RETRY);
    println!(
        "{:<24} {:<6} {:<10} last error",
        "fault", "fired", "outcome"
    );
    for point in points() {
        let email = format!("{}@example.com", point.replace('.', "-"));
        let run = producer
            .submit(&email, &json!({"email": email, "plan": "pro"}))
            .await?;
        let mut pool = Pool::new(&format!("each/{point}"))?;
        pool.spawn(&format!("{point}=once"))?;

        let deadline = Instant::now() + Duration::from_secs(60);
        let (status, error) = loop {
            // A crashed worker is replaced by one without faults.
            if pool.reap() > 0 {
                pool.spawn("")?;
            }
            let row = client
                .query_one(
                    "select case when completed_at is not null then 'completed'
                                 when failed_at is not null then 'failed' end,
                            last_error
                     from resume.runs where id = $1",
                    &[&run.id],
                )
                .await?;
            if let Some(status) = row.try_get::<_, Option<String>>(0)? {
                break (status, row.try_get::<_, Option<String>>(1)?);
            }
            if Instant::now() > deadline {
                break ("pending".to_string(), None);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        pool.stop().await;
        let fired = if pool.faults_fired()?.is_empty() {
            "no"
        } else {
            "yes"
        };
        println!(
            "{point:<24} {fired:<6} {status:<10} {}",
            error.unwrap_or_default()
        );
    }
    check(client).await
}

/// Onboards `customers` under random vendor faults while randomly killing workers, stopping
/// them gracefully, and freezing them past their lease. Then checks the invariants.
pub async fn random(client: &Client, seed: u64, customers: usize, workers: usize) -> Result<bool> {
    const CHAOS_FOR: Duration = Duration::from_secs(20);
    const LIMIT: Duration = Duration::from_secs(120);

    let mut rng = Rng::new(seed);
    reset(client).await?;
    let producer = Producer::new(client, "onboard").retry(RETRY);
    let mut inputs = Vec::new();
    for i in 0..customers {
        let email = if rng.chance(0.05) {
            format!("invalid-{i}")
        } else {
            format!("c{i}@example.com")
        };
        let plan = if rng.chance(0.5) { "pro" } else { "free" };
        let input = json!({"email": email, "plan": plan});
        producer.submit(&email, &input).await?;
        inputs.push((email, input));
    }

    // Each worker gets its own seed, derived from the run's.
    let plan = |n: u64| {
        let mut spec = format!("seed={} latency_ms=150", seed.wrapping_mul(1000) + n);
        for point in points() {
            let chance = if point.ends_with(".error") {
                0.03
            } else {
                0.01
            };
            spec += &format!(" {point}={chance}");
        }
        spec
    };

    let mut pool = Pool::new(&format!("seed-{seed}"))?;
    let (mut kills, mut terms, mut freezes, mut resubmits) = (0, 0, 0, 0);
    let mut duplicate_run = false;
    let start = Instant::now();
    loop {
        pool.reap();
        while pool.workers.len() < workers {
            pool.spawn(&plan(pool.spawned + 1))?;
        }
        let now = Instant::now();
        pool.thaw(now);

        if now - start < CHAOS_FOR {
            let running: Vec<usize> = (0..pool.workers.len())
                .filter(|&i| pool.workers[i].frozen_until.is_none())
                .collect();
            if !running.is_empty() && rng.chance(0.1) {
                let worker = &mut pool.workers[running[rng.below(running.len() as u64) as usize]];
                match rng.below(3) {
                    0 => {
                        signal(&worker.child, "-KILL");
                        kills += 1;
                    }
                    1 => {
                        signal(&worker.child, "-TERM");
                        terms += 1;
                    }
                    _ => {
                        // Past the lease, so another worker can take over its run.
                        signal(&worker.child, "-STOP");
                        let extra = Duration::from_millis(1000 + rng.below(2000));
                        worker.frozen_until = Some(now + LEASE + extra);
                        freezes += 1;
                    }
                }
            }
            if rng.chance(0.05) {
                let (email, input) = &inputs[rng.below(inputs.len() as u64) as usize];
                if producer.submit(email, input).await?.created {
                    println!("VIOLATION: submitting {email} again created a second run");
                    duplicate_run = true;
                }
                resubmits += 1;
            }
        }

        if pending(client).await? == 0 {
            break;
        }
        if now - start > LIMIT {
            println!("stopped waiting after {LIMIT:?}");
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    pool.stop().await;

    println!(
        "seed {seed}: {customers} customers, {workers} workers, {:.1}s",
        start.elapsed().as_secs_f64()
    );
    println!(
        "signals: {kills} SIGKILL, {terms} SIGTERM, {freezes} frozen past the lease; \
         {} workers started; {resubmits} repeated submits",
        pool.spawned
    );
    let fired: Vec<String> = pool
        .faults_fired()?
        .into_iter()
        .map(|(point, n)| format!("{point} x{n}"))
        .collect();
    println!("faults fired: {}", fired.join(", "));
    println!("logs: {}", pool.logs.display());
    Ok(check(client).await? && !duplicate_run)
}

/// Runs the invariants and prints each violation. Returns whether they all hold.
pub async fn check(client: &Client) -> Result<bool> {
    let row = client
        .query_one(
            "select count(*) filter (where completed_at is not null),
                    count(*) filter (where failed_at is not null)
             from resume.runs where workflow = 'onboard'",
            &[],
        )
        .await?;
    let (completed, failed): (i64, i64) = (row.try_get(0)?, row.try_get(1)?);
    println!("runs: {completed} completed, {failed} failed");

    let violations = client
        .query(INVARIANTS.trim_end().trim_end_matches(';'), &[])
        .await?;
    for row in &violations {
        println!("VIOLATION: {}", row.try_get::<_, &str>(0)?);
    }
    if violations.is_empty() {
        println!("invariants: all hold");
    }
    Ok(violations.is_empty())
}

/// Clears the example's data, so the invariants see only this test.
async fn reset(client: &Client) -> Result<()> {
    client
        .batch_execute(
            "truncate onboard.customers, vendors.crm_contacts, vendors.billing_customers,
                 vendors.charges, vendors.emails, vendors.slack_messages restart identity;
             delete from resume.runs where workflow = 'onboard';",
        )
        .await?;
    Ok(())
}

async fn pending(client: &Client) -> Result<i64> {
    Ok(client
        .query_one(
            "select count(*) from resume.runs
             where workflow = 'onboard' and completed_at is null and failed_at is null",
            &[],
        )
        .await?
        .try_get(0)?)
}

fn points() -> impl Iterator<Item = String> {
    CALLS
        .iter()
        .flat_map(|call| KINDS.iter().map(move |kind| format!("{call}.{kind}")))
}

fn signal(child: &Child, signal: &str) {
    let _ = Command::new("kill")
        .args([signal, &child.id().to_string()])
        .status();
}

struct Worker {
    child: Child,
    /// Set while the worker is frozen with SIGSTOP.
    frozen_until: Option<Instant>,
}

/// Worker processes running `onboard work`, each logging to its own file.
struct Pool {
    logs: PathBuf,
    spawned: u64,
    workers: Vec<Worker>,
}

impl Pool {
    fn new(name: &str) -> Result<Self> {
        let logs = PathBuf::from("target/chaos").join(name);
        let _ = fs::remove_dir_all(&logs);
        fs::create_dir_all(&logs)?;
        Ok(Self {
            logs,
            spawned: 0,
            workers: Vec::new(),
        })
    }

    fn spawn(&mut self, faults: &str) -> Result<()> {
        self.spawned += 1;
        let log = File::create(self.logs.join(format!("worker-{}.log", self.spawned)))?;
        let child = Command::new(std::env::current_exe()?)
            .arg("work")
            .env("FAULTS", faults)
            .stdout(log.try_clone()?)
            .stderr(log)
            .spawn()?;
        self.workers.push(Worker {
            child,
            frozen_until: None,
        });
        Ok(())
    }

    /// Drops workers that exited and returns how many.
    fn reap(&mut self) -> usize {
        let before = self.workers.len();
        self.workers
            .retain_mut(|w| !matches!(w.child.try_wait(), Ok(Some(_))));
        before - self.workers.len()
    }

    fn thaw(&mut self, now: Instant) {
        for worker in &mut self.workers {
            if worker.frozen_until.is_some_and(|until| until <= now) {
                signal(&worker.child, "-CONT");
                worker.frozen_until = None;
            }
        }
    }

    /// Thaws and stops every worker, then waits for them to exit.
    async fn stop(&mut self) {
        for worker in &mut self.workers {
            if worker.frozen_until.take().is_some() {
                signal(&worker.child, "-CONT");
            }
            signal(&worker.child, "-TERM");
        }
        let deadline = Instant::now() + Duration::from_secs(15);
        while !self.workers.is_empty() && Instant::now() < deadline {
            self.reap();
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        for worker in &mut self.workers {
            let _ = worker.child.kill();
            let _ = worker.child.wait();
        }
        self.workers.clear();
    }

    /// How many times each fault point fired, from the workers' logs.
    fn faults_fired(&self) -> Result<Vec<(String, usize)>> {
        let mut logs = String::new();
        for entry in fs::read_dir(&self.logs)? {
            logs += &fs::read_to_string(entry?.path())?;
        }
        Ok(points()
            .filter_map(|point| {
                let n = logs.matches(&format!("fault {point}:")).count();
                (n > 0).then_some((point, n))
            })
            .collect())
    }
}

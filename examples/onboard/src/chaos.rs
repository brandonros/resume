use std::collections::HashMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
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

        let start = Instant::now();
        let mut helper = false;
        let (status, error) = loop {
            // A crashed worker is replaced by one without faults, and a frozen one gets a
            // second worker to take over from it.
            if pool.reap() > 0 {
                pool.spawn("")?;
            }
            if point.ends_with(".freeze") && !helper && start.elapsed() > Duration::from_secs(1) {
                pool.spawn("")?;
                helper = true;
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
            if start.elapsed() > Duration::from_secs(60) {
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
    let history = check_history(Path::new("target/chaos/each"))?;
    Ok(check(client).await? && history)
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
        let now = Instant::now();
        pool.reap();
        pool.thaw(now);
        // Like a process manager with liveness probes, replace workers that are frozen, whether
        // by a signal or by a freeze fault.
        pool.notice_frozen(now + LEASE * 4);
        while pool.running() < workers {
            pool.spawn(&plan(pool.spawned + 1))?;
        }

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
                        // Past the lease, so another worker can take over its run. A third of
                        // freezes last long enough that only Postgres's timeouts free the run.
                        signal(&worker.child, "-STOP");
                        let freeze = if rng.chance(1.0 / 3.0) {
                            LEASE * 4
                        } else {
                            LEASE + Duration::from_millis(1000 + rng.below(2000))
                        };
                        worker.frozen_until = Some(now + freeze);
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
    let history = check_history(&pool.logs)?;
    Ok(check(client).await? && history && !duplicate_run)
}

/// Starts `workers` workers, then has `producers` producers, each on its own connection, all
/// submit every one of `customers` at once, in different orders. No faults, only a little
/// vendor latency to widen race windows. Checks that each key created exactly one run, then the
/// history and the invariants.
pub async fn race(
    database_url: &str,
    client: &Client,
    producers: usize,
    workers: usize,
    customers: usize,
) -> Result<bool> {
    const LIMIT: Duration = Duration::from_secs(300);

    reset(client).await?;
    let mut pool = Pool::new(&format!("race-{producers}x{workers}"))?;
    for _ in 0..workers {
        pool.spawn("latency_ms=20")?;
    }

    let start = Instant::now();
    let mut tasks = tokio::task::JoinSet::new();
    for n in 0..producers {
        let database_url = database_url.to_string();
        tasks.spawn(async move {
            let client = crate::connect(&database_url).await?;
            let producer = Producer::new(&client, "onboard").retry(RETRY);
            let mut order: Vec<usize> = (0..customers).collect();
            let mut rng = Rng::new(n as u64);
            for i in (1..order.len()).rev() {
                order.swap(i, rng.below(i as u64 + 1) as usize);
            }
            let mut created = Vec::new();
            for i in order {
                let email = format!("c{i}@example.com");
                let plan = if i % 2 == 0 { "pro" } else { "free" };
                if producer
                    .submit(&email, &json!({"email": email, "plan": plan}))
                    .await?
                    .created
                {
                    created.push(i);
                }
            }
            Ok::<_, resume::Error>(created)
        });
    }
    let mut created = vec![0; customers];
    while let Some(result) = tasks.join_next().await {
        for i in result?? {
            created[i] += 1;
        }
    }
    let mut held = true;
    for (i, n) in created.iter().enumerate() {
        if *n != 1 {
            println!("VIOLATION: c{i}@example.com created {n} runs");
            held = false;
        }
    }
    let submitted = start.elapsed();

    while pending(client).await? > 0 && start.elapsed() < LIMIT {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let elapsed = start.elapsed();
    pool.stop().await;

    println!(
        "{producers} producers x {workers} workers, {customers} customers: {} submits in {:.1}s, \
         all runs done in {:.1}s ({:.0} runs/s)",
        producers * customers,
        submitted.as_secs_f64(),
        elapsed.as_secs_f64(),
        customers as f64 / elapsed.as_secs_f64()
    );
    if held {
        println!("submits: every key created exactly one run");
    }
    let history = check_history(&pool.logs)?;
    Ok(check(client).await? && history && held)
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

/// Checks the workers' logs, across every attempt of every run: no step committed twice, which
/// would mean two attempts both owned the run, and no step_once action started twice.
fn check_history(dir: &Path) -> Result<bool> {
    let mut counts: HashMap<(String, String, String), usize> = HashMap::new();
    for log in log_files(dir)? {
        for line in fs::read_to_string(log)?.lines() {
            if let Some((run, key, message @ ("committed" | "executing once"))) = step_event(line) {
                *counts
                    .entry((run.to_string(), key.to_string(), message.to_string()))
                    .or_default() += 1;
            }
        }
    }
    let mut held = true;
    for ((run, key, message), n) in &counts {
        if *n > 1 {
            println!("VIOLATION: run {run} step {key} logged {message:?} {n} times");
            held = false;
        }
    }
    if held {
        println!("history: {} step events checked, all hold", counts.len());
    }
    Ok(held)
}

/// Parses a log line inside a step into its run ID, step key, and message.
fn step_event(line: &str) -> Option<(&str, &str, &str)> {
    let rest = &line[line.find("run{")?..];
    let run = rest.split(" id=").nth(1)?.split([' ', '}']).next()?;
    let rest = &rest[rest.find(":step{key=\"")? + ":step{key=\"".len()..];
    let (key, message) = rest.split_once("\"}: ")?;
    Some((run, key, message))
}

fn log_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_dir() {
            files.extend(log_files(&path)?);
        } else {
            files.push(path);
        }
    }
    Ok(files)
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

fn is_stopped(child: &Child) -> bool {
    Command::new("ps")
        .args(["-o", "stat=", "-p", &child.id().to_string()])
        .output()
        .is_ok_and(|out| out.stdout.starts_with(b"T"))
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

    fn running(&self) -> usize {
        self.workers
            .iter()
            .filter(|w| w.frozen_until.is_none())
            .count()
    }

    /// Marks workers that stopped themselves as frozen, to thaw at `until`.
    fn notice_frozen(&mut self, until: Instant) {
        for worker in &mut self.workers {
            if worker.frozen_until.is_none() && is_stopped(&worker.child) {
                worker.frozen_until = Some(until);
            }
        }
    }

    fn thaw(&mut self, now: Instant) {
        for worker in &mut self.workers {
            if worker.frozen_until.is_some_and(|until| until <= now) {
                signal(&worker.child, "-CONT");
                worker.frozen_until = None;
            }
        }
    }

    /// Thaws and stops every worker, including ones that froze themselves, then waits for
    /// them to exit.
    async fn stop(&mut self) {
        for worker in &mut self.workers {
            worker.frozen_until = None;
            signal(&worker.child, "-CONT");
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
        for log in log_files(&self.logs)? {
            logs += &fs::read_to_string(log)?;
        }
        Ok(points()
            .filter_map(|point| {
                let n = logs.matches(&format!("fault {point}:")).count();
                (n > 0).then_some((point, n))
            })
            .collect())
    }
}

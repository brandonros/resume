//! Test harness shared by the examples: setup, worker processes to kill, stop and freeze, and
//! checks of the workers' logs and of a workflow's invariants.

mod rng;

use std::collections::HashMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use resume::Result;
use tokio_postgres::{Client, NoTls};

pub use rng::Rng;

/// Starts logging to stderr and returns DATABASE_URL.
pub fn init() -> Result<String> {
    tracing_subscriber::fmt()
        .with_target(false)
        .with_writer(std::io::stderr)
        .init();
    Ok(std::env::var("DATABASE_URL").map_err(|_| "set DATABASE_URL")?)
}

/// Like `init`, and connects to DATABASE_URL. Returns the URL too, for workers and vendors
/// that need connections of their own.
pub async fn start() -> Result<(String, Client)> {
    let url = init()?;
    let client = connect(&url).await?;
    Ok((url, client))
}

pub async fn connect(url: &str) -> Result<Client> {
    let (client, connection) = tokio_postgres::connect(url, NoTls).await?;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            tracing::error!("postgres: {error}");
        }
    });
    Ok(client)
}

/// Parses an environment variable, or returns `default` if it is unset.
pub fn env_or<T: std::str::FromStr>(name: &str, default: T) -> Result<T>
where
    T::Err: std::error::Error + Send + Sync + 'static,
{
    match std::env::var(name) {
        Ok(value) => Ok(value.parse()?),
        Err(_) => Ok(default),
    }
}

/// Turns whether every check held into the process's result.
pub fn passed(held: bool) -> Result<()> {
    if held {
        Ok(())
    } else {
        Err("invariants violated".into())
    }
}

/// Clears an example's tables and its runs, so the invariants see only this test.
pub async fn reset(client: &Client, workflow: &str, tables: &str) -> Result<()> {
    client
        .batch_execute(&format!("truncate {tables} restart identity"))
        .await?;
    client
        .execute("delete from resume.runs where workflow = $1", &[&workflow])
        .await?;
    Ok(())
}

/// Calls `tick` every `interval` until the workflow has no pending runs, or `limit` passes.
pub async fn drive(
    client: &Client,
    workflow: &str,
    limit: Duration,
    interval: Duration,
    mut tick: impl AsyncFnMut() -> Result<()>,
) -> Result<()> {
    let start = Instant::now();
    loop {
        tick().await?;
        if pending(client, workflow).await? == 0 {
            return Ok(());
        }
        if start.elapsed() > limit {
            println!("stopped waiting after {limit:?}");
            return Ok(());
        }
        tokio::time::sleep(interval).await;
    }
}

/// Checks the workers' logs in `logs`, then the workflow's invariants. Returns whether both hold.
pub async fn verify(
    client: &Client,
    workflow: &str,
    invariants: &str,
    logs: &Path,
) -> Result<bool> {
    let history = check_history(logs)?;
    Ok(check_invariants(client, workflow, invariants).await? && history)
}

pub struct Worker {
    pub child: Child,
    /// Set while the worker is frozen with SIGSTOP.
    pub frozen_until: Option<Instant>,
}

/// Worker processes running this executable's `work` command, each logging to its own file
/// under target/chaos.
pub struct Pool {
    pub logs: PathBuf,
    pub spawned: u64,
    pub workers: Vec<Worker>,
}

impl Pool {
    pub fn new(name: &str) -> Result<Self> {
        let logs = PathBuf::from("target/chaos").join(name);
        let _ = fs::remove_dir_all(&logs);
        fs::create_dir_all(&logs)?;
        Ok(Self {
            logs,
            spawned: 0,
            workers: Vec::new(),
        })
    }

    pub fn spawn(&mut self, env: &[(&str, &str)]) -> Result<()> {
        self.spawned += 1;
        let log = File::create(self.logs.join(format!("worker-{}.log", self.spawned)))?;
        let child = Command::new(std::env::current_exe()?)
            .arg("work")
            .envs(env.iter().copied())
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
    pub fn reap(&mut self) -> usize {
        let before = self.workers.len();
        self.workers
            .retain_mut(|w| !matches!(w.child.try_wait(), Ok(Some(_))));
        before - self.workers.len()
    }

    pub fn running(&self) -> usize {
        self.workers
            .iter()
            .filter(|w| w.frozen_until.is_none())
            .count()
    }

    /// Marks workers that stopped themselves as frozen, to thaw at `until`.
    pub fn notice_frozen(&mut self, until: Instant) {
        for worker in &mut self.workers {
            if worker.frozen_until.is_none() && is_stopped(&worker.child) {
                worker.frozen_until = Some(until);
            }
        }
    }

    pub fn thaw(&mut self, now: Instant) {
        for worker in &mut self.workers {
            if worker.frozen_until.is_some_and(|until| until <= now) {
                signal(&worker.child, "-CONT");
                worker.frozen_until = None;
            }
        }
    }

    /// Thaws and stops every worker, including ones that froze themselves, then waits for
    /// them to exit.
    pub async fn stop(&mut self) {
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
}

pub fn signal(child: &Child, signal: &str) {
    let _ = Command::new("kill")
        .args([signal, &child.id().to_string()])
        .status();
}

fn is_stopped(child: &Child) -> bool {
    Command::new("ps")
        .args(["-o", "stat=", "-p", &child.id().to_string()])
        .output()
        .is_ok_and(|out| out.stdout.starts_with(b"T"))
}

pub fn log_files(dir: &Path) -> Result<Vec<PathBuf>> {
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

/// Checks the workers' logs, across every attempt of every run: no step committed twice, which
/// would mean two attempts both owned the run, and no step_once action started twice.
pub fn check_history(dir: &Path) -> Result<bool> {
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

/// Runs a query that returns one row per violated invariant, and prints each. Returns whether
/// they all hold.
pub async fn check_invariants(client: &Client, workflow: &str, invariants: &str) -> Result<bool> {
    let row = client
        .query_one(
            "select count(*) filter (where completed_at is not null),
                    count(*) filter (where failed_at is not null)
             from resume.runs where workflow = $1",
            &[&workflow],
        )
        .await?;
    let (completed, failed): (i64, i64) = (row.try_get(0)?, row.try_get(1)?);
    println!("runs: {completed} completed, {failed} failed");

    let violations = client
        .query(invariants.trim_end().trim_end_matches(';'), &[])
        .await?;
    for row in &violations {
        println!("VIOLATION: {}", row.try_get::<_, &str>(0)?);
    }
    if violations.is_empty() {
        println!("invariants: all hold");
    }
    Ok(violations.is_empty())
}

/// How many of the workflow's runs have neither completed nor failed.
pub async fn pending(client: &Client, workflow: &str) -> Result<i64> {
    Ok(client
        .query_one(
            "select count(*) from resume.runs
             where workflow = $1 and completed_at is null and failed_at is null",
            &[&workflow],
        )
        .await?
        .try_get(0)?)
}

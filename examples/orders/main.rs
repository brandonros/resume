//! Small order application. Install the framework's schema.sql first, then:
//! cargo run --example orders -- init
//! cargo run --example orders -- stock book 10
//! cargo run --example orders -- submit order-1 book 2
//! cargo run --example orders -- drain
//! cargo run --example orders -- status order-1
//! Set DATABASE_URL for every command. `work` runs continuously; `drain` runs until idle.
use std::time::Duration;

use resume::{Error, Job, Result, Retry, Steps, run_one, submit, work};
use serde_json::{Value, json};
use tokio_postgres::{Client, NoTls, error::SqlState};

const ORDERS: &str = "orders-app:order:v1";
const SHIPMENTS: &str = "orders-app:ship:v1";

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
    if transient && job.failures < 3 {
        eprintln!("job {} will retry: {error}", job.id);
        Retry::After(Duration::from_secs(1))
    } else {
        eprintln!("job {} paused: {error:?}", job.id);
        Retry::Stop
    }
}

async fn stock(client: &Client, sku: &str, quantity: i64) -> Result<()> {
    if sku.is_empty() || quantity <= 0 {
        return Err("stock requires a SKU and positive quantity".into());
    }
    client
        .execute(
            "insert into orders_app.inventory values ($1, $2)
        on conflict (sku) do update set available = inventory.available + excluded.available",
            &[&sku, &quantity],
        )
        .await?;
    Ok(())
}

async fn place_order(client: &mut Client, key: &str, sku: &str, quantity: i64) -> Result<i64> {
    if key.is_empty() || sku.is_empty() || quantity <= 0 {
        return Err("order requires a key, SKU and positive quantity".into());
    }
    let tx = client.transaction().await?;
    // Job and business record commit together. Reusing a key with different input is rejected.
    let id = submit(&tx, ORDERS, key, &json!({"sku": sku, "quantity": quantity})).await?;
    tx.execute(
        "insert into orders_app.orders (key, sku, quantity, job_id) values ($1, $2, $3, $4)
        on conflict (key) do nothing",
        &[&key, &sku, &quantity, &id],
    )
    .await?;
    tx.commit().await?;
    Ok(id)
}

async fn fulfill(job: &Job, steps: &mut Steps<'_>) -> Result<Value> {
    let sku = job.input["sku"].as_str().ok_or("missing SKU")?;
    let quantity = job.input["quantity"]
        .as_i64()
        .filter(|q| *q > 0)
        .ok_or("invalid quantity")?;
    let reserved = steps
        .step("reserve", async |tx| {
            // The conditional UPDATE serializes competing reservations without overselling.
            let reserved = tx
                .execute(
                    "update orders_app.inventory set available = available - $2
            where sku = $1 and available >= $2",
                    &[&sku, &quantity],
                )
                .await?
                == 1;
            let state = if reserved { "reserved" } else { "rejected" };
            tx.execute(
                "update orders_app.orders set status = $2 where key = $1",
                &[&job.key, &state],
            )
            .await?;
            Ok(json!(reserved))
        })
        .await?;
    if reserved == json!(false) {
        // Insufficient stock is a business outcome, not an infrastructure retry.
        return Ok(json!({"status": "rejected", "reason": "insufficient stock"}));
    }
    let child = steps
        .spawn("dispatch", SHIPMENTS, &json!({"order": job.key}))
        .await?;
    let receipt = steps.wait_for(child).await?;
    steps
        .step("mark-shipped", async |tx| {
            tx.execute(
                "update orders_app.orders set status = 'shipped' where key = $1",
                &[&job.key],
            )
            .await?;
            Ok(json!(null))
        })
        .await?;
    Ok(json!({"status": "shipped", "receipt": receipt}))
}

async fn dispatch(job: &Job, steps: &mut Steps<'_>) -> Result<Value> {
    let order = job.input["order"].as_str().ok_or("missing order")?;
    steps
        .step("record-dispatch", async |tx| {
            let receipt = format!("dispatch-{}", job.id);
            tx.execute(
                "insert into orders_app.shipments values ($1, $2)",
                &[&order, &receipt],
            )
            .await?;
            Ok(json!(receipt))
        })
        .await
}

async fn order_status(client: &Client, key: &str) -> Result<Value> {
    Ok(client
        .query_one(
            "select jsonb_build_object(
        'order', o.key, 'sku', o.sku, 'quantity', o.quantity, 'status', o.status,
        'job_status', j.status, 'attempts', j.attempt, 'last_error', j.last_error,
        'receipt', s.receipt, 'children', coalesce((
            select jsonb_agg(jsonb_build_object('id', c.id, 'status', c.status,
                'failures', c.failures, 'error', c.last_error) order by c.id)
            from resume.job_status c where c.parent_id = o.job_id
        ), '[]'::jsonb))
        from orders_app.orders o join resume.job_status j on j.id = o.job_id
        left join orders_app.shipments s on s.order_key = o.key where o.key = $1",
            &[&key],
        )
        .await?
        .get(0))
}

// A finite command for local use: queued delays or paused jobs remain for a later run.
async fn drain(client: &mut Client) -> Result<()> {
    loop {
        let order = run_one(client, ORDERS, fulfill, retry).await?;
        let shipment = run_one(client, SHIPMENTS, dispatch, retry).await?;
        if !order && !shipment {
            return Ok(());
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut client = connect().await?;
    match args.iter().map(String::as_str).collect::<Vec<_>>().as_slice() {
        ["init"] => client.batch_execute(include_str!("schema.sql")).await?,
        ["stock", sku, quantity] => stock(&client, sku, quantity.parse()?).await?,
        ["submit", key, sku, quantity] => println!("job {}", place_order(&mut client, key, sku, quantity.parse()?).await?),
        ["status", key] => println!("{}", serde_json::to_string_pretty(&order_status(&client, key).await?)?),
        ["drain"] => drain(&mut client).await?,
        ["work"] => {
            let mut shipments = connect().await?;
            tokio::try_join!(work(&mut client, ORDERS, fulfill, retry), work(&mut shipments, SHIPMENTS, dispatch, retry))?;
        }
        _ => return Err("usage: orders init | stock SKU QUANTITY | submit KEY SKU QUANTITY | status KEY | drain | work".into()),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore = "requires a disposable database with schema.sql installed"]
    async fn orders_are_atomic_idempotent_and_cannot_oversell() -> Result<()> {
        let mut a = connect().await?;
        let mut b = connect().await?;
        a.batch_execute(include_str!("schema.sql")).await?;
        stock(&a, "book", 3).await?;
        let first = place_order(&mut a, "first", "book", 2).await?;
        assert_eq!(first, place_order(&mut a, "first", "book", 2).await?);
        assert!(place_order(&mut a, "first", "book", 1).await.is_err());
        place_order(&mut a, "second", "book", 2).await?;
        assert!(place_order(&mut a, "invalid", "book", 0).await.is_err());
        // Two workers compete for three units; only one two-unit order can reserve.
        let (one, two) = tokio::join!(
            run_one(&mut a, ORDERS, fulfill, retry),
            run_one(&mut b, ORDERS, fulfill, retry)
        );
        assert!(one? && two?);
        drain(&mut a).await?;
        let one = order_status(&a, "first").await?;
        let two = order_status(&a, "second").await?;
        let mut states = [
            one["status"].as_str().unwrap(),
            two["status"].as_str().unwrap(),
        ];
        states.sort();
        assert_eq!(states, ["rejected", "shipped"]);
        assert_eq!(one["job_status"], "completed");
        assert_eq!(two["job_status"], "completed");
        assert_eq!(
            a.query_one(
                "select available from orders_app.inventory where sku = 'book'",
                &[]
            )
            .await?
            .get::<_, i64>(0),
            1
        );
        assert_eq!(
            a.query_one("select count(*) from orders_app.shipments", &[])
                .await?
                .get::<_, i64>(0),
            1
        );
        // Duplicate submission after completion cannot reserve or dispatch again.
        assert_eq!(first, place_order(&mut a, "first", "book", 2).await?);
        drain(&mut a).await?;
        assert_eq!(
            a.query_one(
                "select available from orders_app.inventory where sku = 'book'",
                &[]
            )
            .await?
            .get::<_, i64>(0),
            1
        );
        assert_eq!(
            a.query_one("select count(*) from orders_app.shipments", &[])
                .await?
                .get::<_, i64>(0),
            1
        );
        // Repeat the inventory boundary under contention, with independent worker clients.
        stock(&a, "stress-item", 65).await?;
        for n in 0..64 {
            place_order(&mut a, &format!("stress-order-{n}"), "stress-item", 2).await?;
        }
        let mut workers = tokio::task::JoinSet::new();
        for _ in 0..8 {
            workers.spawn(async {
                let mut client = connect().await?;
                drain(&mut client).await
            });
        }
        tokio::time::timeout(Duration::from_secs(30), async {
            while let Some(result) = workers.join_next().await {
                result??;
            }
            Ok::<_, Error>(())
        })
        .await??;
        let row = a.query_one("select count(*) filter (where status = 'shipped'),
            count(*) filter (where status = 'rejected') from orders_app.orders where sku = 'stress-item'", &[]).await?;
        assert_eq!(row.get::<_, i64>(0), 32);
        assert_eq!(row.get::<_, i64>(1), 32);
        assert_eq!(
            a.query_one(
                "select available from orders_app.inventory where sku = 'stress-item'",
                &[]
            )
            .await?
            .get::<_, i64>(0),
            1
        );
        assert_eq!(a.query_one("select count(*) from orders_app.shipments s join orders_app.orders o on o.key = s.order_key where o.sku = 'stress-item'", &[]).await?.get::<_, i64>(0), 32);
        Ok(())
    }
}

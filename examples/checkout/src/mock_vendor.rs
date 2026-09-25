use resume::{Permanent, Result};
use tokio_postgres::Client;

pub struct MockVendor {
    client: Client,
}

impl MockVendor {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    pub async fn apply(&mut self, order: &str, kind: &str, action: &str) -> Result<()> {
        let tx = self.client.transaction().await?;
        let undone: bool = tx
            .query_one(
                "insert into checkout.effects (order_key, kind, undone) values ($1, $2, false)
             on conflict (order_key, kind) do update set kind = excluded.kind returning undone",
                &[&order, &kind],
            )
            .await?
            .try_get(0)?;
        if undone {
            return Err(Permanent(format!("{order}: {kind} has already been undone")).into());
        }
        tx.execute(
            "insert into checkout.events (order_key, action) values ($1, $2) on conflict do nothing",
            &[&order, &action],
        ).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn undo(&mut self, order: &str, kind: &str, action: &str) -> Result<()> {
        let tx = self.client.transaction().await?;
        tx.execute(
            "insert into checkout.effects (order_key, kind, undone) values ($1, $2, true)
             on conflict (order_key, kind) do update set undone = true",
            &[&order, &kind],
        )
        .await?;
        tx.execute(
            "insert into checkout.events (order_key, action) values ($1, $2) on conflict do nothing",
            &[&order, &action],
        ).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn first_refund_crash(&self, order: &str) -> Result<bool> {
        Ok(self
            .client
            .execute(
                "insert into checkout.events (order_key, action) values ($1, 'crash_refund')
             on conflict do nothing",
                &[&order],
            )
            .await?
            == 1)
    }
}

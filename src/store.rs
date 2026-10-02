use async_trait::async_trait;
use mongodb::bson::{Bson, DateTime, Document, doc, oid::ObjectId};
use mongodb::{Client, Collection, IndexModel};

use crate::domain::Lead;

#[async_trait]
pub trait Store: Send + Sync {
    async fn set_running(&self, job_id: &str) -> Result<(), String>;
    async fn append_leads(&self, job_id: &str, leads: &[Lead], total: usize) -> Result<(), String>;
    async fn set_progress(
        &self,
        job_id: &str,
        tiles_done: i64,
        tiles_total: i64,
    ) -> Result<(), String>;
    async fn set_done(&self, job_id: &str, total: usize) -> Result<(), String>;
    async fn set_error(&self, job_id: &str, msg: &str) -> Result<(), String>;
    async fn set_cancelled(&self, job_id: &str, total: usize) -> Result<(), String>;
}

pub struct Mongo {
    client: Client,
    sessions: Collection<Document>,
    leads: Collection<Document>,
}

impl Mongo {
    pub async fn connect(uri: &str, db: &str) -> Result<Self, String> {
        let client = Client::with_uri_str(uri).await.map_err(|e| e.to_string())?;
        let database = client.database(db);
        database
            .run_command(doc! { "ping": 1 })
            .await
            .map_err(|e| e.to_string())?;

        let mongo = Mongo {
            sessions: database.collection("scrapesessions"),
            leads: database.collection("scrapeleads"),
            client,
        };
        mongo
            .leads
            .create_index(
                IndexModel::builder()
                    .keys(doc! { "jobId": 1, "seq": 1 })
                    .build(),
            )
            .await
            .map_err(|e| e.to_string())?;
        Ok(mongo)
    }

    pub async fn close(&self) {
        self.client.clone().shutdown().await;
    }

    async fn update(&self, job_id: &str, update: Document) -> Result<(), String> {
        let oid = ObjectId::parse_str(job_id).map_err(|e| e.to_string())?;
        let res = self
            .sessions
            .update_one(doc! { "_id": oid }, update)
            .await
            .map_err(|e| e.to_string())?;
        if res.matched_count == 0 {
            return Err(format!("job não encontrado: {job_id}"));
        }
        Ok(())
    }

    async fn set(&self, job_id: &str, fields: Document) -> Result<(), String> {
        self.update(
            job_id,
            doc! { "$set": fields, "$currentDate": { "updatedAt": true } },
        )
        .await
    }
}

pub fn int(n: i64) -> Bson {
    match i32::try_from(n) {
        Ok(small) => Bson::Int32(small),
        Err(_) => Bson::Int64(n),
    }
}

#[async_trait]
impl Store for Mongo {
    async fn set_running(&self, job_id: &str) -> Result<(), String> {
        self.set(job_id, doc! { "status": "running" }).await
    }

    async fn append_leads(&self, job_id: &str, leads: &[Lead], total: usize) -> Result<(), String> {
        if leads.is_empty() {
            return Ok(());
        }
        let oid = ObjectId::parse_str(job_id).map_err(|e| e.to_string())?;
        let first_seq = total as i64 - leads.len() as i64;
        let now = DateTime::now();

        let docs = leads
            .iter()
            .enumerate()
            .map(|(i, lead)| {
                Ok(doc! {
                    "jobId": oid,
                    "seq": int(first_seq + i as i64),
                    "createdAt": now,
                    "lead": mongodb::bson::to_bson(lead).map_err(|e| e.to_string())?,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;

        self.leads
            .insert_many(docs)
            .await
            .map_err(|e| e.to_string())?;
        self.set(
            job_id,
            doc! { "leadCount": int(total as i64), "status": "running" },
        )
        .await
    }

    async fn set_progress(
        &self,
        job_id: &str,
        tiles_done: i64,
        tiles_total: i64,
    ) -> Result<(), String> {
        self.set(
            job_id,
            doc! { "tilesDone": int(tiles_done), "tilesTotal": int(tiles_total) },
        )
        .await
    }

    async fn set_done(&self, job_id: &str, total: usize) -> Result<(), String> {
        self.set(
            job_id,
            doc! { "status": "done", "leadCount": int(total as i64) },
        )
        .await
    }

    async fn set_error(&self, job_id: &str, msg: &str) -> Result<(), String> {
        self.set(job_id, doc! { "status": "error", "error": msg })
            .await
    }

    async fn set_cancelled(&self, job_id: &str, total: usize) -> Result<(), String> {
        self.set(
            job_id,
            doc! { "status": "cancelled", "leadCount": int(total as i64) },
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn int_uses_smallest_bson_type_like_go_driver() {
        assert_eq!(int(42), Bson::Int32(42));
        assert_eq!(
            int(i64::from(i32::MAX) + 1),
            Bson::Int64(i64::from(i32::MAX) + 1)
        );
    }
}

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::browser::{Pool, Tab};
use crate::domain::{Event, EventKind, Job, Lead};
use crate::geocode::{GeocodeError, Geocoder};
use crate::navigation;
use crate::store::Store;
use crate::tiling::{self, ScrapeFn};

const DEFAULT_JOB_TIMEOUT: Duration = Duration::from_secs(30 * 60);

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Task {
    pub id: String,
    pub job: Job,
    pub location: String,
    pub target_leads: i64,
    pub cell_km: f64,
}

impl Task {
    fn tiled(&self) -> bool {
        !self.location.is_empty()
    }
}

pub struct Queue {
    tx: mpsc::Sender<Task>,
    inner: Arc<Inner>,
}

struct Inner {
    pool: Option<Arc<Pool>>,
    store: Arc<dyn Store>,
    geocoder: Option<Arc<dyn Geocoder>>,
    timeout: Duration,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    running: HashMap<String, CancellationToken>,
    cancelled: HashSet<String>,
}

impl Queue {
    pub fn new(
        pool: Arc<Pool>,
        store: Arc<dyn Store>,
        geocoder: Arc<dyn Geocoder>,
        workers: usize,
        buffer: usize,
    ) -> Self {
        let (tx, rx) = mpsc::channel(buffer.max(1));
        let inner = Arc::new(Inner {
            pool: Some(pool),
            store,
            geocoder: Some(geocoder),
            timeout: timeout_from_env(),
            state: Mutex::new(State::default()),
        });

        let rx = Arc::new(tokio::sync::Mutex::new(rx));
        for _ in 0..workers.max(1) {
            let inner = inner.clone();
            let rx = rx.clone();
            tokio::spawn(async move {
                loop {
                    let task = rx.lock().await.recv().await;
                    match task {
                        Some(task) => inner.process(task).await,
                        None => break,
                    }
                }
            });
        }

        Queue { tx, inner }
    }

    pub fn enqueue(&self, task: Task) -> bool {
        self.tx.try_send(task).is_ok()
    }

    pub fn cancel(&self, job_id: &str) -> bool {
        self.inner.cancel(job_id)
    }

    #[cfg(test)]
    pub(crate) fn detached(store: Arc<dyn Store>, buffer: usize) -> (Self, mpsc::Receiver<Task>) {
        let (tx, rx) = mpsc::channel(buffer.max(1));
        let inner = Arc::new(Inner {
            pool: None,
            store,
            geocoder: None,
            timeout: DEFAULT_JOB_TIMEOUT,
            state: Mutex::new(State::default()),
        });
        (Queue { tx, inner }, rx)
    }
}

fn timeout_from_env() -> Duration {
    std::env::var("JOB_TIMEOUT_MINUTES")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|&m| m > 0)
        .map(|m| Duration::from_secs(m * 60))
        .unwrap_or(DEFAULT_JOB_TIMEOUT)
}

impl Inner {
    fn cancel(&self, job_id: &str) -> bool {
        let mut state = self.state.lock().unwrap();
        state.cancelled.insert(job_id.to_string());
        match state.running.get(job_id) {
            Some(token) => {
                token.cancel();
                true
            }
            None => false,
        }
    }

    fn track(&self, job_id: &str, token: CancellationToken) -> bool {
        let mut state = self.state.lock().unwrap();
        if state.cancelled.contains(job_id) {
            return false;
        }
        state.running.insert(job_id.to_string(), token);
        true
    }

    fn untrack(&self, job_id: &str) {
        let mut state = self.state.lock().unwrap();
        state.running.remove(job_id);
        state.cancelled.remove(job_id);
    }

    fn was_cancelled(&self, job_id: &str) -> bool {
        self.state.lock().unwrap().cancelled.contains(job_id)
    }

    async fn mark_cancelled(&self, job_id: &str, total: usize) {
        if let Err(e) = self.store.set_cancelled(job_id, total).await {
            tracing::warn!("[job {job_id}] set cancelled: {e}");
        }
    }

    async fn fail(&self, job_id: &str, msg: &str) {
        if let Err(e) = self.store.set_error(job_id, msg).await {
            tracing::warn!("[job {job_id}] set error: {e}");
        }
    }

    async fn process(&self, task: Task) {
        let token = CancellationToken::new();
        if !self.track(&task.id, token.clone()) {
            self.mark_cancelled(&task.id, 0).await;
            return;
        }

        let deadline = {
            let token = token.clone();
            let timeout = self.timeout;
            tokio::spawn(async move {
                tokio::time::sleep(timeout).await;
                token.cancel();
            })
        };

        self.execute(&task, &token).await;

        deadline.abort();
        token.cancel();
        self.untrack(&task.id);
    }

    async fn execute(&self, task: &Task, token: &CancellationToken) {
        let (mut events, _tab) = match self.events(task, token).await {
            Ok(pair) => pair,
            Err(msg) => {
                self.fail(&task.id, &msg).await;
                return;
            }
        };

        if let Err(e) = self.store.set_running(&task.id).await {
            tracing::warn!("[job {}] set running: {e}", task.id);
        }

        let mut batch: Vec<Lead> = Vec::new();
        let mut total = 0usize;
        let mut terminal = false;

        while let Some(ev) = events.recv().await {
            match ev.kind {
                EventKind::Lead => {
                    if let Some(lead) = ev.data {
                        batch.push(lead);
                        total += 1;
                    }
                }
                EventKind::Progress => {
                    self.flush(&task.id, &mut batch, total).await;
                    if let (Some(tile), Some(tiles)) = (ev.tile, ev.tiles)
                        && let Err(e) = self.store.set_progress(&task.id, tile, tiles).await
                    {
                        tracing::warn!("[job {}] set progress: {e}", task.id);
                    }
                }
                EventKind::Done => {
                    self.flush(&task.id, &mut batch, total).await;
                    terminal = true;
                    if let Err(e) = self.store.set_done(&task.id, total).await {
                        tracing::warn!("[job {}] set done: {e}", task.id);
                    }
                }
                EventKind::Error => {
                    self.flush(&task.id, &mut batch, total).await;
                    terminal = true;
                    if let Err(e) = self.store.set_error(&task.id, &ev.message).await {
                        tracing::warn!("[job {}] set error: {e}", task.id);
                    }
                }
            }
        }

        if terminal {
            return;
        }
        if self.was_cancelled(&task.id) {
            self.mark_cancelled(&task.id, total).await;
            return;
        }
        self.fail(&task.id, "scrape interrompido (timeout ou cancelamento)")
            .await;
    }

    async fn flush(&self, job_id: &str, batch: &mut Vec<Lead>, total: usize) {
        if batch.is_empty() {
            return;
        }
        if let Err(e) = self.store.append_leads(job_id, batch, total).await {
            tracing::warn!("[job {job_id}] append leads: {e}");
        }
        batch.clear();
    }

    async fn events(
        &self,
        task: &Task,
        token: &CancellationToken,
    ) -> Result<(mpsc::Receiver<Event>, Tab), String> {
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| "browser indisponível: pool não configurado".to_string())?;
        let tab = pool
            .tab(token)
            .await
            .map_err(|e| format!("browser indisponível: {e}"))?;

        if !task.tiled() {
            let rx = navigation::scrape(tab.page().clone(), task.job.clone(), tab.token());
            return Ok((rx, tab));
        }

        let geocoder = self
            .geocoder
            .as_ref()
            .ok_or_else(|| "geocoder indisponível".to_string())?;

        let lookup = tokio::select! {
            r = geocoder.lookup(&task.location) => r,
            _ = token.cancelled() => Err(GeocodeError::Other("context canceled".into())),
        };
        let bbox =
            lookup.map_err(|e| format!("não foi possível localizar \"{}\": {e}", task.location))?;

        let request = tiling::Request {
            query: task.job.query.clone(),
            bbox,
            cell_km: task.cell_km,
            target_leads: task.target_leads.max(0) as usize,
            pages_per_tile: task.job.max_pages,
        };
        let page = tab.page().clone();
        let scrape: ScrapeFn =
            Arc::new(move |job, tile_token| navigation::scrape(page.clone(), job, tile_token));
        let rx = tiling::run(tab.token(), request, scrape);
        Ok((rx, tab))
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;

    use super::*;

    #[derive(Default)]
    pub(crate) struct FakeStore {
        pub cancelled: AtomicUsize,
        pub failed: AtomicUsize,
        pub last_total: AtomicUsize,
        pub last_error: Mutex<String>,
    }

    #[async_trait]
    impl Store for FakeStore {
        async fn set_running(&self, _: &str) -> Result<(), String> {
            Ok(())
        }
        async fn append_leads(&self, _: &str, _: &[Lead], _: usize) -> Result<(), String> {
            Ok(())
        }
        async fn set_progress(&self, _: &str, _: i64, _: i64) -> Result<(), String> {
            Ok(())
        }
        async fn set_done(&self, _: &str, _: usize) -> Result<(), String> {
            Ok(())
        }
        async fn set_error(&self, _: &str, msg: &str) -> Result<(), String> {
            self.failed.fetch_add(1, Ordering::SeqCst);
            *self.last_error.lock().unwrap() = msg.to_string();
            Ok(())
        }
        async fn set_cancelled(&self, _: &str, total: usize) -> Result<(), String> {
            self.cancelled.fetch_add(1, Ordering::SeqCst);
            self.last_total.store(total, Ordering::SeqCst);
            Ok(())
        }
    }

    fn queue() -> (Queue, Arc<FakeStore>) {
        let store = Arc::new(FakeStore::default());
        let (q, rx) = Queue::detached(store.clone(), 1);
        std::mem::forget(rx);
        (q, store)
    }

    #[test]
    fn enqueue_full_returns_false() {
        let (q, _) = queue();
        assert!(q.enqueue(Task {
            id: "a".into(),
            ..Task::default()
        }));
        assert!(!q.enqueue(Task {
            id: "b".into(),
            ..Task::default()
        }));
    }

    #[test]
    fn task_routes_by_location() {
        let area = Task {
            id: "a".into(),
            location: "São Paulo".into(),
            target_leads: 100,
            ..Task::default()
        };
        assert!(area.tiled());

        let point = Task {
            id: "b".into(),
            job: Job {
                latitude: Some(-23.5),
                longitude: Some(-46.6),
                ..Job::default()
            },
            ..Task::default()
        };
        assert!(!point.tiled());
    }

    #[test]
    fn cancel_running_job_cancels_token() {
        let (q, _) = queue();
        let token = CancellationToken::new();
        assert!(q.inner.track("job-1", token.clone()));
        assert!(q.cancel("job-1"));
        assert!(token.is_cancelled());
        assert!(q.inner.was_cancelled("job-1"));
    }

    #[test]
    fn cancel_before_start_prevents_execution() {
        let (q, _) = queue();
        assert!(!q.cancel("job-fila"));
        assert!(!q.inner.track("job-fila", CancellationToken::new()));
    }

    #[test]
    fn cancel_unknown_job_is_safe() {
        let (q, _) = queue();
        assert!(!q.cancel("inexistente"));
    }

    #[test]
    fn untrack_clears_state() {
        let (q, _) = queue();
        q.inner.track("job-2", CancellationToken::new());
        q.cancel("job-2");
        q.inner.untrack("job-2");
        assert!(!q.inner.was_cancelled("job-2"));
        assert!(q.inner.track("job-2", CancellationToken::new()));
    }

    #[tokio::test]
    async fn mark_cancelled_persists_partial_total() {
        let (q, store) = queue();
        q.inner.mark_cancelled("job-3", 42).await;
        assert_eq!(store.cancelled.load(Ordering::SeqCst), 1);
        assert_eq!(store.last_total.load(Ordering::SeqCst), 42);
        assert_eq!(store.failed.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn process_without_browser_fails_the_job() {
        let (q, store) = queue();
        q.inner
            .process(Task {
                id: "job-4".into(),
                ..Task::default()
            })
            .await;
        assert_eq!(store.failed.load(Ordering::SeqCst), 1);
        assert!(
            store
                .last_error
                .lock()
                .unwrap()
                .starts_with("browser indisponível")
        );
        assert!(!q.inner.was_cancelled("job-4"));
    }

    #[tokio::test]
    async fn process_of_cancelled_task_marks_cancelled_without_running() {
        let (q, store) = queue();
        q.cancel("job-5");
        q.inner
            .process(Task {
                id: "job-5".into(),
                ..Task::default()
            })
            .await;
        assert_eq!(store.cancelled.load(Ordering::SeqCst), 1);
        assert_eq!(store.last_total.load(Ordering::SeqCst), 0);
        assert_eq!(store.failed.load(Ordering::SeqCst), 0);
    }
}

use std::convert::Infallible;
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, Query, Request, State};
use axum::http::{HeaderValue, Method, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::sse::{Event as SseEvent, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;
use tokio_util::sync::CancellationToken;

use crate::browser::Pool;
use crate::domain::Job;
use crate::jobs::{Queue, Task};
use crate::navigation;

const DEFAULT_MAX_PAGES: i64 = 10;
const MAX_PAGES_CAP: i64 = 50;

#[derive(Clone)]
pub struct AppState {
    pub pool: Arc<Pool>,
    pub queue: Option<Arc<Queue>>,
}

pub fn router(state: AppState, allowed_origin: &str) -> Router {
    let origin =
        HeaderValue::from_str(allowed_origin).unwrap_or_else(|_| HeaderValue::from_static("*"));
    Router::new()
        .route("/health", get(health))
        .route("/scrape", get(scrape_stream).post(scrape_job))
        .route("/scrape/{job_id}", delete(cancel_job))
        .with_state(state)
        .layer(middleware::from_fn_with_state(origin, cors))
}

async fn cors(State(origin): State<HeaderValue>, req: Request, next: Next) -> Response {
    let preflight = req.method() == Method::OPTIONS;
    let mut res = if preflight {
        StatusCode::NO_CONTENT.into_response()
    } else {
        next.run(req).await
    };
    let headers = res.headers_mut();
    headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, origin);
    headers.insert(header::VARY, HeaderValue::from_static("Origin"));
    if preflight {
        headers.insert(
            header::ACCESS_CONTROL_ALLOW_METHODS,
            HeaderValue::from_static("GET, POST, DELETE, OPTIONS"),
        );
    }
    res
}

fn error(status: StatusCode, message: impl Into<String>) -> Response {
    (status, Json(json!({ "error": message.into() }))).into_response()
}

async fn health() -> Json<serde_json::Value> {
    Json(json!({ "ok": true }))
}

fn parse_job(params: &[(String, String)]) -> Result<Job, String> {
    let get = |key: &str| {
        params
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
            .unwrap_or("")
    };
    let opt_float = |key: &str| {
        get(key)
            .parse::<f64>()
            .ok()
            .filter(|_| !get(key).is_empty())
    };

    let query = get("q");
    if query.is_empty() {
        return Err(r#"query param "q" is required"#.into());
    }

    let mut max_pages = DEFAULT_MAX_PAGES;
    let raw = get("maxPages");
    if !raw.is_empty() {
        match raw.parse::<i64>() {
            Ok(n) if n > 0 => max_pages = n.min(MAX_PAGES_CAP),
            _ => return Err(r#""maxPages" deve ser um inteiro positivo"#.into()),
        }
    }

    Ok(Job {
        query: query.to_string(),
        latitude: opt_float("lat"),
        longitude: opt_float("lng"),
        zoom: opt_float("zoom"),
        max_pages: max_pages as usize,
    })
}

async fn scrape_stream(
    State(state): State<AppState>,
    Query(params): Query<Vec<(String, String)>>,
) -> Response {
    let job = match parse_job(&params) {
        Ok(job) => job,
        Err(msg) => return error(StatusCode::BAD_REQUEST, msg),
    };

    let tab = match state.pool.tab(&CancellationToken::new()).await {
        Ok(tab) => tab,
        Err(e) => {
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("browser indisponível: {e}"),
            );
        }
    };

    let events = navigation::scrape(tab.page().clone(), job, tab.token());
    let stream = futures::stream::unfold((events, tab), |(mut events, tab)| async move {
        let ev = events.recv().await?;
        let data = serde_json::to_string(&ev).unwrap_or_default();
        Some((
            Ok::<_, Infallible>(SseEvent::default().data(data)),
            (events, tab),
        ))
    });

    ([("x-accel-buffering", "no")], Sse::new(stream)).into_response()
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
struct ScrapeJobRequest {
    job_id: String,
    query: String,
    lat: Option<f64>,
    lng: Option<f64>,
    zoom: Option<f64>,
    max_pages: i64,
    location: String,
    target_leads: i64,
    cell_km: f64,
}

fn unavailable() -> Response {
    error(
        StatusCode::SERVICE_UNAVAILABLE,
        "fila assíncrona indisponível (sem MONGODB_URI)",
    )
}

async fn scrape_job(State(state): State<AppState>, body: Bytes) -> Response {
    let Some(queue) = state.queue else {
        return unavailable();
    };

    let req: ScrapeJobRequest = match serde_json::from_slice(&body) {
        Ok(req) => req,
        Err(_) => return error(StatusCode::BAD_REQUEST, "corpo JSON inválido"),
    };
    if req.job_id.is_empty() || req.query.is_empty() {
        return error(StatusCode::BAD_REQUEST, "jobId e query são obrigatórios");
    }

    let max_pages = if req.max_pages <= 0 {
        DEFAULT_MAX_PAGES
    } else {
        req.max_pages.min(MAX_PAGES_CAP)
    };

    let task = Task {
        id: req.job_id.clone(),
        job: Job {
            query: req.query,
            latitude: req.lat,
            longitude: req.lng,
            zoom: req.zoom,
            max_pages: max_pages as usize,
        },
        location: req.location.trim().to_string(),
        target_leads: req.target_leads,
        cell_km: req.cell_km,
    };

    if !queue.enqueue(task) {
        return error(StatusCode::SERVICE_UNAVAILABLE, "fila cheia");
    }

    (
        StatusCode::ACCEPTED,
        Json(json!({ "jobId": req.job_id, "status": "queued" })),
    )
        .into_response()
}

async fn cancel_job(State(state): State<AppState>, Path(job_id): Path<String>) -> Response {
    let Some(queue) = state.queue else {
        return unavailable();
    };
    if job_id.is_empty() {
        return error(StatusCode::BAD_REQUEST, "jobId é obrigatório");
    }
    let running = queue.cancel(&job_id);
    Json(json!({ "jobId": job_id, "running": running })).into_response()
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    use super::*;
    use crate::jobs::tests::FakeStore;

    fn app(queue: Option<Queue>) -> Router {
        router(
            AppState {
                pool: Arc::new(Pool::new(1)),
                queue: queue.map(Arc::new),
            },
            "http://localhost:3000",
        )
    }

    fn queue(buffer: usize) -> Queue {
        let (q, rx) = Queue::detached(Arc::new(FakeStore::default()), buffer);
        std::mem::forget(rx);
        q
    }

    async fn call(
        app: Router,
        method: &str,
        uri: &str,
        body: &str,
    ) -> (StatusCode, String, axum::http::HeaderMap) {
        let req = axum::http::Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();
        let res = app.oneshot(req).await.unwrap();
        let status = res.status();
        let headers = res.headers().clone();
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        (status, String::from_utf8(bytes.to_vec()).unwrap(), headers)
    }

    #[tokio::test]
    async fn health() {
        let (status, body, headers) = call(app(None), "GET", "/health", "").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, r#"{"ok":true}"#);
        assert_eq!(
            headers["access-control-allow-origin"],
            "http://localhost:3000"
        );
        assert_eq!(headers["vary"], "Origin");
    }

    #[tokio::test]
    async fn preflight_on_any_path() {
        for uri in ["/scrape", "/scrape/abc", "/qualquer"] {
            let (status, _, headers) = call(app(None), "OPTIONS", uri, "").await;
            assert_eq!(status, StatusCode::NO_CONTENT, "{uri}");
            assert_eq!(
                headers["access-control-allow-methods"],
                "GET, POST, DELETE, OPTIONS"
            );
        }
    }

    #[tokio::test]
    async fn stream_requires_q() {
        let (status, body, _) = call(app(None), "GET", "/scrape", "").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body, r#"{"error":"query param \"q\" is required"}"#);
    }

    #[tokio::test]
    async fn stream_rejects_bad_max_pages() {
        for bad in ["0", "-3", "abc", "2.5"] {
            let (status, body, _) =
                call(app(None), "GET", &format!("/scrape?q=x&maxPages={bad}"), "").await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "maxPages={bad}");
            assert!(body.contains("maxPages"));
        }
    }

    #[test]
    fn parse_job_defaults_and_caps() {
        let p = |pairs: &[(&str, &str)]| {
            parse_job(
                &pairs
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect::<Vec<_>>(),
            )
        };
        let job = p(&[("q", "x")]).unwrap();
        assert_eq!((job.max_pages, job.latitude), (10, None));

        let job = p(&[
            ("q", "x"),
            ("maxPages", "999"),
            ("lat", "-23.5"),
            ("lng", "nope"),
        ])
        .unwrap();
        assert_eq!(job.max_pages, 50);
        assert_eq!(job.latitude, Some(-23.5));
        assert_eq!(job.longitude, None);

        let first_wins = p(&[("q", "primeiro"), ("q", "segundo")]).unwrap();
        assert_eq!(first_wins.query, "primeiro");
    }

    #[tokio::test]
    async fn post_without_mongo_is_unavailable() {
        let (status, body, _) =
            call(app(None), "POST", "/scrape", r#"{"jobId":"a","query":"b"}"#).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(body.contains("MONGODB_URI"));
    }

    #[tokio::test]
    async fn post_validates_body() {
        let (status, body, _) = call(app(Some(queue(4))), "POST", "/scrape", "{nao json").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body, r#"{"error":"corpo JSON inválido"}"#);

        let (status, body, _) =
            call(app(Some(queue(4))), "POST", "/scrape", r#"{"query":"x"}"#).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body, r#"{"error":"jobId e query são obrigatórios"}"#);
    }

    #[tokio::test]
    async fn post_accepts_the_orbita_payload() {
        let payload = r#"{"jobId":"64b7f0c2a1b2c3d4e5f60718","query":"dentista","lat":-23.5,"lng":-46.6,
            "maxPages":10,"location":" São Paulo ","targetLeads":120}"#;
        let (status, body, _) = call(app(Some(queue(4))), "POST", "/scrape", payload).await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(
            body,
            r#"{"jobId":"64b7f0c2a1b2c3d4e5f60718","status":"queued"}"#
        );
    }

    #[tokio::test]
    async fn post_rejects_when_queue_is_full() {
        let app = app(Some(queue(1)));
        let payload = r#"{"jobId":"a","query":"b"}"#;
        assert_eq!(
            call(app.clone(), "POST", "/scrape", payload).await.0,
            StatusCode::ACCEPTED
        );
        let (status, body, _) = call(app, "POST", "/scrape", payload).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body, r#"{"error":"fila cheia"}"#);
    }

    #[tokio::test]
    async fn delete_reports_whether_job_was_running() {
        let (status, body, _) = call(app(Some(queue(4))), "DELETE", "/scrape/xyz", "").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, r#"{"jobId":"xyz","running":false}"#);

        let (status, _, _) = call(app(None), "DELETE", "/scrape/xyz", "").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    }
}

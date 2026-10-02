use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde::Deserialize;

const DEFAULT_ENDPOINT: &str = "https://nominatim.openstreetmap.org/search";
const DEFAULT_AGENT: &str = "orbita-prospectar/1.0 (+https://axolutions.com.br)";
const MIN_INTERVAL: Duration = Duration::from_millis(1100);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct BBox {
    pub min_lat: f64,
    pub min_lng: f64,
    pub max_lat: f64,
    pub max_lng: f64,
}

impl BBox {
    pub fn center(&self) -> (f64, f64) {
        (
            (self.min_lat + self.max_lat) / 2.0,
            (self.min_lng + self.max_lng) / 2.0,
        )
    }

    pub fn valid(&self) -> bool {
        self.min_lat < self.max_lat
            && self.min_lng < self.max_lng
            && self.min_lat >= -90.0
            && self.max_lat <= 90.0
            && self.min_lng >= -180.0
            && self.max_lng <= 180.0
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum GeocodeError {
    NotFound,
    Other(String),
}

impl std::fmt::Display for GeocodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GeocodeError::NotFound => f.write_str("localização não encontrada"),
            GeocodeError::Other(msg) => f.write_str(msg),
        }
    }
}

impl std::error::Error for GeocodeError {}

#[async_trait]
pub trait Geocoder: Send + Sync {
    async fn lookup(&self, query: &str) -> Result<BBox, GeocodeError>;
}

pub struct Nominatim {
    endpoint: String,
    agent: String,
    http: reqwest::Client,
    state: Mutex<State>,
}

struct State {
    next_slot: Option<Instant>,
    cache: HashMap<String, BBox>,
}

impl Nominatim {
    pub fn from_env() -> Self {
        let endpoint = env_or("NOMINATIM_URL", DEFAULT_ENDPOINT);
        let agent = env_or("NOMINATIM_USER_AGENT", DEFAULT_AGENT);
        Self::new(endpoint, agent)
    }

    pub fn new(endpoint: impl Into<String>, agent: impl Into<String>) -> Self {
        Nominatim {
            endpoint: endpoint.into(),
            agent: agent.into(),
            http: reqwest::Client::builder()
                .timeout(REQUEST_TIMEOUT)
                .build()
                .expect("cliente HTTP"),
            state: Mutex::new(State {
                next_slot: None,
                cache: HashMap::new(),
            }),
        }
    }

    fn reserve_slot(&self, query: &str) -> Result<BBox, Duration> {
        let mut state = self.state.lock().unwrap();
        if let Some(cached) = state.cache.get(query) {
            return Ok(*cached);
        }
        let now = Instant::now();
        let start = state.next_slot.map_or(now, |slot| slot.max(now));
        state.next_slot = Some(start + MIN_INTERVAL);
        Err(start - now)
    }

    async fn fetch(&self, query: &str) -> Result<BBox, GeocodeError> {
        let mut url =
            reqwest::Url::parse(&self.endpoint).map_err(|e| GeocodeError::Other(e.to_string()))?;
        let kept: Vec<(String, String)> = url
            .query_pairs()
            .filter(|(k, _)| !matches!(k.as_ref(), "q" | "format" | "limit"))
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        url.query_pairs_mut()
            .clear()
            .extend_pairs(kept)
            .append_pair("q", query)
            .append_pair("format", "json")
            .append_pair("limit", "1");

        let res = self
            .http
            .get(url)
            .header("User-Agent", &self.agent)
            .header("Accept-Language", "pt-BR")
            .send()
            .await
            .map_err(|e| GeocodeError::Other(e.to_string()))?;

        if res.status() != reqwest::StatusCode::OK {
            return Err(GeocodeError::Other(format!(
                "nominatim respondeu {}",
                res.status().as_u16()
            )));
        }

        #[derive(Deserialize)]
        struct Hit {
            #[serde(default)]
            boundingbox: Vec<String>,
        }

        let hits: Vec<Hit> = res
            .json()
            .await
            .map_err(|e| GeocodeError::Other(e.to_string()))?;
        let Some(hit) = hits.first() else {
            return Err(GeocodeError::NotFound);
        };
        if hit.boundingbox.len() != 4 {
            return Err(GeocodeError::NotFound);
        }

        let mut vals = [0.0; 4];
        for (i, raw) in hit.boundingbox.iter().enumerate() {
            vals[i] = raw
                .parse()
                .map_err(|e| GeocodeError::Other(format!("bbox inválida: {e}")))?;
        }

        let bbox = BBox {
            min_lat: vals[0],
            max_lat: vals[1],
            min_lng: vals[2],
            max_lng: vals[3],
        };
        if !bbox.valid() {
            return Err(GeocodeError::NotFound);
        }
        Ok(bbox)
    }
}

#[async_trait]
impl Geocoder for Nominatim {
    async fn lookup(&self, query: &str) -> Result<BBox, GeocodeError> {
        if query.is_empty() {
            return Err(GeocodeError::NotFound);
        }

        let wait = match self.reserve_slot(query) {
            Ok(cached) => return Ok(cached),
            Err(wait) => wait,
        };
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }

        let bbox = self.fetch(query).await?;
        self.state
            .lock()
            .unwrap()
            .cache
            .insert(query.to_string(), bbox);
        Ok(bbox)
    }
}

fn env_or(key: &str, fallback: &str) -> String {
    std::env::var(key)
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| fallback.to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use axum::Router;
    use axum::extract::{Query, State};
    use axum::http::{HeaderMap, StatusCode};
    use axum::routing::get;

    use super::*;

    const SP_RESPONSE: &str = r#"[{"boundingbox":["-24.0079003","-23.3577551","-46.8262692","-46.3650898"],
        "display_name":"São Paulo, Região Sudeste, Brasil"}]"#;

    #[derive(Clone)]
    struct Mock {
        calls: Arc<AtomicUsize>,
        status: StatusCode,
        body: &'static str,
    }

    async fn serve(mock: Mock) -> String {
        async fn handler(
            State(mock): State<Mock>,
            Query(q): Query<HashMap<String, String>>,
            headers: HeaderMap,
        ) -> (StatusCode, &'static str) {
            mock.calls.fetch_add(1, Ordering::SeqCst);
            assert_eq!(q.get("format").map(String::as_str), Some("json"));
            assert!(
                headers.contains_key("user-agent"),
                "User-Agent é obrigatório para o Nominatim"
            );
            (mock.status, mock.body)
        }

        let app = Router::new()
            .route("/search", get(handler))
            .with_state(mock);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{addr}/search")
    }

    fn mock(status: StatusCode, body: &'static str) -> (Mock, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        (
            Mock {
                calls: calls.clone(),
                status,
                body,
            },
            calls,
        )
    }

    #[tokio::test]
    async fn parses_bbox() {
        let (m, _) = mock(StatusCode::OK, SP_RESPONSE);
        let geo = Nominatim::new(serve(m).await, "teste/1.0");

        let bbox = geo.lookup("São Paulo").await.unwrap();
        assert_eq!(bbox.min_lat, -24.0079003);
        assert_eq!(bbox.max_lng, -46.3650898);
        assert!(bbox.valid());

        let (lat, lng) = bbox.center();
        assert!((-24.0..-23.0).contains(&lat) && (-47.0..-46.0).contains(&lng));
    }

    #[tokio::test]
    async fn uses_cache() {
        let (m, calls) = mock(StatusCode::OK, SP_RESPONSE);
        let geo = Nominatim::new(serve(m).await, "teste/1.0");
        for _ in 0..3 {
            geo.lookup("São Paulo").await.unwrap();
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn not_found() {
        let (m, _) = mock(StatusCode::OK, "[]");
        let geo = Nominatim::new(serve(m).await, "teste/1.0");
        assert!(geo.lookup("asdkjhasdkjh").await.is_err());
        assert_eq!(geo.lookup("").await, Err(GeocodeError::NotFound));
    }

    #[tokio::test]
    async fn http_error() {
        let (m, _) = mock(StatusCode::TOO_MANY_REQUESTS, "");
        let geo = Nominatim::new(serve(m).await, "teste/1.0");
        assert!(geo.lookup("qualquer").await.is_err());
    }

    #[tokio::test]
    async fn spaces_requests_by_min_interval() {
        let (m, calls) = mock(StatusCode::OK, SP_RESPONSE);
        let geo = Nominatim::new(serve(m).await, "teste/1.0");
        let started = Instant::now();
        geo.lookup("a").await.unwrap();
        geo.lookup("b").await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(started.elapsed() >= MIN_INTERVAL);
    }
}

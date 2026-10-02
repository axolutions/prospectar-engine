use std::sync::Arc;
use std::time::Duration;

use prospectar_engine::browser::Pool;
use prospectar_engine::geocode::Nominatim;
use prospectar_engine::http::{AppState, router};
use prospectar_engine::jobs::Queue;
use prospectar_engine::store::Mongo;
use tracing_subscriber::EnvFilter;

const QUEUE_BUFFER: usize = 100;
const MONGO_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

fn env_or(key: &str, fallback: &str) -> String {
    std::env::var(key)
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| fallback.to_string())
}

fn concurrency_from_env() -> usize {
    std::env::var("SCRAPE_CONCURRENCY")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(1)
}

async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("handler de SIGTERM");
        tokio::select! {
            _ = ctrl_c => {},
            _ = term.recv() => {},
        }
    }
    #[cfg(not(unix))]
    {
        let _ = ctrl_c.await;
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .init();

    let port = env_or("PORT", "3001");
    let allowed_origin = env_or("ALLOWED_ORIGIN", "http://localhost:3000");
    let concurrency = concurrency_from_env();

    let pool = Arc::new(Pool::new(concurrency));

    let mut mongo: Option<Arc<Mongo>> = None;
    let queue = match std::env::var("MONGODB_URI").ok().filter(|v| !v.is_empty()) {
        Some(uri) => {
            let db = env_or("MONGODB_DB", "orbita");
            let connected = tokio::time::timeout(MONGO_CONNECT_TIMEOUT, Mongo::connect(&uri, &db))
                .await
                .map_err(|_| "timeout ao conectar".to_string())
                .and_then(|r| r);
            let store = match connected {
                Ok(store) => Arc::new(store),
                Err(e) => {
                    tracing::error!("mongo: {e}");
                    std::process::exit(1);
                }
            };
            mongo = Some(store.clone());
            let queue = Queue::new(
                pool.clone(),
                store,
                Arc::new(Nominatim::from_env()),
                concurrency,
                QUEUE_BUFFER,
            );
            tracing::info!("fila assíncrona ativa (concorrência={concurrency})");
            Some(Arc::new(queue))
        }
        None => {
            tracing::info!("MONGODB_URI ausente: POST /scrape (assíncrono) desabilitado");
            None
        }
    };

    let app = router(
        AppState {
            pool: pool.clone(),
            queue,
        },
        &allowed_origin,
    );

    let listener = match tokio::net::TcpListener::bind(format!("0.0.0.0:{port}")).await {
        Ok(listener) => listener,
        Err(e) => {
            tracing::error!("não foi possível ouvir na porta {port}: {e}");
            std::process::exit(1);
        }
    };
    tracing::info!("scraper ouvindo em http://localhost:{port}");

    let server = axum::serve(listener, app).with_graceful_shutdown(shutdown_signal());
    tokio::select! {
        result = server => {
            if let Err(e) = result {
                tracing::error!("servidor: {e}");
            }
        }
        _ = async {
            shutdown_signal().await;
            tokio::time::sleep(SHUTDOWN_GRACE).await;
        } => {
            tracing::warn!("conexões ainda abertas após {}s; encerrando", SHUTDOWN_GRACE.as_secs());
        }
    }

    pool.close().await;
    if let Some(mongo) = mongo {
        mongo.close().await;
    }
}

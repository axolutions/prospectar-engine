use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use chromiumoxide::{Browser, BrowserConfig, Page};
use futures::StreamExt;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

const USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

pub struct Pool {
    slots: Arc<Semaphore>,
    running: Mutex<Option<Running>>,
}

struct Running {
    browser: Browser,
    handler: JoinHandle<()>,
    profile: PathBuf,
}

pub struct Tab {
    page: Page,
    token: CancellationToken,
    _slot: OwnedSemaphorePermit,
}

impl Tab {
    pub fn page(&self) -> &Page {
        &self.page
    }

    pub fn token(&self) -> CancellationToken {
        self.token.clone()
    }
}

impl Drop for Tab {
    fn drop(&mut self) {
        self.token.cancel();
        let page = self.page.clone();
        if let Ok(rt) = tokio::runtime::Handle::try_current() {
            rt.spawn(async move {
                let _ = page.close().await;
            });
        }
    }
}

impl Pool {
    pub fn new(concurrency: usize) -> Self {
        Pool {
            slots: Arc::new(Semaphore::new(concurrency.max(1))),
            running: Mutex::new(None),
        }
    }

    pub async fn tab(&self, parent: &CancellationToken) -> Result<Tab, String> {
        let slot = tokio::select! {
            slot = self.slots.clone().acquire_owned() => slot.map_err(|e| e.to_string())?,
            _ = parent.cancelled() => return Err("cancelado antes de obter a aba".into()),
        };

        let page = self.open_page().await?;

        Ok(Tab {
            page,
            token: parent.child_token(),
            _slot: slot,
        })
    }

    async fn open_page(&self) -> Result<Page, String> {
        let mut running = self.running.lock().await;
        for attempt in 0..2 {
            let alive = running.as_ref().is_some_and(|r| !r.handler.is_finished());
            if !alive {
                if let Some(dead) = running.take() {
                    shutdown(dead).await;
                }
                *running = Some(launch().await?);
            }

            match running
                .as_ref()
                .unwrap()
                .browser
                .new_page("about:blank")
                .await
            {
                Ok(page) => return Ok(page),
                Err(e) if attempt == 0 => {
                    tracing::warn!("nova aba falhou ({e}); relançando o chromium");
                    if let Some(dead) = running.take() {
                        shutdown(dead).await;
                    }
                }
                Err(e) => return Err(e.to_string()),
            }
        }
        Err("chromium indisponível".into())
    }

    pub async fn close(&self) {
        if let Some(r) = self.running.lock().await.take() {
            shutdown(r).await;
        }
    }
}

const CHROME_FLAGS: &[&str] = &[
    "no-first-run",
    "no-default-browser-check",
    "disable-background-networking",
    "disable-background-timer-throttling",
    "disable-backgrounding-occluded-windows",
    "disable-breakpad",
    "disable-client-side-phishing-detection",
    "disable-default-apps",
    "disable-dev-shm-usage",
    "disable-extensions",
    "disable-hang-monitor",
    "disable-ipc-flooding-protection",
    "disable-popup-blocking",
    "disable-prompt-on-repost",
    "disable-renderer-backgrounding",
    "disable-sync",
    "metrics-recording-only",
    "safebrowsing-disable-auto-update",
    "enable-automation",
    "use-mock-keychain",
    "disable-gpu",
    "enable-unsafe-swiftshader",
];

const CHROME_VALUES: &[(&str, &str)] = &[
    ("enable-features", "NetworkService,NetworkServiceInProcess"),
    (
        "disable-features",
        "site-per-process,Translate,BlinkGenPropertyTrees",
    ),
    ("force-color-profile", "srgb"),
    ("password-store", "basic"),
    ("lang", "pt-BR"),
    ("user-agent", USER_AGENT),
];

static LAUNCHES: AtomicU64 = AtomicU64::new(0);

fn profile_dir() -> PathBuf {
    let n = LAUNCHES.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("prospectar-chrome-{}-{n}", std::process::id()))
}

async fn launch() -> Result<Running, String> {
    let profile = profile_dir();
    let mut builder = BrowserConfig::builder()
        .disable_default_args()
        .no_sandbox()
        .window_size(1280, 900)
        .viewport(None)
        .user_data_dir(&profile)
        .args(CHROME_FLAGS.iter().copied());
    for &(key, value) in CHROME_VALUES {
        builder = builder.arg((key, value));
    }
    if let Some(path) = std::env::var("CHROME_PATH").ok().filter(|p| !p.is_empty()) {
        builder = builder.chrome_executable(path);
    }
    let config = builder.build()?;

    let (browser, mut handler) = match Browser::launch(config).await {
        Ok(launched) => launched,
        Err(e) => {
            let _ = tokio::fs::remove_dir_all(&profile).await;
            return Err(e.to_string());
        }
    };
    let handler = tokio::spawn(async move {
        while let Some(event) = handler.next().await {
            if let Err(e) = event {
                tracing::warn!("conexão CDP caiu: {e}");
                break;
            }
        }
    });

    tracing::info!("chromium iniciado");
    Ok(Running {
        browser,
        handler,
        profile,
    })
}

async fn shutdown(mut r: Running) {
    let _ = r.browser.close().await;
    let _ = r.browser.wait().await;
    r.handler.abort();
    let _ = tokio::fs::remove_dir_all(&r.profile).await;
}

#[cfg(test)]
mod tests {
    use chromiumoxide::cdp::js_protocol::runtime::EvaluateParams;

    use super::*;

    fn chrome_available() -> bool {
        let ok = std::env::var("CHROME_PATH").is_ok_and(|p| !p.is_empty());
        if !ok {
            eprintln!("defina CHROME_PATH para rodar testes que sobem o browser");
        }
        ok
    }

    async fn eval<T: serde::de::DeserializeOwned>(page: &Page, expr: &str) -> T {
        page.evaluate_expression(EvaluateParams::new(expr))
            .await
            .unwrap()
            .into_value()
            .unwrap()
    }

    #[tokio::test]
    async fn tab_survives_child_token_cancel() {
        if !chrome_available() {
            return;
        }
        let pool = Pool::new(1);
        let tab = pool.tab(&CancellationToken::new()).await.unwrap();
        tab.token().child_token().cancel();
        assert!(!tab.token().is_cancelled());
        let ok: String = eval(tab.page(), r#""ok""#).await;
        assert_eq!(ok, "ok");
        drop(tab);
        pool.close().await;
    }

    #[tokio::test]
    async fn tab_releases_concurrency_slot() {
        if !chrome_available() {
            return;
        }
        let pool = Pool::new(1);
        drop(pool.tab(&CancellationToken::new()).await.unwrap());
        let second = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            pool.tab(&CancellationToken::new()),
        )
        .await
        .expect("segunda aba deveria conseguir o slot liberado");
        drop(second.unwrap());
        pool.close().await;
    }

    #[tokio::test]
    async fn browser_has_webgl_and_brazilian_locale() {
        if !chrome_available() {
            return;
        }
        let pool = Pool::new(1);
        let tab = pool.tab(&CancellationToken::new()).await.unwrap();
        let webgl: bool = eval(
            tab.page(),
            "!!document.createElement('canvas').getContext('webgl')",
        )
        .await;
        assert!(webgl, "sem WebGL o Bing Maps redireciona para webglerror");
        if cfg!(target_os = "linux") {
            let lang: String = eval(tab.page(), "navigator.language").await;
            assert_eq!(lang, "pt-BR");
        }
        drop(tab);
        pool.close().await;
    }

    #[tokio::test]
    async fn each_launch_gets_its_own_profile() {
        assert_ne!(profile_dir(), profile_dir());
    }
}

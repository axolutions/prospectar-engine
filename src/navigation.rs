use std::collections::HashSet;
use std::time::{Duration, Instant};

use chromiumoxide::Page;
use chromiumoxide::cdp::browser_protocol::emulation::SetFocusEmulationEnabledParams;
use chromiumoxide::cdp::js_protocol::runtime::EvaluateParams;
use serde::de::DeserializeOwned;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::domain::{Event, Job, Lead};
use crate::gofmt::{encode_query, format_g};
use crate::parsing::{self, Version};

const NAV_TIMEOUT: Duration = Duration::from_secs(30);
const CONTAINER_TIMEOUT: Duration = Duration::from_secs(15);
const IDLE_TIMEOUT: Duration = Duration::from_secs(8);
const GROWTH_TIMEOUT: Duration = Duration::from_secs(8);
const POLL_INTERVAL: Duration = Duration::from_millis(250);
const SCROLL_SETTLE: Duration = Duration::from_millis(400);
const DEFAULT_MAX_PAGES: usize = 10;

const VIS_HELPER: &str = "const __vis=e=>{const r=e.getBoundingClientRect(),s=getComputedStyle(e);\
return r.width>0&&r.height>0&&s.visibility!=='hidden'&&s.display!=='none'};";

pub fn build_url(job: &Job) -> String {
    let mut pairs = vec![("q", job.query.clone()), ("style", "r".to_string())];
    if let (Some(lat), Some(lng)) = (job.latitude, job.longitude) {
        pairs.push(("cp", format!("{}~{}", format_g(lat), format_g(lng))));
        pairs.push(("lvl", job.zoom.unwrap_or(12.0).to_string()));
    }
    format!("https://www.bing.com/maps/search?{}", encode_query(&pairs))
}

pub fn scrape(page: Page, job: Job, token: CancellationToken) -> mpsc::Receiver<Event> {
    let (tx, rx) = mpsc::channel(1);
    tokio::spawn(async move {
        let session = Session { page, token, tx };
        session.run(job).await;
    });
    rx
}

struct Session {
    page: Page,
    token: CancellationToken,
    tx: mpsc::Sender<Event>,
}

impl Session {
    async fn emit(&self, ev: Event) -> bool {
        tokio::select! {
            sent = self.tx.send(ev) => sent.is_ok(),
            _ = self.token.cancelled() => false,
        }
    }

    async fn sleep(&self, d: Duration) -> bool {
        tokio::select! {
            _ = tokio::time::sleep(d) => true,
            _ = self.token.cancelled() => false,
        }
    }

    async fn run(self, mut job: Job) {
        if job.max_pages == 0 {
            job.max_pages = DEFAULT_MAX_PAGES;
        }

        let focus = tokio::select! {
            r = self.page.execute(SetFocusEmulationEnabledParams::new(true)) => r,
            _ = self.token.cancelled() => return,
        };
        if let Err(e) = focus {
            self.emit(Event::error(format!("abertura de aba falhou: {e}")))
                .await;
            return;
        }

        let navigation = tokio::select! {
            r = tokio::time::timeout(NAV_TIMEOUT, self.page.goto(build_url(&job))) => r,
            _ = self.token.cancelled() => return,
        };
        match navigation {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                self.emit(Event::error(format!("navegação falhou: {e}")))
                    .await;
                return;
            }
            Err(_) => {
                self.emit(Event::error("navegação falhou: context deadline exceeded"))
                    .await;
                return;
            }
        }

        let version = self.wait_for_container().await;
        let mut seen: HashSet<String> = HashSet::new();

        for page in 1..=job.max_pages {
            self.wait_idle(version).await;

            let leads = match self.extract_leads(version).await {
                Ok(leads) => leads,
                Err(e) => {
                    self.emit(Event::error(format!("extração falhou: {e}")))
                        .await;
                    return;
                }
            };

            for lead in leads {
                if !seen.insert(lead.id.clone()) {
                    continue;
                }
                if !self.emit(Event::lead(lead)).await {
                    return;
                }
            }

            if !self.emit(Event::progress(page, seen.len())).await {
                return;
            }
            if !self.advance(version).await {
                break;
            }
        }

        self.emit(Event::done(seen.len())).await;
    }

    async fn advance(&self, v: &Version) -> bool {
        if v.infinite_scroll {
            return self.scroll_for_more(v).await;
        }
        let signature = self.first_signature(v).await;
        if !self.click_first_visible(&["a.bm_rightChevron"]).await {
            return false;
        }
        self.wait_for_signature_change(v, &signature).await;
        true
    }

    async fn scroll_for_more(&self, v: &Version) -> bool {
        let before = self.count_items(v).await;
        let deadline = Instant::now() + GROWTH_TIMEOUT;
        while Instant::now() < deadline {
            self.scroll_list(v).await;
            if !self.sleep(SCROLL_SETTLE).await {
                return false;
            }
            if self.count_items(v).await > before {
                return true;
            }
        }
        if std::env::var_os("SCRAPER_DEBUG").is_some_and(|v| !v.is_empty()) {
            tracing::info!(
                "[scroll_for_more] lista estabilizou em {before} (version={})",
                v.name
            );
        }
        false
    }

    async fn wait_for_container(&self) -> &'static Version {
        let deadline = Instant::now() + CONTAINER_TIMEOUT;
        while Instant::now() < deadline {
            for v in &parsing::VERSIONS {
                if self.exists(v.list_container).await {
                    return v;
                }
            }
            if !self.sleep(POLL_INTERVAL).await {
                break;
            }
        }
        parsing::fallback()
    }

    async fn wait_idle(&self, v: &Version) {
        let deadline = Instant::now() + IDLE_TIMEOUT;
        while Instant::now() < deadline {
            if !self.any_visible(v.loading_indicator).await {
                return;
            }
            if !self.sleep(POLL_INTERVAL).await {
                return;
            }
        }
    }

    async fn wait_for_signature_change(&self, v: &Version, before: &str) {
        let deadline = Instant::now() + GROWTH_TIMEOUT;
        while Instant::now() < deadline {
            if self.first_signature(v).await != before {
                return;
            }
            if !self.sleep(POLL_INTERVAL).await {
                return;
            }
        }
    }

    async fn evaluate<T: DeserializeOwned>(&self, expr: String) -> Result<T, String> {
        let mut params = EvaluateParams::new(expr);
        params.return_by_value = Some(true);
        let response = tokio::select! {
            r = self.page.execute(params) => r.map_err(|e| e.to_string())?,
            _ = self.token.cancelled() => return Err("cancelado".into()),
        };
        let returns = &response.result;
        if let Some(exception) = &returns.exception_details {
            return Err(exception.text.clone());
        }
        let value = returns
            .result
            .value
            .clone()
            .unwrap_or(serde_json::Value::Null);
        serde_json::from_value(value).map_err(|e| e.to_string())
    }

    async fn evaluate_or_default<T: DeserializeOwned + Default>(&self, expr: String) -> T {
        match self.evaluate(expr).await {
            Ok(value) => value,
            Err(e) => {
                tracing::debug!("avaliação JS falhou: {e}");
                T::default()
            }
        }
    }

    async fn extract_leads(&self, v: &Version) -> Result<Vec<Lead>, String> {
        let expr = format!(
            "(()=>{{for(const s of {}){{const els=document.querySelectorAll(s);\
if(!els.length)continue;const out=[];for(const e of els){{const a=e.getAttribute('data-entity');\
if(a)out.push(a)}}if(out.length)return out}}return[]}})()",
            js_array(v.list_items)
        );
        let raws: Vec<String> = self.evaluate(expr).await?;
        Ok(raws
            .iter()
            .filter_map(|raw| parsing::parse_entity(raw))
            .collect())
    }

    async fn count_items(&self, v: &Version) -> usize {
        let expr = format!(
            "(()=>{{for(const s of {}){{const c=document.querySelectorAll(s).length;\
if(c)return c}}return 0}})()",
            js_array(v.list_items)
        );
        self.evaluate_or_default(expr).await
    }

    async fn first_signature(&self, v: &Version) -> String {
        let expr = format!(
            "(()=>{{for(const s of {}){{const e=document.querySelector(s);\
if(e)return (e.getAttribute('data-entity')||'').slice(0,200)}}return''}})()",
            js_array(v.list_items)
        );
        self.evaluate_or_default(expr).await
    }

    async fn exists(&self, selectors: &[&str]) -> bool {
        if selectors.is_empty() {
            return false;
        }
        let expr = format!(
            "(()=>{{for(const s of {}){{if(document.querySelector(s))return true}}return false}})()",
            js_array(selectors)
        );
        self.evaluate_or_default(expr).await
    }

    async fn any_visible(&self, selectors: &[&str]) -> bool {
        if selectors.is_empty() {
            return false;
        }
        let expr = format!(
            "(()=>{{{VIS_HELPER}for(const s of {}){{for(const e of document.querySelectorAll(s)){{\
if(__vis(e))return true}}}}return false}})()",
            js_array(selectors)
        );
        self.evaluate_or_default(expr).await
    }

    async fn click_first_visible(&self, selectors: &[&str]) -> bool {
        if selectors.is_empty() {
            return false;
        }
        let expr = format!(
            "(()=>{{{VIS_HELPER}for(const s of {}){{for(const e of document.querySelectorAll(s)){{\
if(__vis(e)){{e.click();return true}}}}}}return false}})()",
            js_array(selectors)
        );
        self.evaluate_or_default(expr).await
    }

    async fn scroll_list(&self, v: &Version) -> bool {
        if v.scroll_container.is_empty() {
            return false;
        }
        let expr = format!(
            "(()=>{{for(const s of {}){{const e=document.querySelector(s);if(!e)continue;\
const p=e.scrollTop;e.scrollTop=e.scrollHeight;if(e.scrollTop!==p)return true}}\
window.scrollBy(0,window.innerHeight);return false}})()",
            js_array(v.scroll_container)
        );
        self.evaluate_or_default(expr).await
    }
}

fn js_array(items: &[&str]) -> String {
    serde_json::to_string(items).unwrap_or_else(|_| "[]".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job(query: &str, lat: Option<f64>, lng: Option<f64>, zoom: Option<f64>) -> Job {
        Job {
            query: query.into(),
            latitude: lat,
            longitude: lng,
            zoom,
            max_pages: 0,
        }
    }

    #[test]
    fn build_url_matches_go() {
        assert_eq!(
            build_url(&job("nutricionista em São Paulo", None, None, None)),
            "https://www.bing.com/maps/search?q=nutricionista+em+S%C3%A3o+Paulo&style=r"
        );
        assert_eq!(
            build_url(&job("dentista", Some(-23.55), Some(-46.63), None)),
            "https://www.bing.com/maps/search?cp=-23.55~-46.63&lvl=12&q=dentista&style=r"
        );
        assert_eq!(
            build_url(&job("x", Some(1.0), Some(2.0), Some(16.0))),
            "https://www.bing.com/maps/search?cp=1~2&lvl=16&q=x&style=r"
        );
        assert_eq!(
            build_url(&job("x", Some(1.0), None, None)),
            "https://www.bing.com/maps/search?q=x&style=r"
        );
    }

    #[test]
    fn js_array_quotes_selectors() {
        assert_eq!(
            js_array(&[r#"[data-automation-id="resultsList"]"#, ".b_lstcards"]),
            r#"["[data-automation-id=\"resultsList\"]",".b_lstcards"]"#
        );
    }
}

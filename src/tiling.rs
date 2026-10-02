use std::collections::HashSet;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::domain::{Event, EventKind, Job};
use crate::geocode::BBox;
use crate::place::{Key, dedupe_key};

const KM_PER_DEGREE_LAT: f64 = 111.32;
const MIN_CELL_KM: f64 = 0.25;
pub const MAX_TILES: usize = 2000;
const ZOOM_BASE: f64 = 15.0;
const ZOOM_REF_KM: f64 = 1.0;

pub const DEFAULT_CELL_KM: f64 = 3.0;
pub const DEFAULT_TARGET_LEADS: usize = 100;
pub const DEFAULT_TILE_PAGES: usize = 3;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Tile {
    pub lat: f64,
    pub lng: f64,
    pub zoom: f64,
}

pub fn tiles(bbox: BBox, cell_km: f64) -> Vec<Tile> {
    if !bbox.valid() {
        return Vec::new();
    }
    let cell_km = cell_km.max(MIN_CELL_KM);
    let (center_lat, center_lng) = bbox.center();

    let mut step_lat = cell_km / KM_PER_DEGREE_LAT;
    let mut step_lng =
        cell_km / (KM_PER_DEGREE_LAT * (center_lat * std::f64::consts::PI / 180.0).cos().max(0.01));

    let span = |extent: f64, step: f64| ((extent / step).ceil() as usize).max(1);
    let mut rows = span(bbox.max_lat - bbox.min_lat, step_lat);
    let mut cols = span(bbox.max_lng - bbox.min_lng, step_lng);

    while rows * cols > MAX_TILES {
        step_lat *= 2.0;
        step_lng *= 2.0;
        rows = span(bbox.max_lat - bbox.min_lat, step_lat);
        cols = span(bbox.max_lng - bbox.min_lng, step_lng);
    }

    let zoom = zoom_for_cell(step_lat * KM_PER_DEGREE_LAT);

    let mut out = Vec::with_capacity(rows * cols);
    let mut seen = HashSet::with_capacity(rows * cols);
    for r in 0..rows {
        for c in 0..cols {
            let lat = clamp(
                bbox.min_lat + (r as f64 + 0.5) * step_lat,
                bbox.min_lat,
                bbox.max_lat,
            );
            let lng = clamp(
                bbox.min_lng + (c as f64 + 0.5) * step_lng,
                bbox.min_lng,
                bbox.max_lng,
            );
            if seen.insert((lat.to_bits(), lng.to_bits())) {
                out.push(Tile { lat, lng, zoom });
            }
        }
    }

    out.sort_by(|a, b| {
        dist_sq(a, center_lat, center_lng).total_cmp(&dist_sq(b, center_lat, center_lng))
    });
    out
}

fn clamp(v: f64, lo: f64, hi: f64) -> f64 {
    v.max(lo).min(hi)
}

fn dist_sq(t: &Tile, lat: f64, lng: f64) -> f64 {
    let d_lat = t.lat - lat;
    let d_lng = (t.lng - lng) * (lat * std::f64::consts::PI / 180.0).cos();
    d_lat * d_lat + d_lng * d_lng
}

fn zoom_for_cell(cell_km: f64) -> f64 {
    if cell_km <= 0.0 {
        return ZOOM_BASE;
    }
    let z = ZOOM_BASE - (cell_km / ZOOM_REF_KM).log2();
    z.clamp(10.0, 17.0).round()
}

pub type ScrapeFn = Arc<dyn Fn(Job, CancellationToken) -> mpsc::Receiver<Event> + Send + Sync>;

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Request {
    pub query: String,
    pub bbox: BBox,
    pub cell_km: f64,
    pub target_leads: usize,
    pub pages_per_tile: usize,
}

impl Request {
    fn with_defaults(mut self) -> Self {
        if self.cell_km <= 0.0 {
            self.cell_km = DEFAULT_CELL_KM;
        }
        if self.target_leads == 0 {
            self.target_leads = DEFAULT_TARGET_LEADS;
        }
        if self.pages_per_tile == 0 {
            self.pages_per_tile = DEFAULT_TILE_PAGES;
        }
        self
    }
}

pub fn run(token: CancellationToken, req: Request, scrape: ScrapeFn) -> mpsc::Receiver<Event> {
    let req = req.with_defaults();
    let (tx, rx) = mpsc::channel(1);

    tokio::spawn(async move {
        let emit = |ev: Event| {
            let tx = tx.clone();
            let token = token.clone();
            async move {
                tokio::select! {
                    sent = tx.send(ev) => sent.is_ok(),
                    _ = token.cancelled() => false,
                }
            }
        };

        let grid = tiles(req.bbox, req.cell_km);
        if grid.is_empty() {
            emit(Event::error("área inválida para varredura")).await;
            return;
        }

        let mut seen: HashSet<Key> = HashSet::new();
        let mut last_err = String::new();

        for (i, tile) in grid.iter().enumerate() {
            if token.is_cancelled() {
                return;
            }

            let outcome = run_tile(&token, &req, tile, &scrape, &mut seen, &emit).await;
            if !outcome.err.is_empty() {
                last_err = outcome.err;
            }
            if outcome.aborted {
                return;
            }

            if !emit(Event::tile_progress(i + 1, grid.len(), seen.len())).await {
                return;
            }
            if seen.len() >= req.target_leads {
                break;
            }
        }

        if seen.is_empty() && !last_err.is_empty() {
            emit(Event::error(last_err)).await;
            return;
        }
        emit(Event::done(seen.len())).await;
    });

    rx
}

struct TileOutcome {
    aborted: bool,
    err: String,
}

async fn run_tile<F, Fut>(
    token: &CancellationToken,
    req: &Request,
    tile: &Tile,
    scrape: &ScrapeFn,
    seen: &mut HashSet<Key>,
    emit: &F,
) -> TileOutcome
where
    F: Fn(Event) -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let tile_token = token.child_token();
    let _cancel_on_exit = tile_token.clone().drop_guard();

    let job = Job {
        query: req.query.clone(),
        latitude: Some(tile.lat),
        longitude: Some(tile.lng),
        zoom: Some(tile.zoom),
        max_pages: req.pages_per_tile,
    };

    let mut events = scrape(job, tile_token);
    let mut err = String::new();

    while let Some(ev) = events.recv().await {
        match ev.kind {
            EventKind::Lead => {
                let Some(lead) = ev.data.as_ref() else {
                    continue;
                };
                if !seen.insert(dedupe_key(lead)) {
                    continue;
                }
                if !emit(ev).await {
                    return TileOutcome { aborted: true, err };
                }
            }
            EventKind::Error => err = ev.message,
            _ => {}
        }

        if seen.len() >= req.target_leads {
            break;
        }
    }

    TileOutcome {
        aborted: false,
        err,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::*;
    use crate::domain::Lead;

    const SAO_PAULO: BBox = BBox {
        min_lat: -24.0079003,
        max_lat: -23.3577551,
        min_lng: -46.8262692,
        max_lng: -46.3650898,
    };

    fn lead_at(name: &str, lat: f64, lng: f64) -> Lead {
        Lead {
            id: name.into(),
            name: name.into(),
            latitude: Some(lat),
            longitude: Some(lng),
            ..Lead::default()
        }
    }

    fn canned(events: Vec<Event>) -> mpsc::Receiver<Event> {
        let (tx, rx) = mpsc::channel(events.len().max(1));
        for ev in events {
            tx.try_send(ev).unwrap();
        }
        rx
    }

    #[derive(Default)]
    struct Collected {
        leads: Vec<Lead>,
        progress: Vec<Event>,
        done: Option<i64>,
        error: String,
    }

    async fn collect(mut rx: mpsc::Receiver<Event>) -> Collected {
        let mut out = Collected::default();
        while let Some(ev) = rx.recv().await {
            match ev.kind {
                EventKind::Lead => out.leads.push(ev.data.unwrap()),
                EventKind::Progress => out.progress.push(ev),
                EventKind::Done => out.done = ev.total,
                EventKind::Error => out.error = ev.message,
            }
        }
        out
    }

    #[test]
    fn tiles_cover_box_without_gaps() {
        let small = BBox {
            min_lat: -23.60,
            max_lat: -23.50,
            min_lng: -46.70,
            max_lng: -46.60,
        };
        let grid = tiles(small, 2.0);
        assert!(!grid.is_empty());
        for t in &grid {
            assert!(t.lat >= small.min_lat && t.lat <= small.max_lat);
            assert!(t.lng >= small.min_lng && t.lng <= small.max_lng);
        }

        let step_lat = 2.0 / KM_PER_DEGREE_LAT;
        let rows = ((small.max_lat - small.min_lat) / step_lat).ceil() as usize;
        let bands: HashSet<i64> = grid
            .iter()
            .map(|t| ((t.lat - small.min_lat) / step_lat) as i64)
            .collect();
        assert!(bands.len() >= rows);
    }

    #[test]
    fn first_tile_is_closest_to_center_and_order_goes_outward() {
        let grid = tiles(SAO_PAULO, 5.0);
        assert!(grid.len() >= 4);
        let (lat, lng) = SAO_PAULO.center();
        let mut prev = -1.0;
        for t in &grid {
            let d = dist_sq(t, lat, lng);
            assert!(d >= prev - 1e-12, "ordem centro→fora quebrada");
            prev = d;
        }
    }

    #[test]
    fn smaller_cells_produce_more_tiles() {
        assert!(tiles(SAO_PAULO, 2.0).len() > tiles(SAO_PAULO, 10.0).len());
    }

    #[test]
    fn tiles_capped_and_sane() {
        let grid = tiles(SAO_PAULO, 0.25);
        assert!(!grid.is_empty() && grid.len() <= MAX_TILES);
        assert!(grid.iter().all(|t| (10.0..=17.0).contains(&t.zoom)));
    }

    #[test]
    fn rejects_invalid_box() {
        assert!(tiles(BBox::default(), 2.0).is_empty());
        let inverted = BBox {
            min_lat: 10.0,
            max_lat: -10.0,
            min_lng: 10.0,
            max_lng: -10.0,
        };
        assert!(tiles(inverted, 2.0).is_empty());
    }

    #[test]
    fn tiny_box_yields_one_tile() {
        let tiny = BBox {
            min_lat: -23.5501,
            max_lat: -23.5500,
            min_lng: -46.6334,
            max_lng: -46.6333,
        };
        assert_eq!(tiles(tiny, 5.0).len(), 1);
    }

    #[tokio::test]
    async fn deduplicates_across_tiles() {
        let calls = Arc::new(AtomicUsize::new(0));
        let c = calls.clone();
        let scrape: ScrapeFn = Arc::new(move |job: Job, _| {
            c.fetch_add(1, Ordering::SeqCst);
            canned(vec![
                Event::lead(lead_at("Compartilhado", -23.55, -46.63)),
                Event::lead(lead_at(
                    &format!("Unico-{}", job.latitude.is_some()),
                    -23.60,
                    -46.60,
                )),
                Event::done(2),
            ])
        });

        let req = Request {
            query: "restaurante".into(),
            bbox: SAO_PAULO,
            cell_km: 20.0,
            target_leads: 1000,
            ..Request::default()
        };
        let got = collect(run(CancellationToken::new(), req, scrape)).await;

        assert!(got.error.is_empty());
        assert!(calls.load(Ordering::SeqCst) >= 2);
        assert_eq!(
            got.leads
                .iter()
                .filter(|l| l.name == "Compartilhado")
                .count(),
            1
        );
        assert_eq!(got.done, Some(got.leads.len() as i64));
    }

    #[tokio::test]
    async fn stops_at_target() {
        let calls = Arc::new(AtomicUsize::new(0));
        let c = calls.clone();
        let scrape: ScrapeFn = Arc::new(move |_, _| {
            let n = c.fetch_add(1, Ordering::SeqCst) + 1;
            let mut evs: Vec<Event> = (0..10)
                .map(|i| {
                    Event::lead(lead_at(
                        &format!("lead-{n}-{i}"),
                        -23.5 - i as f64 / 1000.0,
                        -46.6,
                    ))
                })
                .collect();
            evs.push(Event::done(10));
            canned(evs)
        });

        let req = Request {
            query: "x".into(),
            bbox: SAO_PAULO,
            cell_km: 5.0,
            target_leads: 15,
            ..Request::default()
        };
        let got = collect(run(CancellationToken::new(), req, scrape)).await;

        assert!(
            (15..=25).contains(&got.leads.len()),
            "leads = {}",
            got.leads.len()
        );
        let total = tiles(SAO_PAULO, 5.0).len();
        assert!(calls.load(Ordering::SeqCst) < total);
        assert!(got.done.unwrap() >= 15);
        let last = got.progress.last().unwrap();
        assert_eq!(last.tiles, Some(total as i64));
        assert!(last.tile.is_some());
    }

    #[tokio::test]
    async fn surfaces_error_when_nothing_found() {
        let scrape: ScrapeFn = Arc::new(|_, _| canned(vec![Event::error("bing fora do ar")]));
        let req = Request {
            query: "x".into(),
            bbox: SAO_PAULO,
            cell_km: 50.0,
            target_leads: 10,
            ..Request::default()
        };
        let got = collect(run(CancellationToken::new(), req, scrape)).await;
        assert!(got.leads.is_empty());
        assert!(!got.error.is_empty());
        assert_eq!(got.done, None);
    }

    #[tokio::test]
    async fn tolerates_tile_error_when_leads_exist() {
        let calls = Arc::new(AtomicUsize::new(0));
        let c = calls.clone();
        let scrape: ScrapeFn = Arc::new(move |_, _| {
            if c.fetch_add(1, Ordering::SeqCst) == 0 {
                canned(vec![
                    Event::lead(lead_at("bom", -23.55, -46.63)),
                    Event::done(1),
                ])
            } else {
                canned(vec![Event::error("célula falhou")])
            }
        });
        let req = Request {
            query: "x".into(),
            bbox: SAO_PAULO,
            cell_km: 30.0,
            target_leads: 500,
            ..Request::default()
        };
        let got = collect(run(CancellationToken::new(), req, scrape)).await;
        assert_eq!(got.leads.len(), 1);
        assert!(got.error.is_empty());
        assert!(got.done.is_some());
    }

    #[tokio::test]
    async fn cancels_tile_scrape_on_early_stop() {
        let (cancelled_tx, mut cancelled_rx) = mpsc::channel::<()>(8);
        let scrape: ScrapeFn = Arc::new(move |_, token: CancellationToken| {
            let (tx, rx) = mpsc::channel(1);
            let cancelled_tx = cancelled_tx.clone();
            tokio::spawn(async move {
                for i in 0..100 {
                    let ev =
                        Event::lead(lead_at(&format!("l-{i}"), -23.5 - i as f64 / 1000.0, -46.6));
                    tokio::select! {
                        sent = tx.send(ev) => if sent.is_err() { break },
                        _ = token.cancelled() => {
                            let _ = cancelled_tx.send(()).await;
                            return;
                        }
                    }
                }
            });
            rx
        });

        let req = Request {
            query: "x".into(),
            bbox: SAO_PAULO,
            cell_km: 20.0,
            target_leads: 5,
            ..Request::default()
        };
        collect(run(CancellationToken::new(), req, scrape)).await;

        tokio::time::timeout(Duration::from_secs(3), cancelled_rx.recv())
            .await
            .expect(
                "scrape da célula não foi cancelado ao atingir o alvo (vazaria aba do browser)",
            );
    }

    #[tokio::test]
    async fn respects_caller_cancellation() {
        let scrape: ScrapeFn = Arc::new(|_, _| {
            canned(vec![
                Event::lead(lead_at("a", -23.5, -46.6)),
                Event::done(1),
            ])
        });
        let token = CancellationToken::new();
        let mut rx = run(
            token.clone(),
            Request {
                query: "x".into(),
                bbox: SAO_PAULO,
                cell_km: 5.0,
                target_leads: 10_000,
                ..Request::default()
            },
            scrape,
        );
        rx.recv().await;
        token.cancel();
        tokio::time::timeout(Duration::from_secs(3), async {
            while rx.recv().await.is_some() {}
        })
        .await
        .expect("stream deveria fechar após cancelamento");
    }

    #[tokio::test]
    async fn rejects_invalid_area() {
        let scrape: ScrapeFn = Arc::new(|_, _| panic!("não deveria raspar com área inválida"));
        let got = collect(run(
            CancellationToken::new(),
            Request {
                query: "x".into(),
                ..Request::default()
            },
            scrape,
        ))
        .await;
        assert!(!got.error.is_empty());
        assert_eq!(got.done, None);
    }

    #[test]
    fn request_defaults() {
        let r = Request::default().with_defaults();
        assert_eq!(r.cell_km, DEFAULT_CELL_KM);
        assert_eq!(r.target_leads, DEFAULT_TARGET_LEADS);
        assert_eq!(r.pages_per_tile, DEFAULT_TILE_PAGES);

        let custom = Request {
            cell_km: 1.0,
            target_leads: 7,
            pages_per_tile: 2,
            ..Request::default()
        }
        .with_defaults();
        assert_eq!(
            (custom.cell_km, custom.target_leads, custom.pages_per_tile),
            (1.0, 7, 2)
        );
    }
}

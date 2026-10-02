use std::sync::LazyLock;

use regex::Regex;
use serde::Deserialize;

use crate::domain::Lead;
use crate::gofmt::{format_g, query_escape};

#[derive(Debug, Clone, PartialEq)]
pub struct Version {
    pub name: &'static str,
    pub list_container: &'static [&'static str],
    pub list_items: &'static [&'static str],
    pub scroll_container: &'static [&'static str],
    pub search_area_button: &'static [&'static str],
    pub loading_indicator: &'static [&'static str],
    pub infinite_scroll: bool,
}

pub static VERSIONS: [Version; 2] = [
    Version {
        name: "new",
        list_container: &[".b_lstcards", r#"[data-automation-id="resultsList"]"#],
        list_items: &["[data-entity]", "[data-entity-id]", "li[data-key]"],
        scroll_container: &[".b_lstcards", r#"[data-automation-id="resultsList"]"#],
        search_area_button: &[
            "button[class*='searchThisAreaButton']",
            "button[data-automation-id='searchThisAreaButton']",
            "button[aria-label*='Search this area']",
        ],
        loading_indicator: &[
            ".b_waitlayer",
            "[class*='waitlayer']",
            "[class*='spinner']",
            "[class*='loader']",
        ],
        infinite_scroll: true,
    },
    Version {
        name: "legacy",
        list_container: &[".b_vList"],
        list_items: &["a.listings-item[data-entity]", "li a[data-entity]"],
        scroll_container: &[],
        search_area_button: &[],
        loading_indicator: &[".bm_waitlayer"],
        infinite_scroll: false,
    },
];

pub fn fallback() -> &'static Version {
    &VERSIONS[0]
}

#[derive(Deserialize)]
struct BingEntity {
    entity: Option<EntityFields>,
    #[serde(rename = "routablePoint")]
    routable_point: Option<RoutablePoint>,
    #[serde(rename = "infoboxHtml", default)]
    infobox_html: String,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct EntityFields {
    id: String,
    title: String,
    address: String,
    phone: String,
    website: String,
    #[serde(rename = "primaryCategoryName")]
    primary_category_name: String,
    #[serde(rename = "imageUrl")]
    image_url: String,
}

#[derive(Deserialize)]
struct RoutablePoint {
    latitude: Option<f64>,
    longitude: Option<f64>,
}

static RATING_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"([0-9]+\.?[0-9]*)[\t\n\f\r ]*/[\t\n\f\r ]*5").unwrap());
static COUNT_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\(([0-9]+)\)").unwrap());

pub fn parse_entity(raw: &str) -> Option<Lead> {
    let raw = unwrap_json_string(raw)?;
    let e: BingEntity = serde_json::from_str(&raw).ok()?;
    let entity = e.entity?;
    if entity.id.is_empty() {
        return None;
    }

    let (lat, lng) = match e.routable_point {
        Some(p) => (p.latitude, p.longitude),
        None => (None, None),
    };
    let (rating, rating_count) = extract_rating(&e.infobox_html);
    let maps_url = maps_url(&entity.title, lat, lng);

    Some(Lead {
        id: entity.id,
        name: entity.title,
        address: entity.address,
        phone: entity.phone,
        website: entity.website,
        category: entity.primary_category_name,
        rating,
        rating_count,
        latitude: lat,
        longitude: lng,
        open_hours: String::new(),
        image_url: entity.image_url,
        maps_url,
    })
}

fn unwrap_json_string(raw: &str) -> Option<String> {
    let mut raw = raw.to_string();
    for _ in 0..2 {
        if raw.is_empty() {
            return None;
        }
        if !raw.starts_with('"') {
            return Some(raw);
        }
        raw = serde_json::from_str::<String>(&raw).ok()?;
    }
    Some(raw)
}

fn extract_rating(html: &str) -> (String, String) {
    let rating = RATING_RE
        .captures(html)
        .map(|c| c[1].to_string())
        .unwrap_or_default();
    let count = COUNT_RE
        .captures(html)
        .map(|c| c[1].to_string())
        .unwrap_or_default();
    (rating, count)
}

fn maps_url(title: &str, lat: Option<f64>, lng: Option<f64>) -> String {
    match (lat, lng) {
        (Some(lat), Some(lng)) => format!(
            "https://www.bing.com/maps?q={}&cp={}~{}",
            query_escape(title),
            format_g(lat),
            format_g(lng)
        ),
        _ => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FULL: &str = r#"{"entity":{"id":"YN123","title":"Nutri Vida","address":"Rua A, 10",
        "phone":"+55 11 99999-0000","website":"https://nutrivida.com.br",
        "primaryCategoryName":"Nutricionista","imageUrl":"https://img/x.jpg"},
        "routablePoint":{"latitude":-23.55,"longitude":-46.63},
        "infoboxHtml":"<span>4.7 / 5</span><span>(128)</span>"}"#;

    #[test]
    fn full_object() {
        let lead = parse_entity(FULL).expect("ok");
        assert_eq!(lead.id, "YN123");
        assert_eq!(lead.name, "Nutri Vida");
        assert_eq!(lead.rating, "4.7");
        assert_eq!(lead.rating_count, "128");
        assert_eq!(lead.latitude, Some(-23.55));
        assert_eq!(
            lead.maps_url,
            "https://www.bing.com/maps?q=Nutri+Vida&cp=-23.55~-46.63"
        );
    }

    #[test]
    fn coordinates_round_trip_exactly() {
        let raw = r#"{"entity":{"id":"A","title":"T"},"routablePoint":{"latitude":-23.567733764648438,"longitude":-46.611244201660156}}"#;
        let lead = parse_entity(raw).expect("ok");
        let json = serde_json::to_string(&lead).unwrap();
        assert!(json.contains("-23.567733764648438"), "{json}");
        assert!(json.contains("-46.611244201660156"), "{json}");
        assert!(
            lead.maps_url
                .ends_with("cp=-23.567733764648438~-46.611244201660156")
        );
    }

    #[test]
    fn double_encoded_string() {
        let wrapped = serde_json::to_string(FULL).unwrap();
        assert_eq!(parse_entity(&wrapped).expect("ok").id, "YN123");
    }

    #[test]
    fn rejects_entries_without_id() {
        for raw in [
            r#"{"routablePoint":{"latitude":1,"longitude":2}}"#,
            r#"{"entity":{"id":"","title":"x"}}"#,
            r#"{nao e json"#,
            "",
        ] {
            assert!(parse_entity(raw).is_none(), "esperava None para {raw:?}");
        }
    }

    #[test]
    fn without_coordinates() {
        let lead = parse_entity(r#"{"entity":{"id":"A","title":"Sem Geo"}}"#).expect("ok");
        assert_eq!(lead.latitude, None);
        assert_eq!(lead.longitude, None);
        assert_eq!(lead.maps_url, "");
    }

    #[test]
    fn infobox_without_rating() {
        let lead = parse_entity(r#"{"entity":{"id":"A"},"infoboxHtml":"<span>aberto</span>"}"#)
            .expect("ok");
        assert_eq!(lead.rating, "");
        assert_eq!(lead.rating_count, "");
    }

    #[test]
    fn rating_ignores_non_ascii_whitespace_like_go() {
        let lead = parse_entity(r#"{"entity":{"id":"A"},"infoboxHtml":"4.5 / 5"}"#).expect("ok");
        assert_eq!(lead.rating, "");
    }
}

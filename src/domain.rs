use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct Lead {
    pub id: String,
    pub name: String,
    pub address: String,
    pub phone: String,
    pub website: String,
    pub category: String,
    pub rating: String,
    pub rating_count: String,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub open_hours: String,
    pub image_url: String,
    pub maps_url: String,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Job {
    pub query: String,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub zoom: Option<f64>,
    pub max_pages: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EventKind {
    Lead,
    Progress,
    Done,
    Error,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Event {
    #[serde(rename = "type")]
    pub kind: EventKind,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub data: Option<Lead>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub page: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub count: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub total: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub tile: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub tiles: Option<i64>,
    #[serde(skip_serializing_if = "String::is_empty", default)]
    pub message: String,
}

impl Event {
    fn new(kind: EventKind) -> Self {
        Event {
            kind,
            data: None,
            page: None,
            count: None,
            total: None,
            tile: None,
            tiles: None,
            message: String::new(),
        }
    }

    pub fn lead(lead: Lead) -> Self {
        Event {
            data: Some(lead),
            ..Event::new(EventKind::Lead)
        }
    }

    pub fn progress(page: usize, count: usize) -> Self {
        Event {
            page: Some(page as i64),
            count: Some(count as i64),
            ..Event::new(EventKind::Progress)
        }
    }

    pub fn tile_progress(tile: usize, tiles: usize, count: usize) -> Self {
        Event {
            page: Some(tile as i64),
            count: Some(count as i64),
            tile: Some(tile as i64),
            tiles: Some(tiles as i64),
            ..Event::new(EventKind::Progress)
        }
    }

    pub fn done(total: usize) -> Self {
        Event {
            total: Some(total as i64),
            ..Event::new(EventKind::Done)
        }
    }

    pub fn error(message: impl Into<String>) -> Self {
        Event {
            message: message.into(),
            ..Event::new(EventKind::Error)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn json(ev: &Event) -> String {
        serde_json::to_string(ev).unwrap()
    }

    #[test]
    fn event_json_matches_orbita_contract() {
        let lead = Lead {
            id: "A".into(),
            name: "X".into(),
            ..Lead::default()
        };
        assert_eq!(
            json(&Event::lead(lead)),
            r#"{"type":"lead","data":{"id":"A","name":"X","address":"","phone":"","website":"","category":"","rating":"","ratingCount":"","latitude":null,"longitude":null,"openHours":"","imageUrl":"","mapsUrl":""}}"#
        );
        assert_eq!(
            json(&Event::progress(0, 0)),
            r#"{"type":"progress","page":0,"count":0}"#
        );
        assert_eq!(
            json(&Event::progress(3, 42)),
            r#"{"type":"progress","page":3,"count":42}"#
        );
        assert_eq!(json(&Event::done(0)), r#"{"type":"done","total":0}"#);
        assert_eq!(
            json(&Event::error("boom")),
            r#"{"type":"error","message":"boom"}"#
        );
    }

    #[test]
    fn tile_progress_carries_tile_fields() {
        assert_eq!(
            json(&Event::tile_progress(2, 9, 17)),
            r#"{"type":"progress","page":2,"count":17,"tile":2,"tiles":9}"#
        );
    }

    #[test]
    fn lead_bson_keeps_camel_case_and_null_coordinates() {
        let doc = mongodb::bson::to_document(&Lead {
            id: "A".into(),
            rating_count: "12".into(),
            ..Lead::default()
        })
        .unwrap();
        assert_eq!(doc.get_str("ratingCount").unwrap(), "12");
        assert!(matches!(
            doc.get("latitude"),
            Some(mongodb::bson::Bson::Null)
        ));
        assert!(doc.contains_key("mapsUrl"));
    }
}

use std::sync::LazyLock;

use regex::Regex;
use unicode_normalization::{UnicodeNormalization, char::is_combining_mark};

use crate::domain::Lead;

const GEO_CELL_DEGREES: f64 = 0.0005;
const LEGAL_SUFFIXES: [&str; 7] = ["ltda", "me", "epp", "eireli", "sa", "cia", "mei"];

static NON_ALNUM: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"[^a-z0-9 ]+").unwrap());
static SPACES: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"[\t\n\f\r ]+").unwrap());
static NON_DIGIT: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"[^0-9]+").unwrap());

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Key(pub String);

impl std::fmt::Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

pub fn dedupe_key(lead: &Lead) -> Key {
    if let Some(phone) = phone(&lead.phone) {
        return Key(format!("tel:{phone}"));
    }

    let name = name(&lead.name);
    if name.is_empty() {
        if !lead.id.is_empty() {
            return Key(format!("src:{}", lead.id));
        }
        return Key(format!("addr:{}", address(&lead.address)));
    }

    if let (Some(lat), Some(lng)) = (lead.latitude, lead.longitude) {
        return Key(format!("geo:{name}@{}", cell(lat, lng)));
    }

    let addr = address(&lead.address);
    if !addr.is_empty() {
        return Key(format!("name-addr:{name}@{addr}"));
    }
    Key(format!("name:{name}"))
}

fn cell(lat: f64, lng: f64) -> String {
    format!(
        "{},{}",
        (lat / GEO_CELL_DEGREES).round() as i64,
        (lng / GEO_CELL_DEGREES).round() as i64
    )
}

fn strip_accents(s: &str) -> String {
    s.nfd().filter(|c| !is_combining_mark(*c)).nfc().collect()
}

fn clean(s: &str) -> String {
    let s = strip_accents(s).to_lowercase();
    let s = NON_ALNUM.replace_all(&s, " ");
    let s = SPACES.replace_all(&s, " ");
    s.trim().to_string()
}

pub fn name(s: &str) -> String {
    let cleaned = clean(s);
    let mut fields: Vec<&str> = cleaned.split_whitespace().collect();
    while fields.len() > 1 && LEGAL_SUFFIXES.contains(fields.last().unwrap()) {
        fields.pop();
    }
    fields.join(" ")
}

pub fn phone(raw: &str) -> Option<String> {
    let digits = NON_DIGIT.replace_all(raw, "");
    let mut d: &str = digits.strip_prefix("00").unwrap_or(&digits);

    if d.starts_with("55") && (d.len() == 12 || d.len() == 13) {
        d = &d[2..];
    }
    if d.len() != 10 && d.len() != 11 {
        return None;
    }

    let (ddd, sub) = d.split_at(2);
    if ddd.starts_with('0') {
        return None;
    }
    let first = sub.as_bytes()[0];
    if sub.len() == 9 && first != b'9' {
        return None;
    }
    if sub.len() == 8 && first < b'2' {
        return None;
    }

    Some(format!("+55{ddd}{sub}"))
}

pub fn address(s: &str) -> String {
    clean(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(name: &str, lat: f64, lng: f64) -> Lead {
        Lead {
            name: name.into(),
            latitude: Some(lat),
            longitude: Some(lng),
            ..Lead::default()
        }
    }

    #[test]
    fn normalizes_names() {
        for (input, want) in [
            ("Padaria do Zé", "padaria do ze"),
            ("PADARIA DO ZE LTDA", "padaria do ze"),
            ("Padaria  do   Zé!!!", "padaria do ze"),
            ("Restaurante Capim Santo ME", "restaurante capim santo"),
            ("Açaí & Cia", "acai"),
            ("Ltda", "ltda"),
        ] {
            assert_eq!(name(input), want, "name({input:?})");
        }
    }

    #[test]
    fn normalizes_valid_phones() {
        for (input, want) in [
            ("(11) 99999-0000", "+5511999990000"),
            ("11999990000", "+5511999990000"),
            ("+55 11 99999-0000", "+5511999990000"),
            ("5511999990000", "+5511999990000"),
            ("(11) 3088-0766", "+551130880766"),
            ("0055 11 3088 0766", "+551130880766"),
        ] {
            assert_eq!(phone(input).as_deref(), Some(want), "phone({input:?})");
        }
    }

    #[test]
    fn rejects_invalid_phones() {
        for input in [
            "",
            "123",
            "9999-0000",
            "(11) 19999-0000",
            "(01) 99999-0000",
            "abc",
        ] {
            assert_eq!(phone(input), None, "phone({input:?})");
        }
    }

    #[test]
    fn same_business_collides() {
        let a = at("Padaria do Zé", -23.5505, -46.6333);
        let b = at("PADARIA DO ZE LTDA", -23.55052, -46.63332);
        assert_eq!(dedupe_key(&a), dedupe_key(&b));
    }

    #[test]
    fn phone_wins_across_sources() {
        let a = Lead {
            phone: "(11) 3088-0766".into(),
            ..at("Restaurante X", -23.5, -46.6)
        };
        let b = Lead {
            name: "Restaurante X - Unidade Centro".into(),
            phone: "+55 11 3088-0766".into(),
            ..Lead::default()
        };
        assert_eq!(dedupe_key(&a), dedupe_key(&b));
    }

    #[test]
    fn distinct_places_stay_apart() {
        let a = at("Padaria do Zé", -23.5505, -46.6333);
        let far = at("Padaria do Zé", -23.5605, -46.6433);
        let other = at("Padaria da Maria", -23.5505, -46.6333);
        assert_ne!(dedupe_key(&a), dedupe_key(&far));
        assert_ne!(dedupe_key(&a), dedupe_key(&other));
    }

    #[test]
    fn fallbacks() {
        let no_geo = Lead {
            name: "Bar do João".into(),
            address: "Rua A, 10".into(),
            ..Lead::default()
        };
        let same = Lead {
            name: "BAR DO JOAO".into(),
            address: "rua a 10".into(),
            ..Lead::default()
        };
        assert_eq!(dedupe_key(&no_geo), dedupe_key(&same));

        let no_name = Lead {
            id: "ypid:ABC".into(),
            address: "Rua B, 20".into(),
            ..Lead::default()
        };
        assert_eq!(dedupe_key(&no_name), Key("src:ypid:ABC".into()));
    }
}

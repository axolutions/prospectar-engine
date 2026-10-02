pub fn query_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

pub fn encode_query(pairs: &[(&str, String)]) -> String {
    let mut sorted: Vec<&(&str, String)> = pairs.iter().collect();
    sorted.sort_by(|a, b| a.0.cmp(b.0));
    sorted
        .iter()
        .map(|(k, v)| format!("{}={}", query_escape(k), query_escape(v)))
        .collect::<Vec<_>>()
        .join("&")
}

pub fn format_g(v: f64) -> String {
    if v == 0.0 {
        return if v.is_sign_negative() {
            "-0".into()
        } else {
            "0".into()
        };
    }
    if !v.is_finite() {
        return if v.is_nan() {
            "NaN".into()
        } else if v > 0.0 {
            "+Inf".into()
        } else {
            "-Inf".into()
        };
    }
    let sign = if v < 0.0 { "-" } else { "" };
    let (digits, exp) = shortest_digits(v.abs());

    if !(-4..6).contains(&exp) {
        let mantissa = match digits.len() {
            1 => digits.clone(),
            _ => format!("{}.{}", &digits[..1], &digits[1..]),
        };
        let exp_sign = if exp < 0 { '-' } else { '+' };
        return format!("{sign}{mantissa}e{exp_sign}{:02}", exp.abs());
    }

    if exp < 0 {
        let zeros = "0".repeat((-exp - 1) as usize);
        return format!("{sign}0.{zeros}{digits}");
    }
    let int_len = exp as usize + 1;
    if digits.len() <= int_len {
        let zeros = "0".repeat(int_len - digits.len());
        format!("{sign}{digits}{zeros}")
    } else {
        format!("{sign}{}.{}", &digits[..int_len], &digits[int_len..])
    }
}

fn shortest_digits(v: f64) -> (String, i32) {
    let repr = serde_json::to_string(&v).unwrap_or_default();
    let (mantissa, exp) = match repr.split_once(['e', 'E']) {
        Some((m, e)) => (m, e.parse::<i32>().unwrap_or(0)),
        None => (repr.as_str(), 0),
    };
    let (int_part, frac_part) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let all = format!("{int_part}{frac_part}");
    let leading = all.bytes().take_while(|&b| b == b'0').count();
    let digits = all[leading..].trim_end_matches('0');
    let exp = exp + int_part.len() as i32 - 1 - leading as i32;
    if digits.is_empty() {
        ("0".into(), 0)
    } else {
        (digits.to_string(), exp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_escape_matches_go() {
        assert_eq!(
            query_escape("nutricionista em São Paulo"),
            "nutricionista+em+S%C3%A3o+Paulo"
        );
        assert_eq!(query_escape("-23.55~-46.63"), "-23.55~-46.63");
        assert_eq!(query_escape("a&b=c/d*"), "a%26b%3Dc%2Fd%2A");
    }

    #[test]
    fn encode_query_sorts_keys_like_url_values() {
        let q = encode_query(&[
            ("q", "x".into()),
            ("style", "r".into()),
            ("cp", "1~2".into()),
            ("lvl", "16".into()),
        ]);
        assert_eq!(q, "cp=1~2&lvl=16&q=x&style=r");
    }

    #[test]
    fn format_g_matches_go_percent_g() {
        assert_eq!(format_g(-22.899429321289062), "-22.899429321289062");
        assert_eq!(format_g(-22.865737915039062), "-22.865737915039062");
        assert_eq!(format_g(100000.0), "100000");
        assert_eq!(format_g(-0.00012), "-0.00012");
        assert_eq!(format_g(1e21), "1e+21");
        assert_eq!(format_g(-23.55), "-23.55");
        assert_eq!(format_g(1.0), "1");
        assert_eq!(format_g(12.0), "12");
        assert_eq!(format_g(-46.6333), "-46.6333");
        assert_eq!(format_g(0.0001), "0.0001");
        assert_eq!(format_g(0.00001), "1e-05");
        assert_eq!(format_g(1234567.0), "1.234567e+06");
        assert_eq!(format_g(123456.0), "123456");
    }
}

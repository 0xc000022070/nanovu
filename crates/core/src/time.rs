use jiff::Timestamp;
use serde::Serializer;

pub const SECOND: i64 = 1_000;
pub const MINUTE: i64 = 60 * SECOND;
pub const HOUR: i64 = 60 * MINUTE;
pub const DAY: i64 = 24 * HOUR;

pub fn now_ms() -> i64 {
    Timestamp::now().as_millisecond()
}

pub fn iso(ms: i64) -> String {
    Timestamp::from_millisecond(ms)
        .map(|ts| format!("{ts:.3}"))
        .unwrap_or_default()
}

pub fn iso_ser<S: Serializer>(ms: &i64, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&iso(*ms))
}

pub fn iso_ser_opt<S: Serializer>(ms: &Option<i64>, s: S) -> Result<S::Ok, S::Error> {
    match ms {
        Some(ms) => s.serialize_str(&iso(*ms)),
        None => s.serialize_none(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iso_has_millisecond_precision() {
        assert_eq!(iso(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(iso(1_767_323_045_006), "2026-01-02T03:04:05.006Z");
    }
}

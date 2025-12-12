use crate::json::{parse_strict_json, JsonError, JsonValue};

pub fn strict_parse(text: &str) -> Result<JsonValue, JsonError> {
    parse_strict_json(text)
}


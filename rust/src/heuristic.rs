use crate::types::{RepairAction, RepairOptions};

fn fix_smart_quotes(text: &str) -> (String, Vec<RepairAction>) {
    let mut out = String::with_capacity(text.len());
    let mut changed = false;
    for ch in text.chars() {
        match ch {
            '\u{201C}' | '\u{201D}' => {
                out.push('"');
                changed = true;
            }
            '\u{2018}' | '\u{2019}' => {
                out.push('\'');
                changed = true;
            }
            _ => out.push(ch),
        }
    }
    if changed {
        (out, vec![RepairAction::new("fix_smart_quotes", 0.7)])
    } else {
        (text.to_string(), vec![])
    }
}

fn strip_comments(text: &str) -> (String, Vec<RepairAction>) {
    let bytes = text.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut repairs = Vec::new();
    let mut i: usize = 0;
    let mut in_string = false;
    let mut escape = false;
    while i < bytes.len() {
        let ch = bytes[i];
        if in_string {
            out.push(ch);
            if escape {
                escape = false;
            } else if ch == b'\\' {
                escape = true;
            } else if ch == b'"' {
                in_string = false;
            }
            i += 1;
            continue;
        }

        if ch == b'"' {
            in_string = true;
            out.push(ch);
            i += 1;
            continue;
        }

        if ch == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'/' {
            let start = i;
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' && bytes[i] != b'\r' {
                i += 1;
            }
            let mut a = RepairAction::new("strip_line_comment", 0.4);
            a.span = Some((start, i));
            repairs.push(a);
            continue;
        }

        if ch == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
            let start = i;
            i += 2;
            while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                i += 1;
            }
            i = (i + 2).min(bytes.len());
            let mut a = RepairAction::new("strip_block_comment", 0.6);
            a.span = Some((start, i));
            repairs.push(a);
            continue;
        }

        out.push(ch);
        i += 1;
    }
    (String::from_utf8_lossy(&out).to_string(), repairs)
}

fn normalize_python_literals(text: &str) -> (String, Vec<RepairAction>) {
    let bytes = text.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut repairs = Vec::new();
    let mut i: usize = 0;
    let mut in_string = false;
    let mut escape = false;
    while i < bytes.len() {
        let ch = bytes[i];
        if in_string {
            out.push(ch);
            if escape {
                escape = false;
            } else if ch == b'\\' {
                escape = true;
            } else if ch == b'"' {
                in_string = false;
            }
            i += 1;
            continue;
        }

        if ch == b'"' {
            in_string = true;
            out.push(ch);
            i += 1;
            continue;
        }

        if (ch as char).is_ascii_alphabetic() || ch == b'_' {
            let start = i;
            i += 1;
            while i < bytes.len() && (((bytes[i] as char).is_ascii_alphanumeric()) || bytes[i] == b'_') {
                i += 1;
            }
            let word = &text[start..i];
            let mapped = match word {
                "True" => Some("true"),
                "False" => Some("false"),
                "None" => Some("null"),
                "undefined" => Some("null"),
                _ => None,
            };
            if let Some(m) = mapped {
                out.extend_from_slice(m.as_bytes());
                let mut a = RepairAction::new("map_python_literal", 0.4);
                a.span = Some((start, i));
                a.note = Some(format!("{word}->{m}"));
                repairs.push(a);
            } else {
                out.extend_from_slice(word.as_bytes());
            }
            continue;
        }

        out.push(ch);
        i += 1;
    }
    (String::from_utf8_lossy(&out).to_string(), repairs)
}

fn is_ws(b: u8) -> bool {
    matches!(b, b' ' | b'\n' | b'\r' | b'\t')
}

fn remove_trailing_commas(text: &str) -> (String, Vec<RepairAction>) {
    let bytes = text.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut repairs = Vec::new();
    let mut i: usize = 0;
    let mut in_string = false;
    let mut escape = false;
    while i < bytes.len() {
        let ch = bytes[i];
        if in_string {
            out.push(ch);
            if escape {
                escape = false;
            } else if ch == b'\\' {
                escape = true;
            } else if ch == b'"' {
                in_string = false;
            }
            i += 1;
            continue;
        }

        if ch == b'"' {
            in_string = true;
            out.push(ch);
            i += 1;
            continue;
        }

        if ch == b',' {
            let mut j = i + 1;
            while j < bytes.len() && is_ws(bytes[j]) {
                j += 1;
            }
            if j >= bytes.len() || bytes[j] == b'}' || bytes[j] == b']' {
                let mut a = RepairAction::new("remove_trailing_comma", 0.2);
                a.at = Some(i);
                repairs.push(a);
                i += 1;
                continue;
            }
        }

        out.push(ch);
        i += 1;
    }
    (String::from_utf8_lossy(&out).to_string(), repairs)
}

fn append_missing_closers(text: &str) -> (String, Vec<RepairAction>) {
    let bytes = text.as_bytes();
    let mut in_string = false;
    let mut escape = false;
    let mut depth_brace: i64 = 0;
    let mut depth_bracket: i64 = 0;
    let mut i: usize = 0;
    while i < bytes.len() {
        let ch = bytes[i];
        if in_string {
            if escape {
                escape = false;
            } else if ch == b'\\' {
                escape = true;
            } else if ch == b'"' {
                in_string = false;
            }
            i += 1;
            continue;
        }
        if ch == b'"' {
            in_string = true;
            i += 1;
            continue;
        }
        match ch {
            b'{' => depth_brace += 1,
            b'}' => depth_brace -= 1,
            b'[' => depth_bracket += 1,
            b']' => depth_bracket -= 1,
            _ => {}
        }
        i += 1;
    }

    let mut out = text.to_string();
    let mut repairs = Vec::new();
    if in_string {
        out.push('"');
        let mut a = RepairAction::new("close_open_string", 3.0);
        a.at = Some(text.len());
        repairs.push(a);
    }

    let b = depth_brace.max(0) as usize;
    let a = depth_bracket.max(0) as usize;
    if b > 0 || a > 0 {
        out.push_str(&"]".repeat(a));
        out.push_str(&"}".repeat(b));
        let mut act = RepairAction::new("close_containers", 0.5 * ((a + b) as f64));
        act.at = Some(text.len());
        act.note = Some(format!("brace={b}, bracket={a}"));
        repairs.push(act);
    }
    (out, repairs)
}

pub fn heuristic_repair(extracted_text: &str, opt: &RepairOptions) -> (String, Vec<RepairAction>) {
    let mut text = extracted_text.to_string();
    let mut repairs: Vec<RepairAction> = Vec::new();

    let (t2, r2) = fix_smart_quotes(&text);
    if t2 != text {
        text = t2;
        repairs.extend(r2);
    }

    if opt.allow_comments {
        let (t2, r2) = strip_comments(&text);
        if t2 != text {
            text = t2;
            repairs.extend(r2);
        }
    }

    if opt.allow_python_literals {
        let (t2, r2) = normalize_python_literals(&text);
        if t2 != text {
            text = t2;
            repairs.extend(r2);
        }
    }

    let (t2, r2) = remove_trailing_commas(&text);
    if t2 != text {
        text = t2;
        repairs.extend(r2);
    }

    let (t2, r2) = append_missing_closers(&text);
    if t2 != text {
        text = t2;
        repairs.extend(r2);
    }

    (text, repairs)
}


use std::sync::{Arc, Mutex};

use crate::json::{parse_strict_json, JsonValue};
use crate::types::RepairOptions;

pub type SplitMode = &'static str;

pub const SPLIT_NO_SPLIT: SplitMode = "NO_SPLIT";
pub const SPLIT_ROOT_ARRAY_ELEMENTS: SplitMode = "ROOT_ARRAY_ELEMENTS";

#[derive(Debug, Clone, PartialEq)]
pub struct SplitPlan {
    pub mode: SplitMode,
    pub elements: usize,
    pub structural_density: f64,
    pub chunk_count: usize,
}

fn is_ws(b: u8) -> bool {
    matches!(b, b'\t' | b'\n' | b'\r' | b' ')
}

fn trim_ws(data: &[u8]) -> (usize, usize) {
    let mut start = 0usize;
    let mut end = data.len();
    // UTF-8 BOM
    if end >= 3 && &data[..3] == b"\xEF\xBB\xBF" {
        start = 3;
    }
    while start < end && is_ws(data[start]) {
        start += 1;
    }
    while end > start && is_ws(data[end - 1]) {
        end -= 1;
    }
    (start, end)
}

fn iter_root_array_element_spans(data: &[u8], start: usize, end: usize) -> Vec<(usize, usize)> {
    let mut spans: Vec<(usize, usize)> = Vec::new();
    if start >= end || data.get(start) != Some(&b'[') || data.get(end - 1) != Some(&b']') {
        return spans;
    }

    let mut i = start + 1;
    while i < end && is_ws(data[i]) {
        i += 1;
    }
    if i >= end.saturating_sub(1) {
        return spans; // empty array
    }

    let mut elem_start = i;
    let mut in_string = false;
    let mut escape = false;
    let mut depth_brace: i64 = 0;
    let mut depth_bracket: i64 = 1; // root '[' already entered

    for i in (start + 1)..(end - 1) {
        let ch = data[i];
        if in_string {
            if escape {
                escape = false;
            } else if ch == b'\\' {
                escape = true;
            } else if ch == b'"' {
                in_string = false;
            }
            continue;
        }

        if ch == b'"' {
            in_string = true;
            continue;
        }

        match ch {
            b'{' => depth_brace += 1,
            b'}' => depth_brace -= 1,
            b'[' => depth_bracket += 1,
            b']' => depth_bracket -= 1,
            _ => {}
        }

        if ch == b',' && depth_brace == 0 && depth_bracket == 1 {
            let elem_end = i;
            // trim whitespace around element
            let mut s = elem_start;
            let mut e = elem_end;
            while s < e && is_ws(data[s]) {
                s += 1;
            }
            while e > s && is_ws(data[e - 1]) {
                e -= 1;
            }
            if e > s {
                spans.push((s, e));
            }
            elem_start = i + 1;
        }
    }

    // last element
    let mut s = elem_start;
    let mut e = end - 1;
    while s < e && is_ws(data[s]) {
        s += 1;
    }
    while e > s && is_ws(data[e - 1]) {
        e -= 1;
    }
    if e > s {
        spans.push((s, e));
    }

    spans
}

fn parse_task_bytes(data: &[u8], spans: &[(usize, usize)]) -> Result<Vec<JsonValue>, String> {
    let mut payload: Vec<u8> = Vec::new();
    payload.push(b'[');
    for (idx, (s, e)) in spans.iter().enumerate() {
        if idx > 0 {
            payload.push(b',');
        }
        payload.extend_from_slice(&data[*s..*e]);
    }
    payload.push(b']');
    let s = std::str::from_utf8(&payload).map_err(|e| format!("invalid utf-8 in task payload: {e}"))?;
    let v = parse_strict_json(s).map_err(|e| format!("strict parse failed in task payload: {} at {}", e.message, e.pos))?;
    match v {
        JsonValue::Array(a) => Ok(a),
        _ => Err("task payload did not parse to array".to_string()),
    }
}

fn allow_parallel_bool(opt: &RepairOptions) -> Option<bool> {
    let s = opt.allow_parallel.trim().to_ascii_lowercase();
    if s == "auto" {
        None
    } else if s == "true" || s == "1" || s == "yes" {
        Some(true)
    } else {
        Some(false)
    }
}

fn root_array_split_plan(
    data: &[u8],
    start: usize,
    end: usize,
    opt: &RepairOptions,
) -> (SplitPlan, Vec<Vec<(usize, usize)>>) {
    let spans = iter_root_array_element_spans(data, start, end);
    let elements = spans.len();

    // Structural density approximation (outside strings is expensive to recompute);
    // use a small scan for delimiters.
    let mut structural: usize = 0;
    let mut in_string = false;
    let mut escape = false;
    for &ch in &data[start..end] {
        if in_string {
            if escape {
                escape = false;
            } else if ch == b'\\' {
                escape = true;
            } else if ch == b'"' {
                in_string = false;
            }
            continue;
        }
        if ch == b'"' {
            in_string = true;
            continue;
        }
        if matches!(ch, b'{' | b'}' | b'[' | b']' | b',' | b':') {
            structural += 1;
        }
    }
    let structural_density = (structural as f64) / ((end - start).max(1) as f64);

    let do_parallel = match allow_parallel_bool(opt) {
        None => {
            (end - start) >= opt.parallel_threshold_bytes
                && elements >= opt.min_elements_for_parallel
                && structural_density >= opt.density_threshold
        }
        Some(v) => v,
    };

    if !do_parallel || elements <= 1 {
        return (
            SplitPlan {
                mode: SPLIT_NO_SPLIT,
                elements,
                structural_density,
                chunk_count: 1,
            },
            vec![spans],
        );
    }

    let target = std::cmp::max(1_000_000usize, opt.parallel_chunk_bytes);
    let mut tasks: Vec<Vec<(usize, usize)>> = Vec::new();
    let mut cur: Vec<(usize, usize)> = Vec::new();
    let mut cur_bytes: usize = 0;
    for (s, e) in spans {
        cur.push((s, e));
        cur_bytes += e - s;
        if !cur.is_empty() && cur_bytes >= target {
            tasks.push(cur);
            cur = Vec::new();
            cur_bytes = 0;
        }
    }
    if !cur.is_empty() {
        tasks.push(cur);
    }

    (
        SplitPlan {
            mode: SPLIT_ROOT_ARRAY_ELEMENTS,
            elements,
            structural_density,
            chunk_count: tasks.len(),
        },
        tasks,
    )
}

pub fn parse_root_array_scale(data: &[u8], opt: &RepairOptions) -> Result<(JsonValue, SplitPlan), String> {
    let (s0, e0) = trim_ws(data);
    if e0.saturating_sub(s0) <= 2 || data.get(s0) != Some(&b'[') || data.get(e0 - 1) != Some(&b']') {
        let s = std::str::from_utf8(&data[s0..e0]).map_err(|e| format!("invalid utf-8: {e}"))?;
        let value =
            parse_strict_json(s).map_err(|e| format!("strict parse failed: {} at {}", e.message, e.pos))?;
        return Ok((
            value,
            SplitPlan {
                mode: SPLIT_NO_SPLIT,
                elements: 0,
                structural_density: 0.0,
                chunk_count: 1,
            },
        ));
    }

    let (plan, tasks) = root_array_split_plan(data, s0, e0, opt);
    if plan.mode == SPLIT_NO_SPLIT {
        let elems = parse_task_bytes(data, &tasks[0])?;
        return Ok((JsonValue::Array(elems), plan));
    }

    let workers = opt.parallel_workers.unwrap_or_else(|| {
        std::thread::available_parallelism().map(|n| n.get()).unwrap_or(2)
    });
    let workers = std::cmp::max(1usize, workers);

    // NOTE: Python supports a process+shared-memory backend; in Rust std-only we treat
    // both "process" and "thread" as a thread backend.
    let tasks = Arc::new(tasks);
    let data = Arc::new(data.to_vec());
    let results: Arc<Mutex<Vec<Option<Vec<JsonValue>>>>> = Arc::new(Mutex::new(vec![None; tasks.len()]));
    let next_idx: Arc<Mutex<usize>> = Arc::new(Mutex::new(0usize));

    let mut handles = Vec::new();
    for _ in 0..workers.min(tasks.len()) {
        let tasks = Arc::clone(&tasks);
        let data = Arc::clone(&data);
        let results = Arc::clone(&results);
        let next_idx = Arc::clone(&next_idx);
        handles.push(std::thread::spawn(move || -> Result<(), String> {
            loop {
                let idx = {
                    let mut g = next_idx.lock().map_err(|_| "mutex poisoned".to_string())?;
                    let idx = *g;
                    *g += 1;
                    idx
                };
                if idx >= tasks.len() {
                    break;
                }
                let chunk = parse_task_bytes(&data, &tasks[idx])?;
                let mut r = results.lock().map_err(|_| "mutex poisoned".to_string())?;
                r[idx] = Some(chunk);
            }
            Ok(())
        }));
    }

    for h in handles {
        match h.join() {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(e),
            Err(_) => return Err("worker panicked".to_string()),
        }
    }

    let mut out: Vec<JsonValue> = Vec::new();
    let r = results.lock().map_err(|_| "mutex poisoned".to_string())?;
    for vs in r.iter().flatten() {
        out.extend_from_slice(vs);
    }
    Ok((JsonValue::Array(out), plan))
}


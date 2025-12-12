use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};

use json_prob_parser::beam;
use json_prob_parser::json::JsonValue;
use json_prob_parser::scale;
use json_prob_parser::{extract, heuristic, strict};
use json_prob_parser::types::{Candidate, RepairAction, RepairOptions};

fn json_to_py(py: Python<'_>, v: &JsonValue) -> PyObject {
    match v {
        JsonValue::Null => py.None(),
        JsonValue::Bool(b) => b.to_object(py),
        JsonValue::NumberI64(n) => n.to_object(py),
        JsonValue::NumberU64(n) => n.to_object(py),
        JsonValue::NumberF64(n) => n.to_object(py),
        JsonValue::String(s) => s.to_object(py),
        JsonValue::Array(a) => {
            let list = PyList::empty_bound(py);
            for item in a {
                list.append(json_to_py(py, item)).unwrap();
            }
            list.to_object(py)
        }
        JsonValue::Object(obj) => {
            let d = PyDict::new_bound(py);
            for (k, vv) in obj {
                d.set_item(k, json_to_py(py, vv)).unwrap();
            }
            d.to_object(py)
        }
    }
}

fn py_to_repair_action(obj: &Bound<'_, PyAny>) -> PyResult<RepairAction> {
    let d = obj.downcast::<PyDict>()?;

    let op: String = d.get_item("op")?
        .and_then(|x| x.extract().ok())
        .unwrap_or_default();

    let cost_delta: f64 = d.get_item("cost_delta")?
        .and_then(|x| x.extract().ok())
        .unwrap_or(0.0);

    let mut act = RepairAction::new(&op, cost_delta);

    if let Some(span) = d.get_item("span")? {
        if !span.is_none() {
            let (s, e): (usize, usize) = span.extract()?;
            act.span = Some((s, e));
        }
    }

    if let Some(at) = d.get_item("at")? {
        if !at.is_none() {
            act.at = Some(at.extract()?);
        }
    }

    if let Some(token) = d.get_item("token")? {
        if !token.is_none() {
            act.token = Some(token.extract()?);
        }
    }

    if let Some(note) = d.get_item("note")? {
        if !note.is_none() {
            act.note = Some(note.extract()?);
        }
    }

    Ok(act)
}

fn options_from_dict(d: Option<&Bound<'_, PyDict>>) -> PyResult<RepairOptions> {
    let mut opt = RepairOptions::default();
    let Some(d) = d else { return Ok(opt) };

    macro_rules! set_opt {
        ($key:literal, $field:ident, $ty:ty) => {
            if let Some(v) = d.get_item($key)? {
                if !v.is_none() {
                    opt.$field = v.extract::<$ty>()?;
                }
            }
        };
    }

    set_opt!("mode", mode, String);
    set_opt!("top_k", top_k, usize);
    set_opt!("beam_width", beam_width, usize);
    set_opt!("max_repairs", max_repairs, usize);
    set_opt!("max_deleted_tokens", max_deleted_tokens, usize);
    set_opt!("max_close_open_string", max_close_open_string, usize);
    set_opt!("max_garbage_skip_bytes", max_garbage_skip_bytes, usize);
    set_opt!("confidence_alpha", confidence_alpha, f64);
    set_opt!("partial_ok", partial_ok, bool);

    set_opt!("allow_single_quotes", allow_single_quotes, bool);
    set_opt!("allow_unquoted_keys", allow_unquoted_keys, bool);
    set_opt!("allow_unquoted_values", allow_unquoted_values, bool);
    set_opt!("allow_comments", allow_comments, bool);
    set_opt!("allow_python_literals", allow_python_literals, bool);

    set_opt!("allow_parallel", allow_parallel, String);
    set_opt!("parallel_threshold_bytes", parallel_threshold_bytes, usize);
    set_opt!("min_elements_for_parallel", min_elements_for_parallel, usize);
    set_opt!("density_threshold", density_threshold, f64);
    set_opt!("parallel_chunk_bytes", parallel_chunk_bytes, usize);

    if let Some(v) = d.get_item("parallel_workers")? {
        if v.is_none() {
            opt.parallel_workers = None;
        } else {
            let n: usize = v.extract()?;
            opt.parallel_workers = if n == 0 { None } else { Some(n) };
        }
    }
    set_opt!("parallel_backend", parallel_backend, String);
    set_opt!("scale_output", scale_output, String);

    if let Some(v) = d.get_item("schema")? {
        if v.is_none() {
            opt.schema = None;
        } else {
            opt.schema = Some(py_to_json(&v)?);
        }
    }

    Ok(opt)
}

fn py_to_json(obj: &Bound<'_, PyAny>) -> PyResult<JsonValue> {
    if obj.is_none() {
        return Ok(JsonValue::Null);
    }
    if let Ok(b) = obj.extract::<bool>() {
        return Ok(JsonValue::Bool(b));
    }
    if let Ok(i) = obj.extract::<i64>() {
        return Ok(JsonValue::NumberI64(i));
    }
    if let Ok(f) = obj.extract::<f64>() {
        return Ok(JsonValue::NumberF64(f));
    }
    if let Ok(s) = obj.extract::<String>() {
        return Ok(JsonValue::String(s));
    }
    if let Ok(list) = obj.downcast::<PyList>() {
        let mut out: Vec<JsonValue> = Vec::with_capacity(list.len());
        for item in list.iter() {
            out.push(py_to_json(&item)?);
        }
        return Ok(JsonValue::Array(out));
    }
    if let Ok(d) = obj.downcast::<PyDict>() {
        let mut out: Vec<(String, JsonValue)> = Vec::new();
        for (k, v) in d.iter() {
            let kk: String = k.extract()?;
            out.push((kk, py_to_json(&v)?));
        }
        return Ok(JsonValue::Object(out));
    }
    Ok(JsonValue::String(obj.str()?.to_string_lossy().to_string()))
}

fn candidate_to_pydict<'py>(py: Python<'py>, c: &Candidate) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new_bound(py);
    d.set_item("candidate_id", c.candidate_id)?;
    d.set_item("value", c.value.as_ref().map(|v| json_to_py(py, v)).unwrap_or(py.None()))?;
    d.set_item("normalized_json", c.normalized_json.clone())?;
    d.set_item("ir", c.ir.as_ref().map(|v| json_to_py(py, v)).unwrap_or(py.None()))?;
    d.set_item("confidence", c.confidence)?;
    d.set_item("cost", c.cost)?;

    let repairs = PyList::empty_bound(py);
    for r in &c.repairs {
        let rr = PyDict::new_bound(py);
        rr.set_item("op", r.op.clone())?;
        rr.set_item("span", r.span)?;
        rr.set_item("at", r.at)?;
        rr.set_item("token", r.token.clone())?;
        rr.set_item("cost_delta", r.cost_delta)?;
        rr.set_item("note", r.note.clone())?;
        repairs.append(rr)?;
    }
    d.set_item("repairs", repairs)?;

    let validations = PyDict::new_bound(py);
    validations.set_item("strict_json_parse", c.validations.strict_json_parse)?;
    validations.set_item("schema_match", c.validations.schema_match)?;
    d.set_item("validations", validations)?;

    let diag = PyDict::new_bound(py);
    diag.set_item("garbage_skipped_bytes", c.diagnostics.garbage_skipped_bytes)?;
    diag.set_item("deleted_tokens", c.diagnostics.deleted_tokens)?;
    diag.set_item("inserted_tokens", c.diagnostics.inserted_tokens)?;
    diag.set_item("close_open_string_count", c.diagnostics.close_open_string_count)?;
    diag.set_item("beam_width", c.diagnostics.beam_width)?;
    diag.set_item("max_repairs", c.diagnostics.max_repairs)?;
    d.set_item("diagnostics", diag)?;

    let dropped = PyList::empty_bound(py);
    for (s, e) in &c.dropped_spans {
        dropped.append((*s, *e))?;
    }
    d.set_item("dropped_spans", dropped)?;
    Ok(d)
}

fn repair_action_to_pydict<'py>(py: Python<'py>, r: &RepairAction) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new_bound(py);
    d.set_item("op", r.op.clone())?;
    d.set_item("span", r.span)?;
    d.set_item("at", r.at)?;
    d.set_item("token", r.token.clone())?;
    d.set_item("cost_delta", r.cost_delta)?;
    d.set_item("note", r.note.clone())?;
    Ok(d)
}

#[pyfunction]
fn parse_py(py: Python<'_>, input: &Bound<'_, PyAny>, options: Option<&Bound<'_, PyDict>>) -> PyResult<PyObject> {
    let mut opt = options_from_dict(options)?;
    // LLM orchestration is done in Python; keep Rust strictly deterministic here.
    opt.allow_llm = false;

    let result = if let Ok(b) = input.downcast::<PyBytes>() {
        json_prob_parser::parse_bytes(b.as_bytes(), &opt)
    } else if let Ok(s) = input.extract::<String>() {
        json_prob_parser::parse(&s, &opt)
    } else {
        return Err(pyo3::exceptions::PyTypeError::new_err("input must be str or bytes"));
    };

    Ok(json_to_py(py, &result.to_json_value()))
}

#[pyfunction]
fn preprocess_py(py: Python<'_>, input: &Bound<'_, PyAny>, options: Option<&Bound<'_, PyDict>>) -> PyResult<PyObject> {
    let opt = options_from_dict(options)?;

    let text = if let Ok(b) = input.downcast::<PyBytes>() {
        String::from_utf8_lossy(b.as_bytes()).to_string()
    } else if let Ok(s) = input.extract::<String>() {
        s
    } else {
        return Err(pyo3::exceptions::PyTypeError::new_err("input must be str or bytes"));
    };

    let extraction = extract::extract_json_candidate(&text);
    let extracted_text = extraction.extracted.clone();
    let (repaired_text, heuristic_repairs) = heuristic::heuristic_repair(&extracted_text, &opt);

    let mut base_repairs: Vec<RepairAction> = Vec::new();
    base_repairs.extend_from_slice(&extraction.repairs);
    base_repairs.extend_from_slice(&heuristic_repairs);

    let error_pos = strict::strict_parse(&repaired_text).err().map(|e| e.pos);

    let out = PyDict::new_bound(py);
    out.set_item("extracted_span", (extraction.span.0, extraction.span.1))?;
    out.set_item("extracted_text", extracted_text)?;
    out.set_item("repaired_text", repaired_text)?;
    out.set_item("truncated", extraction.truncated)?;
    out.set_item("method", extraction.method)?;
    out.set_item("error_pos", error_pos)?;

    let repairs = PyList::empty_bound(py);
    for r in &base_repairs {
        repairs.append(repair_action_to_pydict(py, r)?)?;
    }
    out.set_item("base_repairs", repairs)?;

    Ok(out.to_object(py))
}

#[pyfunction]
fn probabilistic_repair_py(
    py: Python<'_>,
    extracted_text: &str,
    options: Option<&Bound<'_, PyDict>>,
    base_repairs: Option<&Bound<'_, PyList>>,
) -> PyResult<PyObject> {
    let opt = options_from_dict(options)?;
    let mut repairs: Vec<RepairAction> = Vec::new();
    if let Some(list) = base_repairs {
        for item in list.iter() {
            repairs.push(py_to_repair_action(&item)?);
        }
    }
    let cands = beam::probabilistic_repair(extracted_text, &opt, &repairs);
    let out = PyList::empty_bound(py);
    for c in &cands {
        out.append(candidate_to_pydict(py, c)?)?;
    }
    Ok(out.to_object(py))
}

#[pyfunction]
fn parse_root_array_scale_py(py: Python<'_>, data: &Bound<'_, PyBytes>, options: Option<&Bound<'_, PyDict>>) -> PyResult<PyObject> {
    let opt = options_from_dict(options)?;
    let (value, plan) = scale::parse_root_array_scale(data.as_bytes(), &opt)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e))?;
    let out = PyDict::new_bound(py);
    out.set_item("value", json_to_py(py, &value))?;
    let plan_d = PyDict::new_bound(py);
    plan_d.set_item("mode", plan.mode.to_string())?;
    plan_d.set_item("elements", plan.elements)?;
    plan_d.set_item("structural_density", plan.structural_density)?;
    plan_d.set_item("chunk_count", plan.chunk_count)?;
    out.set_item("plan", plan_d)?;
    Ok(out.to_object(py))
}

#[pymodule]
fn json_prob_parser_rust(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(parse_py, m)?)?;
    m.add_function(wrap_pyfunction!(preprocess_py, m)?)?;
    m.add_function(wrap_pyfunction!(probabilistic_repair_py, m)?)?;
    m.add_function(wrap_pyfunction!(parse_root_array_scale_py, m)?)?;
    Ok(())
}

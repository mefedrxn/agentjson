use json_prob_parser::json::JsonValue;
use json_prob_parser::types::RepairOptions;

fn get_obj_field<'a>(v: &'a JsonValue, key: &str) -> Option<&'a JsonValue> {
    match v {
        JsonValue::Object(obj) => obj.iter().find(|(k, _)| k == key).map(|(_, vv)| vv),
        _ => None,
    }
}

#[test]
fn strict_ok() {
    let opt = RepairOptions::default();
    let r = json_prob_parser::parse(r#"{"a":1}"#, &opt);
    assert_eq!(r.status, "strict_ok");
    let best = r.best().unwrap();
    let v = best.value.as_ref().unwrap();
    assert_eq!(get_obj_field(v, "a"), Some(&JsonValue::NumberI64(1)));
}

#[test]
fn code_fence_extract() {
    let mut opt = RepairOptions::default();
    opt.debug = true;
    let r = json_prob_parser::parse("preface```json\n{\"a\":1}\n```suffix", &opt);
    assert!(r.status == "repaired" || r.status == "strict_ok");
    let best = r.best().unwrap();
    let v = best.value.as_ref().unwrap();
    assert_eq!(get_obj_field(v, "a"), Some(&JsonValue::NumberI64(1)));
}

#[test]
fn trailing_comma_heuristic() {
    let opt = RepairOptions::default();
    let r = json_prob_parser::parse(r#"{"a":1,}"#, &opt);
    assert!(r.status == "repaired" || r.status == "strict_ok");
    let best = r.best().unwrap();
    let v = best.value.as_ref().unwrap();
    assert_eq!(get_obj_field(v, "a"), Some(&JsonValue::NumberI64(1)));
}

#[test]
fn missing_closer_heuristic() {
    let opt = RepairOptions::default();
    let r = json_prob_parser::parse(r#"{"a":1"#, &opt);
    assert!(r.status == "repaired" || r.status == "strict_ok");
    let best = r.best().unwrap();
    let v = best.value.as_ref().unwrap();
    assert_eq!(get_obj_field(v, "a"), Some(&JsonValue::NumberI64(1)));
}

#[test]
fn probabilistic_unquoted_key_and_single_quotes() {
    let mut opt = RepairOptions::default();
    opt.mode = "probabilistic".to_string();
    opt.top_k = 3;
    let r = json_prob_parser::parse("{a: 'b'}", &opt);
    assert!(r.status == "repaired" || r.status == "partial");
    let best = r.best().unwrap();
    let v = best.value.as_ref().unwrap();
    assert_eq!(get_obj_field(v, "a"), Some(&JsonValue::String("b".to_string())));
}

#[test]
fn partial_truncate_suffix() {
    let mut opt = RepairOptions::default();
    opt.mode = "probabilistic".to_string();
    opt.partial_ok = true;
    let r = json_prob_parser::parse(r#"{"a":1,"b":2,"c":3, nonsense nonsense"#, &opt);
    assert_eq!(r.status, "partial");
    let best = r.best().unwrap();
    let v = best.value.as_ref().unwrap();
    assert_eq!(get_obj_field(v, "a"), Some(&JsonValue::NumberI64(1)));
    assert_eq!(get_obj_field(v, "b"), Some(&JsonValue::NumberI64(2)));
    assert_eq!(get_obj_field(v, "c"), Some(&JsonValue::NumberI64(3)));
    assert!(!best.dropped_spans.is_empty());
}

#[test]
fn fix_smart_quotes() {
    let opt = RepairOptions::default();
    let r = json_prob_parser::parse("{“a”: “b”}", &opt);
    assert!(r.status == "repaired" || r.status == "strict_ok");
    let best = r.best().unwrap();
    let v = best.value.as_ref().unwrap();
    assert_eq!(get_obj_field(v, "a"), Some(&JsonValue::String("b".to_string())));
}

#[test]
fn apply_llm_patch_ops_utf8() {
    let text = r#"X{"a":1}Y"#;
    let ops = vec![
        JsonValue::Object(vec![
            ("op".to_string(), JsonValue::String("delete".to_string())),
            (
                "span".to_string(),
                JsonValue::Array(vec![JsonValue::NumberU64(0), JsonValue::NumberU64(1)]),
            ),
        ]),
        JsonValue::Object(vec![
            ("op".to_string(), JsonValue::String("delete".to_string())),
            (
                "span".to_string(),
                JsonValue::Array(vec![
                    JsonValue::NumberU64((text.len() - 1) as u64),
                    JsonValue::NumberU64(text.len() as u64),
                ]),
            ),
        ]),
    ];
    let patched = json_prob_parser::apply_patch_ops_utf8(text, &ops).expect("patch failed");
    assert_eq!(patched, r#"{"a":1}"#);
}

#[test]
fn scale_pipeline_root_array_thread() {
    let data = b"[1, 2, 3]";
    let mut opt = RepairOptions::default();
    opt.mode = "scale_pipeline".to_string();
    opt.allow_parallel = "true".to_string();
    opt.parallel_backend = "thread".to_string();
    opt.min_elements_for_parallel = 1;
    opt.parallel_threshold_bytes = 0;
    opt.parallel_workers = Some(2);
    opt.parallel_chunk_bytes = 1;

    let r = json_prob_parser::parse_bytes(data, &opt);
    assert_eq!(r.status, "strict_ok");
    let best = r.best().unwrap();
    assert_eq!(
        best.value.as_ref().unwrap(),
        &JsonValue::Array(vec![
            JsonValue::NumberI64(1),
            JsonValue::NumberI64(2),
            JsonValue::NumberI64(3)
        ])
    );
}

#[test]
fn scale_pipeline_root_object_pairs_thread() {
    let data = br#"{"a":1,"b":2,"c":3}"#;
    let mut opt = RepairOptions::default();
    opt.mode = "scale_pipeline".to_string();
    opt.allow_parallel = "true".to_string();
    opt.parallel_backend = "thread".to_string();
    opt.min_elements_for_parallel = 1;
    opt.parallel_threshold_bytes = 0;
    opt.parallel_workers = Some(2);
    opt.parallel_chunk_bytes = 1;

    let r = json_prob_parser::parse_bytes(data, &opt);
    assert_eq!(r.status, "strict_ok");
    let best = r.best().unwrap();
    assert_eq!(
        best.value.as_ref().unwrap(),
        &JsonValue::Object(vec![
            ("a".to_string(), JsonValue::NumberI64(1)),
            ("b".to_string(), JsonValue::NumberI64(2)),
            ("c".to_string(), JsonValue::NumberI64(3)),
        ])
    );
    let ir = best.ir.as_ref().unwrap();
    assert_eq!(
        get_obj_field(ir, "split_mode"),
        Some(&JsonValue::String("ROOT_OBJECT_PAIRS".to_string()))
    );
}

#[test]
fn scale_pipeline_tape_output_root_array() {
    let data = b"[1, 2, 3]";
    let mut opt = RepairOptions::default();
    opt.mode = "scale_pipeline".to_string();
    opt.scale_output = "tape".to_string();
    opt.allow_parallel = "false".to_string();

    let r = json_prob_parser::parse_bytes(data, &opt);
    assert_eq!(r.status, "strict_ok");
    let best = r.best().unwrap();
    assert!(best.value.is_none());
    let ir = best.ir.as_ref().unwrap();
    assert!(get_obj_field(ir, "tape").is_some());
}

#[test]
fn llm_deep_repair_patch_suggest() {
    let data = br#"{"a":1,"b":2, nonsense nonsense"#;
    let mut opt = RepairOptions::default();
    opt.mode = "probabilistic".to_string();
    opt.allow_llm = true;
    opt.llm_mode = "patch_suggest".to_string();
    opt.llm_min_confidence = 0.99;
    // LLM provider as an external command: reads payload JSON from stdin, prints patch_suggest JSON.
    opt.llm_command = Some(
        "python3 -c \"import sys,json; p=json.load(sys.stdin); t=p['snippet']['text']; s=p['snippet']['span_in_extracted'][0]; comma=t.index(', nonsense'); last=t.rfind('}'); out={'mode':'patch_suggest','patches':[{'patch_id':'p1','ops':[{'op':'delete','span':[s+comma,s+last]}]}]}; print(json.dumps(out))\""
            .to_string(),
    );

    let r = json_prob_parser::parse_bytes(data, &opt);
    assert_eq!(r.metrics.llm_calls, 1);
    assert!(r.status == "repaired" || r.status == "partial");
    let best = r.best().unwrap();
    let v = best.value.as_ref().unwrap();
    assert_eq!(get_obj_field(v, "a"), Some(&JsonValue::NumberI64(1)));
    assert_eq!(get_obj_field(v, "b"), Some(&JsonValue::NumberI64(2)));
}

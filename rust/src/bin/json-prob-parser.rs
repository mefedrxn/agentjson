use std::env;
use std::io::{self, Read};

use json_prob_parser::types::RepairOptions;

fn parse_usize(arg: &str, name: &str) -> usize {
    arg.parse::<usize>().unwrap_or_else(|_| panic!("invalid {name}: {arg}"))
}

fn parse_f64(arg: &str, name: &str) -> f64 {
    arg.parse::<f64>().unwrap_or_else(|_| panic!("invalid {name}: {arg}"))
}

fn main() {
    let mut mode = "auto".to_string();
    let mut top_k: usize = 5;
    let mut beam_width: usize = 32;
    let mut max_repairs: usize = 20;
    let mut max_deleted_tokens: usize = 3;
    let mut max_close_open_string: usize = 1;
    let mut max_garbage_skip_bytes: usize = 8 * 1024;
    let mut confidence_alpha: f64 = 0.7;
    let mut partial_ok: bool = true;
    let mut debug: bool = false;
    let mut allow_llm: bool = false;
    let mut llm_mode: String = "patch_suggest".to_string();
    let mut llm_min_confidence: f64 = 0.2;
    let mut llm_command: Option<String> = None;

    let mut min_elements_for_parallel: usize = 512;
    let mut density_threshold: f64 = 0.001;
    let mut parallel_chunk_bytes: usize = 8 * 1024 * 1024;
    let mut parallel_workers: usize = 0;
    let mut parallel_backend = "process".to_string();

    let mut input_path: Option<String> = None;

    let args = env::args().skip(1).collect::<Vec<_>>();
    let mut i = 0;
    while i < args.len() {
        let a = &args[i];
        match a.as_str() {
            "--input" | "-i" => {
                i += 1;
                input_path = Some(args.get(i).expect("missing --input value").to_string());
            }
            "--mode" => {
                i += 1;
                mode = args.get(i).expect("missing --mode value").to_string();
            }
            "--top-k" => {
                i += 1;
                top_k = parse_usize(args.get(i).expect("missing --top-k value"), "top_k");
            }
            "--beam-width" => {
                i += 1;
                beam_width = parse_usize(args.get(i).expect("missing --beam-width value"), "beam_width");
            }
            "--max-repairs" => {
                i += 1;
                max_repairs = parse_usize(args.get(i).expect("missing --max-repairs value"), "max_repairs");
            }
            "--max-deleted-tokens" => {
                i += 1;
                max_deleted_tokens = parse_usize(
                    args.get(i).expect("missing --max-deleted-tokens value"),
                    "max_deleted_tokens",
                );
            }
            "--max-close-open-string" => {
                i += 1;
                max_close_open_string = parse_usize(
                    args.get(i).expect("missing --max-close-open-string value"),
                    "max_close_open_string",
                );
            }
            "--max-garbage-skip-bytes" => {
                i += 1;
                max_garbage_skip_bytes = parse_usize(
                    args.get(i).expect("missing --max-garbage-skip-bytes value"),
                    "max_garbage_skip_bytes",
                );
            }
            "--confidence-alpha" => {
                i += 1;
                confidence_alpha = parse_f64(args.get(i).expect("missing --confidence-alpha value"), "confidence_alpha");
            }
            "--partial-ok" => partial_ok = true,
            "--no-partial-ok" => partial_ok = false,
            "--debug" => debug = true,
            "--no-debug" => debug = false,
            "--allow-llm" => allow_llm = true,
            "--no-allow-llm" => allow_llm = false,
            "--llm-mode" => {
                i += 1;
                llm_mode = args.get(i).expect("missing --llm-mode value").to_string();
            }
            "--llm-min-confidence" => {
                i += 1;
                llm_min_confidence =
                    parse_f64(args.get(i).expect("missing --llm-min-confidence value"), "llm_min_confidence");
            }
            "--llm-command" => {
                i += 1;
                llm_command = Some(args.get(i).expect("missing --llm-command value").to_string());
            }
            "--min-elements-for-parallel" => {
                i += 1;
                min_elements_for_parallel = parse_usize(
                    args.get(i).expect("missing --min-elements-for-parallel value"),
                    "min_elements_for_parallel",
                );
            }
            "--density-threshold" => {
                i += 1;
                density_threshold = parse_f64(args.get(i).expect("missing --density-threshold value"), "density_threshold");
            }
            "--parallel-chunk-bytes" => {
                i += 1;
                parallel_chunk_bytes = parse_usize(
                    args.get(i).expect("missing --parallel-chunk-bytes value"),
                    "parallel_chunk_bytes",
                );
            }
            "--parallel-workers" => {
                i += 1;
                parallel_workers = parse_usize(args.get(i).expect("missing --parallel-workers value"), "parallel_workers");
            }
            "--parallel-backend" => {
                i += 1;
                parallel_backend = args.get(i).expect("missing --parallel-backend value").to_string();
            }
            "--help" | "-h" => {
                eprintln!(
                    "Usage: json-prob-parser [--input FILE|-] [--mode auto|strict_only|fast_repair|probabilistic|scale_pipeline] ...\n\
                     Reads stdin if no --input.\n\
                     Outputs JSON (compact)."
                );
                std::process::exit(0);
            }
            _ => {
                eprintln!("Unknown arg: {a}");
                std::process::exit(2);
            }
        }
        i += 1;
    }

    let mut buf: Vec<u8> = Vec::new();
    if let Some(p) = input_path.as_deref() {
        if p != "-" {
            buf = std::fs::read(p).unwrap_or_else(|e| panic!("failed to read {p}: {e}"));
        } else {
            io::stdin().read_to_end(&mut buf).expect("stdin read failed");
        }
    } else {
        io::stdin().read_to_end(&mut buf).expect("stdin read failed");
    }

    let mut opt = RepairOptions::default();
    opt.mode = mode;
    opt.top_k = top_k;
    opt.beam_width = beam_width;
    opt.max_repairs = max_repairs;
    opt.max_deleted_tokens = max_deleted_tokens;
    opt.max_close_open_string = max_close_open_string;
    opt.max_garbage_skip_bytes = max_garbage_skip_bytes;
    opt.confidence_alpha = confidence_alpha;
    opt.partial_ok = partial_ok;
    opt.debug = debug;
    opt.min_elements_for_parallel = min_elements_for_parallel;
    opt.density_threshold = density_threshold;
    opt.parallel_chunk_bytes = parallel_chunk_bytes;
    opt.parallel_workers = if parallel_workers == 0 { None } else { Some(parallel_workers) };
    opt.parallel_backend = parallel_backend;
    opt.allow_llm = allow_llm;
    opt.llm_mode = llm_mode;
    opt.llm_min_confidence = llm_min_confidence;
    opt.llm_command = llm_command;

    let result = json_prob_parser::parse_bytes(&buf, &opt);
    println!("{}", result.to_json_string_pretty(2));
    std::process::exit(if result.status == "failed" { 2 } else { 0 });
}

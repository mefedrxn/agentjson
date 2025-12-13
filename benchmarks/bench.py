#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import platform
import sys
import timeit
import subprocess
import tempfile
import shutil
import re
from dataclasses import dataclass
from typing import Any, Callable, Optional


# ----------------------------
# Bench datasets (Slack-context)
# ----------------------------

# Valid JSON microbench: keep as a baseline/context (not the main story).
VALID_OBJ: dict[str, Any] = {
    "string": "hello world",
    "number": 123.456,
    "boolean": True,
    "null": None,
    "array": [1, 2, 3, 4, 5],
    "object": {"key": "value"},
    "unicode": "한글 🚀",
}
VALID_JSON_STR = (
    '{"string":"hello world","number":123.456,"boolean":true,"null":null,'
    '"array":[1,2,3,4,5],"object":{"key":"value"},"unicode":"한글 🚀"}'
)


@dataclass(frozen=True)
class TextCase:
    name: str
    text: str
    expected: Any


# Common “LLM-ish JSON” failures from our Slack thread:
# - code fences / “json입니다” wrappers / prefix+suffix junk
# - almost-JSON: single quotes, unquoted keys, trailing commas, python literals
# - missing commas, smart quotes, missing closing delimiters
LLM_MESSY_CASES: list[TextCase] = [
    TextCase("markdown_fence", "preface```json\n{\"a\":1}\n```suffix", {"a": 1}),
    TextCase("prefix_suffix", "Response: {\"a\": 1} EOF", {"a": 1}),
    TextCase("single_quotes", "{'a': 1}", {"a": 1}),
    TextCase("unquoted_key", "{a: 1}", {"a": 1}),
    TextCase("trailing_comma_obj", "{\"a\": 1,}", {"a": 1}),
    TextCase("trailing_comma_arr", "[1, 2, 3,]", [1, 2, 3]),
    TextCase("python_literals", "{\"a\": True, \"b\": None}", {"a": True, "b": None}),
    TextCase("missing_comma", "{\"a\": 1 \"b\": 2}", {"a": 1, "b": 2}),
    TextCase("smart_quotes", "{“a”: “b”}", {"a": "b"}),
    TextCase("missing_closer", "{\"a\": \"hello", {"a": "hello"}),
]


# Top‑K repair suite: deterministic “ground truth” targets to validate
# probabilistic candidates & confidence (Noah/Hika angle).
TOPK_CASES: list[TextCase] = [
    TextCase("topk_single_quotes", "{'k': 'v'}", {"k": "v"}),
    TextCase("topk_unquoted_keys", "{name: \"Alice\", age: 30}", {"name": "Alice", "age": 30}),
    TextCase("topk_trailing_comma", "{\"a\": 1, \"b\": 2,}", {"a": 1, "b": 2}),
    TextCase("topk_missing_comma", "{\"a\": 1 \"b\": 2}", {"a": 1, "b": 2}),
    TextCase("topk_python_literals", "{\"ok\": True, \"x\": None}", {"ok": True, "x": None}),
    TextCase("topk_code_fence", "```json\n{\"x\": [1,2,3,]}\n```", {"x": [1, 2, 3]}),
    # Ambiguous-ish case: multiple candidate repairs, where Top‑K matters.
    TextCase("topk_suffix_garbage", "{\"a\":1,\"b\":2,\"c\":3, nonsense nonsense", {"a": 1, "b": 2, "c": 3}),
    TextCase("topk_smart_quotes", "{“x”:1,“y”:2}", {"x": 1, "y": 2}),
]


# ----------------------------
# Helpers
# ----------------------------


def _timeit_best(fn: Callable[[], Any], number: int, repeat: int) -> float:
    t = timeit.Timer(fn)
    runs = t.repeat(repeat=repeat, number=number)
    return min(runs) / number


def _ns(sec: float) -> int:
    return int(sec * 1e9)


def _is_agentjson_orjson_shim(mod: Any) -> bool:
    v = getattr(mod, "__version__", "")
    return isinstance(v, str) and "agentjson" in v


def _json_dumps_compact(o: Any) -> str:
    return json.dumps(o, separators=(",", ":"), ensure_ascii=False)


def _error_counts(types_: list[str]) -> dict[str, int]:
    out: dict[str, int] = {}
    for t in types_:
        out[t] = out.get(t, 0) + 1
    return dict(sorted(out.items(), key=lambda kv: kv[0]))


def _eval_cases(loads: Callable[[Any], Any], cases: list[TextCase]) -> dict[str, Any]:
    ok = 0
    correct = 0
    errors: list[str] = []
    for c in cases:
        try:
            v = loads(c.text)
            ok += 1
            if v == c.expected:
                correct += 1
        except Exception as e:  # noqa: BLE001
            errors.append(type(e).__name__)
    return {"success": ok, "correct": correct, "total": len(cases), "error_types": _error_counts(errors)}


def _bench_suite_attempt_time(loads: Callable[[Any], Any], cases: list[TextCase], number: int, repeat: int) -> dict[str, Any]:
    def run_all() -> None:
        for c in cases:
            try:
                loads(c.text)
            except Exception:
                pass

    best = _timeit_best(run_all, number=number, repeat=repeat)
    per_case = best / max(1, len(cases))
    return {"attempt_best_ns_per_case": _ns(per_case), "number": number, "repeat": repeat}


# ----------------------------
# Microbench: valid JSON
# ----------------------------


@dataclass(frozen=True)
class MicroLib:
    name: str
    loads: Callable[[Any], Any]
    dumps: Optional[Callable[[Any], Any]]
    version: Optional[str]


def _collect_micro_libs() -> list[MicroLib]:
    libs: list[MicroLib] = [MicroLib(name="json", loads=json.loads, dumps=_json_dumps_compact, version=None)]
    try:
        import ujson  # type: ignore

        libs.append(MicroLib(name="ujson", loads=ujson.loads, dumps=ujson.dumps, version=getattr(ujson, "__version__", None)))
    except Exception:
        pass

    try:
        import orjson  # type: ignore

        libs.append(MicroLib(name="orjson", loads=orjson.loads, dumps=orjson.dumps, version=getattr(orjson, "__version__", None)))
    except Exception:
        pass

    return libs


def _bench_micro(libs: list[MicroLib], number: int, repeat: int) -> dict[str, Any]:
    out: dict[str, Any] = {"number": number, "repeat": repeat, "cases": {}}
    for lib in libs:
        loads_best = _timeit_best(lambda: lib.loads(VALID_JSON_STR), number=number, repeat=repeat)
        dumps_best_ns: Optional[int]
        if lib.dumps is None:
            dumps_best_ns = None
        else:
            dumps_best = _timeit_best(lambda: lib.dumps(VALID_OBJ), number=number, repeat=repeat)
            dumps_best_ns = _ns(dumps_best)
        out["cases"][lib.name] = {
            "version": lib.version,
            "loads_best_ns": _ns(loads_best),
            "dumps_best_ns": dumps_best_ns,
        }
    return out


# ----------------------------
# Primary suite: LLM messy JSON robustness
# ----------------------------


def _bench_llm_messy_suite(messy_number: int, messy_repeat: int) -> dict[str, Any]:
    libs: list[tuple[str, Callable[[Any], Any], Optional[Callable[[], None]]]] = []

    libs.append(("json (strict)", json.loads, None))

    try:
        import ujson  # type: ignore

        libs.append(("ujson (strict)", ujson.loads, None))
    except Exception:
        pass

    try:
        import orjson  # type: ignore

        is_shim = _is_agentjson_orjson_shim(orjson)

        def _set_orjson_mode(mode: str) -> Callable[[], None]:
            def setter() -> None:
                os.environ["JSONPROB_ORJSON_MODE"] = mode

            return setter

        libs.append(("orjson (strict)", orjson.loads, _set_orjson_mode("strict") if is_shim else None))
        if is_shim:
            libs.append(("orjson (auto, agentjson shim)", orjson.loads, _set_orjson_mode("auto")))
    except Exception:
        pass

    # Direct agentjson API (not drop-in) for measuring probabilistic / Top‑K path.
    try:
        import agentjson  # noqa: F401
        from agentjson import RepairOptions, parse  # type: ignore

        opt_auto = RepairOptions(mode="auto", allow_llm=False)
        opt_prob = RepairOptions(mode="probabilistic", top_k=5, beam_width=32, max_repairs=50, allow_llm=False)

        def _best_value(text: str, opt: RepairOptions) -> Any:
            r = parse(text, opt)
            best = r.best
            return None if best is None else best.value

        libs.append(("agentjson.parse(mode=auto)", lambda s: _best_value(s, opt_auto), None))
        libs.append(("agentjson.parse(mode=probabilistic)", lambda s: _best_value(s, opt_prob), None))
    except Exception:
        pass

    results: dict[str, Any] = {
        "case_count": len(LLM_MESSY_CASES),
        "case_names": [c.name for c in LLM_MESSY_CASES],
        "cases": {},
    }

    for label, loads, maybe_set_env in libs:
        old_mode = os.environ.get("JSONPROB_ORJSON_MODE")
        if maybe_set_env is not None:
            maybe_set_env()
        try:
            entry = _eval_cases(loads, LLM_MESSY_CASES)
            entry["timing"] = _bench_suite_attempt_time(loads, LLM_MESSY_CASES, number=messy_number, repeat=messy_repeat)
        finally:
            if maybe_set_env is not None:
                if old_mode is None:
                    os.environ.pop("JSONPROB_ORJSON_MODE", None)
                else:
                    os.environ["JSONPROB_ORJSON_MODE"] = old_mode
        results["cases"][label] = entry

    return results


# ----------------------------
# Secondary suite: Top‑K repair quality
# ----------------------------


def _bench_topk_suite(number: int, repeat: int) -> Optional[dict[str, Any]]:
    try:
        from agentjson import RepairOptions, parse  # type: ignore
    except Exception:
        return None

    opt = RepairOptions(mode="probabilistic", top_k=5, beam_width=32, max_repairs=50, partial_ok=False, allow_llm=False)

    hit_top1 = 0
    hit_topk = 0
    statuses: dict[str, int] = {}
    candidate_counts: list[int] = []
    confidences: list[float] = []

    for c in TOPK_CASES:
        r = parse(c.text, opt)
        statuses[r.status] = statuses.get(r.status, 0) + 1
        cand_vals = [x.value for x in (r.candidates or []) if x.value is not None]
        candidate_counts.append(len(cand_vals))
        best = r.best
        if best is not None:
            confidences.append(float(best.confidence))
        if best is not None and best.value == c.expected:
            hit_top1 += 1
        if any(v == c.expected for v in cand_vals):
            hit_topk += 1

    def run_all() -> None:
        for c in TOPK_CASES:
            parse(c.text, opt)

    best = _timeit_best(run_all, number=number, repeat=repeat)
    per_case = best / max(1, len(TOPK_CASES))

    return {
        "case_count": len(TOPK_CASES),
        "case_names": [c.name for c in TOPK_CASES],
        "hit_top1": hit_top1,
        "hit_topk": hit_topk,
        "status_counts": dict(sorted(statuses.items(), key=lambda kv: kv[0])),
        "avg_candidates": (sum(candidate_counts) / len(candidate_counts)) if candidate_counts else 0.0,
        "avg_best_confidence": (sum(confidences) / len(confidences)) if confidences else 0.0,
        "timing": {"attempt_best_ns_per_case": _ns(per_case), "number": number, "repeat": repeat},
    }


# ----------------------------
# Optional suite: Large root-array parsing (big data angle)
# ----------------------------


def _build_large_root_array(target_bytes: int) -> tuple[str, bytes, int]:
    # Use a fixed-size element to keep the generator cheap and predictable.
    elem = '{"id":0,"value":"test"}'
    per = len(elem) + 1  # include comma (except last)
    n = max(1, target_bytes // per)
    if n == 1:
        s = f"[{elem}]"
    else:
        s = "[" + (elem + ",") * (n - 1) + elem + "]"
    return (s, s.encode("utf-8"), n)


def _bench_large_root_array(sizes_mb: list[int], number: int, repeat: int) -> dict[str, Any]:
    libs: list[tuple[str, Callable[[Any], Any], str]] = []

    libs.append(("json.loads(str)", json.loads, "str"))

    try:
        import ujson  # type: ignore

        libs.append(("ujson.loads(str)", ujson.loads, "str"))
    except Exception:
        pass

    try:
        import orjson  # type: ignore

        libs.append(("orjson.loads(bytes)", orjson.loads, "bytes"))
    except Exception:
        pass

    try:
        from agentjson import parse_root_array_scale  # type: ignore

        libs.append(("agentjson.scale(serial)", lambda b: parse_root_array_scale(b, {"mode": "strict", "allow_parallel": "false"}), "bytes"))
        libs.append(("agentjson.scale(parallel)", lambda b: parse_root_array_scale(b, {"mode": "strict", "allow_parallel": "true"}), "bytes"))
    except Exception:
        pass

    out: dict[str, Any] = {"number": number, "repeat": repeat, "sizes": [], "cases": {}}

    for mb in sizes_mb:
        target_bytes = int(mb) * 1024 * 1024
        s, b, n = _build_large_root_array(target_bytes)
        size_entry = {"target_mb": int(mb), "bytes": len(b), "elements": n}
        out["sizes"].append(size_entry)

        for name, loads, kind in libs:
            payload = s if kind == "str" else b

            def run_once() -> None:
                loads(payload)

            best = _timeit_best(run_once, number=number, repeat=repeat)
            mb_s = (len(b) / (1024 * 1024)) / best if best > 0 else 0.0
            out["cases"].setdefault(name, {})[str(mb)] = {"best_ns": _ns(best), "mb_per_s": mb_s}

    return out


def _build_nested_corpus_json(target_bytes: int) -> tuple[str, bytes, int]:
    arr_str, _arr_bytes, n = _build_large_root_array(target_bytes)
    obj = '{"corpus":' + arr_str + ',"x":0}'
    return (obj, obj.encode("utf-8"), n)


def _bench_nested_corpus_suite(
    sizes_mb: list[int],
    number: int,
    repeat: int,
    *,
    force_parallel: bool,
) -> Optional[dict[str, Any]]:
    try:
        from agentjson import RepairOptions, parse  # type: ignore
    except Exception:
        return None

    cases: list[tuple[str, Callable[[bytes], Any]]] = []

    opt_baseline = RepairOptions(
        mode="scale_pipeline",
        scale_output="dom",
        scale_target_keys=None,
        allow_parallel=False,
        debug=False,
        min_elements_for_parallel=1,
        parallel_threshold_bytes=0,
        parallel_workers=0,
        parallel_chunk_bytes=8 * 1024 * 1024,
        partial_ok=False,
        allow_llm=False,
    )

    # Force nested split via scale_target_keys; compare allow_parallel on/off.
    opt_serial = RepairOptions(
        mode="scale_pipeline",
        scale_output="dom",
        scale_target_keys=["corpus"],
        allow_parallel=False,
        debug=False,
        min_elements_for_parallel=1,
        parallel_threshold_bytes=0,
        parallel_workers=0,
        parallel_chunk_bytes=8 * 1024 * 1024,
        partial_ok=False,
        allow_llm=False,
    )

    def run_parse(opt: RepairOptions, b: bytes) -> Any:
        r = parse(b, opt)
        # Touch only lightweight fields so we don't accidentally time Python pretty-printing.
        _ = r.status
        _ = r.metrics.split_mode
        return r

    cases.append(("agentjson.scale_pipeline(no_target)", lambda b: run_parse(opt_baseline, b)))
    cases.append(("agentjson.scale_pipeline(corpus, serial)", lambda b: run_parse(opt_serial, b)))
    if force_parallel:
        opt_parallel = RepairOptions(
            **{**opt_serial.__dict__, "allow_parallel": True},  # type: ignore[attr-defined]
        )
        cases.append(("agentjson.scale_pipeline(corpus, parallel)", lambda b: run_parse(opt_parallel, b)))

    out: dict[str, Any] = {"number": number, "repeat": repeat, "sizes": [], "cases": {}}
    for mb in sizes_mb:
        target_bytes = int(mb) * 1024 * 1024
        _s, b, n = _build_nested_corpus_json(target_bytes)
        out["sizes"].append({"target_mb": int(mb), "bytes": len(b), "corpus_elements": n})

        for name, fn in cases:
            # Capture split mode once (outside the timed loop) so we can tell if/when
            # targeting/parallelism actually triggered.
            try:
                sample = fn(b)
                split_mode = getattr(getattr(sample, "metrics", None), "split_mode", None)
            except Exception:
                split_mode = None

            def run_once() -> None:
                fn(b)

            best = _timeit_best(run_once, number=number, repeat=repeat)
            mb_s = (len(b) / (1024 * 1024)) / best if best > 0 else 0.0
            out["cases"].setdefault(name, {})[str(mb)] = {"best_ns": _ns(best), "mb_per_s": mb_s, "split_mode": split_mode}
    return out


def _write_large_root_array_file(path: str, target_bytes: int) -> dict[str, Any]:
    elem = b'{"id":0,"value":"test"}'
    per = len(elem) + 1
    n = max(1, target_bytes // per)
    with open(path, "wb") as f:
        f.write(b"[")
        for i in range(n):
            if i:
                f.write(b",")
            f.write(elem)
        f.write(b"]")
    size = os.path.getsize(path)
    return {"bytes": int(size), "elements": int(n)}


def _parse_time_output(stderr: str) -> dict[str, Any]:
    # Linux: "Maximum resident set size (kbytes): 12345"
    m = re.search(r"Maximum resident set size \\(kbytes\\):\\s*(\\d+)", stderr)
    if m:
        rss_kb = int(m.group(1))
        return {"max_rss_bytes": rss_kb * 1024}

    # macOS (/usr/bin/time -l): "<num>  maximum resident set size"
    m = re.search(r"^\\s*(\\d+)\\s+maximum resident set size\\s*$", stderr, flags=re.MULTILINE)
    if m:
        return {"max_rss_bytes": int(m.group(1))}

    # macOS: "<sec> real"
    m = re.search(r"^\\s*([0-9.]+)\\s+real\\b", stderr, flags=re.MULTILINE)
    if m:
        return {"elapsed_s": float(m.group(1))}

    return {}


def _bench_cli_mmap_suite(size_mb: int) -> Optional[dict[str, Any]]:
    cli_bin = os.getenv("BENCH_CLI_BIN") or os.path.join("rust", "target", "release", "agentjson")
    if not os.path.exists(cli_bin):
        return None

    # NOTE: In restricted macOS sandboxes, `/usr/bin/time -l` can fail (sysctl permission)
    # and return a non-zero exit code. We keep this suite portable by measuring wall time only.
    time_bin = None
    if platform.system() == "Linux":
        time_bin = shutil.which("time") or "/usr/bin/time"
        if not time_bin or not os.path.exists(time_bin):
            time_bin = None

    size_bytes = int(size_mb) * 1024 * 1024

    with tempfile.TemporaryDirectory() as td:
        path = os.path.join(td, f"large-{size_mb}mb.json")
        meta = _write_large_root_array_file(path, size_bytes)

        base_cmd = [
            cli_bin,
            "--input",
            path,
            "--mode",
            "scale_pipeline",
            "--scale-output",
            "tape",
            "--debug",
            "--top-k",
            "1",
        ]

        def run(label: str, extra: list[str]) -> dict[str, Any]:
            cmd = base_cmd + extra
            start = timeit.default_timer()
            if time_bin is None:
                p = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
                elapsed = timeit.default_timer() - start
                return {"elapsed_s": elapsed, "exit_code": int(p.returncode)}

            # Use /usr/bin/time to capture max RSS in a portable-ish way.
            args = [time_bin] + (["-v"] if platform.system() == "Linux" else ["-l"]) + cmd
            p = subprocess.run(args, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True, check=False)
            elapsed = timeit.default_timer() - start
            parsed = _parse_time_output(p.stderr)
            parsed.setdefault("elapsed_s", elapsed)
            parsed["exit_code"] = int(p.returncode)
            return parsed

        results = {
            "input": {"target_mb": int(size_mb), **meta},
            "cases": {
                "mmap(default)": run("mmap(default)", []),
                "read(--no-mmap)": run("read(--no-mmap)", ["--no-mmap"]),
            },
        }
        return results


def main() -> int:
    micro_number = int(os.getenv("BENCH_MICRO_NUMBER", "20000"))
    micro_repeat = int(os.getenv("BENCH_MICRO_REPEAT", "5"))
    messy_number = int(os.getenv("BENCH_MESSY_NUMBER", "2000"))
    messy_repeat = int(os.getenv("BENCH_MESSY_REPEAT", "5"))
    topk_number = int(os.getenv("BENCH_TOPK_NUMBER", "500"))
    topk_repeat = int(os.getenv("BENCH_TOPK_REPEAT", "5"))

    large_mb = os.getenv("BENCH_LARGE_MB", "5,20")
    sizes_mb = [int(x.strip()) for x in large_mb.split(",") if x.strip()]
    large_number = int(os.getenv("BENCH_LARGE_NUMBER", "3"))
    large_repeat = int(os.getenv("BENCH_LARGE_REPEAT", "3"))

    nested_mb = os.getenv("BENCH_NESTED_MB", "5,20")
    nested_sizes_mb = [int(x.strip()) for x in nested_mb.split(",") if x.strip()]
    nested_number = int(os.getenv("BENCH_NESTED_NUMBER", "1"))
    nested_repeat = int(os.getenv("BENCH_NESTED_REPEAT", "3"))
    nested_force_parallel = os.getenv("BENCH_NESTED_FORCE_PARALLEL", "").strip() not in ("", "0", "false", "False")

    cli_mmap_mb = os.getenv("BENCH_CLI_MMAP_MB")

    meta = {
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor(),
    }

    suites: dict[str, Any] = {}

    libs = _collect_micro_libs()
    suites["valid_json_microbench"] = _bench_micro(libs, number=micro_number, repeat=micro_repeat)
    suites["llm_messy_json_suite"] = _bench_llm_messy_suite(messy_number=messy_number, messy_repeat=messy_repeat)

    topk = _bench_topk_suite(number=topk_number, repeat=topk_repeat)
    if topk is not None:
        suites["topk_repair_suite"] = topk

    suites["large_root_array_suite"] = _bench_large_root_array(sizes_mb=sizes_mb, number=large_number, repeat=large_repeat)

    nested = _bench_nested_corpus_suite(
        sizes_mb=nested_sizes_mb,
        number=nested_number,
        repeat=nested_repeat,
        force_parallel=nested_force_parallel,
    )
    if nested is not None:
        suites["nested_corpus_suite"] = nested

    if cli_mmap_mb:
        cli_suite = _bench_cli_mmap_suite(size_mb=int(cli_mmap_mb))
        if cli_suite is not None:
            suites["cli_mmap_suite"] = cli_suite

    print(json.dumps({"meta": meta, "suites": suites}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

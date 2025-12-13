from __future__ import annotations

import json
import time
import queue
import threading
from dataclasses import asdict
from typing import Any, Mapping, Optional, Sequence, Tuple, Union

from .llm import apply_patch_ops_utf8, build_llm_payload
from .rust_core import parse as rust_parse
from .rust_core import preprocess as rust_preprocess
from .rust_core import probabilistic_repair as rust_probabilistic_repair
from .types import (
    Candidate,
    CandidateDiagnostics,
    CandidateValidations,
    InputStats,
    Metrics,
    ParseError,
    PartialResult,
    RepairAction,
    RepairOptions,
    RepairResult,
)


def _call_with_timeout(fn: Any, payload: dict[str, Any], timeout_ms: int) -> Any:
    if timeout_ms <= 0:
        return fn(payload)

    q: queue.Queue[tuple[str, Any]] = queue.Queue(maxsize=1)

    def runner() -> None:
        try:
            q.put(("ok", fn(payload)))
        except Exception as e:  # noqa: BLE001
            q.put(("err", e))

    t = threading.Thread(target=runner, daemon=True)
    t.start()

    try:
        kind, val = q.get(timeout=(timeout_ms / 1000.0))
    except queue.Empty as e:
        raise TimeoutError(f"llm_provider timeout after {timeout_ms}ms") from e

    if kind == "err":
        raise val
    return val


def _coerce_input_bytes(input_text_or_bytes: Union[str, bytes]) -> bytes:
    if isinstance(input_text_or_bytes, bytes):
        # Fast path: keep bytes as-is when they are valid UTF-8.
        # This avoids an O(n) decode+encode roundtrip for large payloads.
        try:
            input_text_or_bytes.decode("utf-8", errors="strict")
            return input_text_or_bytes
        except UnicodeDecodeError:
            # Keep offsets stable with Rust's from_utf8_lossy behavior.
            text = input_text_or_bytes.decode("utf-8", errors="replace")
            return text.encode("utf-8", errors="strict")
    if isinstance(input_text_or_bytes, str):
        return input_text_or_bytes.encode("utf-8", errors="strict")
    raise TypeError("input must be str or bytes")


def _options_to_rust_dict(opt: RepairOptions) -> dict[str, Any]:
    d = asdict(opt)

    allow_parallel = opt.allow_parallel
    if allow_parallel is True:
        d["allow_parallel"] = "true"
    elif allow_parallel is False:
        d["allow_parallel"] = "false"
    else:
        d["allow_parallel"] = str(allow_parallel)

    # Rust core stays deterministic; Python orchestrates LLM calls.
    d["allow_llm"] = False

    # Python-only fields.
    d.pop("llm_provider", None)
    d.pop("use_rust_backend", None)
    return d


def _repair_action_from_dict(d: Mapping[str, Any]) -> RepairAction:
    span = d.get("span")
    if isinstance(span, (list, tuple)) and len(span) == 2:
        span_t: Optional[Tuple[int, int]] = (int(span[0]), int(span[1]))
    else:
        span_t = None
    return RepairAction(
        op=str(d.get("op") or ""),
        span=span_t,
        at=(int(d["at"]) if d.get("at") is not None else None),
        token=(str(d["token"]) if d.get("token") is not None else None),
        cost_delta=float(d.get("cost_delta") or 0.0),
        note=(str(d["note"]) if d.get("note") is not None else None),
    )


def _candidate_from_dict(d: Mapping[str, Any]) -> Candidate:
    repairs = [_repair_action_from_dict(x) for x in (d.get("repairs") or [])]
    validations_d = d.get("validations") or {}
    validations = CandidateValidations(
        strict_json_parse=bool(validations_d.get("strict_json_parse") or False),
        schema_match=(float(validations_d["schema_match"]) if validations_d.get("schema_match") is not None else None),
    )
    diag_d = d.get("diagnostics") or {}
    diagnostics = CandidateDiagnostics(
        garbage_skipped_bytes=int(diag_d.get("garbage_skipped_bytes") or 0),
        deleted_tokens=int(diag_d.get("deleted_tokens") or 0),
        inserted_tokens=int(diag_d.get("inserted_tokens") or 0),
        close_open_string_count=int(diag_d.get("close_open_string_count") or 0),
        beam_width=(int(diag_d["beam_width"]) if diag_d.get("beam_width") is not None else None),
        max_repairs=(int(diag_d["max_repairs"]) if diag_d.get("max_repairs") is not None else None),
    )
    dropped_spans: list[Tuple[int, int]] = []
    for x in d.get("dropped_spans") or []:
        if isinstance(x, (list, tuple)) and len(x) == 2:
            dropped_spans.append((int(x[0]), int(x[1])))
    return Candidate(
        candidate_id=int(d.get("candidate_id") or 0),
        value=d.get("value"),
        normalized_json=(str(d["normalized_json"]) if d.get("normalized_json") is not None else None),
        ir=d.get("ir"),
        confidence=float(d.get("confidence") or 0.0),
        cost=float(d.get("cost") or 0.0),
        repairs=repairs,
        validations=validations,
        diagnostics=diagnostics,
        dropped_spans=dropped_spans,
    )


def _input_stats_from_dict(d: Mapping[str, Any]) -> InputStats:
    span = d.get("extracted_span") or [0, 0]
    span_t: Tuple[int, int] = (int(span[0]), int(span[1]))
    return InputStats(
        input_bytes=int(d.get("input_bytes") or 0),
        extracted_span=span_t,
        prefix_skipped_bytes=int(d.get("prefix_skipped_bytes") or 0),
        suffix_skipped_bytes=int(d.get("suffix_skipped_bytes") or 0),
    )


def _partial_from_dict(d: Mapping[str, Any]) -> PartialResult:
    dropped_spans: list[Tuple[int, int]] = []
    for x in d.get("dropped_spans") or []:
        if isinstance(x, (list, tuple)) and len(x) == 2:
            dropped_spans.append((int(x[0]), int(x[1])))
    return PartialResult(extracted=d.get("extracted"), dropped_spans=dropped_spans)


def _error_from_dict(d: Mapping[str, Any]) -> ParseError:
    return ParseError(
        kind=str(d.get("kind") or ""),
        at=(int(d["at"]) if d.get("at") is not None else None),
        message=(str(d["message"]) if d.get("message") is not None else None),
    )


def _metrics_from_dict(d: Mapping[str, Any]) -> Metrics:
    return Metrics(
        mode_used=str(d.get("mode_used") or ""),
        elapsed_ms=int(d.get("elapsed_ms") or 0),
        beam_width=int(d.get("beam_width") or 0),
        max_repairs=int(d.get("max_repairs") or 0),
        llm_calls=int(d.get("llm_calls") or 0),
        llm_time_ms=int(d.get("llm_time_ms") or 0),
        llm_trigger=(str(d["llm_trigger"]) if d.get("llm_trigger") is not None else None),
        split_mode=str(d.get("split_mode") or ""),
        parallel_workers=int(d.get("parallel_workers") or 0),
        elements=int(d.get("elements") or 0),
        structural_density=float(d.get("structural_density") or 0.0),
    )


def _repair_result_from_dict(d: Mapping[str, Any]) -> RepairResult:
    candidates = [_candidate_from_dict(x) for x in (d.get("candidates") or [])]
    best_index = d.get("best_index")
    best_i = int(best_index) if best_index is not None else None
    partial_d = d.get("partial")
    partial = _partial_from_dict(partial_d) if isinstance(partial_d, Mapping) else None
    errors = [_error_from_dict(x) for x in (d.get("errors") or [])]
    return RepairResult(
        status=str(d.get("status") or "failed"),
        best_index=best_i,
        input_stats=_input_stats_from_dict(d.get("input_stats") or {}),
        candidates=candidates,
        partial=partial,
        errors=errors,
        metrics=_metrics_from_dict(d.get("metrics") or {}),
        debug=(d.get("debug") if d.get("debug") is not None else None),
    )


def _parse_jsonish(x: Any) -> Any:
    if isinstance(x, (dict, list)):
        return x
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return None
        try:
            return json.loads(s)
        except json.JSONDecodeError:
            return None
    return None


def _llm_trigger_reason(result: RepairResult, opt: RepairOptions) -> Optional[str]:
    if not opt.allow_llm:
        return None
    if opt.mode in ("strict_only", "fast_repair"):
        return None
    if opt.llm_provider is None:
        return None
    if opt.max_llm_calls_per_doc <= 0:
        return None
    if not result.candidates:
        return "no_candidates"
    best = result.best
    if best is None:
        return "no_best"
    if best.confidence < float(opt.llm_min_confidence):
        return "low_confidence"
    return None


def _rank_key(c: Candidate) -> tuple:
    schema = c.validations.schema_match if c.validations.schema_match is not None else 0.0
    dropped_bytes = sum(max(0, e - s) for (s, e) in c.dropped_spans)
    norm_len = len(c.normalized_json) if c.normalized_json is not None else 0
    return (
        -float(schema),
        -float(c.confidence),
        float(c.cost),
        int(c.diagnostics.deleted_tokens),
        int(c.diagnostics.close_open_string_count),
        int(dropped_bytes),
        -int(norm_len),
        int(len(c.repairs)),
        int(c.candidate_id),
    )


def _rerank_in_place(result: RepairResult) -> None:
    result.candidates.sort(key=_rank_key)
    for i, c in enumerate(result.candidates):
        c.candidate_id = i
    result.best_index = 0 if result.candidates else None


def parse(input_text_or_bytes: Union[str, bytes], options: Optional[RepairOptions] = None) -> RepairResult:
    opt = options or RepairOptions()
    input_bytes = _coerce_input_bytes(input_text_or_bytes)

    raw = rust_parse(input_bytes, _options_to_rust_dict(opt))
    result = _repair_result_from_dict(raw)

    trigger = _llm_trigger_reason(result, opt)
    if trigger is None:
        return result

    extra_parse_ms = 0
    t_pre = time.perf_counter()
    pre = rust_preprocess(input_bytes, _options_to_rust_dict(opt))
    extra_parse_ms += int((time.perf_counter() - t_pre) * 1000)

    repaired_text = str(pre.get("repaired_text") or "")
    base_repairs = list(pre.get("base_repairs") or [])
    error_pos = pre.get("error_pos")
    if error_pos is not None:
        error_pos = int(error_pos)

    payload = build_llm_payload(
        extracted_text=repaired_text,
        mode=str(opt.llm_mode),
        error_pos=error_pos,
        schema_hint=opt.schema,
        parser_state=None,
        max_suggestions=5,
        span_window=1200,
    )

    llm_calls = 0
    llm_total_ms = 0
    llm_parsed: Any = None
    llm_mode = ""
    for _ in range(max(1, int(opt.max_llm_calls_per_doc))):
        llm_calls += 1
        t0 = time.perf_counter()
        try:
            llm_raw = (
                _call_with_timeout(opt.llm_provider, dict(payload), int(opt.llm_timeout_ms))
                if opt.llm_provider is not None
                else None
            )
        except Exception:  # noqa: BLE001
            llm_total_ms += int((time.perf_counter() - t0) * 1000)
            result.metrics.elapsed_ms = int(result.metrics.elapsed_ms) + extra_parse_ms
            result.metrics.llm_calls = llm_calls
            result.metrics.llm_time_ms = llm_total_ms
            result.metrics.llm_trigger = trigger
            return result
        llm_total_ms += int((time.perf_counter() - t0) * 1000)

        llm_parsed = _parse_jsonish(llm_raw)
        llm_mode = str(llm_parsed.get("mode") or "") if isinstance(llm_parsed, Mapping) else ""
        if isinstance(llm_parsed, Mapping) and llm_mode in ("patch_suggest", "token_suggest"):
            break

    if not isinstance(llm_parsed, Mapping) or llm_mode not in ("patch_suggest", "token_suggest"):
        result.metrics.elapsed_ms = int(result.metrics.elapsed_ms) + extra_parse_ms
        result.metrics.llm_calls = llm_calls
        result.metrics.llm_time_ms = llm_total_ms
        result.metrics.llm_trigger = trigger
        return result

    if llm_mode == "patch_suggest":
        patches = llm_parsed.get("patches") or []
        for p in patches[: max(1, int(opt.top_k))]:
            ops = p.get("ops") or []
            patch_id = str(p.get("patch_id") or "")
            try:
                patched = apply_patch_ops_utf8(repaired_text, ops)
            except Exception:  # noqa: BLE001
                continue

            llm_action = {
                "op": "llm_patch_suggest",
                "span": None,
                "at": None,
                "token": None,
                "cost_delta": 1.5,
                "note": patch_id or None,
            }
            base2 = list(base_repairs) + [llm_action]

            t_beam = time.perf_counter()
            llm_cands_raw = rust_probabilistic_repair(patched, _options_to_rust_dict(opt), base2)
            extra_parse_ms += int((time.perf_counter() - t_beam) * 1000)
            for cd in llm_cands_raw:
                result.candidates.append(_candidate_from_dict(cd))

            if len(result.candidates) >= int(opt.top_k) * 4:
                break
    else:
        # token_suggest: treat suggestions as minimal token insertions at the strict error position.
        suggestions = llm_parsed.get("suggestions") or []
        at = error_pos
        if at is None:
            at = len(repaired_text.encode("utf-8", errors="strict"))
        at = int(at)

        for s in suggestions[: max(1, int(opt.top_k))]:
            if not isinstance(s, Mapping):
                continue
            if str(s.get("type") or "") != "insert_tokens":
                continue
            tokens = s.get("tokens") or []
            if not isinstance(tokens, (list, tuple)):
                continue
            insert_text = "".join(str(t) for t in tokens)
            if not insert_text:
                continue

            try:
                patched = apply_patch_ops_utf8(repaired_text, [{"op": "insert", "at": at, "text": insert_text}])
            except Exception:  # noqa: BLE001
                continue

            confidence = float(s.get("confidence") or 0.5)
            cost_delta = 1.5 + max(0.0, 1.0 - confidence)
            llm_action = {
                "op": "llm_token_insert",
                "span": None,
                "at": at,
                "token": insert_text,
                "cost_delta": cost_delta,
                "note": (str(s.get("rationale")) if s.get("rationale") is not None else None),
            }
            base2 = list(base_repairs) + [llm_action]

            t_beam = time.perf_counter()
            llm_cands_raw = rust_probabilistic_repair(patched, _options_to_rust_dict(opt), base2)
            extra_parse_ms += int((time.perf_counter() - t_beam) * 1000)
            for cd in llm_cands_raw:
                result.candidates.append(_candidate_from_dict(cd))

            if len(result.candidates) >= int(opt.top_k) * 4:
                break

    _rerank_in_place(result)
    result.candidates = list(result.candidates)[: int(opt.top_k)]
    result.best_index = 0 if result.candidates else None

    if result.candidates:
        best = result.candidates[0]
        if best.dropped_spans and opt.partial_ok:
            result.status = "partial"
            result.partial = PartialResult(extracted=best.value, dropped_spans=list(best.dropped_spans))
        elif result.status == "failed":
            result.status = "repaired"
            result.partial = None

    result.metrics.elapsed_ms = int(result.metrics.elapsed_ms) + extra_parse_ms
    result.metrics.llm_calls = llm_calls
    result.metrics.llm_time_ms = llm_total_ms
    result.metrics.llm_trigger = trigger

    return result


def arbiter_parse(
    input_text_or_bytes: Union[str, bytes],
    options: Optional[Union[RepairOptions, Mapping[str, Any]]] = None,
) -> RepairResult:
    if options is None:
        return parse(input_text_or_bytes, None)
    if isinstance(options, RepairOptions):
        return parse(input_text_or_bytes, options)
    if isinstance(options, Mapping):
        return parse(input_text_or_bytes, RepairOptions(**dict(options)))
    raise TypeError("options must be RepairOptions, mapping, or None")

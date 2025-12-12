from __future__ import annotations

import json
import math
import time
from dataclasses import replace
from typing import Any, Mapping, Optional, Union

from .beam import probabilistic_repair
from .extract import extract_json_candidate
from .heuristic import heuristic_repair
from .llm_arbiter import maybe_llm_rerun
from .scale import parse_root_array_scale
from .schema import schema_match_score
from .strict import strict_parse
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


def _coerce_input(input_text_or_bytes: Union[str, bytes]) -> tuple[str, bytes, int]:
    if isinstance(input_text_or_bytes, bytes):
        return (
            input_text_or_bytes.decode("utf-8", errors="replace"),
            input_text_or_bytes,
            len(input_text_or_bytes),
        )
    b = input_text_or_bytes.encode("utf-8", errors="replace")
    return input_text_or_bytes, b, len(b)


def _coerce_options(options: Optional[Union[RepairOptions, Mapping[str, Any]]]) -> RepairOptions:
    if options is None:
        return RepairOptions()
    if isinstance(options, RepairOptions):
        return options
    if not isinstance(options, Mapping):
        raise TypeError("options must be RepairOptions | Mapping | None")
    return RepairOptions(**dict(options))


def _make_candidate(
    *,
    candidate_id: int,
    value: Any,
    normalized_json: str,
    repairs: list[RepairAction],
    cost: float,
    confidence: float,
    strict_ok: bool,
    schema_match: Optional[float],
    diagnostics: CandidateDiagnostics,
    dropped_spans: list[tuple[int, int]],
) -> Candidate:
    return Candidate(
        candidate_id=candidate_id,
        value=value,
        normalized_json=normalized_json,
        ir=None,
        confidence=confidence,
        cost=cost,
        repairs=repairs,
        validations=CandidateValidations(strict_json_parse=strict_ok, schema_match=schema_match),
        diagnostics=diagnostics,
        dropped_spans=dropped_spans,
    )


def _rank_candidates(candidates: list[Candidate]) -> list[Candidate]:
    def key(c: Candidate) -> tuple:
        schema = c.validations.schema_match if c.validations.schema_match is not None else 0.0
        dropped = sum((end - start) for start, end in (c.dropped_spans or []))
        norm_len = len(c.normalized_json or "")
        deleted = c.diagnostics.deleted_tokens if c.diagnostics else 0
        close_open = c.diagnostics.close_open_string_count if c.diagnostics else 0
        return (-schema, -c.confidence, c.cost, deleted, close_open, dropped, -norm_len, len(c.repairs), c.candidate_id)

    return sorted(candidates, key=key)


def arbiter_parse(
    input_text_or_bytes: Union[str, bytes],
    options: Optional[Union[RepairOptions, Mapping[str, Any]]] = None,
) -> RepairResult:
    return parse(input_text_or_bytes, options)


def parse(
    input_text_or_bytes: Union[str, bytes],
    options: Optional[Union[RepairOptions, Mapping[str, Any]]] = None,
) -> RepairResult:
    opt = _coerce_options(options)
    t0 = time.perf_counter()

    text, raw_bytes, input_bytes = _coerce_input(input_text_or_bytes)

    if opt.mode == "scale_pipeline":
        try:
            value, plan = parse_root_array_scale(raw_bytes, opt)
        except Exception as e:  # noqa: BLE001
            elapsed = int((time.perf_counter() - t0) * 1000)
            return RepairResult(
                status="failed",
                best_index=None,
                input_stats=InputStats(input_bytes=input_bytes, extracted_span=(0, len(text))),
                candidates=[],
                partial=None,
                errors=[ParseError(kind="ScalePipelineError", at=None, message=str(e))],
                metrics=Metrics(mode_used="scale_pipeline", elapsed_ms=elapsed),
                debug=None,
            )

        elapsed = int((time.perf_counter() - t0) * 1000)
        candidate = Candidate(
            candidate_id=0,
            value=value,
            normalized_json=None,
            ir={"split_mode": plan.mode, "chunks": plan.chunk_count, "elements": plan.elements},
            confidence=1.0,
            cost=0.0,
            repairs=[],
            validations=CandidateValidations(strict_json_parse=True, schema_match=None),
            diagnostics=CandidateDiagnostics(beam_width=0, max_repairs=0),
            dropped_spans=[],
        )
        return RepairResult(
            status="strict_ok",
            best_index=0,
            input_stats=InputStats(input_bytes=input_bytes, extracted_span=(0, len(text))),
            candidates=[candidate],
            partial=None,
            errors=[],
            metrics=Metrics(
                mode_used="scale_pipeline",
                elapsed_ms=elapsed,
                split_mode=plan.mode,
                parallel_workers=int(opt.parallel_workers or 0),
                elements=plan.elements,
                structural_density=plan.structural_density,
            ),
            debug=None,
        )
    extraction = extract_json_candidate(text)
    extracted = extraction.extracted

    input_stats = InputStats(
        input_bytes=input_bytes,
        extracted_span=extraction.span,
        prefix_skipped_bytes=extraction.span[0],
        suffix_skipped_bytes=max(0, len(text) - extraction.span[1]),
    )

    extraction_repairs = list(extraction.repairs)

    ok, value, err = strict_parse(extracted)
    if ok:
        normalized = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        cost = sum(a.cost_delta for a in extraction_repairs)
        confidence = 1.0 if cost <= 0 else math.exp(-opt.confidence_alpha * cost)
        status = "strict_ok" if not extraction_repairs else "repaired"
        candidate = _make_candidate(
            candidate_id=0,
            value=value,
            normalized_json=normalized,
            repairs=extraction_repairs,
            cost=cost,
            confidence=confidence,
            strict_ok=True,
            schema_match=schema_match_score(value, opt.schema) if opt.schema else None,
            diagnostics=CandidateDiagnostics(beam_width=0, max_repairs=0),
            dropped_spans=[],
        )
        elapsed = int((time.perf_counter() - t0) * 1000)
        return RepairResult(
            status=status,
            best_index=0,
            input_stats=input_stats,
            candidates=[candidate],
            partial=None,
            errors=[],
            metrics=Metrics(mode_used="strict", elapsed_ms=elapsed),
            debug={"extraction": extraction.debug_dict()} if opt.debug else None,
        )

    if opt.mode == "strict_only":
        elapsed = int((time.perf_counter() - t0) * 1000)
        return RepairResult(
            status="failed",
            best_index=None,
            input_stats=input_stats,
            candidates=[],
            partial=None,
            errors=[ParseError(kind="JSONDecodeError", at=getattr(err, "pos", None), message=str(err))],
            metrics=Metrics(mode_used="strict_only", elapsed_ms=elapsed),
            debug={"extraction": extraction.debug_dict()} if opt.debug else None,
        )

    repaired_text, heuristic_repairs = heuristic_repair(extracted, opt)
    base_repairs = extraction_repairs + heuristic_repairs

    if repaired_text != extracted:
        ok2, value2, err2 = strict_parse(repaired_text)
        if ok2:
            normalized2 = json.dumps(value2, ensure_ascii=False, separators=(",", ":"))
            cost = sum(a.cost_delta for a in base_repairs)
            confidence = 1.0 if cost <= 0 else math.exp(-opt.confidence_alpha * cost)
            schema = schema_match_score(value2, opt.schema) if opt.schema else None
            candidate2 = _make_candidate(
                candidate_id=0,
                value=value2,
                normalized_json=normalized2,
                repairs=base_repairs,
                cost=cost,
                confidence=confidence,
                strict_ok=True,
                schema_match=schema,
                diagnostics=replace(CandidateDiagnostics(), beam_width=0, max_repairs=0),
                dropped_spans=[],
            )
            elapsed = int((time.perf_counter() - t0) * 1000)
            return RepairResult(
                status="repaired",
                best_index=0,
                input_stats=input_stats,
                candidates=[candidate2],
                partial=None,
                errors=[],
                metrics=Metrics(mode_used="fast_repair", elapsed_ms=elapsed),
                debug={"extraction": extraction.debug_dict()} if opt.debug else None,
            )

        err = err2 or err

    if opt.mode == "fast_repair":
        elapsed = int((time.perf_counter() - t0) * 1000)
        return RepairResult(
            status="failed",
            best_index=None,
            input_stats=input_stats,
            candidates=[],
            partial=None,
            errors=[ParseError(kind="JSONDecodeError", at=getattr(err, "pos", None), message=str(err))],
            metrics=Metrics(mode_used="fast_repair", elapsed_ms=elapsed),
            debug={"extraction": extraction.debug_dict()} if opt.debug else None,
        )

    # Probabilistic repair (Top-K). Run on the heuristic-normalized text to reduce search space.
    beam_candidates = probabilistic_repair(
        repaired_text,
        opt,
        base_repairs=tuple(base_repairs),
    )
    for c in beam_candidates:
        if opt.schema and c.value is not None:
            c.validations.schema_match = schema_match_score(c.value, opt.schema)

    beam_candidates = _rank_candidates(beam_candidates)
    for i, c in enumerate(beam_candidates):
        c.candidate_id = i

    llm_calls = 0
    llm_time_ms = 0
    llm_trigger = None
    if opt.allow_llm:
        llm_candidates, calls, ms, trigger = maybe_llm_rerun(
            repaired_text=repaired_text,
            base_repairs=tuple(base_repairs),
            candidates=tuple(beam_candidates),
            error_pos=getattr(err, "pos", None),
            opt=opt,
        )
        llm_calls += int(calls)
        llm_time_ms += int(ms)
        llm_trigger = trigger
        if llm_candidates:
            for c in llm_candidates:
                if opt.schema and c.value is not None:
                    c.validations.schema_match = schema_match_score(c.value, opt.schema)
            # Merge + rerank.
            beam_candidates = _rank_candidates(beam_candidates + llm_candidates)
            for i, c in enumerate(beam_candidates):
                c.candidate_id = i

    elapsed = int((time.perf_counter() - t0) * 1000)
    if not beam_candidates:
        return RepairResult(
            status="failed",
            best_index=None,
            input_stats=input_stats,
            candidates=[],
            partial=None,
            errors=[ParseError(kind="UnrepairableJSON", at=getattr(err, "pos", None), message=str(err))],
            metrics=Metrics(
                mode_used="probabilistic",
                elapsed_ms=elapsed,
                beam_width=opt.beam_width,
                max_repairs=opt.max_repairs,
                llm_calls=llm_calls,
                llm_time_ms=llm_time_ms,
                llm_trigger=llm_trigger,
            ),
            debug={"extraction": extraction.debug_dict()} if opt.debug else None,
        )

    best = beam_candidates[0]
    status = "repaired"
    partial = None
    if extraction.truncated or best.dropped_spans:
        status = "partial"
        if opt.partial_ok:
            partial = PartialResult(extracted=best.value, dropped_spans=list(best.dropped_spans))

    return RepairResult(
        status=status,
        best_index=0,
        input_stats=input_stats,
        candidates=beam_candidates[: opt.top_k],
        partial=partial,
        errors=[],
        metrics=Metrics(
            mode_used="probabilistic",
            elapsed_ms=elapsed,
            beam_width=opt.beam_width,
            max_repairs=opt.max_repairs,
            llm_calls=llm_calls,
            llm_time_ms=llm_time_ms,
            llm_trigger=llm_trigger,
        ),
        debug={"extraction": extraction.debug_dict()} if opt.debug else None,
    )

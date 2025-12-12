from __future__ import annotations

from dataclasses import asdict
from typing import Any, Mapping, Optional, Union

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

HAVE_RUST = False
_rust: Any = None

try:
    import json_prob_parser_rust as _rust  # type: ignore[import-not-found]

    HAVE_RUST = True
except Exception:  # noqa: BLE001
    HAVE_RUST = False
    _rust = None


def _dict_to_repair_result(d: dict) -> RepairResult:
    """Convert dict from Rust to RepairResult dataclass."""
    # Parse input_stats
    input_stats_d = d.get("input_stats", {})
    input_stats = InputStats(
        input_bytes=input_stats_d.get("input_bytes", 0),
        extracted_span=tuple(input_stats_d.get("extracted_span", (0, 0))),
        prefix_skipped_bytes=input_stats_d.get("prefix_skipped_bytes", 0),
        suffix_skipped_bytes=input_stats_d.get("suffix_skipped_bytes", 0),
    )

    # Parse candidates
    candidates = []
    for cd in d.get("candidates", []):
        repairs = [
            RepairAction(
                op=r.get("op", ""),
                span=tuple(r["span"]) if r.get("span") else None,
                at=r.get("at"),
                token=r.get("token"),
                cost_delta=r.get("cost_delta", 0.0),
                note=r.get("note"),
            )
            for r in cd.get("repairs", [])
        ]
        validations = CandidateValidations(
            strict_json_parse=cd.get("validations", {}).get("strict_json_parse", False),
            schema_match=cd.get("validations", {}).get("schema_match"),
        )
        diagnostics_d = cd.get("diagnostics", {})
        diagnostics = CandidateDiagnostics(
            garbage_skipped_bytes=diagnostics_d.get("garbage_skipped_bytes", 0),
            deleted_tokens=diagnostics_d.get("deleted_tokens", 0),
            inserted_tokens=diagnostics_d.get("inserted_tokens", 0),
            close_open_string_count=diagnostics_d.get("close_open_string_count", 0),
            beam_width=diagnostics_d.get("beam_width"),
            max_repairs=diagnostics_d.get("max_repairs"),
        )
        candidate = Candidate(
            candidate_id=cd.get("candidate_id", 0),
            value=cd.get("value"),
            normalized_json=cd.get("normalized_json"),
            ir=cd.get("ir"),
            confidence=cd.get("confidence", 0.0),
            cost=cd.get("cost", 0.0),
            repairs=repairs,
            validations=validations,
            diagnostics=diagnostics,
            dropped_spans=[tuple(x) for x in cd.get("dropped_spans", [])],
        )
        candidates.append(candidate)

    # Parse partial
    partial = None
    if d.get("partial"):
        partial_d = d["partial"]
        partial = PartialResult(
            extracted=partial_d.get("extracted"),
            dropped_spans=[tuple(x) for x in partial_d.get("dropped_spans", [])],
        )

    # Parse errors
    errors = [
        ParseError(
            kind=e.get("kind", ""),
            at=e.get("at"),
            message=e.get("message"),
        )
        for e in d.get("errors", [])
    ]

    # Parse metrics
    metrics_d = d.get("metrics", {})
    metrics = Metrics(
        mode_used=metrics_d.get("mode_used", ""),
        elapsed_ms=metrics_d.get("elapsed_ms", 0),
        beam_width=metrics_d.get("beam_width", 0),
        max_repairs=metrics_d.get("max_repairs", 0),
        llm_calls=metrics_d.get("llm_calls", 0),
        llm_time_ms=metrics_d.get("llm_time_ms", 0),
        llm_trigger=metrics_d.get("llm_trigger"),
        split_mode=metrics_d.get("split_mode", ""),
        parallel_workers=metrics_d.get("parallel_workers", 0),
        elements=metrics_d.get("elements", 0),
        structural_density=metrics_d.get("structural_density", 0.0),
    )

    return RepairResult(
        status=d.get("status", "failed"),
        best_index=d.get("best_index"),
        input_stats=input_stats,
        candidates=candidates,
        partial=partial,
        errors=errors,
        metrics=metrics,
        debug=d.get("debug"),
    )


def parse(
    input_text_or_bytes: Union[str, bytes],
    options: Optional[Union[RepairOptions, Mapping[str, Any]]] = None,
) -> RepairResult:
    """Parse JSON with automatic repair using Rust backend."""
    if not HAVE_RUST or _rust is None:
        raise RuntimeError(
            "Rust backend not installed. Build/install the PyO3 extension:\n"
            "  python -m pip install -U maturin\n"
            "  maturin develop -m rust-pyo3/Cargo.toml\n"
        )

    # Convert options to dict
    if options is None:
        opt_dict = {}
    elif isinstance(options, RepairOptions):
        opt_dict = asdict(options)
        # Remove non-serializable fields
        opt_dict.pop("llm_provider", None)
    else:
        opt_dict = dict(options)

    result_dict = _rust.parse_py(input_text_or_bytes, opt_dict)
    return _dict_to_repair_result(result_dict)


# Alias for backward compatibility
arbiter_parse = parse


def parse_root_array_scale(data: bytes, options: Optional[Union[RepairOptions, Mapping[str, Any]]] = None) -> dict:
    """Parse large JSON array using scale pipeline."""
    if not HAVE_RUST or _rust is None:
        raise RuntimeError(
            "Rust backend not installed. Build/install the PyO3 extension:\n"
            "  python -m pip install -U maturin\n"
            "  maturin develop -m rust-pyo3/Cargo.toml\n"
        )

    if options is None:
        opt_dict = {}
    elif isinstance(options, RepairOptions):
        opt_dict = asdict(options)
        opt_dict.pop("llm_provider", None)
    else:
        opt_dict = dict(options)

    return _rust.parse_root_array_scale_py(data, opt_dict)


from __future__ import annotations

from dataclasses import asdict
from typing import Any, Iterable, List, Optional, Sequence

from .types import Candidate, CandidateDiagnostics, CandidateValidations, RepairAction, RepairOptions

HAVE_RUST = False
_rust: Any = None

try:
    import json_prob_parser_rust as _rust  # type: ignore[import-not-found]

    HAVE_RUST = True
except Exception:  # noqa: BLE001
    HAVE_RUST = False
    _rust = None


def _repair_action_to_dict(a: RepairAction) -> dict:
    return {
        "op": a.op,
        "span": list(a.span) if a.span is not None else None,
        "at": a.at,
        "token": a.token,
        "cost_delta": float(a.cost_delta),
        "note": a.note,
    }


def _candidate_from_dict(d: dict) -> Candidate:
    repairs = [RepairAction(**r) for r in (d.get("repairs") or [])]
    validations = CandidateValidations(**(d.get("validations") or {"strict_json_parse": False}))
    diagnostics = CandidateDiagnostics(**(d.get("diagnostics") or {}))
    return Candidate(
        candidate_id=int(d.get("candidate_id") or 0),
        value=d.get("value"),
        normalized_json=d.get("normalized_json"),
        ir=d.get("ir"),
        confidence=float(d.get("confidence") or 0.0),
        cost=float(d.get("cost") or 0.0),
        repairs=repairs,
        validations=validations,
        diagnostics=diagnostics,
        dropped_spans=[tuple(x) for x in (d.get("dropped_spans") or [])],
    )


def probabilistic_repair_rust(
    extracted_text: str,
    opt: RepairOptions,
    *,
    base_repairs: Sequence[RepairAction] = (),
) -> List[Candidate]:
    if not HAVE_RUST or _rust is None:
        raise RuntimeError("Rust backend not available (json_prob_parser_rust not installed)")

    opt_dict = asdict(opt)
    # Ensure we never do any LLM work inside the Rust backend; that stays in Python.
    opt_dict["allow_llm"] = False

    base_repairs_dicts = [_repair_action_to_dict(a) for a in base_repairs]
    raw = _rust.probabilistic_repair_py(extracted_text, opt_dict, base_repairs_dicts)
    return [_candidate_from_dict(x) for x in raw]


def parse_root_array_scale_rust(data: bytes, opt: RepairOptions) -> Any:
    if not HAVE_RUST or _rust is None:
        raise RuntimeError("Rust backend not available (json_prob_parser_rust not installed)")
    opt_dict = asdict(opt)
    opt_dict["allow_llm"] = False
    return _rust.parse_root_array_scale_py(data, opt_dict)


from __future__ import annotations

import json
import time
from typing import Any, Mapping, Optional, Sequence, Tuple

from .beam import probabilistic_repair
from .llm import apply_patch_ops_utf8, build_llm_payload
from .types import Candidate, RepairAction, RepairOptions


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


def _llm_trigger_reason(
    *,
    candidates: Sequence[Candidate],
    opt: RepairOptions,
) -> Optional[str]:
    if not opt.allow_llm:
        return None
    if not candidates:
        return "no_candidates"
    best = candidates[0]
    if best.confidence < float(opt.llm_min_confidence):
        return "low_confidence"
    # Could add more triggers here (garbage too large, dead-end, etc).
    return None


def _call_llm_provider(opt: RepairOptions, payload: Mapping[str, Any]) -> Any:
    if opt.llm_provider is None:
        raise RuntimeError("allow_llm=True but llm_provider is None")
    return opt.llm_provider(dict(payload))


def _patch_candidates(
    *,
    repaired_text: str,
    base_repairs: Sequence[RepairAction],
    llm_result: Mapping[str, Any],
    opt: RepairOptions,
) -> list[Candidate]:
    patches = llm_result.get("patches") or []
    out: list[Candidate] = []
    for p in patches[: max(1, int(opt.top_k))]:
        ops = p.get("ops") or []
        try:
            patched = apply_patch_ops_utf8(repaired_text, ops)
        except Exception:  # noqa: BLE001
            continue

        patch_action = RepairAction(
            op="llm_patch_suggest",
            span=None,
            at=None,
            token=None,
            cost_delta=1.5,
            note=str(p.get("patch_id") or ""),
        )
        next_base = tuple(base_repairs) + (patch_action,)
        out.extend(probabilistic_repair(patched, opt, base_repairs=next_base))
        if len(out) >= opt.top_k:
            break
    return out


def llm_deep_repair(
    *,
    repaired_text: str,
    opt: RepairOptions,
    base_repairs: Sequence[RepairAction],
    error_pos: Optional[int],
    parser_state: Optional[Mapping[str, Any]] = None,
) -> Tuple[list[Candidate], int, int]:
    """
    Phase 3: Model-in-the-loop deep repair.

    Returns: (candidates, llm_calls, llm_time_ms)
    """
    if not opt.allow_llm:
        return [], 0, 0
    if opt.max_llm_calls_per_doc <= 0:
        return [], 0, 0

    payload = build_llm_payload(
        extracted_text=repaired_text,
        mode=str(opt.llm_mode),
        error_pos=error_pos,
        schema_hint=opt.schema,
        parser_state=parser_state,
        max_suggestions=5,
        span_window=1200,
    )

    t0 = time.perf_counter()
    try:
        raw = _call_llm_provider(opt, payload)
    except Exception:  # noqa: BLE001
        elapsed_ms = int((time.perf_counter() - t0) * 1000)
        return [], 1, elapsed_ms
    elapsed_ms = int((time.perf_counter() - t0) * 1000)

    parsed = _parse_jsonish(raw)
    if not isinstance(parsed, Mapping):
        return [], 1, elapsed_ms

    mode = str(parsed.get("mode") or "")
    if mode == "patch_suggest":
        cands = _patch_candidates(
            repaired_text=repaired_text,
            base_repairs=base_repairs,
            llm_result=parsed,
            opt=opt,
        )
        return cands, 1, elapsed_ms

    # token_suggest not implemented in v1; patch_suggest is the safer default.
    return [], 1, elapsed_ms


def maybe_llm_rerun(
    *,
    repaired_text: str,
    base_repairs: Sequence[RepairAction],
    candidates: Sequence[Candidate],
    error_pos: Optional[int],
    opt: RepairOptions,
) -> Tuple[list[Candidate], int, int, Optional[str]]:
    reason = _llm_trigger_reason(candidates=candidates, opt=opt)
    if reason is None:
        return [], 0, 0, None
    llm_candidates, calls, ms = llm_deep_repair(
        repaired_text=repaired_text,
        opt=opt,
        base_repairs=base_repairs,
        error_pos=error_pos,
        parser_state=None,
    )
    return llm_candidates, calls, ms, reason

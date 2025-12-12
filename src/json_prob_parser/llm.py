from __future__ import annotations

from typing import Any, Mapping, Optional, Sequence, Tuple


def make_snippet(text: str, *, center: Optional[int], window: int = 1200) -> tuple[str, Tuple[int, int]]:
    if center is None:
        center = min(len(text), max(0, len(text) // 2))
    center = max(0, min(len(text), center))
    half = max(1, window // 2)
    start = max(0, center - half)
    end = min(len(text), center + half)
    return text[start:end], (start, end)


def build_llm_payload(
    *,
    extracted_text: str,
    mode: str,
    error_pos: Optional[int] = None,
    schema_hint: Optional[Mapping[str, Any]] = None,
    parser_state: Optional[Mapping[str, Any]] = None,
    max_suggestions: int = 5,
    span_window: int = 1200,
) -> dict[str, Any]:
    snippet_text, span = make_snippet(extracted_text, center=error_pos, window=span_window)
    return {
        "task": "json_deep_repair",
        "mode": mode,
        "snippet": {
            "text": snippet_text,
            "encoding": "utf-8",
            "span_in_extracted": [span[0], span[1]],
        },
        "parser_state": dict(parser_state or {}),
        "schema_hint": dict(schema_hint or {}),
        "constraints": {
            "max_suggestions": int(max_suggestions),
            "prefer_minimal_change": True,
            "return_json_only": True,
        },
    }


def apply_patch_ops_utf8(extracted_text: str, ops: Sequence[Mapping[str, Any]]) -> str:
    """
    Apply LLM patch_suggest ops against UTF-8 byte offsets (extracted_text 기준).
    Supported ops:
      - {"op":"delete", "span":[start,end]}
      - {"op":"replace", "span":[start,end], "text":"..."}
      - {"op":"insert", "at":pos, "text":"..."}
      - {"op":"truncate_after", "at":pos}
    """
    b = extracted_text.encode("utf-8", errors="strict")
    # Apply from back to front to keep offsets stable.
    normalized_ops = []
    for op in ops:
        kind = str(op.get("op"))
        if kind in ("delete", "replace"):
            span = op.get("span")
            if not (isinstance(span, (list, tuple)) and len(span) == 2):
                raise ValueError(f"invalid span for {kind}: {span!r}")
            start, end = int(span[0]), int(span[1])
            normalized_ops.append((max(0, start), max(0, end), kind, op))
        elif kind == "insert":
            at = int(op.get("at"))
            normalized_ops.append((max(0, at), max(0, at), kind, op))
        elif kind == "truncate_after":
            at = int(op.get("at"))
            normalized_ops.append((max(0, at), max(0, at), kind, op))
        else:
            raise ValueError(f"unsupported patch op: {kind!r}")

    normalized_ops.sort(key=lambda t: (t[0], t[1]), reverse=True)
    for start, end, kind, op in normalized_ops:
        start = min(start, len(b))
        end = min(end, len(b))
        if kind == "delete":
            b = b[:start] + b[end:]
        elif kind == "replace":
            repl = str(op.get("text", "")).encode("utf-8", errors="strict")
            b = b[:start] + repl + b[end:]
        elif kind == "insert":
            ins = str(op.get("text", "")).encode("utf-8", errors="strict")
            b = b[:start] + ins + b[start:]
        elif kind == "truncate_after":
            b = b[:start]
        else:
            raise AssertionError("unreachable")
    return b.decode("utf-8", errors="replace")


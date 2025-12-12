from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Tuple

from .types import RepairAction

_FENCE_RE = re.compile(r"```(?:json)?\s*(.*?)```", re.IGNORECASE | re.DOTALL)


@dataclass(frozen=True)
class Extraction:
    extracted: str
    span: Tuple[int, int]
    truncated: bool
    method: str
    repairs: Tuple[RepairAction, ...]

    def debug_dict(self) -> dict:
        return {
            "method": self.method,
            "span": list(self.span),
            "truncated": self.truncated,
            "repairs": [r.__dict__ for r in self.repairs],
        }


def _brace_scan_extract(text: str) -> Extraction:
    start_obj = text.find("{")
    start_arr = text.find("[")
    if start_obj == -1 and start_arr == -1:
        return Extraction(extracted=text, span=(0, len(text)), truncated=True, method="no_json_found", repairs=())

    start = start_obj if start_arr == -1 else start_arr if start_obj == -1 else min(start_obj, start_arr)

    in_string = False
    escape = False
    depth_brace = 0
    depth_bracket = 0
    truncated = True
    end = len(text)

    for i in range(start, len(text)):
        ch = text[i]
        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
            continue

        if ch == '"':
            in_string = True
            continue

        if ch == "{":
            depth_brace += 1
        elif ch == "}":
            depth_brace -= 1
        elif ch == "[":
            depth_bracket += 1
        elif ch == "]":
            depth_bracket -= 1

        if depth_brace == 0 and depth_bracket == 0 and i >= start:
            end = i + 1
            truncated = False
            break

    extracted = text[start:end]
    repairs = []
    if start > 0:
        repairs.append(RepairAction(op="strip_prefix_text", span=(0, start), cost_delta=0.3))
    if end < len(text):
        repairs.append(RepairAction(op="strip_suffix_text", span=(end, len(text)), cost_delta=0.3))

    return Extraction(
        extracted=extracted,
        span=(start, end),
        truncated=truncated,
        method="brace_scan",
        repairs=tuple(repairs),
    )


def extract_json_candidate(text: str) -> Extraction:
    matches = list(_FENCE_RE.finditer(text))
    for m in matches:
        inner = m.group(1).strip()
        if inner.startswith("{") or inner.startswith("["):
            start, end = m.span(1)
            repairs = []
            if start > 0:
                repairs.append(RepairAction(op="strip_prefix_text", span=(0, start), cost_delta=0.3))
            if end < len(text):
                repairs.append(RepairAction(op="strip_suffix_text", span=(end, len(text)), cost_delta=0.3))
            repairs.append(RepairAction(op="strip_code_fence", span=(m.start(), m.end()), cost_delta=0.2))
            return Extraction(
                extracted=inner,
                span=(start, end),
                truncated=False,
                method="code_fence",
                repairs=tuple(repairs),
            )

    return _brace_scan_extract(text)
